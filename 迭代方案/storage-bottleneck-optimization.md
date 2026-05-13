# BanDB 存储引擎瓶颈优化方案

> 分析日期: 2026-05-12 | 分支: feature/snapshot

---

## 一、总体评估

当前存储引擎存在 **21 个性能瓶颈**（存储层 14 + Raft 层 3 + 网络层 2 + 配置 2），此外还有 **4 个架构级优化方向**（租约读、Multi-Raft、并行应用、流水线复制）。

**核心问题**：写入路径每操作 3 次 fsync + 双份 WAL；读取路径 SSTable 全文件扫描无索引；Raft 读走完整共识协议。

---

## 二、瓶颈全量清单与修改建议

### P0 — 必须立即修复

#### 瓶颈 1: 每次写入触发 3 次独立 fsync

| 项目 | 内容 |
|------|------|
| **位置** | `Raft/raft_wal.go:143`, `Raft/raft_wal.go:90`, `storage/zstorage/WAL.go:66` |
| **现状** | 一次 PUT → Raft log fsync → Raft state fsync → 存储 WAL fsync |
| **影响** | HDD ~30-50ms/op, SSD ~1-3ms/op；吞吐上限 ~30 ops/s(HDD) |

**修改方案**:

1. **Raft 侧**: `AppendLog` 移除 `file.Sync()`，改为调用方批量 sync
   - 在 `RaftWAL` 新增 `Sync()` 公开方法（已有，直接用）
   - `AppendEntry` 内不每条目 sync，改为心跳协程或每 N 条/每 T 毫秒批量 sync
   - `raft_wal.go:143` 删除 `return w.file.Sync()`，改为 `return nil`
   
2. **Raft state 合并写入**: `persistStateLocked` 仅在 Term/VotedFor 变更时调用，当前每次 AppendEntry 都调用是无意义的
   - `raft.go` 在 `AppendEntry` 中检查 Term 是否变化，未变化则跳过 `persistStateLocked`
   
3. **消除存储层 WAL**: Raft 日志已提供完整持久性保证，存储层 WAL 是冗余的
   - `zstorage/WAL.go` 整体移除，或保留为空实现
   - `memtable.go` 中移除 `wal *WAL` 字段和 `recoverFromWAL()` 调用
   - 崩溃恢复改为从 Raft log 重放

**预期提升**: 写入延迟降低 60-70%，吞吐量提升 3-5×

---

#### 瓶颈 2: SSTable 全文件扫描 + 无布隆过滤器

| 项目 | 内容 |
|------|------|
| **位置** | `storage/zstorage/SSTable.go:255-263`, `storage/zstorage/memtable.go:375-392` |
| **现状** | `ReadFromSSTable` → `ReadAllFromSSTable` 加载整个文件 → O(n) 线性扫描 |
| **影响** | 10MB SSTable 查一个 key 需要加载 10MB + 全量扫描 |

**修改方案**:

1. **SSTable 文件格式改为分块结构**:
   ```
   [Header: Magic(4B) + Version(4B) + BlockCount(4B)]
   [BlockIndex: 每块 [LastKey(变长) + Offset(8B) + Size(4B)] × N]
   [DataBlock_0: [KeyLen(4B)][Key][ValueLen(4B)][Value] × N]
   [DataBlock_1: ...]
   [BloomFilter: BitSet(N bits)]
   [Footer: BlockIndexOffset(8B) + BloomOffset(8B) + Magic(4B)]
   ```
   每块大小 4KB~64KB 可配

2. **读取流程改为**:
   - 读取 Footer → 定位 BlockIndex + BloomFilter
   - 查 BloomFilter，不存在则直接返回
   - 二分查找 BlockIndex，定位目标 Block
   - 只加载目标 Block 到内存，Block 内二分查找

3. **布隆过滤器参数**: 每 key 10 bits, 7 个 hash 函数，误判率 ~0.8%

4. **新增文件**:
   - `storage/zstorage/sstable_format.go` — 新格式读写
   - `storage/zstorage/bloom.go` — 布隆过滤器实现
   - `storage/zstorage/block.go` — 数据块读写

**预期提升**: 点查延迟降低 90-99%（取决于文件大小）

---

#### 瓶颈 3: Compaction 全量内存合并

| 项目 | 内容 |
|------|------|
| **位置** | `storage/zstorage/SSTable.go:267-369` |
| **现状** | 所有源文件全部读入内存 → 排序 → 去重 → 写回 |
| **影响** | 4×10MB 文件合并需要 40MB+ 内存 |

**修改方案**:

实现 **K 路归并 (K-way merge)**:
- 每个源文件维护一个迭代器 (Iterator)，每次返回当前最小 key 的 entry
- 使用最小堆维护 K 个迭代器的当前 entry
- 每次弹出最小 key，去重（同 key 保留最新），写入输出文件
- 内存占用 O(K) 而非 O(N)

新增文件:
- `storage/zstorage/iterator.go` — SSTable 迭代器接口与实现
- `storage/zstorage/merge_iterator.go` — K 路归并迭代器

**预期提升**: 合并内存峰值降低 80-95%

---

### P1 — 应尽快修复

#### 瓶颈 4: 互斥锁持有 fsync

| 项目 | 内容 |
|------|------|
| **位置** | `storage/zstorage/WAL.go:37-38`, `memtable.go:156-157` |
| **现状** | Mutex/WriteLock 覆盖整个 fsync 过程 |

**修改方案**:
- 若保留存储 WAL：WAL 写入改为无锁队列 + 专用 flush 协程，调用方只写入内存 buffer 立即返回
- 若消除存储 WAL（推荐）：此问题自然消失

---

#### 瓶颈 5: 无 MANIFEST 文件

| 项目 | 内容 |
|------|------|
| **位置** | `storage/zstorage/SSTable.go:33-129` |
| **现状** | 重启时扫描目录发现 .sst 文件，所有文件 Level 重置为 0 |

**修改方案**:

新增 MANIFEST 文件 `MANIFEST`:
- 记录版本编辑序列: `[OpCode(1B)][Level(4B)][FilePath(变长)]`
- OpCode: `0x01`=AddFile, `0x02`=RemoveFile
- 每次 `AddMata` / `RemoveMata` 时追加写入 MANIFEST
- 启动时重放 MANIFEST 恢复元数据
- 定期做 MANIFEST 快照（当前全量状态）清理历史版本

新增文件: `storage/zstorage/manifest.go`

**预期提升**: 重启后保留正确层级，避免不必要的 compaction

---

#### 瓶颈 6: Raft 侧每次 RPC 新建 TCP 连接

| 项目 | 内容 |
|------|------|
| **位置** | `Raft/rpc.go` — `SendAppendEntries`, `SendRequestVote`, `SendInstallSnapshot` |
| **现状** | 每次 RPC 调用 `rpc.Dial("tcp", addr)` 新建连接 |

**修改方案**:

在 `RaftRPC` 中维护连接池:
```go
type RaftRPC struct {
    raft     *Raft
    connPool map[int]*rpc.Client  // peerID → persistent RPC client
    poolMu   sync.Mutex
}
```
- 首次连接后缓存，后续复用
- 心跳检测连接健康，断开时重建
- 连接空闲超过 TTL 后关闭

修改文件: `Raft/rpc.go`

**预期提升**: 集群复制延迟降低 20-30%（省去 TCP 握手）

---

#### 瓶颈 7: InstallSnapshot 重复从磁盘加载

| 项目 | 内容 |
|------|------|
| **位置** | `Raft/raft.go:587` |
| **现状** | 每次 `replicateLog` 发现 follower 落后，重新从磁盘加载完整快照 |

**修改方案**:

在 Raft 结构体中缓存最新快照数据:
```go
type Raft struct {
    // ...
    cachedSnapshot     []byte
    cachedSnapshotIdx  int64
}
```
- `TakeSnapshot` 完成后更新缓存
- `replicateLog` 直接使用缓存而非磁盘加载

**预期提升**: 多 follower 场景下磁盘读取消除

---

### P2 — 应列入迭代计划

#### 瓶颈 8: 无 SSTable 块缓存

**修改方案**: 实现 LRU 缓存，缓存热点数据块
- 新增 `storage/zstorage/block_cache.go`
- 可配置最大内存 (默认 64MB)
- 使用标准库 `container/list` + map 实现 LRU
- 在 Block 读取路径中集成

**预期提升**: 热数据重复读取延迟降低 95%+

---

#### 瓶颈 9: 写入无批处理/组提交

**修改方案**: 客户端/网关层聚合多个写请求为一批，Raft 单次 AppendEntry 携带多条 Command
- 修改 `LogEntry.Command` 支持批量编码
- 或新增 `AppendEntriesBatch` 接口

**预期提升**: 吞吐量提升 5-10×

---

#### 瓶颈 10: 跳表 Flush 阻塞读写

**修改方案**: active/dirty 交换操作优化
- 当前交换是 O(1) 但持写锁
- 考虑使用 atomic.Pointer 无锁交换
- 或将 active 复制改为写入时复制 (COW)

---

#### 瓶颈 11: SSTable 元数据每次全量复制

| 位置 | `storage/zstorage/SSTable.go:198-206` |

**修改方案**: 返回只读快照引用 + 版本号，写入时递增版本号。读方检查版本号变化决定是否重新获取。

---

### P3 — 长期优化

#### 瓶颈 12: SSTable 无数据压缩

**修改方案**: 在 DataBlock 写入前进行 Snappy/ZSTD 压缩
- 新增 `storage/zstorage/compress.go`
- Block 头部增加 `CompressedSize` 和 `Algorithm` 字段
- 读取时透明解压

**预期提升**: 磁盘空间节省 50-70%，I/O 量同比例下降

---

#### 瓶颈 13: 跳表参数未根据负载调优

**修改方案**: 添加 benchmark 测试不同 P 值和 MaxLevel 的效果
- 运行 `storage/bench_test.go` 收集数据
- 根据实际 key 分布选择最优参数

---

#### 瓶颈 14: 无全局内存预算

**修改方案**: 新增 `MemoryBudget` 组件
- 跟踪 MemTable + BlockCache + Compaction 总内存
- 超过阈值时阻塞写或触发强制 Flush
- 新增 `storage/zstorage/memory_budget.go`

---

## 三、架构级优化 (Raft 层)

### 优化 15: 租约读 (Lease Read) — **P0 读吞吐**

| 项目 | 内容 |
|------|------|
| **位置** | `Raft/raft.go` — `Read()` / 读请求处理流, `service/router.go` — `handleGet` |
| **现状** | 每次 GET 请求走完整 Raft 共识 — 领导者向多数派确认心跳后才能响应读，至少 1 次 RTT |
| **影响** | 读延迟 ≥ 网络 RTT（~1ms 局域网），读吞吐受 Raft 心跳频率约束 |

**原理**: 领导者持有"租约"（Lease），在租约有效期内无需与 follower 确认即可直接服务读请求。

**修改方案**:

1. **心跳租约**: Leader 发送 AppendEntries（心跳）后，若在 `electionTimeout/2` 时间内收到多数派确认，则认为在接下来 `electionTimeout/2` 时间内自己仍是合法 Leader，期间可以安全服务读请求。

2. **实现方式**:
   ```go
   // raft.go 新增字段
   type Raft struct {
       // ...
       leaseExpireTime time.Time  // 租约过期时间
       leaseMu         sync.RWMutex
   }

   // 心跳确认后更新租约
   func (r *Raft) renewLease() {
       r.leaseMu.Lock()
       r.leaseExpireTime = time.Now().Add(r.electionTimeout / 2)
       r.leaseMu.Unlock()
   }

   // 读请求检查租约
   func (r *Raft) isLeaseValid() bool {
       r.leaseMu.RLock()
       defer r.leaseMu.RUnlock()
       return time.Now().Before(r.leaseExpireTime) && r.state == Leader
   }
   ```

3. **GET 请求流程变更**:
   ```
   原: Client → handleGet → AppendEntry(Raft共识) → 等待commit → 读memtable → 返回
   新: Client → handleGet → isLeaseValid() 是 → 读memtable → 返回
                                 否 → AppendEntry(Raft共识) → ...
   ```

4. **修改文件**:
   - `Raft/raft.go` — 新增 Lease 字段和检查方法
   - `Raft/rpc.go` — 心跳 RPC 回复成功后更新租约
   - `service/router.go` — `handleGet` 添加 Lease 检查快速路径

5. **注意事项**:
   - 需要单调时钟（`time.Now()` 在 NTP 调整时可能回退），考虑使用 `runtime.Nanotime()` 或 Go 1.9+ monotonic clock
   - 时钟漂移安全边界：Lease 时间设为 `electionTimeout / 3` 更保守

**预期提升**:
- 读延迟: 1 RTT → 0 RTT（本地读），降低 80-90%
- 读吞吐: 受 Raft 共识约束 → 受本地 CPU 约束，提升 10-50×

---

### 优化 16: Multi-Raft / 分区 — **P0 写入横向扩展**

| 项目 | 内容 |
|------|------|
| **位置** | 架构级变更，涉及 `Raft/`, `service/`, `config/` |
| **现状** | 整个集群只有一个 Raft Group，所有 key 共享一个 Raft 日志 |
| **影响** | 写吞吐受单 Raft Leader 约束，无法横向扩展；Raft 日志包含所有 shard 的混合数据 |

**原理**: 将 key space 按 Hash 范围划分为多个 Partition（Shard），每个 Partition 运行独立的 Raft Group。不同 Partitions 可分布到不同节点，不同 Leader 并行处理写入。

**修改方案**:

1. **分片策略 — Hash 分区**:
   ```
   ShardID = CRC32(key) % ShardCount
   ```
   - 初始分片数建议 8~16（后期支持动态分裂）
   - 配置: `config.G.PartitionCount = 8`

2. **架构变更**:
   ```
   原架构:
   Client → TCP → Router → KVServer → Raft(×1) → Storage

   新架构:
   Client → TCP → Router → KVServer
                              ├─ RaftGroup[0] → Storage[0]
                              ├─ RaftGroup[1] → Storage[1]
                              ├─ ...
                              └─ RaftGroup[N] → Storage[N]
   ```

3. **核心组件**:

   **ShardRouter** (新增):
   ```go
   type ShardRouter struct {
       shards   map[int]*Shard     // ShardID → Shard
       hashFunc func([]byte) int   // key → ShardID
   }

   type Shard struct {
       ID       int
       Raft     *Raft
       Storage  *storage.Engine
   }
   ```

   **MultiRaftManager** (新增):
   - 管理多个 Raft Group 的生命周期
   - 每个 Raft Group 有独立的:
     - `raft_log.dat` / `raft_state.dat` / `snapshots/` (放在 `raft_data/shard_<N>/`)
     - 存储层 SSTable 文件（可共享目录或分 shard 目录）
     - ApplyCh / FSM
   - 负责多 Group 间的协调（跨 shard 事务暂不支持）

4. **写请求分流**:
   ```go
   func (kv *KVServer) AppendEntry(cmd Command) (int, error) {
       shardID := crc32.ChecksumIEEE([]byte(cmd.Key)) % uint32(kv.shardCount)
       shard := kv.shards[shardID]
       return shard.Raft.AppendEntry(encodeCommand(cmd))
   }
   ```

5. **读请求聚合**:
   - 单 key GET → 路由到对应 Shard
   - 范围查询 → 需要 Scatter-Gather 到所有 Shard，合并结果

6. **修改/新增文件**:
   | 文件 | 变更 |
   |------|------|
   | `service/router.go` | 修改 — 按 key hash 路由到 Shard |
   | `service/fsm.go` | 修改 — 每个 Shard 独立 FSM |
   | `service/shard_router.go` | **新增** — Shard 管理器 |
   | `config/global.go` | 修改 — 新增 PartitionCount, ShardAddrs 配置 |
   | `config/config.json` | 修改 — 分片配置 |
   | `Server/server.go` | 修改 — 初始化多个 Raft Group |

**预期提升**:
- 写吞吐 ≈ 单 Group 吞吐 × Shard 数量（理想情况 8-16×）
- 不同 Shard 的写入完全并行
- 同 Shard 的读写仍串行通过该 Shard 的 Raft Leader

---

### 优化 17: Raft 日志并行应用 — **P1 FSM 吞吐**

| 项目 | 内容 |
|------|------|
| **位置** | `service/fsm.go:48-53` |
| **现状** | FSM 串行 Apply 日志条目，一个接一个处理 |

**修改方案**: 对于操作不同 key 的日志条目，可并行 Apply:
```go
func (k *KVServer) applyCommittedLogs() {
    var wg sync.WaitGroup
    for entry := range batch {
        wg.Add(1)
        go func(e LogEntry) {
            defer wg.Done()
            k.Apply(e)  // 并行写入不同 key
        }(entry)
    }
    wg.Wait()
}
```
- 同 key 操作必须串行（通过 key hash 分到同一 goroutine）
- 在 `engine.go` 的 `applyWorker` 中实现 key-hash 多 worker

**预期提升**: 多 core 利用率提升，写入吞吐增加 2-4×

---

### 优化 18: Raft 流水线复制 — **P1 集群吞吐**

| 项目 | 内容 |
|------|------|
| **位置** | `Raft/raft.go:573-666` — `replicateLog()` |
| **现状** | 向每个 follower 每次只发送一条新日志，等 RPC 返回后再发下一条 |

**修改方案**:
- 为每个 follower 维护一个独立的发送协程和待发送队列
- 领导者产生新日志时立即推入所有 follower 的发送队列
- 发送协程不断从队列取出、发送、更新 nextIndex
- 类似 etcd 的 `streamWriter`

**预期提升**: 多 follower 场景下复制延迟降低，管道深度加大吞吐

---

## 四、实施路线图

### Phase 1 (1-2 周): 写入路径优化

| 编号 | 任务 | 文件 | 预期收益 |
|------|------|------|----------|
| 1.1 | Raft WAL 批量 sync | `Raft/raft_wal.go`, `Raft/raft.go` | 写入延迟 -30% |
| 1.2 | 消除存储层 WAL | `storage/zstorage/WAL.go`, `memtable.go` | 写入延迟 -30% |
| 1.3 | persistStateLocked 按需调用 | `Raft/raft.go` | 减少冗余 fsync |

**Phase 1 目标**: 写入延迟从 3×fsync 降至 1×fsync

### Phase 2 (2-3 周): 读取路径优化

| 编号 | 任务 | 文件 | 预期收益 |
|------|------|------|----------|
| 2.1 | SSTable 分块格式 | `sstable_format.go`(新) | 读取不再全文件加载 |
| 2.2 | 布隆过滤器 | `bloom.go`(新) | 不存在 key 快速返回 |
| 2.3 | 块索引 + 二分查找 | `block.go`(新) | Block 内精确定位 |
| 2.4 | 兼容旧格式读取 | `SSTable.go` | 平滑升级 |

**Phase 2 目标**: 点查延迟降低 90%+

### Phase 3 (1-2 周): Compaction 优化

| 编号 | 任务 | 文件 |
|------|------|------|
| 3.1 | SSTable 迭代器 | `iterator.go`(新) |
| 3.2 | K 路归并 | `merge_iterator.go`(新) |
| 3.3 | MANIFEST 持久化 | `manifest.go`(新) |

### Phase 4 (1-2 周): 集群与缓存

| 编号 | 任务 | 文件 |
|------|------|------|
| 4.1 | RPC 连接池 | `Raft/rpc.go` |
| 4.2 | 快照缓存 | `Raft/raft.go` |
| 4.3 | Block LRU 缓存 | `block_cache.go`(新) |

### Phase 5 (长期): 性能打磨

| 编号 | 任务 | 文件 |
|------|------|------|
| 5.1 | 数据压缩 | `compress.go`(新) |
| 5.2 | 内存预算 | `memory_budget.go`(新) |
| 5.3 | 跳表参数调优 | `config/` |

### Phase 6 (2-3 周): 租约读 + 流水线复制

| 编号 | 任务 | 文件 | 预期收益 |
|------|------|------|----------|
| 6.1 | Raft Leader Lease 实现 | `Raft/raft.go`, `Raft/rpc.go` | 读延迟降低 80-90% |
| 6.2 | GET 请求 Lease 快速路径 | `service/router.go` | 读走本地，不经 Raft 共识 |
| 6.3 | Raft 流水线复制 | `Raft/raft.go`, `Raft/rpc.go` | 集群写延迟降低 |
| 6.4 | FSM 并行 Apply | `service/fsm.go`, `storage/engine.go` | 写入吞吐提升 2-4× |

**Phase 6 目标**: 单 Raft Group 的读写吞吐达到硬件极限

### Phase 7 (4-6 周): Multi-Raft 分区

| 编号 | 任务 | 文件 | 预期收益 |
|------|------|------|----------|
| 7.1 | Shard 路由层 | `service/shard_router.go`(新) | key → Shard 自动路由 |
| 7.2 | 多 Raft Group 管理 | `Raft/raft.go`, `Server/server.go` | 独立 Raft 日志与状态 |
| 7.3 | 存储分区隔离 | `storage/`, `config/` | 每 Shard 独立 SSTable 目录 |
| 7.4 | 跨 Shard 读 (Scatter-Gather) | `service/router.go` | 范围查询支持 |
| 7.5 | 动态分裂/迁移 (可选) | `service/rebalance.go`(新) | 后期扩展 |

**Phase 7 目标**: 写吞吐线性扩展至 N× (N=Shard 数)

---

## 五、文件变更汇总

| 文件 | 变更类型 | Phase |
|------|----------|-------|
| `Raft/raft_wal.go` | 修改 (移除单条 fsync) | 1 |
| `Raft/raft.go` | 修改 (Lease + 流水线 + 多 Group) | 1/6/7 |
| `Raft/rpc.go` | 修改 (连接池 + Lease 续约 + 流水线) | 4/6 |
| `service/router.go` | 修改 (Lease 快速路径 + Shard 路由) | 6/7 |
| `service/fsm.go` | 修改 (并行 Apply + 多 Shard FSM) | 6/7 |
| `service/shard_router.go` | **新增** | 7 |
| `service/rebalance.go` | **新增** (可选) | 7 |
| `Server/server.go` | 修改 (多 Raft Group 初始化) | 7 |
| `storage/zstorage/WAL.go` | 删除/降级为空实现 | 1 |
| `storage/zstorage/memtable.go` | 修改 (移除 WAL 依赖) | 1 |
| `storage/zstorage/SSTable.go` | 重构 (新格式 + 兼容旧格式) | 2 |
| `storage/zstorage/sstable_format.go` | **新增** | 2 |
| `storage/zstorage/bloom.go` | **新增** | 2 |
| `storage/zstorage/block.go` | **新增** | 2 |
| `storage/zstorage/iterator.go` | **新增** | 3 |
| `storage/zstorage/merge_iterator.go` | **新增** | 3 |
| `storage/zstorage/manifest.go` | **新增** | 3 |
| `storage/zstorage/block_cache.go` | **新增** | 4 |
| `storage/zstorage/compress.go` | **新增** | 5 |
| `storage/zstorage/memory_budget.go` | **新增** | 5 |
| `storage/engine.go` | 修改 (key-hash 多 worker) | 6 |
| `config/global.go` | 修改 (PartitionCount 等) | 7 |
| `config/config.json` | 修改 (分片配置) | 7 |

---

## 六、风险评估

| 风险 | 等级 | 缓解措施 |
|------|------|----------|
| SSTable 格式不兼容 | 中 | Phase 2 保留旧格式读取能力，新写入用新格式，逐步迁移 |
| WAL 移除后崩溃丢数据 | 高 | 确保 Raft log fsync 策略正确；Phase 1 先做批量 sync，Phase 2 再移除存储 WAL |
| Compaction 迭代器正确性 | 中 | 与旧实现对比测试，验证数据一致性 |
| 连接池导致旧连接失效 | 低 | 心跳 + 自动重连机制 |
| 布隆过滤器误判 | 低 | 误判率 0.8%，仅导致无效磁盘读取，不影响正确性 |
| **Lease 时钟漂移导致脑裂读** | **高** | 使用 monotonic clock；Lease 时间设为 `electionTimeout/3` 留足安全边界；Leader 失去多数派确认后立即撤销 Lease |
| **Multi-Raft 跨 Shard 一致性** | **中** | 初期不支持跨 Shard 事务，仅保证单 Shard 内线性一致性；跨 Shard 原子操作后续版本用 2PC 实现 |
| **Multi-Raft 配置复杂度** | **中** | 配置结构化；分片拓扑通过配置中心/启动参数注入；提供默认单 Shard 兼容部署 |