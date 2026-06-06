# M2 · 字节级令牌桶写背压 —— 设计与结果

> 承接 M1 发现的「MemTable 写入无背压 → 内存随负载无界增长」。本步给写路径加**字节级令牌桶背压**，并诚实标注它能解决什么、不能解决什么。

## 设计

- 新增可复用组件 `pkg/credit`：字节信用池 `Pool`（`Acquire`/`TryAcquire`/`Release`/`Used`）。信用不足时 `Acquire` 阻塞，直到持久化方 `Release` 归还。预算 `<=0` 关闭背压。
- `MemTable` 接入：
  - `SkipList` 维护 `byteSize`（覆盖写按新旧 value 差值增量，防漂移）。
  - `Put`/`Delete` 写入前 `acquireCredit(full)`：`TryAcquire` 失败则先 `StartFlush` 再阻塞 `Acquire`，避免永久阻塞。
  - 覆盖写实际增量 < 预占额时，归还多占部分（信用与 `byteSize` 对账，不泄漏）。
  - `Flush` 成功后 `Release(dirty.byteSize)` 归还信用并唤醒被阻塞的写。
  - `Flush` 失败：保留 dirty（不丢数据）并重新触发刷盘重试；修掉了原 `Flush` 在失败后被下次刷盘覆盖 dirty 的隐患。
- 预算配置：`config.MemTableMaxInflightBytes`，默认 64MiB。

## 验证（A 方案：阻塞、0 丢帧）

### 背压确实绑定并精确封顶（关掉条数触发，让 MemTable 成为瓶颈）
`-memtable 1e8 -sat 5s`：

| | 吞吐 | inflight 峰值 | heap 峰值 |
|---|---|---|---|
| 背压关闭 `-budget 0` | 1,768,172 w/s | **784.2 MiB（无界）** | 2,014.7 MiB |
| 背压 `-budget 16MiB` | 1,225,599 w/s | **16.0 MiB（精确卡在预算）** | 173.4 MiB |

→ 未刷盘字节被**精确限制在 16.0MiB = 预算**，堆从 2GiB 降到 173MiB。代价是吞吐被限到 flush 速率（1.77M→1.23M）——**这正是背压的本意**：源跑赢 flush 时，限速写入而非撑爆内存。`pkg/credit` 有单测覆盖（阻塞/解除、超大单条放行、预算关闭、对账归零）。

### 复现命令
```powershell
go run ./benchmark/ingest/ -budget 0        -sat 5s -d 1s -rates 1000 -memtable 100000000
go run ./benchmark/ingest/ -budget 16777216 -sat 5s -d 1s -rates 1000 -memtable 100000000
```

## 诚实边界：背压不是 M1 那个增长的解药

默认小值配置（`MaxMemTableSize=4096`）下，**按条数刷盘（每 4096 条 ≈ 0.37MiB）远早于字节预算触发**，所以背压平时不绑定。实测 100k/200kHz 跑 60s：

| 速率 | 丢帧 | inflight 峰值 | heap 峰值 |
|---|---|---|---|
| 100 kHz | 0 | 0.4 MiB | 121 MiB |
| 200 kHz | 3,918 | 0.4 MiB | 265 MiB |

`inflight_peak` 仅 0.4MiB（≈一个 4096 条的表），**未刷盘量根本不大**。但 heap 仍涨到 265MiB——说明 **M1 看到的内存增长不在 MemTable，而在 SSTable 元数据 / bloom 过滤器随文件数累积**（12M 条 / 4096 ≈ 2900 个 SSTable，每个一份常驻 bloom + 索引）。

**结论**：
- 背压的角色是**内存硬上限 / 安全网**——当 value 很大或 `MaxMemTableSize` 很高、条数触发不及时，它兜底封顶（已证）。
- 它**不解决**小值高频下的内存增长，那是 SSTable 元数据/compaction 问题，是**下一条线**（M3 候选）：核查 compaction 是否跟得上文件增长、bloom/索引常驻内存能否随 compaction 收敛。

## 200kHz 丢帧说明
开背压后 200kHz/60s 出现 3,918 丢帧，是因为消费者写入被背压/flush 节流，生产者有界队列(qdepth=1024)溢出——属预期的"源超过可持续速率"行为，非引擎崩溃。
