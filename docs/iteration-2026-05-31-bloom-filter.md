# 迭代报告：存储层布隆过滤器

> 日期: 2026-05-31 · 范围: `storage/zstorage` · 对应 issue: #7 #3
> 关联 PR: #52 · #58 · #63 · #42(CI 接入) · 衍生 issue: #51 #53–#68

本次迭代为 BanDB 存储引擎落地**布隆过滤器**，对应《存储引擎瓶颈优化方案》瓶颈 2，目标是消除「点查不存在的 key 仍需读 SSTable」的开销。全程自主迭代 + 原子提交 + CI 门禁 + AI 评审闭环。

---

## 一、修改报告（改了什么）

### PR #52 — 布隆过滤器独立模块（零侵入）
新增文件，不改动任何现有读写路径：

| 文件 | 内容 |
|------|------|
| `storage/zstorage/bloom.go` | `BloomFilter`：按 n、p 计算最优 m/k；FNV-1a 双哈希；`Add`/`MayContain`/`Encode`/`Decode` |
| `storage/zstorage/partitioned_bloom.go` | `PartitionedBloom`：按命名空间「仓库」分区 + 前缀删除；`Encode`/`Decode` |
| `storage/zstorage/bloom_test.go` | 无漏报、最优参数、经验误判率、序列化往返 |
| `storage/zstorage/partitioned_bloom_test.go` | 命名空间拆分、仓库隔离、分区往返 |

### PR #58 — 接入 SSTable 读写路径（向后兼容格式变更）
| 文件 | 改动 |
|------|------|
| `SSTable.go` | 写：块索引后写入布隆段；读：`ReadFromSSTable` 先查布隆快速否决；`bloomCache` 缓存（含 nil 负缓存）；`DeleteSSTable` 清理；`LoadSSTableMetaList` 预热 |
| `partitioned_bloom.go` | 新增 `BuildPartitionedBloom`：按仓库预统计、各自最优 sizing 一次性构建 |
| `sstable_bloom_test.go` | v2 往返 + **手写 v1 文件**的向后兼容读测试 |

**文件布局（v2，完全向后兼容）**：
```
[DataBlocks][BlockIndex][BloomBlob][BloomLen(8B)+BLMF(4B)][IndexFooter(16B)]
```
布隆段插在块索引之后、原 16B 索引 Footer 之前 → Footer 布局/magic/位置不变，旧读路径零改动；旧 v1 文件末尾无 `BLMF` trailer，`getBloom` 返回 nil 自动走原块索引路径。

### PR #63 — 健壮性修复（回应 AI 评审 #59/#62）
| 改动 | 说明 |
|------|------|
| 落盘后再缓存 | `writeBloomSection` 拆职责（只写文件、返回过滤器），调用方在 `file.Sync()` 成功后才 `cacheBloom` |
| 去 Stat 竞争 | `loadBloomFromFile` 全程 `SeekEnd` 负偏移定位，不依赖 `Stat().Size()` |
| 防越界/溢出 | 新增 `maxBloomSectionBytes`（1 GiB）上限 |
| 测试补全 | v1 缺失 key 的否定覆盖（范围内空洞/越界/不存在命名空间） |

### 配套
- **PR #42**：在 BanDB 仓库接入 BanGD AI PR 评审 Action（DeepSeek，pin SHA）。
- **Issue #51**：记录 `storage` 顶层包测试非 hermetic（读残留 `.sst`/WAL）导致基线失败，留待修复。

---

## 二、迭代原因报告（为什么这么改）

### 1. 为什么先做独立模块再接入？
布隆过滤器算法与 SSTable 格式是两个正交的关注点。先落地**零侵入、可单测**的纯算法模块（PR #52），把误判率、参数计算、序列化在脱离磁盘 I/O 的环境中验证（经验误判率实测 **0.0056 < 目标 0.01**），再做风险更高的格式接入（PR #58）。两步各自原子、各自可回滚。

### 2. 为什么按「仓库」分区 + 前缀删除？
数据仓库的 key 常带命名空间前缀（`log:...`、`order:...`），相似度高。
- **分区**：每个仓库一份独立子过滤器，互不污染；查 `order:x` 时若 order 仓库根本不存在可立即否决，不受 log 仓库的 key 干扰。
- **前缀删除**：分区内所有 key 共享同一仓库前缀，去掉冗余前缀只对区分性后缀哈希。
- **按仓库各自 sizing**：`BuildPartitionedBloom` 先预统计每个仓库的元素数，再按各自的 n 算最优 m/k，避免一刀切的容量浪费或误判率失控。

### 3. 为什么用 FNV-1a 双哈希而非更快的哈希？
纯 Go、零依赖、无内存分配、可审计。Kirsch-Mitzenmacher 双哈希（`h1 + i·h2`）用两个独立种子的 FNV-1a 模拟 k 个哈希，避免引入第三方依赖。中小 key 下开销可忽略。

### 4. 为什么格式设计成「布隆段插在 Footer 前、Footer 不变」？
最小化向后兼容风险。这是本次唯一的「中等风险」点（格式变更）。把不变量收敛到「旧 16B 索引 Footer 布局与位置完全不动」，使 `loadBlockIndexFromFile`/`readDataEndOffset` 零改动，旧 v1 文件天然走原路径。并用**手写 v1 文件的读测试**正面验证了这一兼容性。

### 5. 为什么采纳「落盘后再缓存」和「去 Stat」两条评审？
- **崩溃一致性**：缓存是「内存加速信号」，不应在 `Sync()` 前被当作「持久化信号」写入，否则崩溃后会出现「缓存说有但文件没有」。把时序依赖通过函数签名显式化（返回过滤器、由调用方 Sync 后缓存），比靠注释约定更可靠。
- **竞争窗口**：`Stat()` 取大小再 `Seek` 是对可变文件状态的两步推断；改用从尾部逆向的 `SeekEnd` 负偏移（与既有 `readDataEndOffset` 一致），从根上消除窗口，无需加锁或重试。

### 6. 为什么驳回了大部分 AI 评审建议？
BanGD 共生成 16 条架构级建议（#53–#68）。其中多数**结论本身即为「保持现状」**（如 FNV-1a/最优 m/k/大端序均为权衡后的正确选择），少数属较大重构（合并缓存、流式解码）。依据协作规范的**外科手术式修改**原则——不为推测性收益做预优化、不引入与当前任务无关的抽象——这些保留为「未来优化候选」并在各 issue 下说明，只采纳真正改善正确性/健壮性的 #59/#62。

---

## 三、流程与门禁

- **分支隔离**：全程在基于 `main` 的独立 worktree 工作，未触碰开发中的 `refactor/proto-text` WIP。
- **本地门禁**：每个 PR 推送前 `go build/vet/test ./storage/zstorage/...` 全绿。
- **CI 门禁**：因仓库未配置必需 status check，代码 PR 不用盲 `--auto`，而是创建 PR 后 `gh run watch` 等 `ci.yml` 绿了再 `--rebase` 合并。
- **AI 评审闭环**：每个 PR 触发 BanGD → 生成架构级 issue → 采纳(修复)/驳回(说明) 逐条闭环。

---

## 四、后续候选

| 项 | 来源 |
|----|------|
| `storage` 包测试 hermetic 化（`t.TempDir`） | #51 |
| 布隆段元信息写入 Footer（一次 Seek 定位全部元数据） | #59 衍生 |
| 缓存分片合并 / 流式解码 / 零分配 key 路由 | #60 #61 #68 |
| K 路归并 compaction（瓶颈 3，当前 Merge 仍全量内存） | 优化方案 |
