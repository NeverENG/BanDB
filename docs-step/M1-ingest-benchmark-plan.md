# M1 · IMU 高频摄入压测 —— 方案

> 目标：用一个能跑出数字的 demo，证明 BanDB 的存储地基扛得住「高频、定速率、内存受限」的传感器摄入。
> 产出五个指标：**写吞吐 / p99 写延迟 / 内存峰值 / 丢帧数=0 / WAL 重启可恢复**。
> 方案确认后再写代码（遵循 CLAUDE.md「方案先行」）。

---

## 核心模型：开环（open-loop），不是闭环

现有 `benchmark/`（TCP 客户端）和 `storage/bench_test.go`（`b.N` 微基准）都是**闭环**：循环里"做完一个立刻做下一个"，测的是"最多能跑多快"。

但 IMU/相机的本质是**固定速率吐数据**（如 1kHz），不管 DB 多快。所以要证"0 丢帧"，必须用**开环**模型：

```
生产者 ──以固定速率 R 投递──▶ 有界队列(深度 D) ──▶ 消费者(写入 BanDB)
                                   │
                          队列满 = DB 跟不上 = 丢帧(+1)
```

- **生产者**：按 `ticker(1/R)` 节奏生成 IMU 样本，投进有界 channel；channel 满则 `dropped++`（这就是"丢帧"的精确定义）。
- **消费者**：从 channel 取出写入引擎，记录每次写延迟。
- **判定**：在速率 R 下跑 T 秒，`dropped == 0` 即"该速率下不丢帧"。

配套两步跑法：
1. **找天花板（闭环）**：先打满，得到"最大可持续写入速率 Rmax"。
2. **证 0 丢帧（开环）**：在 R = 50%~70% × Rmax 下定速跑，证明 `dropped=0` 且 p99 延迟有界。

---

## ★ 唯一要你拍板的岔路：M1 测哪一层？

| 选项 | 测什么 | 优点 | 缺点 | 我的建议 |
|---|---|---|---|---|
| **A1 引擎内压测**（进程内直接驱动 `storage.Engine`） | LSM 存储引擎本身 | 内存测量干净（同进程 `runtime.ReadMemStats`）、可封顶（`GOMEMLIMIT`）、WAL 重启可恢复就是 close→reopen→scan、无网络/Raft 噪声、确定性强 | 不覆盖 BanNet + Raft 全链路 | **✅ M1 选它** |
| **A2 全链路压测**（扩展 `benchmark/` 走 TCP→Raft→LSM） | 端到端真实路径 | 测的是真部署 | 服务端内存难干净测量、单节点 Raft 每写一次 fsync 引入噪声、变量多 | 作为 **M1.5 后续** |
| A3 两个都做 | — | 全 | M1 拖长 | 不在 M1 |

**理由**：M1 要证的"LSM 扛高频顺序写 / 内存封顶 / 0 丢帧 / WAL 可恢复"**全是存储引擎属性**；内存封顶+测量只有进程内才干净；WAL 恢复是引擎操作。先把**地基**这层钉死、拿到干净数字，再用 A2 证全链路。

> **默认推进 A1**。若你要 M1 直接证全链路，告诉我改走 A2。

---

## 已定的默认（无需你决策，除非你想改）

- **IMU 数据形状**：key = `imu:<dev>:<ts_nanos>`（约 24B，时间戳前缀 → 顺序写 + 天然 range scan）；value = 64B（accel/gyro/mag 9×float32 + 头）。
- **速率档位**：200Hz / 1kHz / 5kHz 三档跑（覆盖典型 IMU 到激进档）。
- **内存封顶**：`GOMEMLIMIT` + 调小 `config.G.MaxMemTableSize` 强制频繁 flush，验证内存不随时长无限涨。
- **内存峰值采集**：后台每 100ms `runtime.ReadMemStats`，记录 `HeapAlloc` 峰值与 `Sys`。
- **WAL 重启可恢复**：摄入 N 条 → 关闭引擎 → 重开 → 全量 scan，断言可读条数 == 已 ack 条数（0 丢失）。
- **代码落点**：新建 `benchmark/ingest/`（独立 `package main`，不动现有 `benchmark/` 与 `storage/bench_test.go`）。
- **输出**：跑完把五个指标 + 各速率档表格写进本目录 `M1-ingest-benchmark-result.md`。

---

## 交付物（M1 完成时）

1. `benchmark/ingest/` 开环压测程序（A1）。
2. `docs-step/M1-ingest-benchmark-result.md`：三档速率 × 五指标的真实数据表 + 一句结论。
3. 一条可复现命令（写进 result 文档）。
