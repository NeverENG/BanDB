# BanDB Flux 核心架构图

```mermaid
graph TD
    Client[客户端 / 数仓任务] -->|TLV 协议 + TxnID| BanNet[BanNet 网络层]
    
    subgraph "BanDB Flux 核心架构"
        BanNet -->|PreHandle Hook| Router[路由与事务管理器]
        
        subgraph "MVCC 事务管理层"
            Router -->|Start Txn| TxnMgr[事务管理器]
            TxnMgr -->|分配 ReadTS/WriteTS| Snapshot[快照视图]
            TxnMgr -->|Commit/Rollback| WAL[WAL 预写日志]
        end

        subgraph "LSM-Tree 存储引擎 (MVCC 适配)"
            WAL -->|追加写| MemTable[MemTable (跳表)]
            MemTable -->|Flush| L0[L0 SSTables (重叠)]
            L0 -->|Compaction| L1[L1 SSTables (有序)]
            L1 -->|Compaction| L2[L2 SSTables (归档)]
            
            note1[Key 格式: UserKey + Version(TS)] -.-> MemTable
            note2[删除标记: Tombstone] -.-> L0
        end
    end

    subgraph "双通道优先级调度模型"
        Router -->|心跳包 / ACK| HighPrioChan[抢占式 Channel (无缓冲)]
        Router -->|订单数据 / 导出流| LowPrioChan[Exporter Channel (有缓冲)]
        
        HighPrioChan -->|优先写入 TCP| Writer[TCP Writer 协程]
        LowPrioChan -->|空闲时写入 TCP| Writer
    end

    subgraph "数仓集成"
        LowPrioChan -->|批量推送| DataWarehouse[(ClickHouse / Doris)]
    end

    style BanNet fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style HighPrioChan fill:#ffccbc,stroke:#d84315,stroke-width:2px
    style LowPrioChan fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px
    style DataWarehouse fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2p
```

## 架构说明

### 主要组件

1. **BanNet 网络层**: 处理 TLV 协议和事务 ID
2. **MVCC 事务管理层**: 负责事务管理、快照视图和 WAL 日志
3. **LSM-Tree 存储引擎**: 基于 MVCC 的存储结构，包含 MemTable 和多层 SSTable
4. **双通道优先级调度**: 区分高优先级（心跳/ACK）和低优先级（数据流）通道
5. **数仓集成**: 支持向 ClickHouse/Doris 等数据仓库推送数据

### 关键特性

- **MVCC 支持**: 通过时间戳版本控制实现并发控制
- **优先级调度**: 确保关键消息优先传输
- **LSM-Tree 结构**: 高效的写入性能和压缩机制