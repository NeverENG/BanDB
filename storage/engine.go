package storage

import (
	"github.com/NeverENG/BanDB/storage/istorage"
)

type StorageCommand struct {
	Type  string
	Key   []byte
	Value []byte
}

// Engine 存储引擎，作为 MemTable 的薄封装
// MemTable 内部已使用 RWMutex 自同步，Engine 不再持有自己的锁
type Engine struct {
	memTable istorage.IMemTable
	applyCh  chan StorageCommand
}

func NewEngine(memTable istorage.IMemTable) *Engine {
	e := &Engine{
		memTable: memTable,
		applyCh:  make(chan StorageCommand, 100),
	}
	go e.applyWorker()
	return e
}

func (e *Engine) Put(key []byte, value []byte) error {
	// MemTable.Put 内部已包含同步和刷盘触发逻辑
	return e.memTable.Put(key, value)
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	// MemTable.Get 内部已包含同步逻辑
	return e.memTable.Get(key)
}

func (e *Engine) Delete(key []byte) error {
	// MemTable.Delete 内部已包含同步逻辑
	return e.memTable.Delete(key)
}

func (e *Engine) Apply(cmd StorageCommand) error {
	e.applyCh <- cmd
	return nil
}

func (e *Engine) GetApplyCh() chan StorageCommand {
	return e.applyCh
}

// FlushToSSTable 快照重放到 SSTable（不经过 active 表，走临时表 → Flush → SSTable 路径）
func (e *Engine) FlushToSSTable(entries []istorage.LogEntry) error {
	return e.memTable.FlushToSSTable(entries)
}

func (e *Engine) applyWorker() {
	for cmd := range e.applyCh {
		switch cmd.Type {
		case "Put":
			e.Put(cmd.Key, cmd.Value)
		case "Delete":
			e.Delete(cmd.Key)
		}
	}
}