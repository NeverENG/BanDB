package service

import (
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/NeverENG/BanDB/Raft"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage"
	"github.com/NeverENG/BanDB/storage/istorage"
	"github.com/NeverENG/BanDB/storage/zstorage"
)

type Command struct {
	Type  string
	Key   []byte
	Value []byte
}

type KVServer struct {
	raft    *Raft.Raft
	storage *storage.Engine
}

// NewFSM 创建 FSM，自动从全局配置初始化 Raft 和存储
func NewKVServer() *KVServer {
	// 从全局配置获取集群信息
	peers := config.G.Peers
	me := config.G.Me

	// 初始化 Raft
	raft := Raft.NewRaft(peers, me)

	// 初始化存储
	memTable := zstorage.NewMemTable()
	store := storage.NewEngine(memTable)

	KVServer := &KVServer{
		raft:    raft,
		storage: store,
	}

	return KVServer
}

// Run 运行 FSM
func (k *KVServer) Run() {
	slog.Info("KVServer started, waiting for Raft entries")
	for entry := range k.raft.GetApplyCh() {
		k.Apply(entry)
	}
}

// Apply 应用日志到存储
func (k *KVServer) Apply(entry Raft.LogEntry) {
	if entry.IsSnapshot {
		go k.replaySnapshot(entry)
		return
	}

	var cmd Command
	if err := json.Unmarshal(entry.Command, &cmd); err != nil {
		slog.Error("failed to unmarshal command", "error", err)
		return
	}

	switch cmd.Type {
	case "Put":
		if err := k.storage.Put(cmd.Key, cmd.Value); err != nil {
			slog.Error("failed to put", "error", err)
		}
	case "Delete":
		if err := k.storage.Delete(cmd.Key); err != nil {
			slog.Error("failed to delete", "error", err)
		}
	}
}

// replaySnapshot 异步重放快照中的日志条目到临时表并 Flush 到 SSTable
func (k *KVServer) replaySnapshot(entry Raft.LogEntry) {
	entries := Raft.DeserializeLogEntries(entry.Command)
	if len(entries) == 0 {
		return
	}

	kvEntries := make([]istorage.LogEntry, 0, len(entries))
	for _, e := range entries {
		var cmd Command
		if err := json.Unmarshal(e.Command, &cmd); err != nil {
			continue
		}
		switch cmd.Type {
		case "Put":
			kvEntries = append(kvEntries, istorage.LogEntry{Key: cmd.Key, Value: cmd.Value})
		case "Delete":
			kvEntries = append(kvEntries, istorage.LogEntry{Key: cmd.Key, Value: nil})
		}
	}

	if err := k.storage.FlushToSSTable(kvEntries); err != nil {
		slog.Error("snapshot replay failed", "error", err)
	}
}

func (k *KVServer) Get(key []byte) ([]byte, error) {
	value, err := k.storage.Get(key)
	if value == nil && err == nil {
		return nil, errors.New("key not found")
	}
	return value, err
}

/* Put 直接写入存储（仅用于测试，生产环境应通过 Raft 写入）
func (k *KVServer) Put(key []byte, value []byte) error {
	return k.storage.Put(key, value)
}
*/

/* Delete 直接删除存储中的值（仅用于测试，生产环境应通过 Raft 写入）
func (k *KVServer) Delete(key []byte) error {
	return k.storage.Delete(key)
}
*/

// GetRaft 获取 Raft 实例
func (k *KVServer) GetRaft() *Raft.Raft {
	return k.raft
}

// AppendEntry 通过 Raft 追加日志
func (k *KVServer) AppendEntry(cmd Command) (int, error) {
	cmdBytes, err := EncodeCommand(cmd)
	if err != nil {
		return -1, err
	}
	index, err := k.raft.AppendEntry(cmdBytes)
	if err != nil {
		return -1, err
	}
	return index, nil
}

// WaitForCommit 等待日志被提交
func (k *KVServer) WaitForCommit(index int) error {
	// 检查当前提交索引
	k.raft.WaitCommitIndex(index)
	return nil

}

// EncodeCommand 编码命令为 JSON
func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}
