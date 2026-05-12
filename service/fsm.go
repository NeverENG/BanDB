package service

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/NeverENG/BanDB/Raft"
	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage"
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
	fmt.Println("[INFO] KVServer Run started, waiting for Raft entries...")
	for entry := range k.raft.GetApplyCh() {
		k.Apply(entry)
	}
}

// Apply 应用日志到存储
func (k *KVServer) Apply(entry Raft.LogEntry) {
	var cmd Command
	if err := json.Unmarshal(entry.Command, &cmd); err != nil {
		fmt.Printf("[ERROR] Failed to unmarshal command: %v\n", err)
		return
	}



	switch cmd.Type {
	case "Put":
		err := k.storage.Put(cmd.Key, cmd.Value)
		if err != nil {
			fmt.Printf("[ERROR] Failed to put: %v\n", err)
		} else {
			fmt.Printf("[INFO] Put success: %s = %s\n", string(cmd.Key), string(cmd.Value))
		}
	case "Delete":
		err := k.storage.Delete(cmd.Key)
		if err != nil {
			fmt.Printf("[ERROR] Failed to delete: %v\n", err)
		} else {
			fmt.Printf("[INFO] Delete success: %s\n", string(cmd.Key))
		}
	}
}

// Get 从存储获取值
func (k *KVServer) Get(key []byte) ([]byte, error) {
	value, err := k.storage.Get(key)
	if err != nil {
		fmt.Printf("[ERROR] Get failed: %v\n", err)
	} else {
		fmt.Printf("[INFO] Get result: %s\n", string(value))
	}
	if value == nil {
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
