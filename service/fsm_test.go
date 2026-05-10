package service

import (
	"os"
	"testing"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/Raft"
)

func setupTest(t *testing.T) (*KVServer, func()) {
	oldWALPath := config.G.WALPath
	oldMaxSize := config.G.MaxMemTableSize
	oldPeers := config.G.Peers
	oldMe := config.G.Me

	// 每个测试用唯一的文件名，避免测试间干扰
	testWALPath := "test_service_wal_" + time.Now().Format("20060102150405.000000") + ".log"

	config.G.WALPath = testWALPath
	config.G.MaxMemTableSize = 100
	config.G.Peers = []string{"localhost:9000"}
	config.G.Me = 0

	// 确保文件不存在（如果之前有残留）
	os.Remove(testWALPath)

	fsm := NewKVServer()

	cleanup := func() {
		// 清理文件
		os.Remove(testWALPath)
		config.G.WALPath = oldWALPath
		config.G.MaxMemTableSize = oldMaxSize
		config.G.Peers = oldPeers
		config.G.Me = oldMe
	}

	return fsm, cleanup
}

func TestFSM_BasicOperation(t *testing.T) {
	fsm, cleanup := setupTest(t)
	defer cleanup()

	cmd := Command{
		Type:  "Put",
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}

	cmdBytes, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand failed: %v", err)
	}

	entry := Raft.LogEntry{
		Index:   0,
		Term:    1,
		Command: cmdBytes,
	}

	fsm.Apply(entry)

	val, err := fsm.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("fsm.Get failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(val))
	}
}

func TestFSM_DeleteOperation(t *testing.T) {
	fsm, cleanup := setupTest(t)
	defer cleanup()

	putCmd := Command{
		Type:  "Put",
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	putBytes, _ := EncodeCommand(putCmd)
	fsm.Apply(Raft.LogEntry{Index: 0, Term: 1, Command: putBytes})

	val, _ := fsm.Get([]byte("key1"))
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(val))
	}

	delCmd := Command{
		Type: "Delete",
		Key:  []byte("key1"),
	}
	delBytes, _ := EncodeCommand(delCmd)
	fsm.Apply(Raft.LogEntry{Index: 1, Term: 1, Command: delBytes})

	val, err := fsm.Get([]byte("key1"))
	if err == nil && val != nil {
		t.Errorf("Expected nil after delete, got '%s'", string(val))
	}
}

func TestFSM_UpdateOperation(t *testing.T) {
	fsm, cleanup := setupTest(t)
	defer cleanup()

	cmd1 := Command{Type: "Put", Key: []byte("key1"), Value: []byte("value1")}
	cmdBytes1, _ := EncodeCommand(cmd1)
	fsm.Apply(Raft.LogEntry{Index: 0, Term: 1, Command: cmdBytes1})

	cmd2 := Command{Type: "Put", Key: []byte("key1"), Value: []byte("value2")}
	cmdBytes2, _ := EncodeCommand(cmd2)
	fsm.Apply(Raft.LogEntry{Index: 1, Term: 1, Command: cmdBytes2})

	val, _ := fsm.Get([]byte("key1"))
	if string(val) != "value2" {
		t.Errorf("Expected 'value2', got '%s'", string(val))
	}
}
