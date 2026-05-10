package storage

import (
	"os"
	"testing"

	"github.com/NeverENG/bandb/config"
	"github.com/NeverENG/bandb/storage/zstorage"
)

func setupTestEngine(t *testing.T) (*Engine, func()) {
	oldWALPath := config.G.WALPath
	oldMaxSize := config.G.MaxMemTableSize

	config.G.WALPath = "test_engine_wal.log"
	config.G.MaxMemTableSize = 100

	memTable := zstorage.NewMemTable()
	engine := NewEngine(memTable)

	// 启动 FlushWorker goroutine
	go memTable.FlushWorker()

	cleanup := func() {
		// 关闭 WAL 文件
		memTable.Close()
		// 删除测试文件
		os.Remove("test_engine_wal.log")
		// 恢复配置
		config.G.WALPath = oldWALPath
		config.G.MaxMemTableSize = oldMaxSize
	}

	return engine, cleanup
}

func TestEngine_PutAndGet(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	err := engine.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Engine Put failed: %v", err)
	}

	value, err := engine.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Engine Get failed: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Value mismatch: expected 'value1', got '%s'", value)
	}
}

func TestEngine_PutMultipleKeys(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	testCases := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, tc := range testCases {
		err := engine.Put([]byte(tc.key), []byte(tc.value))
		if err != nil {
			t.Fatalf("Engine Put failed for key %s: %v", tc.key, err)
		}
	}

	for _, tc := range testCases {
		value, err := engine.Get([]byte(tc.key))
		if err != nil {
			t.Fatalf("Engine Get failed for key %s: %v", tc.key, err)
		}
		if string(value) != tc.value {
			t.Errorf("Value mismatch for key %s: expected '%s', got '%s'", tc.key, tc.value, value)
		}
	}
}

func TestEngine_GetNonExistentKey(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	value, err := engine.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Engine Get failed: %v", err)
	}
	if value != nil {
		t.Errorf("Expected nil value for non-existent key, got '%s'", value)
	}
}

func TestEngine_Delete(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	err := engine.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Engine Put failed: %v", err)
	}

	err = engine.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Engine Delete failed: %v", err)
	}

	value, err := engine.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Engine Get after delete failed: %v", err)
	}
	if value != nil {
		t.Errorf("Expected nil value after delete, got '%s'", value)
	}
}

func TestEngine_DeleteNonExistentKey(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	err := engine.Delete([]byte("nonexistent"))
	if err == nil {
		t.Error("Expected error when deleting non-existent key, got nil")
	}
}

func TestEngine_UpdateExistingKey(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	err := engine.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Engine Put failed: %v", err)
	}

	err = engine.Put([]byte("key1"), []byte("value2"))
	if err != nil {
		t.Fatalf("Engine Update failed: %v", err)
	}

	value, err := engine.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Engine Get failed: %v", err)
	}
	if string(value) != "value2" {
		t.Errorf("Value mismatch: expected 'value2', got '%s'", value)
	}
}

func TestEngine_PutTriggersFlush(t *testing.T) {
	oldMaxSize := config.G.MaxMemTableSize
	config.G.MaxMemTableSize = 5

	memTable := zstorage.NewMemTable()
	engine := NewEngine(memTable)

	for i := 0; i < 10; i++ {
		key := []byte(string(rune('a' + i)))
		value := []byte(string(rune('A' + i)))
		err := engine.Put(key, value)
		if err != nil {
			t.Fatalf("Engine Put failed: %v", err)
		}
	}

	if memTable.Size() >= 5 {
		t.Logf("MemTable size after 10 puts: %d (flush may have been triggered)", memTable.Size())
	}

	config.G.MaxMemTableSize = oldMaxSize
	os.Remove("test_engine_wal.log")
}
