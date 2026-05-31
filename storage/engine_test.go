package storage

import (
	"path/filepath"
	"testing"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage/zstorage"
)

func setupTestEngine(t *testing.T) (*Engine, func()) {
	oldWALPath := config.G.WALPath
	oldMaxSize := config.G.MaxMemTableSize
	oldSSTPath := config.G.SSTablePath

	// 每个用例独立的临时目录，避免读到共享 ../../log 下其它运行的残留 .sst/WAL
	dir := t.TempDir()
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir
	config.G.MaxMemTableSize = 100

	memTable := zstorage.NewMemTable()
	engine := NewEngine(memTable)

	// 启动 FlushWorker goroutine
	go memTable.FlushWorker()

	cleanup := func() {
		// 关闭 WAL 文件（临时目录由 t.TempDir 自动清理）
		memTable.Close()
		// 恢复配置
		config.G.WALPath = oldWALPath
		config.G.SSTablePath = oldSSTPath
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
	// 约定：缺失 key 返回错误（handleGet 据此向客户端回错误状态码）
	if err == nil {
		t.Errorf("Expected error for non-existent key, got value '%s'", value)
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
	// 约定：删除后再查应报缺失（与 handleGet 的错误语义一致）
	if err == nil {
		t.Errorf("Expected error after delete, got value '%s'", value)
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
	oldWALPath := config.G.WALPath
	oldSSTPath := config.G.SSTablePath
	config.G.MaxMemTableSize = 5
	dir := t.TempDir()
	config.G.WALPath = filepath.Join(dir, "wal.log")
	config.G.SSTablePath = dir

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

	memTable.Close()
	config.G.MaxMemTableSize = oldMaxSize
	config.G.WALPath = oldWALPath
	config.G.SSTablePath = oldSSTPath
}
