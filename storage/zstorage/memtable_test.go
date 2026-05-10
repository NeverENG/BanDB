package zstorage

import (
	"os"
	"testing"
	"github.com/NeverENG/BanDB/config"
)

func TestMemTable_PutAndDelete(t *testing.T) {
	// 配置临时WAL
	oldWALPath := config.G.WALPath
	testWAL := "test_memtable_wal.log"
	config.G.WALPath = testWAL
	os.Remove(testWAL) // 先删除旧的
	defer func() {
		os.Remove(testWAL)
		config.G.WALPath = oldWALPath
	}()
	
	memTable := NewMemTable()
	t.Log("MemTable created")
	
	err := memTable.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	t.Logf("Put key1 success, size: %d", memTable.Size())
	
	val, err := memTable.Get([]byte("key1"))
	if err != nil || string(val) != "value1" {
		t.Fatalf("Get failed: val='%s', err=%v", string(val), err)
	}
	t.Logf("Get key1 success: %s", string(val))
	
	err = memTable.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	t.Log("Delete key1 success")
	
	val, err = memTable.Get([]byte("key1"))
	if err == nil && val != nil {
		t.Errorf("Expected key1 to be deleted, but got: %s", string(val))
	}
}
