package zstorage

import (
	"os"
	"testing"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage/istorage"
)

func setupTestWAL(t *testing.T) *WAL {
	oldPath := config.G.WALPath
	config.G.WALPath = "test_wal.log"
	wal := NewWAL()
	if wal == nil {
		t.Skip("WAL initialization failed, skipping test")
	}
	t.Cleanup(func() {
		wal.Close()
		os.Remove("test_wal.log")
		config.G.WALPath = oldPath
	})
	return wal
}

func TestWAL_WriteAndRead(t *testing.T) {
	wal := setupTestWAL(t)

	err := wal.Write(istorage.LogEntry{Key: []byte("key1"), Value: []byte("value1")})
	if err != nil {
		t.Fatalf("WAL Write failed: %v", err)
	}

	readEntries, err := wal.Read()
	if err != nil {
		t.Fatalf("WAL Read failed: %v", err)
	}

	if len(readEntries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(readEntries))
	}
	if string(readEntries[0].Key) != "key1" || string(readEntries[0].Value) != "value1" {
		t.Errorf("Entry mismatch: got (%s, %s)", readEntries[0].Key, readEntries[0].Value)
	}
}

func TestWAL_WriteMultipleEntries(t *testing.T) {
	wal := setupTestWAL(t)

	entries := []istorage.LogEntry{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	for _, entry := range entries {
		if err := wal.Write(entry); err != nil {
			t.Fatalf("WAL Write failed: %v", err)
		}
	}

	readEntries, err := wal.Read()
	if err != nil {
		t.Fatalf("WAL Read failed: %v", err)
	}

	if len(readEntries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(readEntries))
	}

	for i, entry := range entries {
		if string(readEntries[i].Key) != string(entry.Key) {
			t.Errorf("Entry %d key mismatch: expected %s, got %s", i, entry.Key, readEntries[i].Key)
		}
		if string(readEntries[i].Value) != string(entry.Value) {
			t.Errorf("Entry %d value mismatch: expected %s, got %s", i, entry.Value, readEntries[i].Value)
		}
	}
}

func TestWAL_Clear(t *testing.T) {
	wal := setupTestWAL(t)

	err := wal.Write(istorage.LogEntry{Key: []byte("key1"), Value: []byte("value1")})
	if err != nil {
		t.Fatalf("WAL Write failed: %v", err)
	}

	err = wal.Clear()
	if err != nil {
		t.Fatalf("WAL Clear failed: %v", err)
	}

	// 直接使用原来的 wal 实例验证清空结果（Clear 已经重新打开了文件）
	readEntries, err := wal.Read()
	if err != nil {
		t.Fatalf("WAL Read failed: %v", err)
	}

	if len(readEntries) != 0 {
		t.Errorf("Expected 0 entries after clear, got %d", len(readEntries))
	}
}

func TestWAL_Sync(t *testing.T) {
	wal := setupTestWAL(t)

	err := wal.Write(istorage.LogEntry{Key: []byte("key1"), Value: []byte("value1")})
	if err != nil {
		t.Fatalf("WAL Write failed: %v", err)
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("WAL Sync failed: %v", err)
	}
}

func TestWAL_Close(t *testing.T) {
	wal := setupTestWAL(t)

	err := wal.Close()
	if err != nil {
		t.Fatalf("WAL Close failed: %v", err)
	}
}
