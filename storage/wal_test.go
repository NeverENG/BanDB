package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

type walRec struct {
	op    uint8
	key   []byte
	value []byte
}

func replayAll(t *testing.T, w *WAL) []walRec {
	t.Helper()
	var recs []walRec
	if err := w.Replay(func(op uint8, key, value []byte) error {
		recs = append(recs, walRec{op: op, key: append([]byte(nil), key...), value: append([]byte(nil), value...)})
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	return recs
}

func TestWALAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}

	if err := w.Append(WALOpPut, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Append(WALOpDelete, []byte("k2"), nil); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Append(WALOpPut, []byte("k3"), []byte{}); err != nil {
		t.Fatalf("append empty value: %v", err)
	}

	recs := replayAll(t, w)
	if len(recs) != 3 {
		t.Fatalf("want 3 records, got %d", len(recs))
	}
	if recs[0].op != WALOpPut || !bytes.Equal(recs[0].key, []byte("k1")) || !bytes.Equal(recs[0].value, []byte("v1")) {
		t.Errorf("rec0 mismatch: %+v", recs[0])
	}
	if recs[1].op != WALOpDelete || !bytes.Equal(recs[1].key, []byte("k2")) || len(recs[1].value) != 0 {
		t.Errorf("rec1 mismatch: %+v", recs[1])
	}
	if recs[2].op != WALOpPut || !bytes.Equal(recs[2].key, []byte("k3")) || len(recs[2].value) != 0 {
		t.Errorf("rec2 (empty-value put) mismatch: %+v", recs[2])
	}
	_ = w.Close()
}

func TestWALReopenPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	if err := w.Append(WALOpPut, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = w.Close()

	// 重新打开应追加而非截断，且能重放出旧记录
	w2, err := NewWAL(path)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	if err := w2.Append(WALOpPut, []byte("b"), []byte("2")); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	recs := replayAll(t, w2)
	if len(recs) != 2 || !bytes.Equal(recs[0].key, []byte("a")) || !bytes.Equal(recs[1].key, []byte("b")) {
		t.Fatalf("reopen replay mismatch: %+v", recs)
	}
	_ = w2.Close()
}

func TestWALTornTailStops(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	if err := w.Append(WALOpPut, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = w.Close()

	// 追加一段残缺记录（只有半个头部），模拟崩溃时的撕裂尾写
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open for torn write: %v", err)
	}
	if _, err := f.Write([]byte{WALOpPut, 0x00, 0x00}); err != nil {
		t.Fatalf("torn write: %v", err)
	}
	_ = f.Close()

	w2, err := NewWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	recs := replayAll(t, w2)
	if len(recs) != 1 || !bytes.Equal(recs[0].key, []byte("k1")) {
		t.Fatalf("torn tail should yield exactly the 1 intact record, got %+v", recs)
	}
	_ = w2.Close()
}

func TestWALReplayMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent", "wal.log")
	// 不创建文件，直接构造一个指向不存在路径的 WAL 实例做重放
	w := &WAL{path: path}
	recs := replayAll(t, w)
	if len(recs) != 0 {
		t.Fatalf("missing file should replay 0 records, got %d", len(recs))
	}
}
