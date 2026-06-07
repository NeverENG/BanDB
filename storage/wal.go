package storage

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

// WAL 操作码
const (
	WALOpPut    uint8 = 1
	WALOpDelete uint8 = 2
)

// WAL 存储层预写日志：standalone 模式下，写先 append + fsync 到此处再进 memtable，
// 提供单机崩溃恢复。记录格式 [op u8][klen u32][vlen u32][key][value]（BigEndian）。
// 与 Raft/raft_wal.go 一致：重放读到残缺尾部记录时直接停止（撕裂的尾写按 EOF 处理），
// 不使用 CRC。重放是幂等盲写（Put/Delete），未截断的 WAL 反复重放也安全。
type WAL struct {
	file *os.File
	path string
}

// NewWAL 打开（或创建）WAL 文件，以追加模式准备写入。
func NewWAL(path string) (*WAL, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, path: path}, nil
}

// Close 关闭底层文件。
func (w *WAL) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// Append 追加一条记录并 fsync，确保返回时数据已落盘。
// 删除墓碑用 op=WALOpDelete、value 传 nil。
func (w *WAL) Append(op uint8, key, value []byte) error {
	buf := make([]byte, 9, 9+len(key)+len(value))
	buf[0] = op
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(value)))
	buf = append(buf, key...)
	buf = append(buf, value...)

	if _, err := w.file.Write(buf); err != nil {
		return err
	}
	return w.file.Sync()
}

// Replay 从头读取全部记录，对每条调用 fn。读到残缺记录（撕裂尾写）即停止重放，
// 返回 nil；底层 IO 错误或 fn 返回错误则向上抛出。
func (w *WAL) Replay(fn func(op uint8, key, value []byte) error) error {
	f, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var hdr [9]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // EOF 或残缺头部：正常结束
		}
		op := hdr[0]
		klen := binary.BigEndian.Uint32(hdr[1:5])
		vlen := binary.BigEndian.Uint32(hdr[5:9])

		key := make([]byte, klen)
		if _, err := io.ReadFull(r, key); err != nil {
			break // 残缺尾写
		}
		var value []byte
		if vlen > 0 {
			value = make([]byte, vlen)
			if _, err := io.ReadFull(r, value); err != nil {
				break // 残缺尾写
			}
		}

		if err := fn(op, key, value); err != nil {
			return err
		}
	}
	return nil
}
