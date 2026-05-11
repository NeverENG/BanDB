package zstorage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage/istorage"
)

const headerLength = 12

var _ istorage.IWal = &WAL{}

type WAL struct {
	mu        sync.Mutex
	file      *os.File
	headerBuf [headerLength]byte
}

func NewWAL() *WAL {
	file, err := os.OpenFile(config.G.WALPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		slog.Warn("cannot open WAL, running in disabled mode", "path", config.G.WALPath, "error", err)
		return &WAL{file: nil}
	}
	slog.Info("WAL opened", "path", config.G.WALPath)
	return &WAL{file: file}
}

func (w *WAL) Write(entry istorage.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	hasher := crc32.NewIEEE()
	hasher.Write(entry.Key)
	hasher.Write(entry.Value)
	crc := hasher.Sum32()

	binary.BigEndian.PutUint32(w.headerBuf[:], crc)
	binary.BigEndian.PutUint32(w.headerBuf[4:], uint32(len(entry.Key)))
	binary.BigEndian.PutUint32(w.headerBuf[8:], uint32(len(entry.Value)))

	if _, err := w.file.Write(w.headerBuf[:]); err != nil {
		slog.Error("write WAL header failed", "error", err)
		return err
	}
	if _, err := w.file.Write(entry.Key); err != nil {
		slog.Error("write WAL key failed", "error", err)
		return err
	}
	if _, err := w.file.Write(entry.Value); err != nil {
		slog.Error("write WAL value failed", "error", err)
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Read() ([]istorage.LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil, nil
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	entries := make([]istorage.LogEntry, 0)

	for {
		header := make([]byte, headerLength)
		_, err := io.ReadFull(w.file, header)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, os.ErrClosed) {
				break
			}
			return entries, err
		}

		crc := binary.BigEndian.Uint32(header[:])
		keyLen := binary.BigEndian.Uint32(header[4:])
		valueLen := binary.BigEndian.Uint32(header[8:])

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(w.file, key); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return entries, err
		}

		value := make([]byte, valueLen)
		if _, err := io.ReadFull(w.file, value); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return entries, err
		}

		hasher := crc32.NewIEEE()
		hasher.Write(key)
		hasher.Write(value)
		if crc != hasher.Sum32() {
			slog.Error("WAL data corruption detected")
			return entries, errors.New("data corruption detected")
		}

		entries = append(entries, istorage.LogEntry{Key: key, Value: value})
	}

	return entries, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	return w.file.Sync()
}

func (w *WAL) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		// WAL disabled mode: try to remove old file by path, then reopen
		if err := os.Remove(config.G.WALPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		f, err := os.OpenFile(config.G.WALPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			slog.Warn("WAL disabled, cannot reopen after clear", "error", err)
			return nil
		}
		w.file = f
		return nil
	}

	path := w.file.Name()
	if err := w.file.Close(); err != nil {
		slog.Error("close WAL before clear failed", "error", err)
		return err
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		slog.Error("reopen WAL after clear failed", "error", err)
		return err
	}
	w.file = f
	return w.file.Sync()
}
