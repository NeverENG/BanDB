package Raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	MagicNumber = 0x52415654 // "RAFT" in ASCII
	Version     = 1
	StateFile   = "raft_state.dat"
	LogFile     = "raft_log.dat"
	SnapshotDir = "snapshots"
)

type RaftWAL struct {
	file     *os.File
	logPath  string
	metaPath string
}

type WALState struct {
	Term     int64
	VotedFor int64
}

// PersistData 包含所有需要持久化的 Raft 状态
type PersistData struct {
	CurrentTerm       int64
	VotedFor          int64
	Log               []LogEntry
	LastIncludedIndex int64
	LastIncludedTerm  int64
}

func NewRaftWAL(dir string) (*RaftWAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	wal := &RaftWAL{
		logPath:  filepath.Join(dir, LogFile),
		metaPath: filepath.Join(dir, StateFile),
	}

	f, err := os.OpenFile(wal.logPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	wal.file = f

	return wal, nil
}

func (w *RaftWAL) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

func (w *RaftWAL) SaveState(term int64, votedFor int64) error {
	state := WALState{Term: term, VotedFor: votedFor}

	f, err := os.Create(w.metaPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := binary.Write(f, binary.BigEndian, MagicNumber); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, Version); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, int64(state.Term)); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, int64(state.VotedFor)); err != nil {
		return err
	}

	return f.Sync()
}

func (w *RaftWAL) LoadState() (int64, int64, error) {
	f, err := os.Open(w.metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, -1, nil
		}
		return 0, -1, err
	}
	defer f.Close()

	var magic uint32
	if err := binary.Read(f, binary.BigEndian, &magic); err != nil {
		return 0, -1, err
	}
	if magic != MagicNumber {
		return 0, -1, nil
	}

	var version uint32
	if err := binary.Read(f, binary.BigEndian, &version); err != nil {
		return 0, -1, err
	}

	var term int64
	if err := binary.Read(f, binary.BigEndian, &term); err != nil {
		return 0, -1, err
	}

	var votedFor int64
	if err := binary.Read(f, binary.BigEndian, &votedFor); err != nil {
		return 0, -1, err
	}

	return term, votedFor, nil
}

func (w *RaftWAL) AppendLog(entry LogEntry) error {
	if err := binary.Write(w.file, binary.BigEndian, int64(entry.Index)); err != nil {
		return err
	}
	if err := binary.Write(w.file, binary.BigEndian, int64(entry.Term)); err != nil {
		return err
	}
	if err := binary.Write(w.file, binary.BigEndian, int64(len(entry.Command))); err != nil {
		return err
	}
	if _, err := w.file.Write(entry.Command); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *RaftWAL) LoadLogs() ([]LogEntry, error) {
	f, err := os.Open(w.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var logs []LogEntry
	for {
		var index int64
		if err := binary.Read(f, binary.BigEndian, &index); err != nil {
			break
		}
		var term int64
		if err := binary.Read(f, binary.BigEndian, &term); err != nil {
			break
		}
		var cmdLen int64
		if err := binary.Read(f, binary.BigEndian, &cmdLen); err != nil {
			break
		}
		cmd := make([]byte, cmdLen)
		if _, err := io.ReadFull(f, cmd); err != nil {
			break
		}

		logs = append(logs, LogEntry{
			Index:   int(index),
			Term:    int(term),
			Command: cmd,
		})
	}

	return logs, nil
}

func (w *RaftWAL) Clear() error {
	if err := w.Close(); err != nil {
		return err
	}
	w.file = nil

	if err := os.Remove(w.logPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(w.metaPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	f, err := os.Create(w.logPath)
	if err != nil {
		return err
	}
	w.file = f

	return nil
}

// 从data中获得数据转换成快照

func (w *RaftWAL) SaveSnapshot(data []byte, lastIncludedIndex int64, lastIncludedTerm int64) error {
	snapshotDir := filepath.Join(filepath.Dir(w.logPath), SnapshotDir)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return err
	}
	snapshotPath := filepath.Join(snapshotDir, fmt.Sprintf("%d_%d.snap", lastIncludedIndex, lastIncludedTerm))
	f, err := os.Create(snapshotPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := binary.Write(f, binary.BigEndian, MagicNumber); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, Version); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, lastIncludedIndex); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, lastIncludedTerm); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, int64(len(data))); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

// LoadLatestSnapshot 加载最新的快照
// 返回: data, lastIndex, lastTerm, error
func (w *RaftWAL) LoadLatestSnapshot() ([]byte, int64, int64, error) {
	snapshotDir := filepath.Join(filepath.Dir(w.logPath), SnapshotDir)

	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, err
	}

	if len(files) == 0 {
		return nil, 0, 0, nil
	}

	var latestFile string
	var latestIndex int64

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".snap") {
			name := file.Name()[:len(file.Name())-5]
			parts := strings.Split(name, "_")
			if len(parts) == 2 {
				index, err1 := strconv.ParseInt(parts[0], 10, 64)
				if err1 == nil && index > latestIndex {
					latestIndex = index
					latestFile = filepath.Join(snapshotDir, file.Name())
				}
			}
		}
	}

	if latestFile == "" {
		return nil, 0, 0, nil
	}

	f, err := os.Open(latestFile)
	if err != nil {
		return nil, 0, 0, err
	}
	defer f.Close()

	var magic uint32
	if err := binary.Read(f, binary.BigEndian, &magic); err != nil {
		return nil, 0, 0, err
	}
	if magic != MagicNumber {
		return nil, 0, 0, errors.New("invalid snapshot file")
	}

	var version uint32
	if err := binary.Read(f, binary.BigEndian, &version); err != nil {
		return nil, 0, 0, err
	}

	var lastIndex int64
	if err := binary.Read(f, binary.BigEndian, &lastIndex); err != nil {
		return nil, 0, 0, err
	}

	var lastTerm int64
	if err := binary.Read(f, binary.BigEndian, &lastTerm); err != nil {
		return nil, 0, 0, err
	}

	var dataLen int64
	if err := binary.Read(f, binary.BigEndian, &dataLen); err != nil {
		return nil, 0, 0, err
	}

	data := make([]byte, dataLen)
	if _, err := f.Read(data); err != nil {
		return nil, 0, 0, err
	}

	return data, lastIndex, lastTerm, nil
}

// DeleteOldSnapshots 删除旧版本的快照文件（保留指定的最新快照）
func (w *RaftWAL) DeleteOldSnapshots(keepIndex int64) error {
	snapshotDir := filepath.Join(filepath.Dir(w.logPath), SnapshotDir)

	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".snap") {
			name := file.Name()[:len(file.Name())-5]
			parts := strings.Split(name, "_")
			if len(parts) == 2 {
				index, err := strconv.ParseInt(parts[0], 10, 64)
				if err == nil && index < keepIndex {
					os.Remove(filepath.Join(snapshotDir, file.Name()))
				}
			}
		}
	}

	return nil
}

func (w *RaftWAL) TruncateLogs(lastIncludedIndex int64) error {
	logs, err := w.LoadLogs()
	if err != nil {
		return err
	}

	var remainingLogs []LogEntry
	for _, log := range logs {
		if int64(log.Index) > lastIncludedIndex {
			remainingLogs = append(remainingLogs, log)
		}
	}

	w.Close()

	f, err := os.Create(w.logPath)
	if err != nil {
		return err
	}
	w.file = f

	for _, log := range remainingLogs {
		if err := w.AppendLog(log); err != nil {
			return err
		}
	}

	return nil
}

// SavePersist 保存所有 Raft 持久化状态（currentTerm, votedFor, log）
func (w *RaftWAL) SavePersist(data PersistData) error {
	// 1. 保存 currentTerm 和 votedFor
	if err := w.SaveState(data.CurrentTerm, data.VotedFor); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}

	// 2. 重建日志文件
	w.Close()

	f, err := os.Create(w.logPath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	w.file = f

	// 3. 写入所有日志条目
	for _, entry := range data.Log {
		if err := w.AppendLog(entry); err != nil {
			return fmt.Errorf("failed to append log entry: %w", err)
		}
	}

	return nil
}

// LoadPersist 加载所有 Raft 持久化状态
func (w *RaftWAL) LoadPersist() (PersistData, error) {
	data := PersistData{}

	// 1. 加载 currentTerm 和 votedFor
	term, votedFor, err := w.LoadState()
	if err != nil {
		return data, fmt.Errorf("failed to load state: %w", err)
	}
	data.CurrentTerm = term
	data.VotedFor = votedFor

	// 2. 加载日志
	logs, err := w.LoadLogs()
	if err != nil {
		return data, fmt.Errorf("failed to load logs: %w", err)
	}
	data.Log = logs

	// 3. 加载快照元数据
	snapshotData, lastIndex, lastTerm, err := w.LoadLatestSnapshot()
	if err == nil && snapshotData != nil {
		data.LastIncludedIndex = lastIndex
		data.LastIncludedTerm = lastTerm
	}

	return data, nil
}

// ... existing code ...
