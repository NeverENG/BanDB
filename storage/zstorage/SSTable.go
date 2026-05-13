package zstorage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage/istorage"
)

const (
	SSTableBlockSize   = 64
	indexFooterMagic   uint32 = 0x49445846 // "IDXF"
	indexFooterSize    int64  = 16         // BlockCount(4) + IndexOffset(8) + Magic(4)
)

type BlockIndexEntry struct {
	LastKey     []byte
	BlockOffset int64
}

var _ istorage.ISSTable = &SSTable{}

type SSTable struct {
	mata       []*istorage.SSTableMata
	mu         sync.RWMutex
	indexCache map[string][]BlockIndexEntry
	idxMu      sync.RWMutex
}

func NewSSTable() *SSTable {
	return &SSTable{
		mata:       make([]*istorage.SSTableMata, 0),
		indexCache: make(map[string][]BlockIndexEntry),
	}
}

func (ss *SSTable) LoadSSTableMetaList() {
	dir := config.G.SSTablePath

	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("cannot create SSTable directory", "error", err)
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		slog.Warn("cannot read SSTable directory", "error", err)
		return
	}

	metas := make([]*istorage.SSTableMata, 0)
	count := 0

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sst" {
			continue
		}

		fullPath := filepath.Join(dir, entry.Name())

		file, err := os.Open(fullPath)
		if err != nil {
			slog.Warn("failed to open SSTable", "file", entry.Name(), "error", err)
			continue
		}

		var keyLen uint32
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			slog.Warn("failed to read key length", "file", entry.Name(), "error", err)
			file.Close()
			continue
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			slog.Warn("failed to read key", "file", entry.Name(), "error", err)
			file.Close()
			continue
		}

		file.Close()

		info, err := os.Stat(fullPath)
		if err != nil {
			slog.Warn("failed to stat SSTable", "file", entry.Name(), "error", err)
			continue
		}

		meta := &istorage.SSTableMata{
			Level:        0,
			Filepath:     fullPath,
			MinKey:       keyBytes,
			MaxKey:       nil,
			Size:         info.Size(),
			MaxKeyLoaded: false,
		}

		metas = append(metas, meta)
		count++
	}

	sort.Slice(metas, func(i, j int) bool {
		return metas[i].Filepath < metas[j].Filepath
	})

	ss.mu.Lock()
	ss.mata = metas
	ss.mu.Unlock()

	for _, meta := range metas {
		go meta.EnsureMeta()
	}

	slog.Info("SSTable index loaded", "files", count, "dir", dir)
}

// 实现持久化跳表数据持久化到磁盘
func (ss *SSTable) WriteToSSTable(entries []istorage.LogEntry) error {
	if len(entries) == 0 {
		return errors.New("dont keep")
	}

	// 跳表本身是有序的，collectAllEntry 按顺序遍历，所�?entries 已经有序

	// 2. 生成文件名并创建目录
	filename := fmt.Sprintf("sstable_%d.sst", time.Now().UnixNano())
	dir := config.G.SSTablePath
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create data directory failed: %v", err)
	}
	fullPath := filepath.Join(dir, filename)

	// 3. 创建文件
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("create SSTable file failed: %v", err)
	}
	defer file.Close()

	// 4. 写入数据：先写 buffer，再一次落盘避免大量小 syscall
	// 格式: [KeyLen(4B)][Key][ValueLen(4B)][Value]
	var buf bytes.Buffer
	for _, entry := range entries {
		keyLen := uint32(len(entry.Key))
		valueLen := uint32(len(entry.Value))

		binary.Write(&buf, binary.BigEndian, keyLen)
		buf.Write(entry.Key)
		binary.Write(&buf, binary.BigEndian, valueLen)
		buf.Write(entry.Value)
	}
	if _, err := file.Write(buf.Bytes()); err != nil {
		return err
	}

	// 5. 确保数据刷入磁盘（Raft 日志已保证持久化，此处跳过 fsync 避免与 Raft 抢磁盘 IO）
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat SSTable file failed: %v", err)
	}
	meta := &istorage.SSTableMata{
		Level:        0,
		Filepath:     fullPath,
		MinKey:       entries[0].Key,
		MaxKey:       entries[len(entries)-1].Key,
		Size:         info.Size(),
		MaxKeyLoaded: true,
	}
	ss.AddMata(meta)
	return nil
}

func (ss *SSTable) GetAllMata() []*istorage.SSTableMata {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	// 注意：返回指针的副本，但指针指向的对象是同一个！
	result := make([]*istorage.SSTableMata, len(ss.mata))
	copy(result, ss.mata) // 注意：复制的是指针，不是对象本身
	return result
}

func (ss *SSTable) ReadAllFromSSTable(filepath string) ([]*istorage.LogEntry, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	entries := make([]*istorage.LogEntry, 0)
	for {
		// 读取 Key 长度
		var keyLen uint32
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF {
				break // 正常结束
			}
			return nil, fmt.Errorf("failed to read key length: %v", err)
		}

		// 读取 Key 内容
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key: %v", err)
		}

		// 读取 Value 长度
		var valueLen uint32
		if err := binary.Read(file, binary.BigEndian, &valueLen); err != nil {
			return nil, fmt.Errorf("failed to read value length: %v", err)
		}

		// 读取 Value 内容
		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(file, valueBytes); err != nil {
			return nil, fmt.Errorf("failed to read value: %v", err)
		}

		// 添加到结果列表
		entry := &istorage.LogEntry{
			Key:   keyBytes,
			Value: valueBytes,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (ss *SSTable) ReadFromSSTable(filepath string, key []byte) ([]byte, bool) {
	entries, _ := ss.ReadAllFromSSTable(filepath)

	for _, entry := range entries {
		if bytes.Equal(entry.Key, key) {
			return entry.Value, true
		}
	}
	return nil, false
}

// 合并多个 SSTable 文件
func (ss *SSTable) MergeSSTable(files []*istorage.SSTableMata, targetLevel int) *istorage.SSTableMata {
	if len(files) == 0 {
		return nil
	}

	slog.Info("merging SSTable files", "files", len(files), "targetLevel", targetLevel)

	allEntries := make([]*istorage.LogEntry, 0)
	for _, meta := range files {
		entries, err := ss.ReadAllFromSSTable(meta.Filepath)
		if err != nil {
			slog.Error("failed to read SSTable for merge", "file", meta.Filepath, "error", err)
			continue
		}
		allEntries = append(allEntries, entries...)
	}

	if len(allEntries) == 0 {
		slog.Warn("no entries to merge")
		return nil
	}

	// 2. 按 key 排序
	sort.Slice(allEntries, func(i, j int) bool {
		return bytes.Compare(allEntries[i].Key, allEntries[j].Key) < 0
	})

	// 3. 去重：同一个key保留最后一个（最新版本）
	deduped := make([]*istorage.LogEntry, 0)
	keyMap := make(map[string]int) // key -> index in deduped

	for _, entry := range allEntries {
		keyStr := string(entry.Key)
		if idx, exists := keyMap[keyStr]; exists {
			// key 已存在，覆盖旧的
			deduped[idx] = entry
		} else {
			// 新key，添加到列表
			keyMap[keyStr] = len(deduped)
			deduped = append(deduped, entry)
		}
	}

	// 4. 写入新的 SSTable 文件
	filename := fmt.Sprintf("sstable_merged_%d.sst", time.Now().UnixNano())
	dir := config.G.SSTablePath
	fullPath := filepath.Join(dir, filename)

	file, err := os.Create(fullPath)
	if err != nil {
		slog.Error("failed to create merged SSTable", "error", err)
		return nil
	}
	defer file.Close()

	for _, entry := range deduped {
		keyLen := uint32(len(entry.Key))
		valueLen := uint32(len(entry.Value))

		if err := binary.Write(file, binary.BigEndian, keyLen); err != nil {
			return nil
		}
		if _, err := file.Write(entry.Key); err != nil {
			return nil
		}
		if err := binary.Write(file, binary.BigEndian, valueLen); err != nil {
			return nil
		}
		if _, err := file.Write(entry.Value); err != nil {
			return nil
		}
	}

	// 5. 获取文件信息（Raft 日志已保证持久化，跳过 fsync）
	info, err := file.Stat()
	if err != nil {
		slog.Error("failed to stat merged SSTable", "error", err)
		return nil
	}

	// 6. 创建新文件的元数据
	newMeta := &istorage.SSTableMata{
		Level:        targetLevel,
		Filepath:     fullPath,
		MinKey:       deduped[0].Key,
		MaxKey:       deduped[len(deduped)-1].Key,
		Size:         info.Size(),
		MaxKeyLoaded: true, // 新文件已有MaxKey
	}

	ss.AddMata(newMeta)
	slog.Info("SSTable merged", "level", targetLevel, "file", filename, "keys", len(deduped), "size", info.Size())

	return newMeta
}
func (ss *SSTable) AddMata(meta *istorage.SSTableMata) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.mata = append(ss.mata, meta)
}

func (ss *SSTable) RemoveMata(target *istorage.SSTableMata) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	for i, meta := range ss.mata {
		if meta == target {
			ss.mata = append(ss.mata[:i], ss.mata[i+1:]...)
			return
		}
	}
}

// GetLevelFiles 获取指定层级的文件列表
func (ss *SSTable) GetLevelFiles(level int) []*istorage.SSTableMata {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	var result []*istorage.SSTableMata
	for _, meta := range ss.mata {
		if meta.Level == level {
			result = append(result, meta)
		}
	}
	return result
}

func (ss *SSTable) DeleteSSTable(meta *istorage.SSTableMata) {
	if err := os.Remove(meta.Filepath); err != nil {
		slog.Warn("failed to delete SSTable", "file", meta.Filepath, "error", err)
	}
}
