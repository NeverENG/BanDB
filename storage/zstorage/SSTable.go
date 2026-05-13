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
		go ss.getBlockIndex(meta.Filepath) // 异步预热块索引
	}

	slog.Info("SSTable index loaded", "files", count, "dir", dir)
}

// WriteToSSTable 将有序 entries 写入 SSTable 文件（含块索引）
func (ss *SSTable) WriteToSSTable(entries []istorage.LogEntry) error {
	if len(entries) == 0 {
		return errors.New("dont keep")
	}

	filename := fmt.Sprintf("sstable_%d.sst", time.Now().UnixNano())
	dir := config.G.SSTablePath
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create data directory failed: %v", err)
	}
	fullPath := filepath.Join(dir, filename)

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("create SSTable file failed: %v", err)
	}
	defer file.Close()

	// 构建数据 buffer + 块索引
	type blk struct {
		lastKey     []byte
		blockOffset int64
	}
	var blockIdx []blk

	var buf bytes.Buffer
	for i, entry := range entries {
		bi := i / SSTableBlockSize
		if i%SSTableBlockSize == 0 {
			blockIdx = append(blockIdx, blk{blockOffset: int64(buf.Len())})
		}
		blockIdx[bi].lastKey = entry.Key

		binary.Write(&buf, binary.BigEndian, uint32(len(entry.Key)))
		buf.Write(entry.Key)
		binary.Write(&buf, binary.BigEndian, uint32(len(entry.Value)))
		buf.Write(entry.Value)
	}

	// 写数据
	if _, err := file.Write(buf.Bytes()); err != nil {
		return err
	}

	// 写块索引: [LastKeyLen(4B)][LastKey][BlockOffset(8B)] × N
	indexStart, _ := file.Seek(0, io.SeekCurrent)
	for _, b := range blockIdx {
		binary.Write(file, binary.BigEndian, uint32(len(b.lastKey)))
		file.Write(b.lastKey)
		binary.Write(file, binary.BigEndian, b.blockOffset)
	}

	// 写 Footer: BlockCount(4B) + IndexOffset(8B) + Magic(4B)
	binary.Write(file, binary.BigEndian, uint32(len(blockIdx)))
	binary.Write(file, binary.BigEndian, indexStart)
	binary.Write(file, binary.BigEndian, indexFooterMagic)

	// 缓存块索引
	cache := make([]BlockIndexEntry, len(blockIdx))
	for i, b := range blockIdx {
		cache[i] = BlockIndexEntry{LastKey: b.lastKey, BlockOffset: b.blockOffset}
	}
	ss.idxMu.Lock()
	ss.indexCache[fullPath] = cache
	ss.idxMu.Unlock()

	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync SSTable file failed: %v", err)
	}

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

	// 尝试读 Footer，确定数据结束位置（新格式有块索引在末尾）
	dataEnd := ss.readDataEndOffset(file)
	if dataEnd > 0 {
		file.Seek(0, io.SeekStart)
	}

	entries := make([]*istorage.LogEntry, 0)
	for {
		if dataEnd > 0 {
			pos, _ := file.Seek(0, io.SeekCurrent)
			if pos >= dataEnd {
				break
			}
		}

		var keyLen uint32
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read key length: %v", err)
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key: %v", err)
		}

		var valueLen uint32
		if err := binary.Read(file, binary.BigEndian, &valueLen); err != nil {
			return nil, fmt.Errorf("failed to read value length: %v", err)
		}

		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(file, valueBytes); err != nil {
			return nil, fmt.Errorf("failed to read value: %v", err)
		}

		entry := &istorage.LogEntry{
			Key:   keyBytes,
			Value: valueBytes,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// readDataEndOffset 读 Footer 返回数据结束偏移，老格式返回 -1
func (ss *SSTable) readDataEndOffset(f *os.File) int64 {
	info, err := f.Stat()
	if err != nil || info.Size() < indexFooterSize {
		return -1
	}

	if _, err := f.Seek(-indexFooterSize, io.SeekEnd); err != nil {
		return -1
	}
	var blockCount uint32
	var indexOffset int64
	var magic uint32
	binary.Read(f, binary.BigEndian, &blockCount)
	binary.Read(f, binary.BigEndian, &indexOffset)
	binary.Read(f, binary.BigEndian, &magic)

	if magic == indexFooterMagic && blockCount > 0 && indexOffset > 0 {
		return indexOffset
	}
	return -1
}

func (ss *SSTable) ReadFromSSTable(filepath string, key []byte) ([]byte, bool) {
	if idx := ss.getBlockIndex(filepath); idx != nil {
		return ss.searchBlock(filepath, key, idx)
	}
	// 老格式 fallback
	return ss.readFromSSTableFull(filepath, key)
}

// getBlockIndex 从缓存取块索引，miss 时从文件加载
func (ss *SSTable) getBlockIndex(filepath string) []BlockIndexEntry {
	ss.idxMu.RLock()
	idx, ok := ss.indexCache[filepath]
	ss.idxMu.RUnlock()
	if ok {
		return idx
	}
	idx = ss.loadBlockIndexFromFile(filepath)
	if idx == nil {
		return nil
	}
	ss.idxMu.Lock()
	ss.indexCache[filepath] = idx
	ss.idxMu.Unlock()
	return idx
}

// loadBlockIndexFromFile 从文件末尾读取块索引
func (ss *SSTable) loadBlockIndexFromFile(filepath string) []BlockIndexEntry {
	f, err := os.Open(filepath)
	if err != nil {
		return nil
	}
	defer f.Close()

	if _, err := f.Seek(-indexFooterSize, io.SeekEnd); err != nil {
		return nil
	}
	var blockCount uint32
	var indexOffset int64
	var magic uint32
	binary.Read(f, binary.BigEndian, &blockCount)
	binary.Read(f, binary.BigEndian, &indexOffset)
	binary.Read(f, binary.BigEndian, &magic)

	if magic != indexFooterMagic || blockCount == 0 || indexOffset <= 0 {
		return nil
	}

	if _, err := f.Seek(indexOffset, io.SeekStart); err != nil {
		return nil
	}
	idx := make([]BlockIndexEntry, blockCount)
	for i := uint32(0); i < blockCount; i++ {
		var keyLen uint32
		if err := binary.Read(f, binary.BigEndian, &keyLen); err != nil {
			return nil
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(f, key); err != nil {
			return nil
		}
		var offset int64
		if err := binary.Read(f, binary.BigEndian, &offset); err != nil {
			return nil
		}
		idx[i] = BlockIndexEntry{LastKey: key, BlockOffset: offset}
	}
	return idx
}

// searchBlock 二分索引定位块 → 只读目标块内扫描
func (ss *SSTable) searchBlock(filepath string, key []byte, idx []BlockIndexEntry) ([]byte, bool) {
	lo, hi := 0, len(idx)-1
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(key, idx[mid].LastKey) <= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo >= len(idx) {
		return nil, false
	}

	f, err := os.Open(filepath)
	if err != nil {
		return nil, false
	}
	defer f.Close()

	if _, err := f.Seek(idx[lo].BlockOffset, io.SeekStart); err != nil {
		return nil, false
	}

	for j := 0; j < SSTableBlockSize; j++ {
		var kLen uint32
		if err := binary.Read(f, binary.BigEndian, &kLen); err != nil {
			break
		}
		k := make([]byte, kLen)
		if _, err := io.ReadFull(f, k); err != nil {
			break
		}
		var vLen uint32
		if err := binary.Read(f, binary.BigEndian, &vLen); err != nil {
			break
		}
		v := make([]byte, vLen)
		if _, err := io.ReadFull(f, v); err != nil {
			break
		}

		cmp := bytes.Compare(k, key)
		if cmp == 0 {
			return v, true
		}
		if cmp > 0 {
			break
		}
	}
	return nil, false
}

// readFromSSTableFull 老格式文件全量读取（兼容）
func (ss *SSTable) readFromSSTableFull(filepath string, key []byte) ([]byte, bool) {
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

	// 4. 写入新的 SSTable 文件（含块索引）
	filename := fmt.Sprintf("sstable_merged_%d.sst", time.Now().UnixNano())
	dir := config.G.SSTablePath
	fullPath := filepath.Join(dir, filename)

	file, err := os.Create(fullPath)
	if err != nil {
		slog.Error("failed to create merged SSTable", "error", err)
		return nil
	}
	defer file.Close()

	type blk struct {
		lastKey     []byte
		blockOffset int64
	}
	var blockIdx []blk
	var buf bytes.Buffer

	for i, entry := range deduped {
		bi := i / SSTableBlockSize
		if i%SSTableBlockSize == 0 {
			blockIdx = append(blockIdx, blk{blockOffset: int64(buf.Len())})
		}
		blockIdx[bi].lastKey = entry.Key

		binary.Write(&buf, binary.BigEndian, uint32(len(entry.Key)))
		buf.Write(entry.Key)
		binary.Write(&buf, binary.BigEndian, uint32(len(entry.Value)))
		buf.Write(entry.Value)
	}

	if _, err := file.Write(buf.Bytes()); err != nil {
		return nil
	}

	indexStart, _ := file.Seek(0, io.SeekCurrent)
	for _, b := range blockIdx {
		binary.Write(file, binary.BigEndian, uint32(len(b.lastKey)))
		file.Write(b.lastKey)
		binary.Write(file, binary.BigEndian, b.blockOffset)
	}
	binary.Write(file, binary.BigEndian, uint32(len(blockIdx)))
	binary.Write(file, binary.BigEndian, indexStart)
	binary.Write(file, binary.BigEndian, indexFooterMagic)

	cache := make([]BlockIndexEntry, len(blockIdx))
	for i, b := range blockIdx {
		cache[i] = BlockIndexEntry{LastKey: b.lastKey, BlockOffset: b.blockOffset}
	}
	ss.idxMu.Lock()
	ss.indexCache[fullPath] = cache
	ss.idxMu.Unlock()

	if err := file.Sync(); err != nil {
		slog.Error("failed to sync merged SSTable", "error", err)
		return nil
	}

	info, err := file.Stat()
	if err != nil {
		slog.Error("failed to stat merged SSTable", "error", err)
		return nil
	}

	newMeta := &istorage.SSTableMata{
		Level:        targetLevel,
		Filepath:     fullPath,
		MinKey:       deduped[0].Key,
		MaxKey:       deduped[len(deduped)-1].Key,
		Size:         info.Size(),
		MaxKeyLoaded: true,
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
	ss.idxMu.Lock()
	delete(ss.indexCache, meta.Filepath)
	ss.idxMu.Unlock()
}
