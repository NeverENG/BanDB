package zstorage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage/istorage"
)

type SSTable struct {
	mata []*istorage.SSTableMata
	mu   sync.RWMutex
}

func NewSSTable() *SSTable {
	return &SSTable{
		mata: make([]*istorage.SSTableMata, 0),
	}
}

// 查询文件 元数据
func (ss *SSTable) LoadSSTableMetaList() {
	fmt.Println("[INFO] Loading SSTable index from disk...")

	dir := config.G.SSTablePath

	// 确保目录存在
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Printf("[ERROR] Cannot create SSTable directory: %v\n", err)
		return
	}

	// 读取目录
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("[WARN] Cannot read SSTable directory: %v\n", err)
		return
	}

	metas := make([]*istorage.SSTableMata, 0)
	count := 0

	for _, entry := range entries {
		// 只处理.sst 文件
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sst" {
			continue
		}

		fullPath := filepath.Join(dir, entry.Name())

		// 打开文件
		file, err := os.Open(fullPath)
		if err != nil {
			fmt.Printf("[WARN] Failed to open SSTable %s: %v\n", entry.Name(), err)
			continue
		}

		// 只读取第一个entry的key（MinKey）
		var minKey []byte
		var keyLen uint32

		// 读取 Key 长度
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			fmt.Printf("[WARN] Failed to read key length from %s: %v\n", entry.Name(), err)
			file.Close()
			continue
		}

		// 读取 Key 内容
		keyBytes := make([]byte, keyLen)
		if _, err := file.Read(keyBytes); err != nil {
			fmt.Printf("[WARN] Failed to read key from %s: %v\n", entry.Name(), err)
			file.Close()
			continue
		}

		minKey = keyBytes

		// 关闭文件（不读取后续内容
		file.Close()

		// 获取文件大小
		info, err := os.Stat(fullPath)
		if err != nil {
			fmt.Printf("[WARN] Failed to stat %s: %v\n", entry.Name(), err)
			continue
		}

		// 创建元数据对象（MaxKey设为nil，延迟加载）
		meta := &istorage.SSTableMata{
			Level:        0, // 默认 Level 0，后续可通过 Compaction 调整
			Filepath:     fullPath,
			MinKey:       minKey,
			MaxKey:       nil, // ⚠️ 延迟加载
			Size:         info.Size(),
			MaxKeyLoaded: false,
		}

		metas = append(metas, meta)
		count++
	}

	// 按文件名排序（时间戳顺序，旧的在前）
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].Filepath < metas[j].Filepath
	})

	// 线程安全地更新元数据列表
	ss.mu.Lock()
	ss.mata = metas
	ss.mu.Unlock()

	for _, meta := range metas {
		go meta.EnsureMeta()
	}

	fmt.Printf("[INFO] Loaded %d SSTable files from %s (fast mode, MaxKey lazy load)\n", count, dir)
}

// 实现持久化跳表数据持久化到磁盘
func (ss *SSTable) writeToSSTable(entries []istorage.LogEntry) error {
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

	// 4. 写入数据
	// 格式: [KeyLen(4B)][Key][ValueLen(4B)][Value]
	for _, entry := range entries {
		keyLen := uint32(len(entry.Key))
		valueLen := uint32(len(entry.Value))

		// 写入 Key 长度和内容
		if err := binary.Write(file, binary.BigEndian, keyLen); err != nil {
			return err
		}
		if _, err := file.Write(entry.Key); err != nil {
			return err
		}

		// 写入 Value 长度和内容
		if err := binary.Write(file, binary.BigEndian, valueLen); err != nil {
			return err
		}
		if _, err := file.Write(entry.Value); err != nil {
			return err
		}
	}

	// 5. 确保数据刷入磁盘
	if err := file.Sync(); err != nil {
		return err
	}

	info, _ := file.Stat()
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
		if _, err := file.Read(keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key: %v", err)
		}

		// 读取 Value 长度
		var valueLen uint32
		if err := binary.Read(file, binary.BigEndian, &valueLen); err != nil {
			return nil, fmt.Errorf("failed to read value length: %v", err)
		}

		// 读取 Value 内容
		valueBytes := make([]byte, valueLen)
		if _, err := file.Read(valueBytes); err != nil {
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

	fmt.Printf("[COMPACTION] Merging %d files to Level %d...\n", len(files), targetLevel)

	// 1. 读取所有文件的数据
	allEntries := make([]*istorage.LogEntry, 0)
	for _, meta := range files {
		entries, err := ss.ReadAllFromSSTable(meta.Filepath)
		if err != nil {
			fmt.Printf("[ERROR] Failed to read %s: %v\n", meta.Filepath, err)
			continue
		}
		allEntries = append(allEntries, entries...)
		fmt.Printf("[COMPACTION] Read %d entries from %s\n", len(entries), meta.Filepath)
	}

	if len(allEntries) == 0 {
		fmt.Println("[WARN] No entries to merge")
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
		fmt.Printf("[ERROR] Failed to create merged SSTable: %v\n", err)
		return nil
	}
	defer file.Close()

	for _, entry := range deduped {
		keyLen := uint32(len(entry.Key))
		valueLen := uint32(len(entry.Value))

		binary.Write(file, binary.BigEndian, keyLen)
		file.Write(entry.Key)
		binary.Write(file, binary.BigEndian, valueLen)
		file.Write(entry.Value)
	}

	if err := file.Sync(); err != nil {
		fmt.Printf("[ERROR] Failed to sync SSTable: %v\n", err)
		return nil
	}

	// 5. 获取文件信息
	info, _ := file.Stat()

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
	fmt.Printf("[SSTABLE] Merged to Level %d: %s (keys: %d, size: %d bytes)\n",
		targetLevel, filename, len(deduped), info.Size())

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
		fmt.Printf("[WARN] Failed to delete SSTable %s: %v\n", meta.Filepath, err)
	} else {
		fmt.Printf("[SSTABLE] Deleted: %s\n", meta.Filepath)
	}
}
