package zstorage

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage/istorage"
)

var _ istorage.IMemTable = &MemTable{}

var (
	maxLevel    = config.G.MaxMemTableLevel
	probability = config.G.MaxMemTableP
)

// SkipList 跳表数据结构，封装跳表的 size、level 和 head 指针
type SkipList struct {
	size  int
	level int
	head  *SkipNode
}

// MemTable 基于跳表的双表内存表实现
// active:  当前写入表，接收所有 Put/Delete 操作
// dirty:   正在刷盘的不可变快照，刷盘完成后置 nil
// 采用双表模式避免 Flush 与写入之间的数据竞争
type MemTable struct {
	active *SkipList
	dirty  *SkipList // 正在刷盘中的不可变表，可能仍包含未刷盘的旧数据（供 Get 回退查询）
	mu     sync.RWMutex

	FlushChan chan bool
	compactCh chan bool
	stopCh    chan struct{}

	wal *WAL
	sst *SSTable
}

// SkipNode 跳表节点
type SkipNode struct {
	Next  []*SkipNode
	Key   []byte
	Value []byte
}

// newSkipList 创建一个新的空跳表
func newSkipList() *SkipList {
	return &SkipList{
		head: newSkipNode(maxLevel, nil, nil),
	}
}

// NewMemTable 创建新的 MemTable
func NewMemTable() *MemTable {
	mt := &MemTable{
		active:    newSkipList(),
		FlushChan: make(chan bool, 1),
		compactCh: make(chan bool, 1),
		stopCh:    make(chan struct{}),
		wal:       NewWAL(),
		sst:       NewSSTable(),
	}
	go mt.FlushWorker()
	go mt.ListenCompactCh()

	go mt.sst.LoadSSTableMetaList()
	mt.recoverFromWAL()
	return mt
}

// newSkipNode 创建新的跳表节点
func newSkipNode(level int, key []byte, value []byte) *SkipNode {
	return &SkipNode{
		Next:  make([]*SkipNode, level),
		Key:   key,
		Value: value,
	}
}

// randomLevel 生成随机层级
func randomLevel() int {
	level := 1
	for rand.Float64() < probability && level < maxLevel {
		level++
	}
	return level
}

// Size 返回 active 表中的元素个数
func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.active == nil {
		return 0
	}
	return m.active.size
}

// Get 获取指定 key 的值
// 查找顺序：active → dirty → SSTable
func (m *MemTable) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	active := m.active
	dirty := m.dirty
	m.mu.RUnlock()

	if active == nil || active.head == nil {
		return nil, errors.New("NO DATA IN MEM")
	}

	// 先在 active 中查找（最新数据）
	if val, found := active.search(key); found {
		fmt.Printf("[MEMTABLE] Get found in active: key=%s, value=%s\n", string(key), string(val))
		return val, nil
	}

	// 再在 dirty 中查找（正在刷盘的旧表，可能还有尚未刷到磁盘的数据）
	if dirty != nil && dirty.head != nil {
		if val, found := dirty.search(key); found {
			fmt.Printf("[MEMTABLE] Get found in dirty: key=%s, value=%s\n", string(key), string(val))
			return val, nil
		}
	}

	// 最后在 SSTable 中查找
	if val, found := m.getFromSSTables(key); found {
		fmt.Printf("[MEMTABLE] Get found in SSTable: key=%s\n", string(key))
		return val, nil
	}

	fmt.Printf("[MEMTABLE] Get not found: key=%s\n", string(key))
	return nil, errors.New("Key not found")
}

// search 在跳表中查找指定 key，返回值和是否找到
func (sl *SkipList) search(key []byte) ([]byte, bool) {
	p := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for p.Next[i] != nil && bytes.Compare(p.Next[i].Key, key) < 0 {
			p = p.Next[i]
		}
	}
	p = p.Next[0]
	if p != nil && bytes.Compare(p.Key, key) == 0 {
		return p.Value, true
	}
	return nil, false
}

// Put 插入或更新键值对，始终操作 active 表
func (m *MemTable) Put(key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active == nil || m.active.head == nil {
		return errors.New("NO DATA IN MEMTABLE")
	}

	err := m.wal.Write(istorage.LogEntry{Key: key, Value: value})
	if err != nil {
		fmt.Println("Error writing to WAL:", err)
		return err
	}

	m.active.insert(key, value)

	// 检查 active 表大小是否超过阈值，触发刷盘
	if m.active.size > config.G.MaxMemTableSize {
		m.StartFlush()
	}

	return nil
}

// insert 在跳表中插入键值对（无锁版本，由调用者保证线程安全）
func (sl *SkipList) insert(key []byte, value []byte) {
	update := make([]*SkipNode, maxLevel)
	p := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for p.Next[i] != nil && bytes.Compare(p.Next[i].Key, key) < 0 {
			p = p.Next[i]
		}
		update[i] = p
	}

	// 检查 key 是否已存在
	p = p.Next[0]
	if p != nil && bytes.Compare(p.Key, key) == 0 {
		// key 已存在，更新值
		p.Value = value
		return
	}

	// 生成新节点的随机层级
	newLevel := randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	// 创建新节点并插入每一层
	newNode := newSkipNode(newLevel, key, value)
	for i := 0; i < newLevel; i++ {
		newNode.Next[i] = update[i].Next[i]
		update[i].Next[i] = newNode
	}

	sl.size++
	fmt.Printf("[MEMTABLE] Put success: key=%s, value=%s, size=%d\n", string(key), string(value), sl.size)
}

// Delete 删除指定 key 的节点，始终操作 active 表
func (m *MemTable) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active == nil || m.active.head == nil {
		return errors.New("NO DATA IN MEMTABLE")
	}

	if !m.active.delete(key) {
		fmt.Println("the key does not exist")
		return errors.New("KEY NOT FOUND")
	}
	return nil
}

// delete 从跳表中删除节点，返回是否成功删除
func (sl *SkipList) delete(key []byte) bool {
	update := make([]*SkipNode, maxLevel)
	p := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for p.Next[i] != nil && bytes.Compare(p.Next[i].Key, key) < 0 {
			p = p.Next[i]
		}
		update[i] = p
	}

	p = p.Next[0]
	if p == nil || bytes.Compare(p.Key, key) != 0 {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].Next[i] != p {
			break
		}
		update[i].Next[i] = p.Next[i]
	}

	for sl.level > 0 && sl.head.Next[sl.level-1] == nil {
		sl.level--
	}

	sl.size--
	return true
}

func (m *MemTable) recoverFromWAL() {
	entries, err := m.wal.Read()
	if err != nil {
		fmt.Printf("[WARN] Failed to read WAL: %v\n", err)
		return
	}

	if len(entries) == 0 {
		fmt.Println("[INFO] No WAL entries to recover")
		return
	}

	fmt.Printf("[INFO] Recovering %d entries from WAL...\n", len(entries))

	for _, entry := range entries {
		// 直接插入 active 表，不写入 WAL（避免重复写入）
		m.active.insert(entry.Key, entry.Value)
	}

	fmt.Printf("[INFO] WAL recovery completed, memtable size: %d\n", m.active.size)
}

func (m *MemTable) Sync() error {
	return m.wal.Sync()
}

func (m *MemTable) Clear() error {
	return m.wal.Clear()
}

func (m *MemTable) Close() error {
	close(m.stopCh)
	return m.wal.Close()
}

func (m *MemTable) StartFlush() {
	select {
	case m.FlushChan <- true:
	default:
	}
}

// Flush 将 dirty 表数据刷入 SSTable
// 流程：
//  1. 持锁交换 active → dirty（active 变为 dirty 的不可变快照）
//  2. 创建新的空 active 表用于接受后续写入
//  3. 释放锁，在锁外将 dirty 数据写入 SSTable
//  4. 刷盘完成后清除 WAL，将 dirty 置 nil
func (m *MemTable) Flush() {
	// 步骤 1-2: 持锁进行交换（快速操作）
	m.mu.Lock()
	if m.active.size == 0 {
		m.mu.Unlock()
		return
	}

	m.dirty = m.active // 旧表变为 dirty 快照
	m.active = newSkipList()
	dirty := m.dirty // 保存本地引用
	m.mu.Unlock()

	fmt.Printf("Flushing MemTable with %d entries...\n", dirty.size)

	// 步骤 3: 在锁外将 dirty 数据写入 SSTable（慢速 I/O，不阻塞读写）
	allEntries := collectAllEntry(dirty)
	err := m.sst.writeToSSTable(allEntries)
	if err != nil {
		fmt.Printf("Flush error: %v\n", err)
		return
	}

	// 步骤 4: 刷盘成功后清除 WAL，将 dirty 置 nil
	err = m.Clear()
	if err != nil {
		fmt.Printf("WAL clear error: %v\n", err)
	}

	m.mu.Lock()
	m.dirty = nil
	m.mu.Unlock()

	fmt.Println("Flush completed successfully")
}

func (m *MemTable) FlushWorker() {
	for {
		select {
		case <-m.FlushChan:
			fmt.Println("Flush")
			m.Flush()
		case <-m.stopCh:
			fmt.Println("[INFO]STOP FLUSHWORKER")
			return
		}
	}
}

// collectAllEntry 收集跳表中的所有 entry（从第 0 层按序遍历）
func collectAllEntry(sl *SkipList) []istorage.LogEntry {
	logEntries := make([]istorage.LogEntry, 0, sl.size)

	p := sl.head.Next[0]
	for p != nil {
		logEntries = append(logEntries, istorage.LogEntry{
			Key:   p.Key,
			Value: p.Value,
		})
		p = p.Next[0]
	}
	return logEntries
}

func (m *MemTable) getFromSSTables(key []byte) ([]byte, bool) {
	for _, meta := range m.sst.GetAllMata() {
		// 首次访问时自动加载 MaxKey
		meta.EnsureMeta()

		// 用 MinKey 和 MaxKey 过滤
		if bytes.Compare(key, meta.MinKey) < 0 ||
			bytes.Compare(key, meta.MaxKey) > 0 {
			continue
		}

		// 在文件中查找
		if value, found := m.sst.ReadFromSSTable(meta.Filepath, key); found {
			return value, true
		}
	}
	return nil, false
}

func (m *MemTable) WriteSSTable() error {
	m.mu.RLock()
	active := m.active
	m.mu.RUnlock()

	err := m.sst.writeToSSTable(collectAllEntry(active))
	select {
	case m.compactCh <- true:
	default:
	}
	return err
}

func (m *MemTable) ListenCompactCh() {
	for {
		select {
		case <-m.compactCh:
			m.CompactSSTable(0)
		case <-m.stopCh:
			fmt.Println("[INFO]STOP COMPACTLISTENER")
			return
		}
	}
}

func (m *MemTable) CompactSSTable(startLevel int) {
	maxLevel := 10

	for level := startLevel; level < maxLevel; level++ {
		files := m.sst.GetLevelFiles(level)

		if len(files) < config.G.MaxCompactionSize {
			continue
		}

		fmt.Printf("[COMPACTION] Level %d has %d files, merging...\n", level, len(files))

		newMeta := m.sst.MergeSSTable(files, level+1)
		if newMeta == nil {
			fmt.Printf("[ERROR] Failed to merge level %d\n", level)
			continue
		}

		for _, meta := range files {
			m.sst.DeleteSSTable(meta)
			m.sst.RemoveMata(meta)
		}

		fmt.Printf("[COMPACTION] Level %d compaction completed\n", level)
	}
}
