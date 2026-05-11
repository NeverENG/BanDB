package zstorage

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"

	"github.com/NeverENG/BanDB/config"
	"github.com/NeverENG/BanDB/storage/istorage"
)

var _ istorage.IMemTable = &MemTable{}

var (
	MAXL = config.G.MaxMemTableLevel
	P    = config.G.MaxMemTableP
)

// MemTable 基于跳表的内存表实现
type MemTable struct {
	size  int
	level int
	head  *SkipNode

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

// NewMemTable 创建新的 MemTable
func NewMemTable() *MemTable {
	mt := &MemTable{
		size:      0,
		level:     0,
		FlushChan: make(chan bool, 1),
		compactCh: make(chan bool, 1),
		stopCh:    make(chan struct{}),
		head:      newSkipNode(MAXL, nil, nil),
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
	for rand.Float64() < P && level < MAXL {
		level++
	}
	return level
}

// Size 返回跳表中元素个数
func (m *MemTable) Size() int {
	return m.size
}

// Get 获取指定 key 的值，如果不存在返回零值和 false
func (m *MemTable) Get(key []byte) ([]byte, error) {

	if m.head == nil {
		return nil, errors.New("NO DATA IN MEM")
	}

	p := m.head
	// 从最高层开始查找
	for i := m.level - 1; i >= 0; i-- {
		for p.Next[i] != nil && bytes.Compare(p.Next[i].Key, key) < 0 {
			p = p.Next[i]
		}
	}

	// 检查下一层的节点是否匹配
	p = p.Next[0]
	if p != nil && bytes.Compare(p.Key, key) == 0 {
		fmt.Printf("[MEMTABLE] Get found: key=%s, value=%s\n", string(key), string(p.Value))
		return p.Value, nil
	}
	fmt.Printf("[MEMTABLE] Get not found: key=%s\n", string(key))
	return nil, errors.New("Key not found")
}

// Put 插入或更新键值对
func (m *MemTable) Put(key []byte, value []byte) error {

	if m.head == nil {
		return errors.New("NO DATA IN MEMTABLE")
	}

	err := m.wal.Write(istorage.LogEntry{Key: key, Value: value})

	if err != nil {
		fmt.Println("Error writing to WAL:", err)
		return err
	}

	// update 数组记录每一层需要更新的节点
	update := make([]*SkipNode, MAXL)
	p := m.head

	// 从最高层开始查找插入位置
	for i := m.level - 1; i >= 0; i-- {
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
		return nil
	}

	// 生成新节点的随机层级
	newLevel := randomLevel()
	if newLevel > m.level {
		// 如果新层级大于当前最大层级，更新 update 数组
		for i := m.level; i < newLevel; i++ {
			update[i] = m.head
		}
		m.level = newLevel
	}

	// 创建新节点
	newNode := newSkipNode(newLevel, key, value)

	// 在每一层插入新节点
	for i := 0; i < newLevel; i++ {
		newNode.Next[i] = update[i].Next[i]
		update[i].Next[i] = newNode
	}

	m.size++
	fmt.Printf("[MEMTABLE] Put success: key=%s, value=%s, size=%d\n", string(key), string(value), m.size)
	return nil
}

// Delete 删除指定 key 的节点
func (m *MemTable) Delete(key []byte) error {

	if m.head == nil {
		return errors.New("NO DATA IN MEMTABLE")
	}

	// update 数组记录每一层需要更新的节点
	update := make([]*SkipNode, MAXL)
	p := m.head

	// 从最高层开始查找要删除的节点
	for i := m.level - 1; i >= 0; i-- {
		for p.Next[i] != nil && bytes.Compare(p.Next[i].Key, key) < 0 {
			p = p.Next[i]
		}
		update[i] = p
	}

	// 检查目标节点是否存在
	p = p.Next[0]
	if p == nil || bytes.Compare(p.Key, key) != 0 {
		// key 不存在
		fmt.Println("the key does not exist")
		return errors.New("KEY NOT FOUND")
	}

	// 在每一层删除节�?
	for i := 0; i < m.level; i++ {
		if update[i].Next[i] != p {
			break
		}
		update[i].Next[i] = p.Next[i]
	}

	// 更新最大层级
	for m.level > 0 && m.head.Next[m.level-1] == nil {
		m.level--
	}

	m.size--
	return nil
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
		// 直接插入跳表，不�?WAL（避免重复写入）
		m.insertWithoutWAL(entry.Key, entry.Value)
	}

	fmt.Printf("[INFO] WAL recovery completed, memtable size: %d\n", m.size)
}

// insertWithoutWAL 不写 WAL 的情况下插入数据（用于恢复）
func (m *MemTable) insertWithoutWAL(key []byte, value []byte) {
	if m.head == nil {
		return
	}

	update := make([]*SkipNode, MAXL)
	p := m.head

	for i := m.level - 1; i >= 0; i-- {
		for p.Next[i] != nil && bytes.Compare(p.Next[i].Key, key) < 0 {
			p = p.Next[i]
		}
		update[i] = p
	}

	p = p.Next[0]
	if p != nil && bytes.Compare(p.Key, key) == 0 {
		p.Value = value
		return
	}

	newLevel := randomLevel()
	if newLevel > m.level {
		for i := m.level; i < newLevel; i++ {
			update[i] = m.head
		}
		m.level = newLevel
	}

	newNode := newSkipNode(newLevel, key, value)

	for i := 0; i < newLevel; i++ {
		newNode.Next[i] = update[i].Next[i]
		update[i].Next[i] = newNode
	}

	m.size++
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

func (m *MemTable) Flush() {
	fmt.Printf("Flushing MemTable with %d entries...\n", m.size)

	allEntries := m.collectAllEntry()

	err := m.sst.WriteToSSTable(allEntries)
	if err != nil {
		fmt.Printf("Flush error: %v\n", err)
		return
	}

	m.resetMemTable()

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

func (m *MemTable) collectAllEntry() []istorage.LogEntry {
	logEntries := make([]istorage.LogEntry, 0, m.size)

	p := m.head.Next[0]
	for p != nil {
		logEntries = append(logEntries, istorage.LogEntry{
			Key:   p.Key,
			Value: p.Value,
		})
		p = p.Next[0]
	}
	return logEntries
}

// resetMemTable 重置内存表
func (m *MemTable) resetMemTable() error {
	m.head = newSkipNode(MAXL, nil, nil)
	m.size = 0
	m.level = 0

	err := m.Clear()
	return err
}

func (m *MemTable) getFromSSTables(key []byte) ([]byte, bool) {
	for _, meta := range m.sst.GetAllMata() {
		// 首次访问时自动加载MaxKey

		meta.EnsureMeta()

		// 现在可以用 MinKey 和 MaxKey 过滤
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
	err := m.sst.WriteToSSTable(m.collectAllEntry())
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
