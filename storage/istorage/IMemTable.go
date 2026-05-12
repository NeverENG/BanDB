package istorage

type IMemTable interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Size() int
	StartFlush()
	// FlushToSSTable 将 entries 写入临时表并立即 Flush 到 SSTable
	// 不经过 active 表，不阻塞正常读写
	FlushToSSTable(entries []LogEntry) error
}
