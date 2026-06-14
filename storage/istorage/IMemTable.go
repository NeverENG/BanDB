package istorage

type IMemTable interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	// ScanRange 在 [start,end] 闭区间升序遍历最新可见键值，跳过墓碑；
	// fn 返回 false 提前停止。start/end 为空表示该侧不限。
	ScanRange(start, end []byte, fn func(key, value []byte) bool)
	Size() int
	StartFlush()
	// FlushToSSTable 将 entries 写入临时表并立即 Flush 到 SSTable
	// 不经过 active 表，不阻塞正常读写
	FlushToSSTable(entries []LogEntry) error
}
