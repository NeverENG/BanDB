package istorage

type ISSTable interface {
	LoadSSTableMetaList()
	AddMata(meta *SSTableMata)
	RemoveMata(target *SSTableMata)
	GetLevelFiles(level int) []*SSTableMata
	GetAllMata() []*SSTableMata

	WriteToSSTable(entry []LogEntry) error
	ReadFromSSTable(filepath string, key []byte) ([]byte, bool)
	ReadAllFromSSTable(filepath string) ([]*LogEntry, error)
	MergeSSTable(files []*SSTableMata, targetLevel int) *SSTableMata
	DeleteSSTable(meta *SSTableMata)
}
