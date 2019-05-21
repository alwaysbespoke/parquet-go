package file

type File struct {
	key       string
	columns   []*Column
	rows      int
	totalSize int64
}

func New(key string) *File {
	columns := make([]*Column, 26)
	return &File{key, columns, 0, 0}
}

func (f *File) Process(row []string) {
	// advanced -> add data to page within columnChunk within rowGroup
	//
	// basic -> add data to column
	for i := 0; i < len(row); i++ {
		data := row[i]
		column := f.columns[i]
		column.addData(data, 0)
	}
	f.rows++
}

func (f *File) Flush(key string) {
	f.write()
}
