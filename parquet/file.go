package parquet

type File struct {
	columns   []*Column
	rows      int64
	totalSize int64
}

func New(schema []string) *File {

	var columns []*Column
	for i := 0; i < len(schema); i++ {
		column := newColumn(schema[i])
		columns = append(columns, column)
	}
	return &File{columns, 0, 0}
}

func (f *File) Process(row []string) {

	for i := 0; i < len(row); i++ {

		data := row[i]
		column := f.columns[i]
		var isFirstRow bool
		if len(column.data) == 0 {
			isFirstRow = true
		}
		column.addData(data, isFirstRow)

	}
	f.rows++

}

func (f *File) Flush() []byte {
	return f.write()
}
