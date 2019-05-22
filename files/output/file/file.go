package file

import (
	"github.com/alwaysbespoke/parquet-go/schema"
)

type File struct {
	key       string
	columns   []*Column
	rows      int
	totalSize int64
}

func New(key string) *File {

	var columns []*Column
	outputSchema := schema.GetOutputSchema()
	for i := 0; i < len(outputSchema); i++ {
		column := NewColumn(outputSchema[i])
		columns = append(columns, column)
	}
	return &File{key, columns, 0, 0}
}

func (f *File) Process(row []string) {
	// advanced -> add data to page within columnChunk within rowGroup
	//
	// basic -> add data to column
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
