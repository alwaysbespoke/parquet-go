package file

type Column struct {
	name                  string
	data                  []byte
	min                   string
	max                   string
	rows                  int
	totalCompressedSize   int64
	totalUncompressedSize int64
	offset                int64
}

func (column *Column) addData(value string, rowIndex int) {
	column.data = append(column.data, encodeValue(value)...)
}

func (column *Column) updateMinMax(value string, rowIndex int) {
	if rowIndex == 2 {
		column.max = value
		column.min = value
	} else {
		if value > column.max {
			column.max = value
		}
		if value < column.min {
			column.min = value
		}
	}
}

func (column *Column) GetName() string {
	return column.name
}

func (column *Column) GetMax() []byte {
	return encodeValue(column.max)
}

func (column *Column) GetMin() []byte {
	return encodeValue(column.min)
}

func (column *Column) GetTotalCompressedSize() int64 {
	return column.totalCompressedSize
}

func (column *Column) GetTotalUncompressedSize() int64 {
	return column.totalUncompressedSize
}

func (column *Column) UpdateOffset(offset int64) {
	column.offset += offset
}

func (column *Column) GetOffset() int64 {
	return column.offset
}

func (column *Column) GetRows() int64 {
	return int64(column.rows)
}
