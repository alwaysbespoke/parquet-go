package file

import (
	"encoding/binary"
)

type Column struct {
	name                  string
	data                  []byte
	min                   string
	max                   string
	rows                  int32
	totalCompressedSize   int64
	totalUncompressedSize int64
	offset                int64
}

func NewColumn(name string) *Column {
	return &Column{name, []byte{}, "", "", 0, 0, 0, 0}
}

func (column *Column) addData(value string, isFirstRow bool) {
	//fmt.Println(value)
	column.updateMinMax(value, isFirstRow)
	column.data = append(column.data, encodeValue(value)...)
	column.rows++
}

func (column *Column) updateMinMax(value string, isFirstRow bool) {
	if isFirstRow {
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

func (column *Column) getName() string {
	return column.name
}

func (column *Column) getMax() []byte {
	return encodeValue(column.max)
}

func (column *Column) getMin() []byte {
	return encodeValue(column.min)
}

func (column *Column) getTotalCompressedSize() int64 {
	return column.totalCompressedSize
}

func (column *Column) getTotalUncompressedSize() int64 {
	return column.totalUncompressedSize
}

func (column *Column) updateOffset(offset int64) {
	column.offset += offset
}

func (column *Column) getOffset() int64 {
	return column.offset
}

func (column *Column) getRows() int64 {
	return int64(column.rows)
}

func encodeValue(value string) []byte {

	lenBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuffer, uint32(len(value)))

	var buffer []byte
	buffer = append(buffer, lenBuffer...)
	buffer = append(buffer, []byte(value)...)

	return buffer

}
