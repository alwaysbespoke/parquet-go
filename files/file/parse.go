package file

import (
	"encoding/binary"
	"strings"
)

func (f *File) parse() error {

	// parse column names
	columnNamesRow := f.readData[1]
	f.columnNames = strings.Split(columnNamesRow[0], " ")
	f.columnNames = f.columnNames[1:]

	// create empty columns
	for i := 0; i < len(f.columnNames); i++ {
		var column Column
		column.name = f.columnNames[i]
		f.columns = append(f.columns, &column)
	}

	// parse rows
	for i := 2; i < len(f.readData); i++ {
		row := f.readData[i]
		f.parseRow(row, i)
	}

	return nil

}

func (f *File) parseRow(row []string, rowIndex int) {

	for i := 0; i < len(row); i++ {
		var column = f.columns[i]
		value := row[i]
		column.addData(value, rowIndex)
		column.updateMinMax(value, rowIndex)
		column.rows++
	}
	f.rows++

}

func encodeValue(value string) []byte {

	lenBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuffer, uint32(len(value)))

	var buffer []byte
	buffer = append(buffer, lenBuffer...)
	buffer = append(buffer, []byte(value)...)

	return buffer

}
