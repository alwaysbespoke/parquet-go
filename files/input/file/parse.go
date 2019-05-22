package file

import (
	"errors"
	"strings"

	"github.com/alwaysbespoke/parquet-go/schema"
)

func (f *File) validateHeader() error {

	headerRowIndex := schema.GetInputSchemaHeaderRowIndex()
	headerRow := f.readData[headerRowIndex]
	headers := headerRow[0]
	headersArr := strings.Split(headers, " ")

	schema := schema.GetInputSchema()

	// the first index is not a header
	if len(headersArr)-1 != len(schema) {
		return errors.New("Invalid header")
	}

	// the first index is not a header
	for i := 1; i < len(headersArr); i++ {
		if headersArr[i] != schema[i-1] {
			return errors.New("Invalid header")
		}
	}

	return nil

}

func (f *File) parse() error {

	err := f.validateHeader()
	if err != nil {
		return err
	}

	dataRowIndex := schema.GetInputSchemaDataRowIndex()

	for i := dataRowIndex; i < len(f.readData); i++ {
		row := f.readData[i]
		f.parseRow(row, i)
	}
	return nil
}

func (f *File) parseRow(row []string, rowIndex int) {

	outputSchemaMap := schema.GetOutputSchemaMap()

	var parsedRow []string
	for i := 0; i < len(outputSchemaMap); i++ {
		index := outputSchemaMap[i]
		data := row[index]
		parsedRow = append(parsedRow, data)
	}
	f.route(parsedRow[5], parsedRow)

}
