package parquet

import (
	"context"

	"github.com/apache/thrift/lib/go/thrift"
)

func (f *File) writeFooter() []byte {

	var fileMetaData FileMetaData
	fileMetaData.Version = int32(1)
	fileMetaData.Schema = f.newSchema()
	fileMetaData.NumRows = f.rows
	fileMetaData.RowGroups = f.newRowGroups()
	fileMetaData.KeyValueMetadata = nil // if empty it doesn't work (for some reason the example prints it empty)
	fileMetaData.CreatedBy = nil

	// serialize
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	footerBuf, _ := ts.Write(context.TODO(), &fileMetaData)

	// print out struct
	// fmt.Println()
	// fmt.Printf("%+v\n", fileMetaData)

	return footerBuf

}

func (f *File) newRowGroups() []*RowGroup {

	var rowGroups []*RowGroup

	var rowGroup RowGroup
	rowGroup.Columns = f.newColumns()
	rowGroup.TotalByteSize = f.totalSize
	rowGroup.NumRows = f.rows
	rowGroup.SortingColumns = nil // if empty it doesn't work (for some reason the example prints it empty)
	rowGroups = append(rowGroups, &rowGroup)
	return rowGroups

}

func (f *File) newColumns() []*ColumnChunk {

	var columnChunks []*ColumnChunk

	for i := 0; i < len(f.columns); i++ {

		column := f.columns[i]

		var columnChunk ColumnChunk
		columnChunk.FilePath = nil
		columnChunk.FileOffset = column.getOffset() // 4 for the first columnChunk
		columnChunk.MetaData = column.newMetaData()
		columnChunks = append(columnChunks, &columnChunk)

	}

	return columnChunks

}

func (column *Column) newMetaData() *ColumnMetaData {

	var columnMetaData ColumnMetaData
	columnMetaData.Type = Type_BYTE_ARRAY
	columnMetaData.Encodings = []Encoding{Encoding_RLE, Encoding_BIT_PACKED, Encoding_PLAIN}
	columnMetaData.PathInSchema = []string{column.getName()}
	columnMetaData.Codec = CompressionCodec_UNCOMPRESSED
	columnMetaData.NumValues = column.getRows()
	columnMetaData.TotalUncompressedSize = column.getTotalCompressedSize()
	columnMetaData.TotalCompressedSize = column.getTotalUncompressedSize()
	columnMetaData.KeyValueMetadata = nil              // if empty it doesn't work (for some reason the example prints it empty)
	columnMetaData.DataPageOffset = column.getOffset() // 4 for the first
	columnMetaData.IndexPageOffset = nil
	columnMetaData.DictionaryPageOffset = nil
	columnMetaData.Statistics = column.newStatistics()
	columnMetaData.EncodingStats = nil // if empty it doesn't work (for some reason the example prints it empty)
	return &columnMetaData

}

func (column *Column) newStatistics() *Statistics {

	var statistics Statistics
	statistics.Max = column.getMax()
	statistics.Min = column.getMin()
	statistics.NullCount = nil
	statistics.DistinctCount = nil
	return &statistics

}

func (f *File) newSchema() []*SchemaElement {

	var schema []*SchemaElement

	// rootElement
	numChildren := int32(len(f.columns))
	rootElement := newSchemaElement("parquet_go_root", &numChildren, nil, nil, nil, nil, nil, true)
	schema = append(schema, rootElement)

	// childElements
	for i := 0; i < len(f.columns); i++ {

		column := f.columns[i]

		t := Type_BYTE_ARRAY
		typeLength := int32(0)
		scale := int32(0)
		precision := int32(0)
		fieldID := int32(0)
		element := newSchemaElement(column.getName(), nil, &t, &typeLength, &scale, &precision, &fieldID, false)
		schema = append(schema, element)

	}

	return schema

}

func newSchemaElement(name string, numChildren *int32, t *Type, typeLength *int32, scale *int32, precision *int32, fieldID *int32, first bool) *SchemaElement {

	var schemaElement SchemaElement

	repetitionType := FieldRepetitionType_REQUIRED

	if first {
		schemaElement.Type = nil
		schemaElement.TypeLength = typeLength
		schemaElement.RepetitionType = &repetitionType
		schemaElement.Name = name
		schemaElement.NumChildren = numChildren // number of struct members in the struct we are basing the schema from
		schemaElement.ConvertedType = nil
		schemaElement.Scale = scale
		schemaElement.Precision = precision
		schemaElement.FieldID = fieldID
		return &schemaElement
	}

	schemaElement.Type = t
	schemaElement.TypeLength = typeLength
	schemaElement.RepetitionType = &repetitionType
	schemaElement.Name = name
	schemaElement.NumChildren = numChildren
	schemaElement.ConvertedType = nil
	schemaElement.Scale = scale
	schemaElement.Precision = precision
	schemaElement.FieldID = fieldID

	return &schemaElement
}
