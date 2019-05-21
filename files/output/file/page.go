package file

import (
	"context"

	"github.com/apache/thrift/lib/go/thrift"
)

func (f *File) WritePage(columnIndex int) []byte {

	column := f.columns[columnIndex]

	var repetitionLevelBuf []byte
	var definitionLevelBuf []byte

	// TODO -> encode values
	valuesEncodedBuf := column.data

	var uncompressedDataBuf []byte
	uncompressedDataBuf = append(uncompressedDataBuf, repetitionLevelBuf...)
	uncompressedDataBuf = append(uncompressedDataBuf, definitionLevelBuf...)
	uncompressedDataBuf = append(uncompressedDataBuf, valuesEncodedBuf...)

	// TODO -> compress values
	compressedDataBuf := uncompressedDataBuf

	// create statistics
	var statistics Statistics
	statistics.Max = encodeValue(column.max)
	statistics.Min = encodeValue(column.min)
	statistics.NullCount = nil
	statistics.DistinctCount = nil

	// create dataPageHeader
	var dataPageHeader DataPageHeader
	dataPageHeader.NumValues = column.rows
	dataPageHeader.Encoding = Encoding_PLAIN
	dataPageHeader.DefinitionLevelEncoding = Encoding_RLE
	dataPageHeader.RepetitionLevelEncoding = Encoding_RLE
	dataPageHeader.Statistics = &statistics

	// create pageHeader
	var header PageHeader
	header.Type = PageType_DATA_PAGE
	header.UncompressedPageSize = int32(len(uncompressedDataBuf))
	header.CompressedPageSize = int32(len(compressedDataBuf))
	header.Crc = nil
	header.DataPageHeader = &dataPageHeader

	// serialize
	serializer := thrift.NewTSerializer()
	serializer.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(serializer.Transport)
	pageHeaderBuf, _ := serializer.Write(context.TODO(), &header)

	// append to buffer
	var responseBuffer []byte
	responseBuffer = append(responseBuffer, pageHeaderBuf...)
	responseBuffer = append(responseBuffer, compressedDataBuf...)

	column.totalCompressedSize = int64(len(responseBuffer))
	column.totalUncompressedSize = int64(len(responseBuffer))

	//fmt.Println()
	//fmt.Printf("%+v\n", header)
	//fmt.Println(responseBuffer)

	return responseBuffer

}
