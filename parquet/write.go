package parquet

import (
	"bytes"
	"encoding/binary"
)

func (f *File) write() []byte {

	var buffer bytes.Buffer

	// write version
	buffer.WriteString("PAR1")

	// write pages
	totalSize := int64(4)
	for i := 0; i < len(f.columns); i++ {
		// update offset
		column := f.columns[i]
		column.updateOffset(totalSize)
		// write page
		page := f.writePage(i)
		buffer.Write(page)
		// update totalSize
		totalSize += column.getTotalCompressedSize()
	}
	f.totalSize = totalSize - 4

	// write footer
	footer := f.writeFooter()
	buffer.Write(footer)

	// write footer size
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footer)))
	buffer.Write(footerSizeBuf)

	// write version
	buffer.WriteString("PAR1")

	// free up memory
	f.columns = nil

	return buffer.Bytes()

}
