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
	lastSize := int64(4)
	for i := 0; i < len(f.columns); i++ {
		column := f.columns[i]
		column.updateOffset(lastSize)
		page := f.writePage(i)
		lastSize += column.getTotalCompressedSize()
		buffer.Write(page)
	}
	f.totalSize = lastSize - 4

	// write footer
	footer := f.writeFooter()
	buffer.Write(footer)

	// write footer size
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footer)))
	buffer.Write(footerSizeBuf)

	// write version
	buffer.WriteString("PAR1")

	return buffer.Bytes()

}
