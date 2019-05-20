package file

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
)

func (f *File) write() error {

	var buffer bytes.Buffer

	// write version
	buffer.WriteString("PAR1")

	// write pages
	lastSize := int64(4)
	for i := 0; i < len(f.columns); i++ {
		column := f.columns[i]
		column.UpdateOffset(lastSize)
		page := f.WritePage(i)
		lastSize += column.GetTotalCompressedSize()
		buffer.Write(page)
	}
	f.totalSize = lastSize - 4

	// write footer
	footer := f.WriteFooter()
	buffer.Write(footer)

	//fmt.Println(footer)

	// write footer size
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footer)))
	buffer.Write(footerSizeBuf)

	// write version
	buffer.WriteString("PAR1")

	// write file
	err := ioutil.WriteFile("datalake/"+f.Key+".parquet", buffer.Bytes(), 0644)
	return err

}
