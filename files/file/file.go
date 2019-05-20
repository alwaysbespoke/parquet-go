package file

import (
	"compress/gzip"
	"fmt"

	"github.com/alwaysbespoke/parquet-go/utils"
)

type File struct {
	Key         string
	loadData    []byte
	extractData *gzip.Reader
	readData    [][]string
	columns     []*Column
	rows        int
	columnNames []string
	totalSize   int64
}

func New(key string) *File {
	return &File{key, []byte{}, &gzip.Reader{}, [][]string{}, []*Column{}, 0, []string{}, 0}
}

func (f *File) Process() error {

	var err error

	fmt.Println()

	err = utils.Timer("Load", f.load)
	if err != nil {
		return err
	}

	err = utils.Timer("Extract", f.extract)
	if err != nil {
		return err
	}

	err = utils.Timer("Read", f.read)
	if err != nil {
		return err
	}

	err = utils.Timer("Parse", f.parse)
	if err != nil {
		return err
	}

	err = utils.Timer("Write", f.write)
	if err != nil {
		return err
	}

	fmt.Println("Rows: ", len(f.readData))

	return nil

}
