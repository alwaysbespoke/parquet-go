package file

import (
	"compress/gzip"
	"fmt"

	"github.com/alwaysbespoke/parquet-go/files/output"
	"github.com/alwaysbespoke/parquet-go/utils"
)

type File struct {
	key         string
	out         *output.Output
	loadData    []byte
	extractData *gzip.Reader
	readData    [][]string
}

func New(key string, out *output.Output) *File {
	return &File{key, out, []byte{}, &gzip.Reader{}, [][]string{}}
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

	fmt.Println("Rows: ", len(f.readData))

	return nil

}

func (f *File) route(key string, row []string) {

	f.out.Process(key, row)

}
