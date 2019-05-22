package output

import (
	"github.com/alwaysbespoke/parquet-go/files/output/file"
)

type Output struct {
	files map[string]*file.File
}

func New() *Output {
	return &Output{make(map[string]*file.File)}
}

func (out *Output) Process(key string, row []string) {
	f, ok := out.files[key]
	if ok {
		f.Process(row)
		return
	}
	out.files[key] = file.New(key)
	out.files[key].Process(row)
}

func (out *Output) Flush() {

	for key, file := range out.files {
		file.Write(key)
	}

}
