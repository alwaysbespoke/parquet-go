package files

import (
	"github.com/alwaysbespoke/parquet-go/files/input"
	"github.com/alwaysbespoke/parquet-go/files/output"
)

type Files struct {
}

func New() *Files {
	return &Files{}
}

func (fs *Files) Process() error {

	out := output.New()

	in := input.New(out)
	in.Process()

	out.Flush()

	return nil

}
