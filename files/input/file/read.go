package file

import (
	"encoding/csv"

	log "github.com/alwaysbespoke/jlog"
)

const (
	TAB = '\t'
)

func (f *File) read() error {
	reader := csv.NewReader(f.extractData)
	reader.Comma = TAB
	reader.FieldsPerRecord = -1
	var err error
	f.readData, err = reader.ReadAll()
	if err != nil {
		log.Log(log.ERROR, err.Error(), log.Fields{
			"file-name": f.key,
		})
		return err
	}
	return err
}
