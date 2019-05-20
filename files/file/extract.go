package file

import (
	"bytes"
	"compress/gzip"

	log "github.com/alwaysbespoke/jlog"
)

func (f *File) extract() error {

	var err error
	f.extractData, err = gzip.NewReader(bytes.NewBuffer(f.loadData))
	if err != nil {
		log.Log(log.ERROR, err.Error(), log.Fields{
			"file-name": f.Key,
		})
		return err
	}
	defer f.extractData.Close()

	return err

}
