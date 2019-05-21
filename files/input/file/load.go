package file

import (
	"io/ioutil"

	log "github.com/alwaysbespoke/jlog"
)

func (f *File) load() error {
	var err error
	f.loadData, err = ioutil.ReadFile("cf/" + f.key)
	if err != nil {
		log.Log(log.ERROR, err.Error(), log.Fields{
			"file-name": f.key,
		})
		return err
	}
	return err
}
