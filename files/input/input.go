package input

import (
	"fmt"
	"io/ioutil"

	log "github.com/alwaysbespoke/jlog"
	"github.com/alwaysbespoke/parquet-go/config"
	"github.com/alwaysbespoke/parquet-go/files/input/file"
	"github.com/alwaysbespoke/parquet-go/files/output"
	"github.com/alwaysbespoke/parquet-go/utils"
)

type Input struct {
	out *output.Output
}

func New(out *output.Output) *Input {
	return &Input{out}
}

func (in *Input) Process() error {

	if !config.IsBatch() {

		log.Log(log.INFO, "Process single file", nil)

		// process single file
		key := config.GetFileName()
		err := in.processFile(key)
		if err != nil {
			return err
		}

	} else {

		log.Log(log.INFO, "Process batch", nil)

		// process batch
		err := in.processBatch()
		if err != nil {
			return err
		}

	}

	return nil

}

func (in *Input) processBatch() error {

	// list directory
	fileObjs, err := ioutil.ReadDir("./cf")
	if err != nil {
		log.Log(log.ERROR, err.Error(), nil)
		return err
	}
	files_len := len(fileObjs)

	// get start date
	start, err := utils.ConvertToEpoch(config.GetStartDate())
	if err != nil {
		log.Log(log.ERROR, err.Error(), nil)
		return err
	}

	// get end date
	end, err := utils.ConvertToEpoch(config.GetEndDate())
	if err != nil {
		log.Log(log.ERROR, err.Error(), nil)
		return err
	}

	var i int

	for _, fileObj := range fileObjs {

		// get file name
		fileName := fileObj.Name()

		// get file date as epoch
		epoch, err := utils.GetEpoch(fileName)
		if err != nil {
			log.Log(log.ERROR, err.Error(), nil)
			continue
		}

		// process files within date range
		if epoch >= start && epoch <= end {

			i++
			utils.PrintRow(i, files_len)
			err := in.processFile(fileName)
			if err != nil {
				continue
			}

		}

	}

	return nil

}

func (in *Input) processFile(key string) error {

	f := file.New(key, in.out)
	err := f.Process()
	if err != nil {
		return err
	}
	fmt.Println(f.Key)

	return nil

}
