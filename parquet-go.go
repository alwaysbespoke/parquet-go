package main

import (
	log "github.com/alwaysbespoke/jlog"
	"github.com/alwaysbespoke/parquet-go/config"
	"github.com/alwaysbespoke/parquet-go/files"
	"github.com/alwaysbespoke/parquet-go/schema"
)

func main() {

	log.Log(log.INFO, "parquet-go started", nil)

	var err error

	err = configure()
	if err != nil {
		return
	}

	err = process()
	if err != nil {
		return
	}

}

func configure() error {

	var err error

	// process configuration
	err = config.Process()
	if err != nil {
		return err
	}

	// process schema
	err = schema.Process()
	if err != nil {
		return nil
	}

	return nil

}

func process() error {

	fs := files.New()
	fs.Process()

	return nil

}
