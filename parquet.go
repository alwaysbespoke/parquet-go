package main

import (
	log "github.com/alwaysbespoke/jlog"
	"github.com/alwaysbespoke/parquet-go/config"
	"github.com/alwaysbespoke/parquet-go/files"
)

func main() {

	log.Log(log.INFO, "App started", nil)

	err := config.Load()
	if err != nil {
		return
	}

	process()

}

func process() {

	fs := files.New()
	fs.Process()

}
