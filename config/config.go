package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	log "bitbucket.org/bitcentral/fuel-analytics-logparser-local/jlog"
)

type Config struct {
	Batch     bool              `json:"batch"`
	FileName  string            `json:"fileName"`
	Dates     map[string]string `json:"dates"`
	Benchmark bool              `json:"benchmark"`
}

const (
	FILE = "./config/config.json"
)

var c Config

func Load() error {

	log.Log(log.INFO, "Loading configuration file", log.Fields{
		"file": FILE,
	})

	file, err := ioutil.ReadFile(FILE)
	if err != nil {
		log.Log(log.ERROR, err.Error(), nil)
		return err
	}

	if err := json.Unmarshal(file, &c); err != nil {
		log.Log(log.ERROR, err.Error(), nil)
		return err
	}
	fmt.Println(string(file))

	return err

}

func IsBatch() bool {
	return c.Batch
}

func GetFileName() string {
	return c.FileName
}

func GetStartDate() string {
	return c.Dates["start"]
}

func GetEndDate() string {
	return c.Dates["end"]
}

func IsBenchmark() bool {
	return c.Benchmark
}
