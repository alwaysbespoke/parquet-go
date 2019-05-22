package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	log "github.com/alwaysbespoke/jlog"
)

const (
	INPUT_SCHEMA  = "./schema/input.json"
	OUTPUT_SCHEMA = "./schema/output.json"
)

type InputSchema struct {
	HeaderRowIndex int      `json:"header-row-index"`
	DataRowIndex   int      `json:"data-row-index"`
	SchemaOriginal []string `json:"schema-original"`
}

type OutputSchema struct {
	SchemaOriginal []string `json:"schema-original"`
	SchemaDerived  []string `json:"schema-derived"`
}

var inputSchema *InputSchema
var outputSchema *OutputSchema
var outputSchemaMap []int

func Process() error {

	var err error

	err = processInputSchema()
	if err != nil {
		return err
	}

	err = processOutputSchema()
	if err != nil {
		return err
	}

	err = mapSchemaOriginal()
	if err != nil {
		return err
	}

	fmt.Println(outputSchemaMap)

	return nil
}

func mapSchemaOriginal() error {

	if len(outputSchema.SchemaOriginal) > len(inputSchema.SchemaOriginal) {
		return errors.New("Invalid output schema: outputSchema.SchemaOriginal length > inputSchema.SchemaOriginal length")
	}

	m := make(map[string]int)
	for i := 0; i < len(inputSchema.SchemaOriginal); i++ {
		column := inputSchema.SchemaOriginal[i]
		m[column] = i
	}
	for i := 0; i < len(outputSchema.SchemaOriginal); i++ {
		column := outputSchema.SchemaOriginal[i]
		index, ok := m[column]
		if ok {
			outputSchemaMap = append(outputSchemaMap, index)
		}
	}

	return nil

}

func processInputSchema() error {

	var err error
	var file []byte

	file, err = load(INPUT_SCHEMA)
	if err != nil {
		return err
	}

	err = marshalInputSchema(file)
	if err != nil {
		return err
	}

	return nil

}

func processOutputSchema() error {

	var err error
	var file []byte

	file, err = load(OUTPUT_SCHEMA)
	if err != nil {
		return err
	}

	err = marshalOutputSchema(file)
	if err != nil {
		return err
	}

	fmt.Println()

	return nil

}

func load(filePath string) ([]byte, error) {

	log.Log(log.INFO, "Loading schema", log.Fields{
		"file": filePath,
	})

	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Log(log.ERROR, err.Error(), nil)
		return nil, err
	}
	fmt.Println(string(file))
	return file, nil

}

func marshalInputSchema(file []byte) error {

	var schema InputSchema
	err := json.Unmarshal(file, &schema)
	if err != nil {
		log.Log(log.ERROR, err.Error(), nil)
		return err
	}
	inputSchema = &schema
	return nil

}

func marshalOutputSchema(file []byte) error {

	var schema OutputSchema
	err := json.Unmarshal(file, &schema)
	if err != nil {
		log.Log(log.ERROR, err.Error(), nil)
		return err
	}
	outputSchema = &schema
	return nil

}

func GetInputSchema() []string {
	return inputSchema.SchemaOriginal
}

func GetInputSchemaHeaderRowIndex() int {
	return inputSchema.HeaderRowIndex
}

func GetInputSchemaDataRowIndex() int {
	return inputSchema.DataRowIndex
}

func GetOutputSchema() []string {
	return outputSchema.SchemaOriginal
}

func GetOutputSchemaMap() []int {
	return outputSchemaMap
}
