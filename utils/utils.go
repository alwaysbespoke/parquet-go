package utils

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alwaysbespoke/parquet-go/config"
)

func ConvertToEpoch(date string) (int64, error) {
	iso := "2006-01-02"
	t, err := time.Parse(iso, date)
	if err != nil {
		return 0, err
	}
	return t.Unix(), nil
}

func GetEpoch(fileName string) (int64, error) {

	// general format validation
	dotSplit := strings.Split(fileName, ".")
	if len(dotSplit) != 4 {
		return 0, errors.New("Invalid file name format")
	}

	// validate compression
	if dotSplit[3] != "gz" {
		return 0, errors.New("Invalide compression type, expected .gz")
	}

	// validate date
	dateArr := strings.Split(dotSplit[1], "-")
	if len(dateArr) != 4 {
		return 0, errors.New("Invalid file name format")
	}

	// write date
	year := dateArr[0]
	month := dateArr[1]
	day := dateArr[2]

	var buffer strings.Builder
	buffer.WriteString(year)
	buffer.WriteString("-")
	buffer.WriteString(month)
	buffer.WriteString("-")
	buffer.WriteString(day)

	epoch, err := ConvertToEpoch(buffer.String())
	if err != nil {
		return 0, err
	}

	return epoch, nil

}

func PrintRow(i int, files_len int) {

	var buffer strings.Builder
	buffer.WriteString(strconv.Itoa(i))
	buffer.WriteString(" / ")
	buffer.WriteString(strconv.Itoa(files_len))

	fmt.Println(buffer.String())

}

func Timer(header string, f func() error) error {

	start := time.Now().UnixNano()
	err := f()
	if err != nil {
		return err
	}

	if !config.IsBenchmark() {
		return nil
	}
	duration := time.Now().UnixNano() - start
	fmt.Println(header + ":")
	fmt.Println(duration, " nanoseconds")
	fmt.Println(duration/1000, " microseconds")
	fmt.Println(duration/1000/1000, " milliseconds")
	fmt.Println()

	return nil

}
