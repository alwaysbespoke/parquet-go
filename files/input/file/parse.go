package file

func (f *File) parse() error {
	// map headers
	for i := 2; i < len(f.readData); i++ {
		row := f.readData[i]
		f.parseRow(row, i)
	}
	return nil
}

func (f *File) parseRow(row []string, rowIndex int) {
	for i := 0; i < len(row); i++ {
		// parse out fields
	}
	f.route(row[0], row)
}
