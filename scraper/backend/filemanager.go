package backend

import (
	"encoding/csv"
	"io"
)

type CSVReader struct {
	*csv.Reader
}

func NewCSVReader(r io.Reader) *CSVReader {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1
	return &CSVReader{reader}
}

func (r *CSVReader) ReadAll() ([][]string, error) {
	return r.Reader.ReadAll()
}
