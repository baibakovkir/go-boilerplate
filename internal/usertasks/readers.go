package usertasks

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/xuri/excelize/v2"

	"go-boilerplate/internal/config"
)

type rowReader interface {
	Next() ([]string, error)
	Close() error
}
type csvRows struct {
	f       *os.File
	scanner *bufio.Scanner
}

func (c *csvRows) Next() ([]string, error) {
	for c.scanner.Scan() {
		line := c.scanner.Text()
		if line == "" {
			continue
		}
		// Одна CSV-запись занимает физическую строку: это ограничивает память парсера.
		r := csv.NewReader(strings.NewReader(line))
		r.FieldsPerRecord = -1
		return r.Read()
	}
	if e := c.scanner.Err(); e != nil {
		return nil, e
	}
	return nil, io.EOF
}
func (c *csvRows) Close() error { return c.f.Close() }

type xlsxRows struct {
	f *excelize.File
	r *excelize.Rows
}

func (x *xlsxRows) Next() ([]string, error) {
	if !x.r.Next() {
		if e := x.r.Error(); e != nil {
			return nil, e
		}
		return nil, io.EOF
	}
	return x.r.Columns()
}
func (x *xlsxRows) Close() error { return errors.Join(x.r.Close(), x.f.Close()) }

func openRows(path, format string, c config.JobsConfig) (rowReader, error) {
	if format == "csv" {
		f, e := os.Open(path)
		if e != nil {
			return nil, e
		}
		scan := bufio.NewScanner(f)
		scan.Buffer(make([]byte, 4096), 64*1024)
		return &csvRows{f, scan}, nil
	}
	f, e := excelize.OpenFile(
		path,
		excelize.Options{
			UnzipSizeLimit:    c.MaxUnzipBytes,
			UnzipXMLSizeLimit: 16 << 20,
			TmpDir:            c.Directory,
			RawCellValue:      true,
		},
	)
	if e != nil {
		return nil, e
	}
	sheets := f.GetSheetList()
	if len(sheets) != 1 {
		f.Close()
		return nil, fmt.Errorf("XLSX must contain exactly one sheet")
	}
	rows, e := f.Rows(sheets[0])
	if e != nil {
		f.Close()
		return nil, e
	}
	return &xlsxRows{f, rows}, nil
}

func readHeader(r rowReader) error {
	h, e := r.Next()
	if e != nil {
		return fmt.Errorf("missing header: %w", e)
	}
	if len(h) != 2 || strings.TrimPrefix(h[0], "\ufeff") != "email" || h[1] != "password" {
		return fmt.Errorf("expected email,password header")
	}
	return nil
}

// SafeText экранирует начало CSV-значения, которое табличный редактор может принять за формулу.
func SafeText(v string) string {
	if v != "" && strings.ContainsAny(string(v[0]), "=+-@\t\r\n") {
		return "'" + v
	}
	return v
}
