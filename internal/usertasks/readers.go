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

	appconfig "go-boilerplate/internal/config"
)

type rowReader interface {
	Next() ([]string, error)
	Close() error
}
type csvRows struct {
	file    *os.File
	scanner *bufio.Scanner
}

func (rows *csvRows) Next() ([]string, error) {
	for rows.scanner.Scan() {
		line := rows.scanner.Text()
		if line == "" {
			continue
		}
		// Одна CSV-запись занимает физическую строку: это ограничивает память парсера.
		reader := csv.NewReader(strings.NewReader(line))
		reader.FieldsPerRecord = -1
		return reader.Read()
	}
	if err := rows.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}
func (rows *csvRows) Close() error { return rows.file.Close() }

type xlsxRows struct {
	workbook *excelize.File
	reader   *excelize.Rows
}

func (rows *xlsxRows) Next() ([]string, error) {
	if !rows.reader.Next() {
		if err := rows.reader.Error(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	return rows.reader.Columns()
}

func (rows *xlsxRows) Close() error { return errors.Join(rows.reader.Close(), rows.workbook.Close()) }

func openRows(path, format string, config appconfig.JobsConfig) (rowReader, error) {
	if format == "csv" {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		scan := bufio.NewScanner(file)
		scan.Buffer(make([]byte, 4096), 64*1024)
		return &csvRows{file, scan}, nil
	}
	workbook, err := excelize.OpenFile(
		path,
		excelize.Options{
			UnzipSizeLimit:    config.MaxUnzipBytes,
			UnzipXMLSizeLimit: 16 << 20,
			TmpDir:            config.Directory,
			RawCellValue:      true,
		},
	)
	if err != nil {
		return nil, err
	}
	sheets := workbook.GetSheetList()
	if len(sheets) != 1 {
		workbook.Close()
		return nil, fmt.Errorf("XLSX must contain exactly one sheet")
	}
	rows, err := workbook.Rows(sheets[0])
	if err != nil {
		workbook.Close()
		return nil, err
	}
	return &xlsxRows{workbook, rows}, nil
}

func readHeader(reader rowReader) error {
	handler, err := reader.Next()
	if err != nil {
		return fmt.Errorf("missing header: %w", err)
	}
	if len(handler) != 2 || strings.TrimPrefix(handler[0], "\ufeff") != "email" || handler[1] != "password" {
		return fmt.Errorf("expected email,password header")
	}
	return nil
}

// SafeText экранирует начало CSV-значения, которое табличный редактор может принять за формулу.
func SafeText(value string) string {
	if value != "" && strings.ContainsAny(string(value[0]), "=+-@\t\r\n") {
		return "'" + value
	}
	return value
}
