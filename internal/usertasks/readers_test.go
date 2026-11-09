package usertasks

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"

	appconfig "go-boilerplate/internal/config"
)

func TestReadersAndLimits(t *testing.T) {
	dir := t.TempDir()
	config := appconfig.JobsConfig{Directory: dir, MaxUnzipBytes: 32 << 20}
	parts := filepath.Join(dir, "users.csv")
	if err := os.WriteFile(parts, []byte("\ufeffemail,password\nu@example.test,123456\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := openRows(parts, "csv", config)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err = readHeader(reader); err != nil {
		t.Fatal(err)
	}
	values, err := reader.Next()
	if err != nil || len(values) != 2 || values[0] != "u@example.test" {
		t.Fatal(values, err)
	}
	err = os.WriteFile(parts, []byte("email,password\n"+strings.Repeat("x", 70<<10)+",secret1\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	limited, _ := openRows(parts, "csv", config)
	defer limited.Close()
	_ = readHeader(limited)
	if _, err = limited.Next(); err == nil {
		t.Fatal("unbounded CSV line accepted")
	}
	workbook := excelize.NewFile()
	_ = workbook.SetSheetRow("Sheet1", "A1", &[]string{"email", "password"})
	_ = workbook.SetSheetRow("Sheet1", "A2", &[]string{"x@example.test", "123456"})
	path := filepath.Join(dir, "users.xlsx")
	if err = workbook.SaveAs(path); err != nil {
		t.Fatal(err)
	}
	workbook.Close()
	xr, err := openRows(path, "xlsx", config)
	if err != nil {
		t.Fatal(err)
	}
	if err = readHeader(xr); err != nil {
		t.Fatal(err)
	}
	value, err := xr.Next()
	if err != nil || value[0] != "x@example.test" {
		t.Fatal(value, err)
	}
	xr.Close()
	workbook = excelize.NewFile()
	_, _ = workbook.NewSheet("Second")
	_ = workbook.SaveAs(path)
	workbook.Close()
	if reader, err = openRows(path, "xlsx", config); err == nil {
		reader.Close()
		t.Fatal("multiple sheets accepted")
	}
}

func TestSafeExport(t *testing.T) {
	for _, value := range []string{"=SUM(A1)", "+cmd", "-cmd", "@cmd", "\tcmd"} {
		if SafeText(value) == value {
			t.Fatal("formula not escaped")
		}
	}
	if SafeText("normal@example.test") != "normal@example.test" {
		t.Fatal("normal text changed")
	}
}

func TestMillionRows(t *testing.T) {
	if os.Getenv("RUN_LOAD") != "1" {
		t.Skip("RUN_LOAD=1 enables million-row CSV/XLSX streaming checks")
	}
	for _, format := range []string{"csv", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			dir := t.TempDir()
			config := appconfig.JobsConfig{Directory: dir, MaxUnzipBytes: 2 << 30}
			path := filepath.Join(dir, "million."+format)
			service := &Handlers{config: &appconfig.Config{Jobs: config}}
			writer, err := service.writer(path, format)
			if err != nil {
				t.Fatal(err)
			}
			if err = writer.Write([]string{"email", "password"}); err != nil {
				t.Fatal(err)
			}
			for number := 0; number < 1000000; number++ {
				if err = writer.Write([]string{fmt.Sprintf("user%d@example.test", number), "secret123"}); err != nil {
					t.Fatal(err)
				}
			}
			if err = writer.Finish(); err != nil {
				t.Fatal(err)
			}
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			peak := before.HeapAlloc
			reader, err := openRows(path, format, config)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if err = readHeader(reader); err != nil {
				t.Fatal(err)
			}
			count := 0
			for {
				row, err := reader.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(row) != 2 {
					t.Fatal("corrupt row")
				}
				count++
				if count%10000 == 0 {
					runtime.ReadMemStats(&after)
					peak = max(peak, after.HeapAlloc)
				}
			}
			if count != 1000000 {
				t.Fatal(count)
			}
			if peak > before.HeapAlloc+(128<<20) {
				t.Fatalf("streaming heap budget exceeded: %d MiB", peak>>20)
			}
			t.Logf("streamed %d rows; sampled peak Go heap %d MiB", count, peak>>20)
		})
	}
}
