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

	"go-boilerplate/internal/config"
	"go-boilerplate/internal/jobs"
)

func TestReadersAndLimits(t *testing.T) {
	dir := t.TempDir()
	c := config.JobsConfig{Directory: dir, MaxUnzipBytes: 32 << 20}
	p := filepath.Join(dir, "users.csv")
	if e := os.WriteFile(p, []byte("\ufeffemail,password\nu@example.test,123456\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	r, e := openRows(p, "csv", c)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if e = readHeader(r); e != nil {
		t.Fatal(e)
	}
	values, e := r.Next()
	if e != nil || len(values) != 2 || values[0] != "u@example.test" {
		t.Fatal(values, e)
	}
	if e = os.WriteFile(p, []byte("email,password\n"+strings.Repeat("x", 70<<10)+",secret1\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	limited, _ := openRows(p, "csv", c)
	defer limited.Close()
	_ = readHeader(limited)
	if _, e = limited.Next(); e == nil {
		t.Fatal("unbounded CSV line accepted")
	}
	x := excelize.NewFile()
	_ = x.SetSheetRow("Sheet1", "A1", &[]string{"email", "password"})
	_ = x.SetSheetRow("Sheet1", "A2", &[]string{"x@example.test", "123456"})
	path := filepath.Join(dir, "users.xlsx")
	if e = x.SaveAs(path); e != nil {
		t.Fatal(e)
	}
	x.Close()
	xr, e := openRows(path, "xlsx", c)
	if e != nil {
		t.Fatal(e)
	}
	if e = readHeader(xr); e != nil {
		t.Fatal(e)
	}
	v, e := xr.Next()
	if e != nil || v[0] != "x@example.test" {
		t.Fatal(v, e)
	}
	xr.Close()
	x = excelize.NewFile()
	_, _ = x.NewSheet("Second")
	_ = x.SaveAs(path)
	x.Close()
	if r, e = openRows(path, "xlsx", c); e == nil {
		r.Close()
		t.Fatal("multiple sheets accepted")
	}
}

func TestSafeExport(t *testing.T) {
	for _, v := range []string{"=SUM(A1)", "+cmd", "-cmd", "@cmd", "\tcmd"} {
		if SafeText(v) == v {
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
			c := config.JobsConfig{Directory: dir, MaxUnzipBytes: 2 << 30}
			path := filepath.Join(dir, "million."+format)
			service := &Handlers{Service: &jobs.Service{Config: &config.Config{Jobs: c}}}
			writer, e := service.writer(path, format)
			if e != nil {
				t.Fatal(e)
			}
			if e = writer.Write([]string{"email", "password"}); e != nil {
				t.Fatal(e)
			}
			for n := 0; n < 1000000; n++ {
				if e = writer.Write([]string{fmt.Sprintf("user%d@example.test", n), "secret123"}); e != nil {
					t.Fatal(e)
				}
			}
			if e = writer.Finish(); e != nil {
				t.Fatal(e)
			}
			if e = writer.Close(); e != nil {
				t.Fatal(e)
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			peak := before.HeapAlloc
			reader, e := openRows(path, format, c)
			if e != nil {
				t.Fatal(e)
			}
			defer reader.Close()
			if e = readHeader(reader); e != nil {
				t.Fatal(e)
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
