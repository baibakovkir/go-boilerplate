package artifacts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	appconfig "go-boilerplate/internal/config"

	"github.com/google/uuid"
)

func TestS3Integration(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("requires local Garage")
	}
	t.Setenv("CONFIG_PATH", "../../config/local.yaml")
	config, err := appconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	config.S3.Prefix = "artifacts-test-" + uuid.NewString()
	store, err := New(context.Background(), config.S3)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = store.Check(ctx); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"../escape", "a/b", "", "a\\b"} {
		if store.Put(ctx, key, bytes.NewReader(nil), 0) == nil {
			t.Fatalf("unsafe key %q", key)
		}
	}
	body := []byte("0123456789abcdef")
	if err = store.Put(ctx, "object", bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Delete(ctx, "object"); err != nil {
			t.Error(err)
		}
	})
	reader, err := store.Open(ctx, "object")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.Info().Size != int64(len(body)) {
		t.Fatal(reader.Info())
	}
	if _, err = reader.Seek(5, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	part := make([]byte, 4)
	if _, err = io.ReadFull(reader, part); err != nil || string(part) != "5678" {
		t.Fatal(string(part), err)
	}
	if _, err = reader.Seek(-2, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	tail, err := io.ReadAll(reader)
	if err != nil || string(tail) != "ef" {
		t.Fatal(string(tail), err)
	}
	if _, err = reader.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	full, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(full, body) {
		t.Fatal(string(full), err)
	}
	objects, _, err := store.List(ctx, "")
	if err != nil || len(objects) != 1 {
		t.Fatal(objects, err)
	}
	if err = store.Delete(ctx, "object"); err != nil {
		t.Fatal(err)
	}
	if err = store.Delete(ctx, "object"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Open(ctx, "object"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing: %v", err)
	}
}

// zeroStream создаёт содержимое большого файла без выделения буфера его размера.
type zeroStream struct{}

func (zeroStream) Read(buffer []byte) (int, error) { clear(buffer); return len(buffer), nil }

func TestS3StreamingIntegration(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("requires local Garage")
	}
	t.Setenv("CONFIG_PATH", "../../config/local.yaml")
	config, err := appconfig.Load()
	if err != nil {
		t.Fatal(err)
	}
	config.S3.Prefix = "stream-test-" + uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	store, err := New(ctx, config.S3)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "stream")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	const size int64 = 64 << 20
	if _, err = io.CopyN(file, zeroStream{}, size); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if err = store.Put(ctx, "large-object", file, size); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := store.Delete(cleanup, "large-object"); err != nil {
			t.Error(err)
		}
	})
	reader, err := store.Open(ctx, "large-object")
	if err != nil {
		t.Fatal(err)
	}
	count, err := io.Copy(io.Discard, reader)
	reader.Close()
	if err != nil || count != size {
		t.Fatal(count, err)
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("64 MiB S3 roundtrip: %.2f MiB allocated", float64(allocated)/(1<<20))
	if allocated > uint64(size/2) {
		t.Fatalf("streaming allocated %d bytes for %d-byte object", allocated, size)
	}
}
