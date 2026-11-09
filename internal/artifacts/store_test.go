package artifacts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

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
