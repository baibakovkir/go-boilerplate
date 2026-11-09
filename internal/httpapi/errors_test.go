package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"go-boilerplate/internal/apperror"
)

func TestApplicationErrors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for code, status := range map[apperror.Code]int{
		apperror.BadRequest: 400, apperror.Unauthorized: 401,
		apperror.Forbidden: 403, apperror.NotFound: 404, apperror.Conflict: 409,
		apperror.TooManyRequests: 429, apperror.Unavailable: 503,
	} {
		mapped := transportError(context.Background(), logger, apperror.New(code, "safe message"))
		if statusOf(mapped) != status || !strings.Contains(mapped.Error(), "safe message") {
			t.Fatalf("%s: %v", code, mapped)
		}
	}
	cause := errors.New("postgres password=private")
	failure := apperror.Failure("load account", cause)
	if !errors.Is(failure, cause) {
		t.Fatal("technical cause lost")
	}
	for _, err := range []error{failure, cause} {
		mapped := transportError(context.Background(), logger, err)
		if statusOf(mapped) != 503 || strings.Contains(mapped.Error(), "private") {
			t.Fatalf("unsafe error: %v", mapped)
		}
	}
}
