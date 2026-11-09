package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	appconfig "go-boilerplate/internal/config"

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

func TestReadinessFailures(t *testing.T) {
	for _, dependency := range []string{"postgres", "redis", "artifacts"} {
		t.Run(dependency, func(t *testing.T) {
			server, err := New(
				Dependencies{
					Config: &appconfig.Config{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
					Ready: func(context.Context) error {
						return apperror.Failure(dependency, errors.New("private credentials"))
					},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			response := httptest.NewRecorder()
			server.Router.ServeHTTP(response, httptest.NewRequest("GET", "/health/ready", nil))
			if response.Code != 503 || strings.Contains(response.Body.String(), "private") {
				t.Fatal(response.Code, response.Body.String())
			}
			response = httptest.NewRecorder()
			server.Router.ServeHTTP(response, httptest.NewRequest("GET", "/health/live", nil))
			if response.Code != 200 {
				t.Fatal(response.Code)
			}
		})
	}
}
