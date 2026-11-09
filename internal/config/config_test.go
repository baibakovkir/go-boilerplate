package config

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAndOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`env: local
postgres:
  user: postgres
  password: local
  database: test
  port: 25432
auth:
  jwt_secret: test-secret-at-least-thirty-two-bytes-long
admins:
  - email: admin@example.test
    password: secret1
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONFIG_PATH", path)
	t.Setenv("POSTGRES_PORT", "25433")
	config, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if config.Postgres.Port != 25433 || config.Jobs.MaxRows != 1000000 || config.Jobs.BatchSize != 500 ||
		config.Auth.OTPAttempts != 5 {
		t.Fatal("defaults/overrides not applied")
	}
	config.Env = "prod"
	if err = config.Validate(); err == nil {
		t.Fatal("insecure production config accepted")
	}
	config.Env = "local"
	config.HTTPServer.AllowedOrigins = []string{"*"}
	if err = config.Validate(); err == nil {
		t.Fatal("wildcard CORS accepted")
	}
	config.HTTPServer.AllowedOrigins = nil
	config.HTTPServer.TrustedProxies = []string{"bad"}
	if err = config.Validate(); err == nil {
		t.Fatal("invalid proxy accepted")
	}
	config.HTTPServer.TrustedProxies = nil
	config.Audit.Schedule = "broken"
	if err = config.Validate(); err == nil {
		t.Fatal("invalid cron accepted")
	}
}

func TestDatabaseCredentialsEscaped(t *testing.T) {
	config := PostgresConfig{
		Host:     "::1",
		Port:     5432,
		User:     "user@host",
		Password: "x:@/?#%",
		Database: "test",
		SSLMode:  "require",
	}
	parsedURL, err := url.Parse(config.URL())
	if err != nil {
		t.Fatal(err)
	}
	parts, _ := parsedURL.User.Password()
	if parsedURL.User.Username() != config.User || parts != config.Password || parsedURL.Host != "[::1]:5432" {
		t.Fatal("credentials were not escaped correctly")
	}
}

func TestJobConcurrencyBounds(t *testing.T) {
	for _, concurrency := range []int{0, -1, 33} {
		config := Config{}
		config.Jobs.Concurrency = concurrency
		if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "jobs.concurrency") {
			t.Fatalf("concurrency %d: expected concurrency validation error, got %v", concurrency, err)
		}
	}
}
