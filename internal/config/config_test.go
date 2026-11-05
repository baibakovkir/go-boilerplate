package config

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAndOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(path, []byte(`env: local
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
`), 0o600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("CONFIG_PATH", path)
	t.Setenv("POSTGRES_PORT", "25433")
	c, e := Load()
	if e != nil {
		t.Fatal(e)
	}
	if c.Postgres.Port != 25433 || c.Jobs.MaxRows != 1000000 || c.Jobs.BatchSize != 500 || c.Auth.OTPAttempts != 5 {
		t.Fatal("defaults/overrides not applied")
	}
	c.Env = "prod"
	if e = c.Validate(); e == nil {
		t.Fatal("insecure production config accepted")
	}
	c.Env = "local"
	c.HTTPServer.AllowedOrigins = []string{"*"}
	if e = c.Validate(); e == nil {
		t.Fatal("wildcard CORS accepted")
	}
	c.HTTPServer.AllowedOrigins = nil
	c.HTTPServer.TrustedProxies = []string{"bad"}
	if e = c.Validate(); e == nil {
		t.Fatal("invalid proxy accepted")
	}
	c.HTTPServer.TrustedProxies = nil
	c.Audit.Schedule = "broken"
	if e = c.Validate(); e == nil {
		t.Fatal("invalid cron accepted")
	}
}

func TestDatabaseCredentialsEscaped(t *testing.T) {
	c := PostgresConfig{
		Host:     "::1",
		Port:     5432,
		User:     "user@host",
		Password: "x:@/?#%",
		Database: "test",
		SSLMode:  "require",
	}
	u, e := url.Parse(c.URL())
	if e != nil {
		t.Fatal(e)
	}
	p, _ := u.User.Password()
	if u.User.Username() != c.User || p != c.Password || u.Host != "[::1]:5432" {
		t.Fatal("credentials were not escaped correctly")
	}
}
