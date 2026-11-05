package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/robfig/cron/v3"
)

// Config объединяет настройки API, инфраструктуры и фоновой обработки.
type Config struct {
	Env        string           `yaml:"env" env:"APP_ENV" env-default:"local"`
	HTTPServer HTTPServerConfig `yaml:"http_server"`
	Postgres   PostgresConfig   `yaml:"postgres"`
	Redis      RedisConfig      `yaml:"redis"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	Auth       AuthConfig       `yaml:"auth"`
	SMTP       SMTPConfig       `yaml:"smtp"`
	Jobs       JobsConfig       `yaml:"jobs"`
	Audit      AuditConfig      `yaml:"audit"`
	Admins     []AdminConfig    `yaml:"admins"`
}

// HTTPServerConfig задаёт адрес сервера, таймауты и границы доверия HTTP.
type HTTPServerConfig struct {
	Address          string        `yaml:"address" env:"HTTP_ADDRESS" env-default:"localhost:8181"`
	Timeout          time.Duration `yaml:"timeout" env-default:"30s"`
	IdleTimeout      time.Duration `yaml:"idle_timeout" env-default:"60s"`
	AllowedOrigins   []string      `yaml:"allowed_origins"`
	TrustedProxies   []string      `yaml:"trusted_proxies"`
	DocsAllowedCIDRs []string      `yaml:"docs_allowed_cidrs"`
	RatePerMinute    int           `yaml:"rate_per_minute" env-default:"300"`
}

// PostgresConfig содержит отдельные параметры подключения к PostgreSQL.
type PostgresConfig struct {
	Host     string `yaml:"host" env:"POSTGRES_HOST" env-default:"localhost"`
	Port     uint16 `yaml:"port" env:"POSTGRES_PORT" env-default:"25432"`
	User     string `yaml:"user" env:"POSTGRES_USER" env-required:"true"`
	Password string `yaml:"password" env:"POSTGRES_PASSWORD" env-required:"true"`
	Database string `yaml:"database" env:"POSTGRES_DB" env-required:"true"`
	SSLMode  string `yaml:"ssl_mode" env:"POSTGRES_SSLMODE" env-default:"disable"`
}

// URL собирает строку подключения с экранированием имени и пароля.
func (p PostgresConfig) URL() string {
	u := url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(p.Host, strconv.Itoa(int(p.Port))),
		Path:   "/" + p.Database,
		User:   url.UserPassword(p.User, p.Password),
	}
	q := u.Query()
	q.Set("sslmode", p.SSLMode)
	u.RawQuery = q.Encode()
	return u.String()
}

// RedisConfig настраивает хранилище сессий, лимитов и уведомлений.
type RedisConfig struct {
	Address  string `yaml:"address" env:"REDIS_ADDRESS" env-default:"localhost:26379"`
	Password string `yaml:"password" env:"REDIS_PASSWORD"`
	DB       int    `yaml:"db" env-default:"0"`
	Prefix   string `yaml:"prefix" env-default:"boilerplate:"`
}

// KafkaConfig задаёт брокеры, тему заданий и группу worker.
type KafkaConfig struct {
	Brokers []string `yaml:"brokers" env:"KAFKA_BROKERS" env-default:"localhost:29092"`
	Topic   string   `yaml:"topic" env-default:"boilerplate.jobs.v1"`
	Group   string   `yaml:"group" env-default:"boilerplate.workers.v1"`
}

// AuthConfig задаёт секрет подписи, сроки токенов и ограничения попыток авторизации.
type AuthConfig struct {
	JWTSecret       string        `yaml:"jwt_secret" env:"JWT_SECRET" env-required:"true"`
	Issuer          string        `yaml:"issuer" env-default:"go-boilerplate"`
	CookieSecure    bool          `yaml:"cookie_secure" env:"COOKIE_SECURE"`
	UserAccessTTL   time.Duration `yaml:"user_access_ttl" env-default:"15m"`
	AdminAccessTTL  time.Duration `yaml:"admin_access_ttl" env-default:"5m"`
	UserRefreshTTL  time.Duration `yaml:"user_refresh_ttl" env-default:"720h"`
	AdminRefreshTTL time.Duration `yaml:"admin_refresh_ttl" env-default:"8h"`
	OTPTTL          time.Duration `yaml:"otp_ttl" env-default:"5m"`
	OTPAttempts     int           `yaml:"otp_attempts" env-default:"5"`
	ResetTTL        time.Duration `yaml:"reset_ttl" env-default:"30m"`
	LoginLimit      int           `yaml:"login_limit" env-default:"10"`
	EmailLimit      int           `yaml:"email_limit" env-default:"3"`
}

// SMTPConfig описывает SMTP-сервер и явно выбранный режим TLS.
type SMTPConfig struct {
	Host     string `yaml:"host" env:"SMTP_HOST" env-default:"localhost"`
	Port     int    `yaml:"port" env:"SMTP_PORT" env-default:"21025"`
	Username string `yaml:"username" env:"SMTP_USERNAME"`
	Password string `yaml:"password" env:"SMTP_PASSWORD"`
	From     string `yaml:"from" env-default:"noreply@boilerplate.local"`
	TLS      string `yaml:"tls" env-default:"none"`
}

// JobsConfig ограничивает размер файлов, обработку пакетов и срок хранения результатов.
type JobsConfig struct {
	Directory      string        `yaml:"directory" env:"JOBS_DIRECTORY" env-default:"var/jobs"`
	MaxRows        int64         `yaml:"max_rows" env-default:"1000000"`
	MaxUploadBytes int64         `yaml:"max_upload_bytes" env-default:"536870912"`
	MaxUnzipBytes  int64         `yaml:"max_unzip_bytes" env-default:"2147483648"`
	BatchSize      int           `yaml:"batch_size" env-default:"500"`
	HashWorkers    int           `yaml:"hash_workers" env-default:"2"`
	LeaseTTL       time.Duration `yaml:"lease_ttl" env-default:"2m"`
	MaxAttempts    int           `yaml:"max_attempts" env-default:"5"`
	ArtifactTTL    time.Duration `yaml:"artifact_ttl" env-default:"168h"`
}

// AuditConfig задаёт срок хранения аудита и расписание очистки в UTC.
type AuditConfig struct {
	RetentionDays int    `yaml:"retention_days" env-default:"90"`
	Schedule      string `yaml:"schedule" env-default:"0 3 * * *"`
}

// AdminConfig описывает начального администратора; применяется только при его отсутствии в БД.
type AdminConfig struct {
	Email    string `yaml:"email"`
	Password string `yaml:"password"`
}

// Load читает YAML, применяет окружение и значения по умолчанию, затем проверяет настройки.
func Load() (*Config, error) {
	p := os.Getenv("CONFIG_PATH")
	if p == "" {
		p = "config/local.yaml"
	}
	var c Config
	if err := cleanenv.ReadConfig(p, &c); err != nil {
		return nil, fmt.Errorf("read config %q: %w", p, err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate отклоняет небезопасные или некорректные настройки до запуска зависимостей.
func (c *Config) Validate() error {
	if _, e := cron.ParseStandard(c.Audit.Schedule); e != nil {
		return fmt.Errorf("invalid audit.schedule: %w", e)
	}
	if len(c.Auth.JWTSecret) < 32 {
		return fmt.Errorf("auth.jwt_secret must contain at least 32 bytes")
	}
	if c.Env != "local" && strings.HasPrefix(c.Auth.JWTSecret, "local-development-") {
		return fmt.Errorf("replace the example JWT secret outside local")
	}
	if c.Env != "local" && !c.Auth.CookieSecure {
		return fmt.Errorf("auth.cookie_secure must be true outside local")
	}
	if c.Env != "local" && c.SMTP.TLS == "none" {
		return fmt.Errorf("SMTP TLS is required outside local")
	}
	if c.SMTP.TLS != "none" && c.SMTP.TLS != "starttls" && c.SMTP.TLS != "tls" {
		return fmt.Errorf("smtp.tls must be none, starttls or tls")
	}
	if c.HTTPServer.RatePerMinute < 1 || c.Jobs.BatchSize < 1 || c.Jobs.BatchSize > 5000 || c.Jobs.HashWorkers < 1 ||
		c.Jobs.HashWorkers > 32 ||
		c.Jobs.MaxRows < 1 ||
		c.Jobs.MaxRows > 1000000 ||
		c.Jobs.MaxUploadBytes < 1 ||
		c.Jobs.MaxUnzipBytes < 1 ||
		c.Jobs.MaxAttempts < 1 ||
		c.Auth.OTPAttempts < 1 ||
		c.Auth.OTPAttempts > 10 ||
		c.Auth.LoginLimit < 1 ||
		c.Auth.EmailLimit < 1 ||
		c.Audit.RetentionDays < 1 {
		return fmt.Errorf("invalid numeric limits")
	}
	for _, d := range []time.Duration{
		c.HTTPServer.Timeout,
		c.HTTPServer.IdleTimeout,
		c.Auth.UserAccessTTL,
		c.Auth.AdminAccessTTL,
		c.Auth.UserRefreshTTL,
		c.Auth.AdminRefreshTTL,
		c.Auth.OTPTTL,
		c.Auth.ResetTTL,
		c.Jobs.ArtifactTTL,
	} {
		if d <= 0 {
			return fmt.Errorf("durations must be positive")
		}
	}
	if c.Jobs.LeaseTTL < 30*time.Second {
		return fmt.Errorf("jobs.lease_ttl must be at least 30s")
	}
	for _, list := range [][]string{c.HTTPServer.TrustedProxies, c.HTTPServer.DocsAllowedCIDRs} {
		for _, v := range list {
			if _, _, e := net.ParseCIDR(v); e != nil {
				return fmt.Errorf("invalid CIDR %q", v)
			}
		}
	}
	for _, origin := range c.HTTPServer.AllowedOrigins {
		u, e := url.Parse(origin)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Path != "" ||
			u.RawQuery != "" ||
			strings.Contains(origin, "*") {
			return fmt.Errorf("invalid allowed origin %q", origin)
		}
	}
	return nil
}
