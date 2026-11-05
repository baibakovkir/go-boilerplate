// Package logger настраивает формат и минимальный уровень журналирования.
package logger

import (
	"log/slog"
	"os"
)

// SetupLogger создаёт логгер stdout: текст и Debug для local, JSON и Debug
// для dev, JSON и Info для остальных окружений.
func SetupLogger(env string) *slog.Logger {
	switch env {
	case "local":
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	case "dev":
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	default:
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
}
