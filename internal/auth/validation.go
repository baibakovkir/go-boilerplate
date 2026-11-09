package auth

import "go-boilerplate/internal/credentials"

// EmailInput описывает email для восстановления пароля.
type EmailInput = credentials.EmailInput

// Credentials задаёт общий контракт входа и импорта.
type Credentials = credentials.Credentials

// NewPassword задаёт ограничения нового пароля.
type NewPassword = credentials.NewPassword

// NormalizeEmail нормализует email для хранения и поиска.
func NormalizeEmail(value string) string { return credentials.NormalizeEmail(value) }

// ValidateCredentials проверяет credentials по общей схеме HTTP и импорта.
func ValidateCredentials(input Credentials) bool { return credentials.ValidateCredentials(input) }
