// Package credentials задаёт общий контракт проверки входных данных HTTP и импорта.
package credentials

import (
	"reflect"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/danielgtaylor/huma/v2"
)

// EmailInput описывает email для запроса восстановления пароля.
type EmailInput struct {
	Email string `json:"email" format:"email" maxLength:"254" doc:"Email address"`
}

// Credentials задаёт общие ограничения email и пароля для HTTP и импорта.
type Credentials struct {
	Email    string `json:"email" format:"email" maxLength:"254"`
	Password string `json:"password" maxLength:"128" minLength:"6" writeOnly:"true"`
}

// NewPassword задаёт ограничения нового пароля при смене и восстановлении.
type NewPassword struct {
	Password string `json:"password" minLength:"6" maxLength:"128" writeOnly:"true"`
}

// NormalizeEmail убирает внешние пробелы и приводит email к регистру, используемому в БД.
func NormalizeEmail(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

var credentialsSchema = sync.OnceValue(func() *huma.Schema {
	registry := huma.NewMapRegistry("#/components/schemas/", huma.DefaultSchemaNamer)
	return huma.SchemaFromType(registry, reflect.TypeOf(Credentials{}))
})

// ValidateCredentials проверяет строку импорта по той же JSON Schema, что используется в HTTP.
func ValidateCredentials(credentials Credentials) bool {
	if !utf8.ValidString(credentials.Email) || !utf8.ValidString(credentials.Password) {
		return false
	}
	reader := &huma.ValidateResult{}
	path := huma.NewPathBuffer(nil, 0)
	huma.Validate(
		huma.NewMapRegistry("#/components/schemas/", huma.DefaultSchemaNamer),
		credentialsSchema(),
		path,
		huma.ModeWriteToServer,
		map[string]any{"email": credentials.Email, "password": credentials.Password},
		reader,
	)
	return len(reader.Errors) == 0
}
