package security

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// HashPassword создаёт Argon2id-хеш с уникальной солью и параметрами для последующей проверки.
func HashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, e := rand.Read(salt); e != nil {
		return "", e
	}
	hash := argon2.IDKey([]byte(password), salt, 2, 19*1024, 1, 32)
	b := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=19$m=19456,t=2,p=1$%s$%s", b.EncodeToString(salt), b.EncodeToString(hash)), nil
}

// CheckPassword проверяет пароль и ограничивает параметры хеша до выделения памяти Argon2id.
func CheckPassword(encoded, password string) bool {
	p := strings.Split(encoded, "$")
	if len(p) != 6 || p[1] != "argon2id" || p[2] != "v=19" {
		return false
	}
	var memory, iterations uint32
	var threads uint8
	if _, e := fmt.Sscanf(p[3], "m=%d,t=%d,p=%d", &memory, &iterations, &threads); e != nil {
		return false
	}
	if memory < 8 || memory > 128*1024 || iterations < 1 || iterations > 10 || threads < 1 || threads > 8 {
		return false
	}
	salt, e := base64.RawStdEncoding.DecodeString(p[4])
	if e != nil || len(salt) != 16 {
		return false
	}
	want, e := base64.RawStdEncoding.DecodeString(p[5])
	if e != nil || len(want) != 32 {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, iterations, memory, threads, 32)
	return subtle.ConstantTimeCompare(want, got) == 1
}

// RandomToken создаёт криптографически случайный секрет; сбой источника случайности вызывает panic.
func RandomToken() string {
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
