package auth

import (
	"strings"
	"testing"
)

func TestSharedCredentialSchema(t *testing.T) {
	cases := []struct {
		email, password string
		valid           bool
	}{
		{
			"user@example.test",
			"123456",
			true,
		},
		{
			"bad",
			"123456",
			false,
		},
		{
			"user@example.test",
			"12345",
			false,
		},
		{
			"user@example.test",
			strings.Repeat("a",
				129),
			false,
		},
		{
			"user@example.test",
			"япарол",
			true,
		},
		{
			"user@example.test",
			"      ",
			true,
		},
	}
	for _, tt := range cases {
		if got := ValidateCredentials(Credentials{tt.email, tt.password}); got != tt.valid {
			t.Errorf("email %q length %d: got %v", tt.email, len(tt.password), got)
		}
	}
}
