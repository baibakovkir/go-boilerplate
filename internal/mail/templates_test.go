package mail

import (
	"strings"
	"testing"
	"time"
)

func TestSecurityMessages(t *testing.T) {
	for _, build := range []func(string, time.Duration) (Message, error){SignInMessage, PasswordResetMessage} {
		message, err := build(`<secret&value>`, 5*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(message.Text, `<secret&value>`) || !strings.Contains(message.Text, "5m0s") {
			t.Fatal("plain text must preserve the secret and its validity")
		}
		if strings.Contains(message.HTML, `<secret&value>`) ||
			!strings.Contains(message.HTML, "&lt;secret&amp;value&gt;") {
			t.Fatal("HTML must escape the secret")
		}
		if !strings.Contains(message.HTML, "5m0s") {
			t.Fatal("HTML is missing the validity")
		}
	}
}
