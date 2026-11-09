package mail

import (
	"bytes"
	_ "embed"
	"fmt"
	"html/template"
	"time"
)

// Message содержит текстовую и HTML-версии одного письма.
type Message struct {
	Text string
	HTML string
}

//go:embed templates/security.html
var securityHTML string

var securityTemplate = template.Must(template.New("security").Parse(securityHTML))

type securityContent struct {
	Title        string
	Preview      string
	Intro        string
	SecretLabel  string
	Secret       string
	Validity     string
	Instructions string
	Safety       string
	IsCode       bool
}

// SignInMessage оформляет одноразовый код входа администратора.
func SignInMessage(code string, ttl time.Duration) (Message, error) {
	return renderSecurity(securityContent{
		Title:        "Confirm your sign-in",
		Preview:      "Your administrator sign-in code is ready.",
		Intro:        "Enter this one-time code to finish signing in to your administrator account.",
		SecretLabel:  "SIGN-IN CODE",
		Secret:       code,
		IsCode:       true,
		Validity:     ttl.String(),
		Instructions: "Do not share this code with anyone.",
		Safety:       "If you did not try to sign in, do not use or share this code.",
	}, fmt.Sprintf("Your sign-in code: %s\nValid for %s. Do not share it.", code, ttl))
}

// PasswordResetMessage оформляет токен восстановления без выдуманной ссылки на frontend.
func PasswordResetMessage(token string, ttl time.Duration) (Message, error) {
	return renderSecurity(securityContent{
		Title:        "Reset your password",
		Preview:      "Use your one-time token to reset your password.",
		Intro:        "We received a request to reset your account password.",
		SecretLabel:  "PASSWORD RESET TOKEN",
		Secret:       token,
		Validity:     ttl.String(),
		Instructions: "Submit this token with your new password to the password reset API.",
		Safety: "If you did not request a password reset, you can ignore this email. " +
			"Your password has not been changed.",
	}, fmt.Sprintf(
		"Your one-time password reset token:\n%s\nValid for %s. "+
			"Submit it with your new password to the password reset API. "+
			"If you did not request this, ignore this message.",
		token,
		ttl,
	))
}

func renderSecurity(content securityContent, text string) (Message, error) {
	var html bytes.Buffer
	if err := securityTemplate.Execute(&html, content); err != nil {
		return Message{}, err
	}
	return Message{Text: text, HTML: html.String()}, nil
}
