package mail

import (
	"context"
	"time"

	gomail "github.com/wneessen/go-mail"

	"go-boilerplate/internal/config"
)

// Sender отделяет прикладную отправку писем от конкретного SMTP-транспорта.
type Sender interface {
	Send(context.Context, string, string, Message) error
}

// SMTP отправляет письма с настройками TLS и аутентификации из конфигурации.
type SMTP struct{ config config.SMTPConfig }

// New создаёт SMTP-транспорт; соединение устанавливается при отправке письма.
func New(c config.SMTPConfig) *SMTP { return &SMTP{c} }

// Send отправляет письмо с текстовой и HTML-версиями с ограничением времени и поддержкой отмены контекста.
func (s *SMTP) Send(ctx context.Context, to, subject string, body Message) error {
	opts := []gomail.Option{gomail.WithPort(s.config.Port), gomail.WithTimeout(15 * time.Second)}
	switch s.config.TLS {
	case "none":
		opts = append(opts, gomail.WithTLSPolicy(gomail.NoTLS))
	case "tls":
		opts = append(opts, gomail.WithSSL())
	default:
		opts = append(opts, gomail.WithTLSPolicy(gomail.TLSMandatory))
	}
	if s.config.Username != "" {
		opts = append(
			opts,
			gomail.WithSMTPAuth(gomail.SMTPAuthPlain),
			gomail.WithUsername(s.config.Username),
			gomail.WithPassword(s.config.Password),
		)
	}
	client, e := gomail.NewClient(s.config.Host, opts...)
	if e != nil {
		return e
	}
	msg := gomail.NewMsg()
	if e = msg.From(s.config.From); e != nil {
		return e
	}
	if e = msg.To(to); e != nil {
		return e
	}
	msg.Subject(subject)
	msg.SetBodyString(gomail.TypeTextPlain, body.Text)
	if body.HTML != "" {
		msg.AddAlternativeString(gomail.TypeTextHTML, body.HTML)
	}
	return client.DialAndSendWithContext(ctx, msg)
}
