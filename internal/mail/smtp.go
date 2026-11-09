package mail

import (
	"context"
	"time"

	gomail "github.com/wneessen/go-mail"

	appconfig "go-boilerplate/internal/config"
)

// Sender отделяет прикладную отправку писем от конкретного SMTP-транспорта.
type Sender interface {
	Send(context.Context, string, string, Message) error
}

// SMTP отправляет письма с настройками TLS и аутентификации из конфигурации.
type SMTP struct{ config appconfig.SMTPConfig }

// New создаёт SMTP-транспорт; соединение устанавливается при отправке письма.
func New(smtp appconfig.SMTPConfig) *SMTP { return &SMTP{smtp} }

// Send отправляет письмо с текстовой и HTML-версиями с ограничением времени и поддержкой отмены контекста.
func (smtp *SMTP) Send(ctx context.Context, to, subject string, body Message) error {
	opts := []gomail.Option{gomail.WithPort(smtp.config.Port), gomail.WithTimeout(15 * time.Second)}
	switch smtp.config.TLS {
	case "none":
		opts = append(opts, gomail.WithTLSPolicy(gomail.NoTLS))
	case "tls":
		opts = append(opts, gomail.WithSSL())
	default:
		opts = append(opts, gomail.WithTLSPolicy(gomail.TLSMandatory))
	}
	if smtp.config.Username != "" {
		opts = append(
			opts,
			gomail.WithSMTPAuth(gomail.SMTPAuthPlain),
			gomail.WithUsername(smtp.config.Username),
			gomail.WithPassword(smtp.config.Password),
		)
	}
	client, err := gomail.NewClient(smtp.config.Host, opts...)
	if err != nil {
		return err
	}
	msg := gomail.NewMsg()
	if err = msg.From(smtp.config.From); err != nil {
		return err
	}
	if err = msg.To(to); err != nil {
		return err
	}
	msg.Subject(subject)
	msg.SetBodyString(gomail.TypeTextPlain, body.Text)
	if body.HTML != "" {
		msg.AddAlternativeString(gomail.TypeTextHTML, body.HTML)
	}
	return client.DialAndSendWithContext(ctx, msg)
}
