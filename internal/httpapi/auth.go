package httpapi

import (
	"context"
	"net/http"
	"time"

	"go-boilerplate/internal/auth"
)

// LoginInput задаёт JSON-тело входа с общими правилами проверки credentials.
type LoginInput struct{ Body auth.Credentials }

// TokenOutput возвращает access и CSRF в JSON, а refresh — в заголовке Set-Cookie.
type TokenOutput struct {
	SetCookie string `header:"Set-Cookie"`
	Body      auth.Tokens
}

// ChallengeOutput возвращает идентификатор проверки OTP и срок её действия.
type ChallengeOutput struct {
	Body struct {
		ChallengeID string `json:"challenge_id"`
		ExpiresIn   int64  `json:"expires_in"`
	}
}

// OTPInput задаёт идентификатор проверки и шестизначный код из письма.
type OTPInput struct {
	Body struct {
		ChallengeID string `json:"challenge_id" minLength:"43" maxLength:"43"`
		Code        string `json:"code" pattern:"^[0-9]{6}$"`
	}
}

// RefreshInput читает refresh-cookie и заголовок CSRF для операций с сессией.
type RefreshInput struct {
	UserCookie  string `cookie:"user_refresh"`
	AdminCookie string `cookie:"admin_refresh"`
	CSRF        string `header:"X-CSRF-Token"`
}

func (input RefreshInput) cookie(role auth.Role) string {
	if role == auth.Admin {
		return input.AdminCookie
	}
	return input.UserCookie
}

// MessageOutput возвращает краткий результат операции без раскрытия внутренних данных.
type MessageOutput struct {
	Body struct {
		Message string `json:"message"`
	}
}

func message(text string) *MessageOutput {
	value := &MessageOutput{}
	value.Body.Message = text
	return value
}

func (server *Server) cookie(role auth.Role, token string, ttl time.Duration) string {
	name := "user_refresh"
	path := "/api/v1/auth"
	if role == auth.Admin {
		name = "admin_refresh"
		path = "/api/v1/admin/auth"
	}
	age := int(ttl.Seconds())
	if token == "" {
		age = -1
	}
	return (&http.Cookie{
		Name:     name,
		Value:    token,
		Path:     path,
		HttpOnly: true,
		Secure:   server.Config.Auth.CookieSecure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   age,
	}).String()
}

func (server *Server) tokens(role auth.Role, tokens auth.Tokens) *TokenOutput {
	return &TokenOutput{SetCookie: server.cookie(role, tokens.Refresh, tokens.RefreshTTL), Body: tokens}
}

func (server *Server) registerAuth() {
	register(
		server,
		server.op("POST", "/api/v1/auth/login", "user-login", "Sign in", ""),
		func(ctx context.Context, input *LoginInput) (*TokenOutput, error) {
			tokens, err := server.Auth.Login(ctx, input.Body)
			if err != nil {
				return nil, err
			}
			return server.tokens(auth.User, tokens), nil
		},
	)
	register(
		server,
		server.op("POST", "/api/v1/admin/auth/login", "admin-login", "Administrator password step", ""),
		func(ctx context.Context, input *LoginInput) (*ChallengeOutput, error) {
			id, err := server.Auth.AdminLogin(ctx, input.Body)
			if err != nil {
				return nil, err
			}
			output := &ChallengeOutput{}
			output.Body.ChallengeID = id
			output.Body.ExpiresIn = int64(server.Config.Auth.OTPTTL.Seconds())
			return output, nil
		},
	)
	register(
		server,
		server.op("POST", "/api/v1/admin/auth/otp", "admin-otp", "Complete administrator sign-in", ""),
		func(ctx context.Context, input *OTPInput) (*TokenOutput, error) {
			tokens, err := server.Auth.VerifyOTP(ctx, input.Body.ChallengeID, input.Body.Code)
			if err != nil {
				return nil, err
			}
			return server.tokens(auth.Admin, tokens), nil
		},
	)
	for _, role := range []auth.Role{auth.User, auth.Admin} {
		base := "/api/v1/auth"
		if role == auth.Admin {
			base = "/api/v1/admin/auth"
		}
		tag := string(role)
		register(
			server,
			server.op("GET", base+"/csrf", tag+"-csrf", "Get session-bound CSRF token", ""),
			func(ctx context.Context, input *RefreshInput) (*struct {
				Body struct {
					Token string `json:"csrf_token"`
				}
			}, error,
			) {
				value, err := server.Auth.CSRF(ctx, role, input.cookie(role))
				if err != nil {
					return nil, err
				}
				output := &struct {
					Body struct {
						Token string `json:"csrf_token"`
					}
				}{}
				output.Body.Token = value
				return output, nil
			},
		)
		register(
			server,
			server.op("POST", base+"/refresh", tag+"-refresh", "Rotate refresh token", ""),
			func(ctx context.Context, input *RefreshInput) (*TokenOutput, error) {
				tokens, err := server.Auth.Refresh(ctx, role, input.cookie(role), input.CSRF)
				if err != nil {
					return nil, err
				}
				return server.tokens(role, tokens), nil
			},
		)
		register(
			server,
			server.op("POST", base+"/logout", tag+"-logout", "Revoke current session", ""),
			func(ctx context.Context, input *RefreshInput) (*struct {
				SetCookie string `header:"Set-Cookie"`
				Body      struct {
					Message string `json:"message"`
				}
			}, error,
			) {
				if err := server.Auth.Logout(ctx, role, input.cookie(role), input.CSRF); err != nil {
					return nil, err
				}
				output := &struct {
					SetCookie string `header:"Set-Cookie"`
					Body      struct {
						Message string `json:"message"`
					}
				}{SetCookie: server.cookie(role, "", 0)}
				output.Body.Message = "signed out"
				return output, nil
			},
		)
		register(
			server,
			server.op("POST", base+"/password/forgot", tag+"-forgot-password", "Request password reset", ""),
			func(ctx context.Context, input *struct{ Body auth.EmailInput }) (*MessageOutput, error) {
				if err := server.Auth.ForgotPassword(ctx, role, input.Body.Email); err != nil {
					return nil, err
				}
				return message("if the account exists, a reset email will be sent"), nil
			},
		)
		register(
			server,
			server.op(
				"POST",
				base+"/password/reset",
				tag+"-reset-password",
				"Reset password and revoke all sessions",
				"",
			),
			func(ctx context.Context, input *struct {
				Body struct {
					auth.NewPassword
					Token string `json:"token" minLength:"43" maxLength:"43"`
				}
			},
			) (*MessageOutput, error) {
				if err := server.Auth.ResetPassword(ctx, role, input.Body.Token, input.Body.Password); err != nil {
					return nil, err
				}
				return message("password updated; sign in again"), nil
			},
		)
		me := "/api/v1/me"
		if role == auth.Admin {
			me = "/api/v1/admin/me"
		}
		register(
			server,
			server.op("GET", me, tag+"-me", "Get current account", role),
			func(ctx context.Context, input *struct{}) (*struct{ Body auth.PublicAccount }, error) {
				return &struct{ Body auth.PublicAccount }{Body: auth.Current(ctx).Account}, nil
			},
		)
		register(
			server,
			server.op("POST", me+"/password", tag+"-change-password", "Change password and revoke all sessions", role),
			func(ctx context.Context, input *struct {
				Body struct {
					auth.NewPassword
					CurrentPassword string `json:"current_password" minLength:"6" maxLength:"128" writeOnly:"true"`
				}
			},
			) (*MessageOutput, error) {
				principal := auth.Current(ctx)
				if err := server.Auth.Limit(ctx, "password-change:"+principal.ID.String(), 5, time.Minute); err != nil {
					return nil, err
				}
				err := server.Auth.ChangePassword(ctx, principal, input.Body.CurrentPassword, input.Body.Password)
				if err != nil {
					return nil, err
				}
				return message("password updated; sign in again"), nil
			},
		)
	}
}
