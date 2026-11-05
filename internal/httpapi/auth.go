package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"

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

func (i RefreshInput) cookie(role auth.Role) string {
	if role == auth.Admin {
		return i.AdminCookie
	}
	return i.UserCookie
}

// MessageOutput возвращает краткий результат операции без раскрытия внутренних данных.
type MessageOutput struct {
	Body struct {
		Message string `json:"message"`
	}
}

func message(text string) *MessageOutput {
	v := &MessageOutput{}
	v.Body.Message = text
	return v
}

func (s *Server) cookie(role auth.Role, token string, ttl time.Duration) string {
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
		Secure:   s.Config.Auth.CookieSecure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   age,
	}).String()
}

func (s *Server) tokens(role auth.Role, t auth.Tokens) *TokenOutput {
	return &TokenOutput{SetCookie: s.cookie(role, t.Refresh, t.RefreshTTL), Body: t}
}

func (s *Server) registerAuth() {
	huma.Register(
		s.API,
		s.op("POST", "/api/v1/auth/login", "user-login", "Sign in", ""),
		func(ctx context.Context, i *LoginInput) (*TokenOutput, error) {
			t, e := s.Auth.Login(ctx, i.Body)
			if e != nil {
				return nil, e
			}
			return s.tokens(auth.User, t), nil
		},
	)
	huma.Register(
		s.API,
		s.op("POST", "/api/v1/admin/auth/login", "admin-login", "Administrator password step", ""),
		func(ctx context.Context, i *LoginInput) (*ChallengeOutput, error) {
			id, e := s.Auth.AdminLogin(ctx, i.Body)
			if e != nil {
				return nil, e
			}
			o := &ChallengeOutput{}
			o.Body.ChallengeID = id
			o.Body.ExpiresIn = int64(s.Config.Auth.OTPTTL.Seconds())
			return o, nil
		},
	)
	huma.Register(
		s.API,
		s.op("POST", "/api/v1/admin/auth/otp", "admin-otp", "Complete administrator sign-in", ""),
		func(ctx context.Context, i *OTPInput) (*TokenOutput, error) {
			t, e := s.Auth.VerifyOTP(ctx, i.Body.ChallengeID, i.Body.Code)
			if e != nil {
				return nil, e
			}
			return s.tokens(auth.Admin, t), nil
		},
	)
	for _, role := range []auth.Role{auth.User, auth.Admin} {
		base := "/api/v1/auth"
		if role == auth.Admin {
			base = "/api/v1/admin/auth"
		}
		tag := string(role)
		huma.Register(
			s.API,
			s.op("GET", base+"/csrf", tag+"-csrf", "Get session-bound CSRF token", ""),
			func(ctx context.Context, i *RefreshInput) (*struct {
				Body struct {
					Token string `json:"csrf_token"`
				}
			}, error,
			) {
				v, e := s.Auth.CSRF(ctx, role, i.cookie(role))
				if e != nil {
					return nil, e
				}
				o := &struct {
					Body struct {
						Token string `json:"csrf_token"`
					}
				}{}
				o.Body.Token = v
				return o, nil
			},
		)
		huma.Register(
			s.API,
			s.op("POST", base+"/refresh", tag+"-refresh", "Rotate refresh token", ""),
			func(ctx context.Context, i *RefreshInput) (*TokenOutput, error) {
				t, e := s.Auth.Refresh(ctx, role, i.cookie(role), i.CSRF)
				if e != nil {
					return nil, e
				}
				return s.tokens(role, t), nil
			},
		)
		huma.Register(
			s.API,
			s.op("POST", base+"/logout", tag+"-logout", "Revoke current session", ""),
			func(ctx context.Context, i *RefreshInput) (*struct {
				SetCookie string `header:"Set-Cookie"`
				Body      struct {
					Message string `json:"message"`
				}
			}, error,
			) {
				if e := s.Auth.Logout(ctx, role, i.cookie(role), i.CSRF); e != nil {
					return nil, e
				}
				o := &struct {
					SetCookie string `header:"Set-Cookie"`
					Body      struct {
						Message string `json:"message"`
					}
				}{SetCookie: s.cookie(role, "", 0)}
				o.Body.Message = "signed out"
				return o, nil
			},
		)
		huma.Register(
			s.API,
			s.op("POST", base+"/password/forgot", tag+"-forgot-password", "Request password reset", ""),
			func(ctx context.Context, i *struct{ Body auth.EmailInput }) (*MessageOutput, error) {
				if e := s.Auth.ForgotPassword(ctx, role, i.Body.Email); e != nil {
					return nil, e
				}
				return message("if the account exists, a reset email will be sent"), nil
			},
		)
		huma.Register(
			s.API,
			s.op("POST", base+"/password/reset", tag+"-reset-password", "Reset password and revoke all sessions", ""),
			func(ctx context.Context, i *struct {
				Body struct {
					auth.NewPassword
					Token string `json:"token" minLength:"43" maxLength:"43"`
				}
			},
			) (*MessageOutput, error) {
				if e := s.Auth.ResetPassword(ctx, role, i.Body.Token, i.Body.Password); e != nil {
					return nil, e
				}
				return message("password updated; sign in again"), nil
			},
		)
		me := "/api/v1/me"
		if role == auth.Admin {
			me = "/api/v1/admin/me"
		}
		huma.Register(
			s.API,
			s.op("GET", me, tag+"-me", "Get current account", role),
			func(ctx context.Context, i *struct{}) (*struct{ Body auth.PublicAccount }, error) {
				return &struct{ Body auth.PublicAccount }{Body: auth.Current(ctx).Account}, nil
			},
		)
		huma.Register(
			s.API,
			s.op("POST", me+"/password", tag+"-change-password", "Change password and revoke all sessions", role),
			func(ctx context.Context, i *struct {
				Body struct {
					auth.NewPassword
					CurrentPassword string `json:"current_password" minLength:"6" maxLength:"128" writeOnly:"true"`
				}
			},
			) (*MessageOutput, error) {
				p := auth.Current(ctx)
				if e := s.Auth.Limit(ctx, "password-change:"+p.ID.String(), 5, time.Minute); e != nil {
					return nil, e
				}
				if e := s.Auth.ChangePassword(ctx, p, i.Body.CurrentPassword, i.Body.Password); e != nil {
					return nil, e
				}
				return message("password updated; sign in again"), nil
			},
		)
	}
}
