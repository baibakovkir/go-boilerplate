package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"go-boilerplate/internal/config"
	"go-boilerplate/internal/database"
	"go-boilerplate/internal/database/dbgen"
	appmail "go-boilerplate/internal/mail"
	"go-boilerplate/internal/security"
)

// Role различает пользовательские и административные учётные записи.
type Role string

const (
	// User обозначает обычного пользователя, создаваемого импортом.
	User Role = Role(dbgen.ActorTypeUser)
	// Admin обозначает администратора с обязательным подтверждением входа через OTP.
	Admin Role = Role(dbgen.ActorTypeAdmin)
)

// PublicAccount содержит только поля аккаунта, разрешённые для ответа API.
type PublicAccount struct {
	ID        uuid.UUID `json:"id"`
	Email     string    `json:"email"`
	Blocked   bool      `json:"blocked"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Account дополняет публичные данные хешем пароля и версией отзыва сессий.
type Account struct {
	PublicAccount
	PasswordHash string
	Version      int64
}

// Principal описывает проверенную сессию и её владельца в контексте запроса.
type Principal struct {
	ID        uuid.UUID
	Role      Role
	SessionID string
	Version   int64
	Account   PublicAccount
	ExpiresAt time.Time
}
type principalKey struct{}

// WithPrincipal добавляет проверенного владельца сессии в контекст запроса.
func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// Current возвращает владельца сессии или nil, если middleware ещё не проверил авторизацию.
func Current(ctx context.Context) *Principal {
	p, _ := ctx.Value(principalKey{}).(*Principal)
	return p
}

// Tokens содержит результат входа; refresh передаётся только через HttpOnly cookie.
type Tokens struct {
	AccessToken string        `json:"access_token"`
	TokenType   string        `json:"token_type"`
	ExpiresIn   int64         `json:"expires_in"`
	CSRFToken   string        `json:"csrf_token"`
	Refresh     string        `json:"-"`
	RefreshTTL  time.Duration `json:"-"`
}

// Claims связывает access JWT с ролью, сессией и версией авторизации аккаунта.
type Claims struct {
	Role      Role   `json:"role"`
	SessionID string `json:"sid"`
	Version   int64  `json:"ver"`
	jwt.RegisteredClaims
}

// Service связывает аккаунты PostgreSQL, сессии Redis и отправку писем.
type Service struct {
	DB     *database.Store
	Redis  *redis.Client
	Config *config.Config
	Mail   appmail.Sender
	Log    *slog.Logger
	dummy  string
}

// New создаёт сервис авторизации и заранее подготавливает хеш для проверки неизвестного аккаунта.
func New(db *database.Store, r *redis.Client, c *config.Config, m appmail.Sender, l *slog.Logger) *Service {
	dummy, _ := security.HashPassword(security.RandomToken())
	return &Service{db, r, c, m, l, dummy}
}
func (s *Service) key(suffix string) string { return s.Config.Redis.Prefix + suffix }

// Digest возвращает SHA-256 секрета для хранения и поиска без записи исходного токена.
func Digest(s string) string {
	v := sha256.Sum256([]byte(s))
	return hex.EncodeToString(v[:])
}

// Failure записывает внутреннюю причину с request ID и возвращает клиенту обезличенную ошибку 503.
func (s *Service) Failure(ctx context.Context, op string, e error) error {
	s.Log.ErrorContext(ctx, op, "request_id", RequestID(ctx), "error", e)
	return huma.Error503ServiceUnavailable("service temporarily unavailable")
}

// Account загружает аккаунт из таблицы, соответствующей указанной роли.
func (s *Service) Account(ctx context.Context, role Role, id uuid.UUID) (Account, error) {
	if role == Admin {
		v, e := s.DB.Q.GetAdmin(ctx, id)
		return Account{
			PublicAccount{v.ID, v.Email, v.Blocked, v.CreatedAt, v.UpdatedAt},
			v.PasswordHash,
			v.AuthVersion,
		}, e
	}
	v, e := s.DB.Q.GetUser(ctx, id)
	return Account{PublicAccount{v.ID, v.Email, v.Blocked, v.CreatedAt, v.UpdatedAt}, v.PasswordHash, v.AuthVersion}, e
}

func (s *Service) accountByEmail(ctx context.Context, role Role, email string) (Account, error) {
	if role == Admin {
		v, e := s.DB.Q.GetAdminByEmail(ctx, email)
		return Account{
			PublicAccount{v.ID, v.Email, v.Blocked, v.CreatedAt, v.UpdatedAt},
			v.PasswordHash,
			v.AuthVersion,
		}, e
	}
	v, e := s.DB.Q.GetUserByEmail(ctx, email)
	return Account{PublicAccount{v.ID, v.Email, v.Blocked, v.CreatedAt, v.UpdatedAt}, v.PasswordHash, v.AuthVersion}, e
}

// Audit записывает событие аудита; сбой записи отражается в техническом логе.
func (s *Service) Audit(ctx context.Context,
	role Role,
	id string,
	action dbgen.AuditAction,
	object string,
	result dbgen.AuditResult,
) {
	if e := s.DB.Q.InsertAudit(ctx,
		dbgen.InsertAuditParams{
			ActorType: dbgen.ActorType(role),
			ActorID:   id,
			Action:    action,
			ObjectID:  object,
			Result:    result,
			RequestID: RequestID(ctx),
		}); e != nil {
		s.Log.ErrorContext(ctx, "write audit", "error", e, "action", action)
	}
}

type requestIDKey struct{}

// WithRequestID сохраняет идентификатор запроса для связанных логов и аудита.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID возвращает идентификатор запроса или пустую строку вне HTTP-контекста.
func RequestID(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey{}).(string)
	return v
}

// Limit проверяет лимит Redis; при недоступности хранилища операция не допускается.
func (s *Service) Limit(ctx context.Context, key string, n int, window time.Duration) error {
	allowed, e := security.Allow(ctx, s.Redis, s.Config.Redis.Prefix, key, n, window)
	if e != nil {
		return s.Failure(ctx, "rate limiter", e)
	}
	if !allowed {
		return huma.Error429TooManyRequests("too many attempts; try again later")
	}
	return nil
}

func (s *Service) checkLogin(ctx context.Context, role Role, c Credentials) (Account, error) {
	email := NormalizeEmail(c.Email)
	if e := s.Limit(ctx, "login:"+string(role)+":"+email, s.Config.Auth.LoginLimit, 15*time.Minute); e != nil {
		return Account{}, e
	}
	a, e := s.accountByEmail(ctx, role, email)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return Account{}, s.Failure(ctx, "find account", e)
	}
	hash := a.PasswordHash
	if e != nil {
		hash = s.dummy
	}
	valid := security.CheckPassword(hash, c.Password)
	if e != nil || !valid || a.Blocked {
		s.Audit(ctx, role, "", "auth.login", "", "denied")
		return Account{}, huma.Error401Unauthorized("invalid credentials")
	}
	return a, nil
}

// Login проверяет пароль пользователя и создаёт новую сессию.
func (s *Service) Login(ctx context.Context, c Credentials) (Tokens, error) {
	a, e := s.checkLogin(ctx, User, c)
	if e != nil {
		return Tokens{}, e
	}
	return s.issue(ctx, User, a)
}

// AdminLogin проверяет пароль администратора и отправляет OTP без выдачи access JWT.
func (s *Service) AdminLogin(ctx context.Context, c Credentials) (string, error) {
	a, e := s.checkLogin(ctx, Admin, c)
	if e != nil {
		return "", e
	}
	if e = s.Limit(ctx, "otp:send:"+a.ID.String(), 1, time.Minute); e != nil {
		return "", e
	}
	n, e := rand.Int(rand.Reader, big.NewInt(1000000))
	if e != nil {
		return "", s.Failure(ctx, "generate otp", e)
	}
	code := fmt.Sprintf("%06d", n.Int64())
	id := security.RandomToken()
	k := s.key("challenge:" + Digest(id))
	ttl := s.Config.Auth.OTPTTL
	e = s.Redis.HSet(ctx,
		k,
		"hash",
		s.otpHash(code),
		"id",
		a.ID.String(),
		"version",
		a.Version,
		"attempts",
		s.Config.Auth.OTPAttempts).
		Err()
	if e == nil {
		e = s.Redis.Expire(ctx, k, ttl).Err()
	}
	if e != nil {
		return "", s.Failure(ctx, "store otp", e)
	}
	message, e := appmail.SignInMessage(code, ttl)
	if e == nil {
		e = s.Mail.Send(ctx, a.Email, "Administrator sign-in code", message)
	}
	if e != nil {
		s.Redis.Del(ctx, k)
		return "", s.Failure(ctx, "send otp", e)
	}
	return id, nil
}

func (s *Service) otpHash(code string) string {
	h := hmac.New(sha256.New, []byte(s.Config.Auth.JWTSecret))
	h.Write([]byte("otp:" + code))
	return hex.EncodeToString(h.Sum(nil))
}

var consumeOTP = redis.NewScript(`
 if redis.call('EXISTS',KEYS[1])==0 then return {} end
 if redis.call('HGET',KEYS[1],'hash')~=ARGV[1] then
  if redis.call('HINCRBY',KEYS[1],'attempts',-1)<=0 then redis.call('DEL',KEYS[1]) end
  return {}
 end
 local id=redis.call('HGET',KEYS[1],'id');
 local ver=redis.call('HGET',KEYS[1],'version');
 redis.call('DEL',KEYS[1]);return {id,ver}
`)

// VerifyOTP одноразово погашает OTP и создаёт сессию после проверки актуальности аккаунта.
func (s *Service) VerifyOTP(ctx context.Context, id, code string) (Tokens, error) {
	v, e := consumeOTP.Run(ctx, s.Redis, []string{s.key("challenge:" + Digest(id))}, s.otpHash(code)).StringSlice()
	if e != nil {
		return Tokens{}, s.Failure(ctx, "verify otp", e)
	}
	if len(v) != 2 {
		return Tokens{}, huma.Error401Unauthorized("invalid or expired code")
	}
	uid, e := uuid.Parse(v[0])
	if e != nil {
		return Tokens{}, huma.Error401Unauthorized("invalid challenge")
	}
	a, e := s.Account(ctx, Admin, uid)
	if e != nil {
		return Tokens{}, s.Failure(ctx, "load admin", e)
	}
	if a.Blocked || strconv.FormatInt(a.Version, 10) != v[1] {
		return Tokens{}, huma.Error401Unauthorized("challenge is no longer valid")
	}
	return s.issue(ctx, Admin, a)
}

func (s *Service) ttl(role Role) (time.Duration, time.Duration) {
	if role == Admin {
		return s.Config.Auth.AdminAccessTTL, s.Config.Auth.AdminRefreshTTL
	}
	return s.Config.Auth.UserAccessTTL, s.Config.Auth.UserRefreshTTL
}

func (s *Service) issue(ctx context.Context, role Role, a Account) (Tokens, error) {
	_, ttl := s.ttl(role)
	sid := uuid.NewString()
	secret := security.RandomToken()
	csrf := security.RandomToken()
	k := s.key("session:" + sid)
	pipe := s.Redis.TxPipeline()
	pipe.HSet(
		ctx,
		k,
		"id",
		a.ID.String(),
		"role",
		string(role),
		"version",
		a.Version,
		"hash",
		Digest(secret),
		"csrf",
		csrf,
	)
	pipe.Expire(ctx, k, ttl)
	if _, e := pipe.Exec(ctx); e != nil {
		return Tokens{}, s.Failure(ctx, "create session", e)
	}
	t, e := s.sign(role, a, sid, csrf, sid+"."+secret, ttl)
	if e != nil {
		s.Redis.Del(ctx, k)
		return Tokens{}, s.Failure(ctx, "sign token", e)
	}
	s.Audit(ctx, role, a.ID.String(), "auth.login", sid, "success")
	return t, nil
}

func (s *Service) sign(role Role, a Account, sid, csrf, refresh string, ttl time.Duration) (Tokens, error) {
	accessTTL, _ := s.ttl(role)
	now := time.Now()
	if ttl < accessTTL {
		accessTTL = ttl
	}
	claims := Claims{
		role,
		sid,
		a.Version,
		jwt.RegisteredClaims{
			Issuer:    s.Config.Auth.Issuer,
			Subject:   a.ID.String(),
			Audience:  jwt.ClaimStrings{string(role)},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(accessTTL)),
			ID:        uuid.NewString(),
		},
	}
	token, e := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(s.Config.Auth.JWTSecret))
	return Tokens{token, "Bearer", int64(accessTTL.Seconds()), csrf, refresh, ttl}, e
}

func parseRefresh(raw string) (string, string, error) {
	p := strings.Split(raw, ".")
	if len(p) != 2 || len(p[1]) != 43 {
		return "", "", huma.Error401Unauthorized("invalid refresh token")
	}
	if _, e := uuid.Parse(p[0]); e != nil {
		return "", "", huma.Error401Unauthorized("invalid refresh token")
	}
	return p[0], p[1], nil
}

func (s *Service) refreshSession(
	ctx context.Context,
	role Role,
	raw string,
) (string, string, map[string]string, error) {
	sid, secret, e := parseRefresh(raw)
	if e != nil {
		return "", "", nil, e
	}
	v, e := s.Redis.HGetAll(ctx, s.key("session:"+sid)).Result()
	if e != nil {
		return "", "", nil, s.Failure(ctx, "read session", e)
	}
	if v["role"] != string(role) {
		return "", "", nil, huma.Error401Unauthorized("invalid session")
	}
	return sid, secret, v, nil
}

var rotate = redis.NewScript(`
 if redis.call('EXISTS',KEYS[1])==0 then return -1 end
 if redis.call('HGET',KEYS[1],'hash')==ARGV[1] then
  redis.call('HSET',KEYS[1],'used:'..ARGV[1],1,'hash',ARGV[2]);return redis.call('PTTL',KEYS[1])
 end
 if redis.call('HEXISTS',KEYS[1],'used:'..ARGV[1])==1 then redis.call('DEL',KEYS[1]) end
 return -1
`)

// Refresh атомарно заменяет refresh; повторное использование старого секрета отзывает сессию.
func (s *Service) Refresh(ctx context.Context, role Role, raw, csrf string) (Tokens, error) {
	sid, secret, v, e := s.refreshSession(ctx, role, raw)
	if e != nil {
		return Tokens{}, e
	}
	if csrf == "" || !hmac.Equal([]byte(v["csrf"]), []byte(csrf)) {
		return Tokens{}, huma.Error403Forbidden("invalid CSRF token")
	}
	uid, e := uuid.Parse(v["id"])
	if e != nil {
		return Tokens{}, huma.Error401Unauthorized("invalid session")
	}
	a, e := s.Account(ctx, role, uid)
	if e != nil {
		if errors.Is(e, pgx.ErrNoRows) {
			return Tokens{}, huma.Error401Unauthorized("invalid session")
		}
		return Tokens{}, s.Failure(ctx, "refresh account", e)
	}
	if a.Blocked || strconv.FormatInt(a.Version, 10) != v["version"] {
		s.Redis.Del(ctx, s.key("session:"+sid))
		return Tokens{}, huma.Error401Unauthorized("session revoked")
	}
	next := security.RandomToken()
	ms, e := rotate.Run(ctx, s.Redis, []string{s.key("session:" + sid)}, Digest(secret), Digest(next)).Int64()
	if e != nil {
		return Tokens{}, s.Failure(ctx, "rotate refresh", e)
	}
	if ms <= 0 {
		return Tokens{}, huma.Error401Unauthorized("invalid or reused refresh token")
	}
	t, e := s.sign(role, a, sid, v["csrf"], sid+"."+next, time.Duration(ms)*time.Millisecond)
	if e != nil {
		return Tokens{}, s.Failure(ctx, "sign refresh", e)
	}
	return t, nil
}

// CSRF возвращает привязанный к сессии CSRF-токен после проверки текущего refresh.
func (s *Service) CSRF(ctx context.Context, role Role, raw string) (string, error) {
	_, secret, v, e := s.refreshSession(ctx, role, raw)
	if e != nil {
		return "", e
	}
	if !hmac.Equal([]byte(Digest(secret)), []byte(v["hash"])) {
		return "", huma.Error401Unauthorized("invalid refresh token")
	}
	return v["csrf"], nil
}

// Logout проверяет refresh и CSRF, затем отзывает текущую сессию.
func (s *Service) Logout(ctx context.Context, role Role, raw, csrf string) error {
	sid, secret, v, e := s.refreshSession(ctx, role, raw)
	if e != nil {
		return e
	}
	if csrf == "" || !hmac.Equal([]byte(csrf), []byte(v["csrf"])) {
		return huma.Error403Forbidden("invalid CSRF token")
	}
	if !hmac.Equal([]byte(Digest(secret)), []byte(v["hash"])) {
		return huma.Error401Unauthorized("invalid refresh token")
	}
	if e = s.Redis.Del(ctx, s.key("session:"+sid)).Err(); e != nil {
		return s.Failure(ctx, "logout", e)
	}
	s.Audit(ctx, role, v["id"], "auth.logout", sid, "success")
	return nil
}

// Authenticate проверяет JWT, сессию Redis и текущую версию аккаунта в PostgreSQL.
func (s *Service) Authenticate(ctx context.Context, raw string, role Role) (*Principal, error) {
	claims := &Claims{}
	_, e := jwt.ParseWithClaims(
		raw,
		claims,
		func(t *jwt.Token) (any, error) { return []byte(s.Config.Auth.JWTSecret), nil },
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithIssuer(s.Config.Auth.Issuer),
		jwt.WithAudience(string(role)),
		jwt.WithExpirationRequired(),
	)
	if e != nil || claims.Role != role {
		return nil, huma.Error401Unauthorized("invalid access token")
	}
	id, e := uuid.Parse(claims.Subject)
	if e != nil {
		return nil, huma.Error401Unauthorized("invalid access token")
	}
	v, e := s.Redis.HGetAll(ctx, s.key("session:"+claims.SessionID)).Result()
	if e != nil {
		return nil, s.Failure(ctx, "validate session", e)
	}
	if v["id"] != id.String() || v["role"] != string(role) || v["version"] != strconv.FormatInt(claims.Version, 10) {
		return nil, huma.Error401Unauthorized("session revoked")
	}
	a, e := s.Account(ctx, role, id)
	if e != nil {
		if errors.Is(e, pgx.ErrNoRows) {
			return nil, huma.Error401Unauthorized("account unavailable")
		}
		return nil, s.Failure(ctx, "validate account", e)
	}
	if a.Blocked || a.Version != claims.Version {
		return nil, huma.Error401Unauthorized("session revoked")
	}
	return &Principal{
		ID:        id,
		Role:      role,
		SessionID: claims.SessionID,
		Version:   a.Version,
		Account:   a.PublicAccount,
		ExpiresAt: claims.ExpiresAt.Time,
	}, nil
}

// ChangePassword проверяет текущий пароль и меняет его с отзывом всех сессий аккаунта.
func (s *Service) ChangePassword(ctx context.Context, p *Principal, current, next string) error {
	a, e := s.Account(ctx, p.Role, p.ID)
	if e != nil {
		return s.Failure(ctx, "load password", e)
	}
	if !security.CheckPassword(a.PasswordHash, current) {
		return huma.Error400BadRequest("current password is incorrect")
	}
	return s.setPassword(ctx, p.Role, a, next, "account.password_changed")
}

func (s *Service) setPassword(ctx context.Context,
	role Role,
	a Account,
	password string,
	action dbgen.AuditAction,
) error {
	hash, e := security.HashPassword(password)
	if e != nil {
		return s.Failure(ctx, "hash password", e)
	}
	tx, e := s.DB.Pool.Begin(ctx)
	if e != nil {
		return s.Failure(ctx, "begin password update", e)
	}
	defer tx.Rollback(ctx)
	q := dbgen.New(tx)
	var n int64
	if role == Admin {
		n, e = q.ChangeAdminPassword(
			ctx,
			dbgen.ChangeAdminPasswordParams{ID: a.ID, PasswordHash: hash, AuthVersion: a.Version},
		)
	} else {
		n,
			e = q.ChangeUserPassword(ctx,
			dbgen.ChangeUserPasswordParams{
				ID:           a.ID,
				PasswordHash: hash,
				AuthVersion:  a.Version,
			})
	}
	if e != nil {
		return s.Failure(ctx, "update password", e)
	}
	if n != 1 {
		return huma.Error409Conflict("account changed; try again")
	}
	if e = q.InsertAudit(ctx,
		dbgen.InsertAuditParams{
			ActorType: dbgen.ActorType(role),
			ActorID:   a.ID.String(),
			Action:    action,
			ObjectID:  a.ID.String(),
			Result:    "success",
			RequestID: RequestID(ctx),
		}); e != nil {
		return s.Failure(ctx, "audit password", e)
	}
	if e = tx.Commit(ctx); e != nil {
		return s.Failure(ctx, "commit password", e)
	}
	return nil
}

// ForgotPassword отправляет одноразовый токен восстановления; отсутствие аккаунта не раскрывается.
func (s *Service) ForgotPassword(ctx context.Context, role Role, email string) error {
	email = NormalizeEmail(email)
	if e := s.Limit(ctx, "reset:"+string(role)+":"+email, s.Config.Auth.EmailLimit, 15*time.Minute); e != nil {
		return e
	}
	a, e := s.accountByEmail(ctx, role, email)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil
	}
	if e != nil {
		return s.Failure(ctx, "find reset account", e)
	}
	if a.Blocked {
		return nil
	}
	token := security.RandomToken()
	value := a.ID.String() + ":" + strconv.FormatInt(a.Version, 10)
	key := s.key("reset:" + string(role) + ":" + Digest(token))
	if e = s.Redis.Set(ctx, key, value, s.Config.Auth.ResetTTL).Err(); e != nil {
		return s.Failure(ctx, "store reset", e)
	}
	message, e := appmail.PasswordResetMessage(token, s.Config.Auth.ResetTTL)
	if e == nil {
		e = s.Mail.Send(ctx, a.Email, "Password reset", message)
	}
	if e != nil {
		s.Redis.Del(ctx, key)
		return s.Failure(ctx, "send reset", e)
	}
	return nil
}

// ResetPassword погашает токен восстановления и меняет пароль с отзывом всех сессий.
func (s *Service) ResetPassword(ctx context.Context, role Role, token, password string) error {
	v, e := s.Redis.GetDel(ctx, s.key("reset:"+string(role)+":"+Digest(token))).Result()
	if errors.Is(e, redis.Nil) {
		return huma.Error400BadRequest("invalid or expired reset token")
	}
	if e != nil {
		return s.Failure(ctx, "consume reset", e)
	}
	p := strings.Split(v, ":")
	if len(p) != 2 {
		return huma.Error400BadRequest("invalid reset token")
	}
	id, e := uuid.Parse(p[0])
	if e != nil {
		return huma.Error400BadRequest("invalid reset token")
	}
	a, e := s.Account(ctx, role, id)
	if e != nil {
		return s.Failure(ctx, "load reset account", e)
	}
	if a.Blocked || strconv.FormatInt(a.Version, 10) != p[1] {
		return huma.Error400BadRequest("reset token revoked")
	}
	return s.setPassword(ctx, role, a, password, "account.password_reset")
}

// Seed создаёт отсутствующих администраторов из настроек, сохраняя пароли существующих.
func Seed(ctx context.Context, db *database.Store, admins []config.AdminConfig) error {
	for _, a := range admins {
		a.Email = NormalizeEmail(a.Email)
		if !ValidateCredentials(Credentials{a.Email, a.Password}) {
			return fmt.Errorf("invalid configured administrator credentials")
		}
		_, e := db.Q.GetAdminByEmail(ctx, a.Email)
		if e == nil {
			continue
		}
		if !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		hash, e := security.HashPassword(a.Password)
		if e != nil {
			return e
		}
		if e = db.Q.CreateAdmin(ctx,
			dbgen.CreateAdminParams{
				ID:           uuid.New(),
				Email:        a.Email,
				PasswordHash: hash,
			}); e != nil {
			return e
		}
	}
	return nil
}

// ValidatePrincipal повторно проверяет срок и отзыв сессии для длительного WebSocket-соединения.
func (s *Service) ValidatePrincipal(ctx context.Context, p *Principal) error {
	if p == nil || time.Now().After(p.ExpiresAt) {
		return huma.Error401Unauthorized("access token expired")
	}
	v, e := s.Redis.HGetAll(ctx, s.key("session:"+p.SessionID)).Result()
	if e != nil {
		return s.Failure(ctx, "validate realtime session", e)
	}
	if v["id"] != p.ID.String() || v["role"] != string(p.Role) || v["version"] != strconv.FormatInt(p.Version, 10) {
		return huma.Error401Unauthorized("session revoked")
	}
	a, e := s.Account(ctx, p.Role, p.ID)
	if e != nil {
		return s.Failure(ctx, "validate realtime account", e)
	}
	if a.Blocked || a.Version != p.Version {
		return huma.Error401Unauthorized("session revoked")
	}
	return nil
}
