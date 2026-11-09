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

	"go-boilerplate/internal/apperror"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	appconfig "go-boilerplate/internal/config"
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
func WithPrincipal(ctx context.Context, principal *Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

// Current возвращает владельца сессии или nil, если middleware ещё не проверил авторизацию.
func Current(ctx context.Context) *Principal {
	principal, _ := ctx.Value(principalKey{}).(*Principal)
	return principal
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
	db     *database.Store
	redis  *redis.Client
	config *appconfig.Config
	mail   appmail.Sender
	log    *slog.Logger
	dummy  string
}

// New создаёт сервис авторизации и заранее подготавливает хеш для проверки неизвестного аккаунта.
func New(
	db *database.Store,
	redisClient *redis.Client,
	config *appconfig.Config,
	message appmail.Sender,
	logger *slog.Logger,
) *Service {
	dummy, _ := security.HashPassword(security.RandomToken())
	return &Service{db, redisClient, config, message, logger, dummy}
}
func (service *Service) key(suffix string) string { return service.config.Redis.Prefix + suffix }

// Digest возвращает SHA-256 секрета для хранения и поиска без записи исходного токена.
func Digest(value string) string {
	digestBytes := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digestBytes[:])
}

// Failure сохраняет причину для логирования на границе приложения.
func (service *Service) Failure(ctx context.Context, op string, err error) error {
	return apperror.Failure(op, err)
}

// Account загружает аккаунт из таблицы, соответствующей указанной роли.
func (service *Service) Account(ctx context.Context, role Role, id uuid.UUID) (Account, error) {
	if role == Admin {
		value, err := service.db.Queries.GetAdmin(ctx, id)
		return Account{
			PublicAccount{value.ID, value.Email, value.Blocked, value.CreatedAt, value.UpdatedAt},
			value.PasswordHash,
			value.AuthVersion,
		}, err
	}
	value, err := service.db.Queries.GetUser(ctx, id)
	return Account{
		PublicAccount{value.ID, value.Email, value.Blocked, value.CreatedAt, value.UpdatedAt},
		value.PasswordHash,
		value.AuthVersion,
	}, err
}

func (service *Service) accountByEmail(ctx context.Context, role Role, email string) (Account, error) {
	if role == Admin {
		value, err := service.db.Queries.GetAdminByEmail(ctx, email)
		return Account{
			PublicAccount{value.ID, value.Email, value.Blocked, value.CreatedAt, value.UpdatedAt},
			value.PasswordHash,
			value.AuthVersion,
		}, err
	}
	value, err := service.db.Queries.GetUserByEmail(ctx, email)
	return Account{
		PublicAccount{value.ID, value.Email, value.Blocked, value.CreatedAt, value.UpdatedAt},
		value.PasswordHash,
		value.AuthVersion,
	}, err
}

// Audit записывает событие аудита; сбой записи отражается в техническом логе.
func (service *Service) Audit(ctx context.Context,
	role Role,
	id string,
	action dbgen.AuditAction,
	object string,
	result dbgen.AuditResult,
) {
	if err := service.db.Queries.InsertAudit(ctx,
		dbgen.InsertAuditParams{
			ActorType: dbgen.ActorType(role),
			ActorID:   id,
			Action:    action,
			ObjectID:  object,
			Result:    result,
			RequestID: RequestID(ctx),
		}); err != nil {
		service.log.ErrorContext(ctx, "write audit", "error", err, "action", action)
	}
}

type requestIDKey struct{}

// WithRequestID сохраняет идентификатор запроса для связанных логов и аудита.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID возвращает идентификатор запроса или пустую строку вне HTTP-контекста.
func RequestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)
	return value
}

// Limit проверяет лимит Redis; при недоступности хранилища операция не допускается.
func (service *Service) Limit(ctx context.Context, key string, number int, window time.Duration) error {
	allowed, err := security.Allow(ctx, service.redis, service.config.Redis.Prefix, key, number, window)
	if err != nil {
		return service.Failure(ctx, "rate limiter", err)
	}
	if !allowed {
		return apperror.New(apperror.TooManyRequests, "too many attempts; try again later")
	}
	return nil
}

func (service *Service) checkLogin(ctx context.Context, role Role, credentials Credentials) (Account, error) {
	email := NormalizeEmail(credentials.Email)
	err := service.Limit(ctx, "login:"+string(role)+":"+email, service.config.Auth.LoginLimit, 15*time.Minute)
	if err != nil {
		return Account{}, err
	}
	account, err := service.accountByEmail(ctx, role, email)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Account{}, service.Failure(ctx, "find account", err)
	}
	hash := account.PasswordHash
	if err != nil {
		hash = service.dummy
	}
	valid := security.CheckPassword(hash, credentials.Password)
	if err != nil || !valid || account.Blocked {
		service.Audit(ctx, role, "", "auth.login", "", "denied")
		return Account{}, apperror.New(apperror.Unauthorized, "invalid credentials")
	}
	return account, nil
}

// Login проверяет пароль пользователя и создаёт новую сессию.
func (service *Service) Login(ctx context.Context, credentials Credentials) (Tokens, error) {
	account, err := service.checkLogin(ctx, User, credentials)
	if err != nil {
		return Tokens{}, err
	}
	return service.issue(ctx, User, account)
}

// AdminLogin проверяет пароль администратора и отправляет OTP без выдачи access JWT.
func (service *Service) AdminLogin(ctx context.Context, credentials Credentials) (string, error) {
	account, err := service.checkLogin(ctx, Admin, credentials)
	if err != nil {
		return "", err
	}
	if err = service.Limit(ctx, "otp:send:"+account.ID.String(), 1, time.Minute); err != nil {
		return "", err
	}
	otpNumber, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return "", service.Failure(ctx, "generate otp", err)
	}
	code := fmt.Sprintf("%06d", otpNumber.Int64())
	challengeID := security.RandomToken()
	challengeKey := service.key("challenge:" + Digest(challengeID))
	otpTTL := service.config.Auth.OTPTTL
	err = service.redis.HSet(ctx,
		challengeKey,
		"hash",
		service.otpHash(code),
		"id",
		account.ID.String(),
		"version",
		account.Version,
		"attempts",
		service.config.Auth.OTPAttempts).
		Err()
	if err == nil {
		err = service.redis.Expire(ctx, challengeKey, otpTTL).Err()
	}
	if err != nil {
		return "", service.Failure(ctx, "store otp", err)
	}
	message, err := appmail.SignInMessage(code, otpTTL)
	if err == nil {
		err = service.mail.Send(ctx, account.Email, "Administrator sign-in code", message)
	}
	if err != nil {
		service.redis.Del(ctx, challengeKey)
		return "", service.Failure(ctx, "send otp", err)
	}
	return challengeID, nil
}

func (service *Service) otpHash(code string) string {
	handler := hmac.New(sha256.New, []byte(service.config.Auth.JWTSecret))
	handler.Write([]byte("otp:" + code))
	return hex.EncodeToString(handler.Sum(nil))
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
func (service *Service) VerifyOTP(ctx context.Context, id, code string) (Tokens, error) {
	value, err := consumeOTP.Run(
		ctx, service.redis, []string{service.key("challenge:" + Digest(id))}, service.otpHash(code),
	).
		StringSlice()
	if err != nil {
		return Tokens{}, service.Failure(ctx, "verify otp", err)
	}
	if len(value) != 2 {
		return Tokens{}, apperror.New(apperror.Unauthorized, "invalid or expired code")
	}
	uid, err := uuid.Parse(value[0])
	if err != nil {
		return Tokens{}, apperror.New(apperror.Unauthorized, "invalid challenge")
	}
	account, err := service.Account(ctx, Admin, uid)
	if err != nil {
		return Tokens{}, service.Failure(ctx, "load admin", err)
	}
	if account.Blocked || strconv.FormatInt(account.Version, 10) != value[1] {
		return Tokens{}, apperror.New(apperror.Unauthorized, "challenge is no longer valid")
	}
	return service.issue(ctx, Admin, account)
}

func (service *Service) ttl(role Role) (time.Duration, time.Duration) {
	if role == Admin {
		return service.config.Auth.AdminAccessTTL, service.config.Auth.AdminRefreshTTL
	}
	return service.config.Auth.UserAccessTTL, service.config.Auth.UserRefreshTTL
}

func (service *Service) issue(ctx context.Context, role Role, account Account) (Tokens, error) {
	_, ttl := service.ttl(role)
	sid := uuid.NewString()
	secret := security.RandomToken()
	csrf := security.RandomToken()
	key := service.key("session:" + sid)
	pipe := service.redis.TxPipeline()
	pipe.HSet(
		ctx,
		key,
		"id",
		account.ID.String(),
		"role",
		string(role),
		"version",
		account.Version,
		"hash",
		Digest(secret),
		"csrf",
		csrf,
	)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return Tokens{}, service.Failure(ctx, "create session", err)
	}
	tokens, err := service.sign(role, account, sid, csrf, sid+"."+secret, ttl)
	if err != nil {
		service.redis.Del(ctx, key)
		return Tokens{}, service.Failure(ctx, "sign token", err)
	}
	service.Audit(ctx, role, account.ID.String(), "auth.login", sid, "success")
	return tokens, nil
}

func (service *Service) sign(role Role, account Account, sid, csrf, refresh string, ttl time.Duration) (Tokens, error) {
	accessTTL, _ := service.ttl(role)
	now := time.Now()
	if ttl < accessTTL {
		accessTTL = ttl
	}
	claims := Claims{
		role,
		sid,
		account.Version,
		jwt.RegisteredClaims{
			Issuer:    service.config.Auth.Issuer,
			Subject:   account.ID.String(),
			Audience:  jwt.ClaimStrings{string(role)},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(accessTTL)),
			ID:        uuid.NewString(),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(service.config.Auth.JWTSecret))
	return Tokens{token, "Bearer", int64(accessTTL.Seconds()), csrf, refresh, ttl}, err
}

func parseRefresh(raw string) (string, string, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 2 || len(parts[1]) != 43 {
		return "", "", apperror.New(apperror.Unauthorized, "invalid refresh token")
	}
	if _, err := uuid.Parse(parts[0]); err != nil {
		return "", "", apperror.New(apperror.Unauthorized, "invalid refresh token")
	}
	return parts[0], parts[1], nil
}

func (service *Service) refreshSession(
	ctx context.Context,
	role Role,
	raw string,
) (string, string, map[string]string, error) {
	sid, secret, err := parseRefresh(raw)
	if err != nil {
		return "", "", nil, err
	}
	value, err := service.redis.HGetAll(ctx, service.key("session:"+sid)).Result()
	if err != nil {
		return "", "", nil, service.Failure(ctx, "read session", err)
	}
	if value["role"] != string(role) {
		return "", "", nil, apperror.New(apperror.Unauthorized, "invalid session")
	}
	return sid, secret, value, nil
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
func (service *Service) Refresh(ctx context.Context, role Role, raw, csrf string) (Tokens, error) {
	sid, secret, value, err := service.refreshSession(ctx, role, raw)
	if err != nil {
		return Tokens{}, err
	}
	if csrf == "" || !hmac.Equal([]byte(value["csrf"]), []byte(csrf)) {
		return Tokens{}, apperror.New(apperror.Forbidden, "invalid CSRF token")
	}
	uid, err := uuid.Parse(value["id"])
	if err != nil {
		return Tokens{}, apperror.New(apperror.Unauthorized, "invalid session")
	}
	account, err := service.Account(ctx, role, uid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Tokens{}, apperror.New(apperror.Unauthorized, "invalid session")
		}
		return Tokens{}, service.Failure(ctx, "refresh account", err)
	}
	if account.Blocked || strconv.FormatInt(account.Version, 10) != value["version"] {
		service.redis.Del(ctx, service.key("session:"+sid))
		return Tokens{}, apperror.New(apperror.Unauthorized, "session revoked")
	}
	next := security.RandomToken()
	ms, err := rotate.Run(ctx, service.redis, []string{service.key("session:" + sid)}, Digest(secret), Digest(next)).
		Int64()
	if err != nil {
		return Tokens{}, service.Failure(ctx, "rotate refresh", err)
	}
	if ms <= 0 {
		return Tokens{}, apperror.New(apperror.Unauthorized, "invalid or reused refresh token")
	}
	tokens, err := service.sign(role, account, sid, value["csrf"], sid+"."+next, time.Duration(ms)*time.Millisecond)
	if err != nil {
		return Tokens{}, service.Failure(ctx, "sign refresh", err)
	}
	return tokens, nil
}

// CSRF возвращает привязанный к сессии CSRF-токен после проверки текущего refresh.
func (service *Service) CSRF(ctx context.Context, role Role, raw string) (string, error) {
	_, secret, value, err := service.refreshSession(ctx, role, raw)
	if err != nil {
		return "", err
	}
	if !hmac.Equal([]byte(Digest(secret)), []byte(value["hash"])) {
		return "", apperror.New(apperror.Unauthorized, "invalid refresh token")
	}
	return value["csrf"], nil
}

// Logout проверяет refresh и CSRF, затем отзывает текущую сессию.
func (service *Service) Logout(ctx context.Context, role Role, raw, csrf string) error {
	sid, secret, value, err := service.refreshSession(ctx, role, raw)
	if err != nil {
		return err
	}
	if csrf == "" || !hmac.Equal([]byte(csrf), []byte(value["csrf"])) {
		return apperror.New(apperror.Forbidden, "invalid CSRF token")
	}
	if !hmac.Equal([]byte(Digest(secret)), []byte(value["hash"])) {
		return apperror.New(apperror.Unauthorized, "invalid refresh token")
	}
	if err = service.redis.Del(ctx, service.key("session:"+sid)).Err(); err != nil {
		return service.Failure(ctx, "logout", err)
	}
	service.Audit(ctx, role, value["id"], "auth.logout", sid, "success")
	return nil
}

// Authenticate проверяет JWT, сессию Redis и текущую версию аккаунта в PostgreSQL.
func (service *Service) Authenticate(ctx context.Context, raw string, role Role) (*Principal, error) {
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(
		raw,
		claims,
		func(tokens *jwt.Token) (any, error) { return []byte(service.config.Auth.JWTSecret), nil },
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithIssuer(service.config.Auth.Issuer),
		jwt.WithAudience(string(role)),
		jwt.WithExpirationRequired(),
	)
	if err != nil || claims.Role != role {
		return nil, apperror.New(apperror.Unauthorized, "invalid access token")
	}
	id, err := uuid.Parse(claims.Subject)
	if err != nil {
		return nil, apperror.New(apperror.Unauthorized, "invalid access token")
	}
	value, err := service.redis.HGetAll(ctx, service.key("session:"+claims.SessionID)).Result()
	if err != nil {
		return nil, service.Failure(ctx, "validate session", err)
	}
	if value["id"] != id.String() || value["role"] != string(role) ||
		value["version"] != strconv.FormatInt(claims.Version, 10) {
		return nil, apperror.New(apperror.Unauthorized, "session revoked")
	}
	account, err := service.Account(ctx, role, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperror.New(apperror.Unauthorized, "account unavailable")
		}
		return nil, service.Failure(ctx, "validate account", err)
	}
	if account.Blocked || account.Version != claims.Version {
		return nil, apperror.New(apperror.Unauthorized, "session revoked")
	}
	return &Principal{
		ID:        id,
		Role:      role,
		SessionID: claims.SessionID,
		Version:   account.Version,
		Account:   account.PublicAccount,
		ExpiresAt: claims.ExpiresAt.Time,
	}, nil
}

// ChangePassword проверяет текущий пароль и меняет его с отзывом всех сессий аккаунта.
func (service *Service) ChangePassword(ctx context.Context, principal *Principal, current, next string) error {
	account, err := service.Account(ctx, principal.Role, principal.ID)
	if err != nil {
		return service.Failure(ctx, "load password", err)
	}
	if !security.CheckPassword(account.PasswordHash, current) {
		return apperror.New(apperror.BadRequest, "current password is incorrect")
	}
	return service.setPassword(ctx, principal.Role, account, next, "account.password_changed")
}

func (service *Service) setPassword(ctx context.Context,
	role Role,
	account Account,
	password string,
	action dbgen.AuditAction,
) error {
	hash, err := security.HashPassword(password)
	if err != nil {
		return service.Failure(ctx, "hash password", err)
	}
	tx, err := service.db.Pool.Begin(ctx)
	if err != nil {
		return service.Failure(ctx, "begin password update", err)
	}
	defer tx.Rollback(ctx)
	queries := dbgen.New(tx)
	var number int64
	if role == Admin {
		number, err = queries.ChangeAdminPassword(
			ctx,
			dbgen.ChangeAdminPasswordParams{ID: account.ID, PasswordHash: hash, AuthVersion: account.Version},
		)
	} else {
		number,
			err = queries.ChangeUserPassword(ctx,
			dbgen.ChangeUserPasswordParams{
				ID:           account.ID,
				PasswordHash: hash,
				AuthVersion:  account.Version,
			})
	}
	if err != nil {
		return service.Failure(ctx, "update password", err)
	}
	if number != 1 {
		return apperror.New(apperror.Conflict, "account changed; try again")
	}
	if err = queries.InsertAudit(ctx,
		dbgen.InsertAuditParams{
			ActorType: dbgen.ActorType(role),
			ActorID:   account.ID.String(),
			Action:    action,
			ObjectID:  account.ID.String(),
			Result:    "success",
			RequestID: RequestID(ctx),
		}); err != nil {
		return service.Failure(ctx, "audit password", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return service.Failure(ctx, "commit password", err)
	}
	return nil
}

// ForgotPassword отправляет одноразовый токен восстановления; отсутствие аккаунта не раскрывается.
func (service *Service) ForgotPassword(ctx context.Context, role Role, email string) error {
	email = NormalizeEmail(email)
	err := service.Limit(ctx, "reset:"+string(role)+":"+email, service.config.Auth.EmailLimit, 15*time.Minute)
	if err != nil {
		return err
	}
	account, err := service.accountByEmail(ctx, role, email)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return service.Failure(ctx, "find reset account", err)
	}
	if account.Blocked {
		return nil
	}
	token := security.RandomToken()
	value := account.ID.String() + ":" + strconv.FormatInt(account.Version, 10)
	key := service.key("reset:" + string(role) + ":" + Digest(token))
	if err = service.redis.Set(ctx, key, value, service.config.Auth.ResetTTL).Err(); err != nil {
		return service.Failure(ctx, "store reset", err)
	}
	message, err := appmail.PasswordResetMessage(token, service.config.Auth.ResetTTL)
	if err == nil {
		err = service.mail.Send(ctx, account.Email, "Password reset", message)
	}
	if err != nil {
		service.redis.Del(ctx, key)
		return service.Failure(ctx, "send reset", err)
	}
	return nil
}

// ResetPassword погашает токен восстановления и меняет пароль с отзывом всех сессий.
func (service *Service) ResetPassword(ctx context.Context, role Role, token, password string) error {
	value, err := service.redis.GetDel(ctx, service.key("reset:"+string(role)+":"+Digest(token))).Result()
	if errors.Is(err, redis.Nil) {
		return apperror.New(apperror.BadRequest, "invalid or expired reset token")
	}
	if err != nil {
		return service.Failure(ctx, "consume reset", err)
	}
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return apperror.New(apperror.BadRequest, "invalid reset token")
	}
	id, err := uuid.Parse(parts[0])
	if err != nil {
		return apperror.New(apperror.BadRequest, "invalid reset token")
	}
	account, err := service.Account(ctx, role, id)
	if err != nil {
		return service.Failure(ctx, "load reset account", err)
	}
	if account.Blocked || strconv.FormatInt(account.Version, 10) != parts[1] {
		return apperror.New(apperror.BadRequest, "reset token revoked")
	}
	return service.setPassword(ctx, role, account, password, "account.password_reset")
}

// Seed создаёт отсутствующих администраторов из настроек, сохраняя пароли существующих.
func Seed(ctx context.Context, db *database.Store, admins []appconfig.AdminConfig) error {
	for _, config := range admins {
		config.Email = NormalizeEmail(config.Email)
		if !ValidateCredentials(Credentials{Email: config.Email, Password: config.Password}) {
			return fmt.Errorf("invalid configured administrator credentials")
		}
		_, err := db.Queries.GetAdminByEmail(ctx, config.Email)
		if err == nil {
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		hash, err := security.HashPassword(config.Password)
		if err != nil {
			return err
		}
		if err = db.Queries.CreateAdmin(ctx,
			dbgen.CreateAdminParams{
				ID:           uuid.New(),
				Email:        config.Email,
				PasswordHash: hash,
			}); err != nil {
			return err
		}
	}
	return nil
}

// ValidatePrincipal повторно проверяет срок и отзыв сессии для длительного WebSocket-соединения.
func (service *Service) ValidatePrincipal(ctx context.Context, principal *Principal) error {
	if principal == nil || time.Now().After(principal.ExpiresAt) {
		return apperror.New(apperror.Unauthorized, "access token expired")
	}
	value, err := service.redis.HGetAll(ctx, service.key("session:"+principal.SessionID)).Result()
	if err != nil {
		return service.Failure(ctx, "validate realtime session", err)
	}
	if value["id"] != principal.ID.String() || value["role"] != string(principal.Role) ||
		value["version"] != strconv.FormatInt(principal.Version, 10) {
		return apperror.New(apperror.Unauthorized, "session revoked")
	}
	account, err := service.Account(ctx, principal.Role, principal.ID)
	if err != nil {
		return service.Failure(ctx, "validate realtime account", err)
	}
	if account.Blocked || account.Version != principal.Version {
		return apperror.New(apperror.Unauthorized, "session revoked")
	}
	return nil
}
