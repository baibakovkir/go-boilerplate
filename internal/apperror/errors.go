// Package apperror описывает безопасные ошибки прикладных операций без зависимости от HTTP.
package apperror

// Code различает причины отказа прикладной операции.
type Code string

const (
	BadRequest      Code = "bad_request"
	Unauthorized    Code = "unauthorized"
	Forbidden       Code = "forbidden"
	NotFound        Code = "not_found"
	Conflict        Code = "conflict"
	TooManyRequests Code = "too_many_requests"
	Unavailable     Code = "unavailable"
)

// Error хранит безопасное сообщение и закрытую техническую причину.
type Error struct {
	Code    Code
	Message string
	Op      string
	Cause   error
}

func (appError *Error) Error() string { return appError.Message }
func (appError *Error) Unwrap() error { return appError.Cause }

// New создаёт ожидаемый отказ с безопасным сообщением.
func New(code Code, message string) error { return &Error{Code: code, Message: message} }

// Failure сохраняет причину сбоя зависимости, не раскрывая её клиенту.
func Failure(op string, cause error) error {
	return &Error{Code: Unavailable, Message: "service temporarily unavailable", Op: op, Cause: cause}
}
