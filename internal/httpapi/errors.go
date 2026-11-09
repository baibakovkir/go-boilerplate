package httpapi

import (
	"context"
	"errors"
	"log/slog"

	"go-boilerplate/internal/apperror"
	"go-boilerplate/internal/auth"

	"github.com/danielgtaylor/huma/v2"
)

func transportError(ctx context.Context, logger *slog.Logger, err error) error {
	if err == nil {
		return nil
	}
	var appError *apperror.Error
	if errors.As(err, &appError) {
		status := map[apperror.Code]int{
			apperror.BadRequest: 400, apperror.Unauthorized: 401,
			apperror.Forbidden: 403, apperror.NotFound: 404, apperror.Conflict: 409,
			apperror.TooManyRequests: 429, apperror.Unavailable: 503,
		}[appError.Code]
		if appError.Cause != nil {
			logger.ErrorContext(ctx, appError.Op, "request_id", auth.RequestID(ctx), "error", appError.Cause)
		}
		if status == 0 {
			status = 503
		}
		return huma.NewError(status, appError.Message)
	}
	var statusError huma.StatusError
	if errors.As(err, &statusError) {
		return err
	}
	logger.ErrorContext(ctx, "HTTP operation failed", "request_id", auth.RequestID(ctx), "error", err)
	return huma.Error503ServiceUnavailable("service temporarily unavailable")
}

func register[Input, Output any](server *Server, operation huma.Operation,
	handler func(context.Context, *Input) (*Output, error),
) {
	huma.Register(server.API, operation, func(ctx context.Context, input *Input) (*Output, error) {
		output, err := handler(ctx, input)
		return output, transportError(ctx, server.Log, err)
	})
}
