package domain

import (
	"errors"
	"fmt"
)

var (
	ErrValidation          = errors.New("validation error")
	ErrNotFound            = errors.New("not found")
	ErrConflict            = errors.New("conflict")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrAccountNotActive    = errors.New("account is not active")
	ErrAccountFrozen       = errors.New("account is frozen")
	ErrIdempotencyConflict = errors.New("idempotency key was reused with a different request")
	ErrHoldNotActive       = errors.New("hold is not active")
	ErrSameAccount         = errors.New("source and destination accounts must differ")
)

type Code string

const (
	CodeValidation          Code = "validation_error"
	CodeNotFound            Code = "not_found"
	CodeConflict            Code = "conflict"
	CodeInsufficientFunds   Code = "insufficient_funds"
	CodeCurrencyMismatch    Code = "currency_mismatch"
	CodeAccountNotActive    Code = "account_not_active"
	CodeIdempotencyConflict Code = "idempotency_key_reuse"
	CodeHoldNotActive       Code = "hold_not_active"
	CodeUnauthorized        Code = "unauthorized"
	CodeForbidden           Code = "forbidden"
	CodeRateLimited         Code = "rate_limited"
	CodeInternal            Code = "internal_error"
)

// Error is a domain error with a stable machine-readable code. Transport layers
// map Code to HTTP status codes; the domain never imports net/http.
type Error struct {
	Code    Code
	Message string
	Field   string
	Err     error
}

func (e *Error) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Code, e.Message, e.Field)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Err }

func NewError(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func Validation(format string, args ...any) *Error {
	return &Error{Code: CodeValidation, Message: fmt.Sprintf(format, args...), Err: ErrValidation}
}

func NotFound(format string, args ...any) *Error {
	return &Error{Code: CodeNotFound, Message: fmt.Sprintf(format, args...), Err: ErrNotFound}
}

func Conflict(format string, args ...any) *Error {
	return &Error{Code: CodeConflict, Message: fmt.Sprintf(format, args...), Err: ErrConflict}
}

func IsCode(err error, code Code) bool {
	var de *Error
	return errors.As(err, &de) && de.Code == code
}

func AsError(err error) (*Error, bool) {
	var de *Error
	if errors.As(err, &de) {
		return de, true
	}
	return nil, false
}
