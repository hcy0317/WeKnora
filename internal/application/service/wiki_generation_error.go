package service

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/Tencent/WeKnora/internal/models/openaiapi"
)

type WikiGenerationErrorClass string

const (
	WikiGenerationErrorDeterministicOutput WikiGenerationErrorClass = "deterministic_output"
	WikiGenerationErrorTransientTransport  WikiGenerationErrorClass = "transient_transport"
	WikiGenerationErrorCancelled           WikiGenerationErrorClass = "cancelled"
	WikiGenerationErrorPersistence         WikiGenerationErrorClass = "persistence"
	WikiGenerationErrorBudgetExhausted     WikiGenerationErrorClass = "budget_exhausted"
	WikiGenerationErrorAmbiguousCall       WikiGenerationErrorClass = "ambiguous_call"
)

type WikiGenerationError struct {
	Class WikiGenerationErrorClass
	Err   error
}

func (e *WikiGenerationError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return string(e.Class)
	}
	return e.Err.Error()
}

func (e *WikiGenerationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func newWikiGenerationError(class WikiGenerationErrorClass, err error) error {
	if err == nil {
		return nil
	}
	var typed *WikiGenerationError
	if errors.As(err, &typed) {
		return err
	}
	return &WikiGenerationError{Class: class, Err: err}
}

func wikiGenerationErrorClassOf(err error) WikiGenerationErrorClass {
	var typed *WikiGenerationError
	if errors.As(err, &typed) {
		return typed.Class
	}
	return ""
}

func classifyWikiGenerationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return newWikiGenerationError(WikiGenerationErrorCancelled, err)
	}
	if class := wikiGenerationErrorClassOf(err); class != "" {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return newWikiGenerationError(WikiGenerationErrorCancelled, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return newWikiGenerationError(WikiGenerationErrorTransientTransport, err)
	}

	var protocolErr *openaiapi.ProtocolHTTPError
	if errors.As(err, &protocolErr) {
		if protocolErr.StatusCode == 408 || protocolErr.StatusCode == 429 || protocolErr.StatusCode >= 500 {
			return newWikiGenerationError(WikiGenerationErrorTransientTransport, err)
		}
		return newWikiGenerationError(WikiGenerationErrorDeterministicOutput, err)
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return newWikiGenerationError(WikiGenerationErrorTransientTransport, err)
	}
	if errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) {
		return newWikiGenerationError(WikiGenerationErrorTransientTransport, err)
	}

	return newWikiGenerationError(WikiGenerationErrorDeterministicOutput, err)
}
