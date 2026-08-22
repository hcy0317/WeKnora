package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/Tencent/WeKnora/internal/models/openaiapi"
	"github.com/Tencent/WeKnora/internal/types"
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

type wikiProviderStreamError struct {
	Err     error
	Details types.StreamErrorDetails
}

func (e *wikiProviderStreamError) Error() string {
	if e == nil {
		return ""
	}
	message := "provider stream failed"
	if e.Err != nil {
		message = e.Err.Error()
	}
	var facts []string
	if e.Details.ProviderRequestID != "" {
		facts = append(facts, fmt.Sprintf("request_id=%q", e.Details.ProviderRequestID))
	}
	if e.Details.LastSSEEventType != "" {
		facts = append(facts, fmt.Sprintf("last_sse_event_type=%q", e.Details.LastSSEEventType))
	}
	if e.Details.Code != "" {
		facts = append(facts, fmt.Sprintf("error_code=%q", e.Details.Code))
	}
	if e.Details.Type != "" {
		facts = append(facts, fmt.Sprintf("error_type=%q", e.Details.Type))
	}
	facts = append(facts,
		fmt.Sprintf("output_started=%t", e.Details.OutputStarted),
		fmt.Sprintf("usage_observed=%t", e.Details.UsageObserved),
	)
	if e.Details.HTTPStatus > 0 {
		facts = append(facts, fmt.Sprintf("http_status=%d", e.Details.HTTPStatus))
	}
	return message + " [" + strings.Join(facts, " ") + "]"
}

func (e *wikiProviderStreamError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func newWikiProviderStreamError(err error, data map[string]interface{}) error {
	details, ok := types.StreamErrorDetailsFromData(data)
	if !ok {
		return err
	}
	return &wikiProviderStreamError{Err: err, Details: details}
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

	var streamErr *wikiProviderStreamError
	if errors.As(err, &streamErr) {
		if streamErr.Details.PossiblyBilled() {
			return newWikiGenerationError(WikiGenerationErrorAmbiguousCall, err)
		}
		if streamErr.Details.HTTPStatus == 408 || streamErr.Details.HTTPStatus == 429 || streamErr.Details.HTTPStatus >= 500 ||
			isRetryableWikiStreamCode(streamErr.Details.Code) {
			return newWikiGenerationError(WikiGenerationErrorTransientTransport, err)
		}
		return newWikiGenerationError(WikiGenerationErrorDeterministicOutput, err)
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

func isRetryableWikiStreamCode(code string) bool {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "stream_read_error",
		"stream_timeout",
		"upstream_http2_stream_error",
		"invalid_responses_sse_envelope",
		"stream_closed_before_completion",
		"stream_ended_before_completion":
		return true
	default:
		return false
	}
}
