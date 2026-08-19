package service

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/openaiapi"
)

type testWikiNetError struct{}

func (testWikiNetError) Error() string   { return "temporary network outage" }
func (testWikiNetError) Timeout() bool   { return true }
func (testWikiNetError) Temporary() bool { return true }

func TestWikiGenerationErrorClassificationUsesTypedTransportFacts(t *testing.T) {
	for _, status := range []int{
		http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	} {
		err := classifyWikiGenerationError(context.Background(), openaiapi.NewProtocolHTTPError(
			openaiapi.ProtocolResponses, status, "provider failure",
		))
		if wikiGenerationErrorClassOf(err) != WikiGenerationErrorTransientTransport {
			t.Fatalf("status %d class = %q", status, wikiGenerationErrorClassOf(err))
		}
	}

	if class := wikiGenerationErrorClassOf(classifyWikiGenerationError(context.Background(), testWikiNetError{})); class != WikiGenerationErrorTransientTransport {
		t.Fatalf("net.Error class = %q", class)
	}
	if isTransientLLMError(context.Background(), errors.New("API request failed with status 503: words only")) {
		t.Fatal("free-form status text must not be treated as structured transient evidence")
	}
	if isTransientLLMError(context.Background(), errors.New("stream_read_error")) {
		t.Fatal("ambiguous error wording must fail closed")
	}
}

func TestWikiGenerationErrorClassificationHonorsParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := classifyWikiGenerationError(ctx, testWikiNetError{})
	if wikiGenerationErrorClassOf(err) != WikiGenerationErrorCancelled {
		t.Fatalf("cancelled parent class = %q", wikiGenerationErrorClassOf(err))
	}
}
