package chat

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestProcessRawHTTPStreamSurfacesSSEErrorEnvelope(t *testing.T) {
	response := &http.Response{
		Body: io.NopCloser(strings.NewReader(
			"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"\"}]}\n\n" +
				"data: {\"error\":{\"message\":\"Upstream request failed\",\"type\":\"upstream_error\",\"code\":\"bad_gateway\"}}\n\n",
		)),
	}
	stream := make(chan types.StreamResponse, 4)
	client := &RemoteAPIChat{modelName: "provider-independent-model"}

	client.processRawHTTPStream(context.Background(), response, stream, nil)

	var events []types.StreamResponse
	for event := range stream {
		events = append(events, event)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want answer + error: %#v", len(events), events)
	}
	if events[1].ResponseType != types.ResponseTypeError || !events[1].Done {
		t.Fatalf("terminal event = %#v, want done error", events[1])
	}
	if !strings.Contains(events[1].Content, "Upstream request failed") ||
		!strings.Contains(events[1].Content, "upstream_error") {
		t.Fatalf("provider error detail lost: %#v", events[1])
	}
}

func TestProcessRawHTTPStreamRejectsEOFBeforeDone(t *testing.T) {
	response := &http.Response{Body: io.NopCloser(strings.NewReader(
		"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"\"}]}\n\n",
	))}
	stream := make(chan types.StreamResponse, 4)
	client := &RemoteAPIChat{modelName: "provider-independent-model"}

	client.processRawHTTPStream(context.Background(), response, stream, nil)

	var terminal types.StreamResponse
	for event := range stream {
		if event.Done {
			terminal = event
		}
	}
	if terminal.ResponseType != types.ResponseTypeError ||
		!strings.Contains(terminal.Content, "before [DONE]") {
		t.Fatalf("terminal event = %#v, want premature EOF error", terminal)
	}
}

func TestProcessRawHTTPStreamRejectsDoneWithoutFinishReason(t *testing.T) {
	response := &http.Response{Body: io.NopCloser(strings.NewReader(
		"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"\"}]}\n\n" +
			"data: [DONE]\n\n",
	))}
	stream := make(chan types.StreamResponse, 4)
	client := &RemoteAPIChat{modelName: "provider-independent-model"}

	client.processRawHTTPStream(context.Background(), response, stream, nil)

	var terminal types.StreamResponse
	for event := range stream {
		if event.Done {
			terminal = event
		}
	}
	if terminal.ResponseType != types.ResponseTypeError ||
		!strings.Contains(terminal.Content, "without finish_reason") {
		t.Fatalf("terminal event = %#v, want missing finish reason error", terminal)
	}
}
