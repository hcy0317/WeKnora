package vlm

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestRemoteAPIVLMReportsTruncatedCompletion preserves the user-visible
// truncation diagnosis independently of model naming and request-shape
// negotiation.
func TestRemoteAPIVLMReportsTruncatedCompletion(t *testing.T) {
	withVLMSSRFWhitelist(t, "127.0.0.1")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id": "chatcmpl-test",
			"object": "chat.completion",
			"choices": [
				{"index": 0, "message": {"role": "assistant", "content": ""}, "finish_reason": "length"}
			]
		}`))
	}))
	defer server.Close()

	v, err := NewRemoteAPIVLM(&Config{
		BaseURL:   server.URL,
		ModelName: "private-vision-alias",
		APIKey:    "sk-test",
	})
	if err != nil {
		t.Fatalf("NewRemoteAPIVLM: %v", err)
	}

	_, err = v.Predict(t.Context(), [][]byte{[]byte("\x89PNG\r\n\x1a\n" + strings.Repeat("\x00", 16))}, "extract the text")
	if err == nil {
		t.Fatal("Predict returned nil error for a truncated completion")
	}
	if !strings.Contains(err.Error(), "completion truncated") || strings.Contains(err.Error(), v.modelName) {
		t.Errorf("error = %q, want model-name-free truncation diagnosis", err.Error())
	}
}
