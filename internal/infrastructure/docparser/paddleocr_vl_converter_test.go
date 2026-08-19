package docparser

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/sirupsen/logrus"
)

func allowPaddleOCRVLLoopbackTest(t *testing.T) {
	t.Helper()
	t.Setenv("SSRF_WHITELIST", "127.0.0.1,::1,localhost")
	utils.ResetSSRFWhitelistForTest()
	t.Cleanup(utils.ResetSSRFWhitelistForTest)
}

func TestNewPaddleOCRVLReaderRoutesManagedLocalEndpointThroughEngineGateway(t *testing.T) {
	t.Setenv("WEKNORA_ENGINE_GATEWAY_URL", "http://engine-gateway:18084")
	reader := NewPaddleOCRVLReader(map[string]string{
		"paddleocr_vl_endpoint": "http://paddleocr-vl:8080",
	})
	if reader.endpoint != "http://engine-gateway:18084/paddleocr" {
		t.Fatalf("managed Paddle endpoint = %q", reader.endpoint)
	}
}

func TestPaddleOCRVLReaderReadRejectsEmptyLayoutParsingResults(t *testing.T) {
	allowPaddleOCRVLLoopbackTest(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/layout-parsing" {
			t.Errorf("request = %s %s, want POST /layout-parsing", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"errorCode":0,"errorMsg":"","result":{"layoutParsingResults":[]}}`)
	}))
	t.Cleanup(server.Close)

	var logOutput bytes.Buffer
	testLogger := logrus.New()
	testLogger.SetOutput(&logOutput)
	testLogger.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})
	ctx := context.WithValue(
		context.Background(),
		types.LoggerContextKey,
		logrus.NewEntry(testLogger).WithField("document_process", "doc-process-empty"),
	)

	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": server.URL})
	result, err := reader.Read(ctx, &types.ReadRequest{
		FileContent: []byte("sensitive document bytes"),
		FileName:    "report.pdf",
		FileType:    "pdf",
		RequestID:   "req-empty-pages",
	})
	if err == nil {
		t.Fatalf("Read() error = nil, result = %#v; want empty layoutParsingResults failure", result)
	}
	for _, want := range []string{"layoutParsingResults", server.URL, "report.pdf", "req-empty-pages"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Read() error %q missing diagnostic %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "sensitive document bytes") {
		t.Fatalf("Read() error leaked file content: %q", err)
	}
	logs := logOutput.String()
	for _, want := range []string{"document_process=doc-process-empty", "request_id=req-empty-pages", "no layoutParsingResults"} {
		if !strings.Contains(logs, want) {
			t.Errorf("failure logs missing %q:\n%s", want, logs)
		}
	}
}

func TestPingPaddleOCRVLUsesHealthEndpointFirst(t *testing.T) {
	var healthRequests atomic.Int32
	var layoutRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			healthRequests.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/layout-parsing":
			layoutRequests.Add(1)
			http.Error(w, "layout endpoint should not be probed", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	ok, reason := pingPaddleOCRVL(server.URL, server.Client())
	if !ok || reason != "" {
		t.Fatalf("PingPaddleOCRVL() = (%v, %q), want (true, empty reason)", ok, reason)
	}
	if got := healthRequests.Load(); got != 1 {
		t.Errorf("GET /health requests = %d, want 1", got)
	}
	if got := layoutRequests.Load(); got != 0 {
		t.Errorf("GET /layout-parsing requests = %d, want 0", got)
	}
}

func TestPingPaddleOCRVLFallsBackToLegacyLayout405(t *testing.T) {
	var healthRequests atomic.Int32
	var layoutRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("request method = %s, want GET", r.Method)
			http.Error(w, "unexpected method", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/health":
			healthRequests.Add(1)
			w.WriteHeader(http.StatusMethodNotAllowed)
		case "/layout-parsing":
			layoutRequests.Add(1)
			w.WriteHeader(http.StatusMethodNotAllowed)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	ok, reason := pingPaddleOCRVL(server.URL, server.Client())
	if !ok || reason != "" {
		t.Fatalf("pingPaddleOCRVL() = (%v, %q), want legacy 405 reachability success", ok, reason)
	}
	if got := healthRequests.Load(); got != 1 {
		t.Errorf("GET /health requests = %d, want 1", got)
	}
	if got := layoutRequests.Load(); got != 1 {
		t.Errorf("GET /layout-parsing requests = %d, want 1", got)
	}
}

func TestPingPaddleOCRVLDoesNotFallbackOnHealthFailure(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var layoutRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/health":
					w.WriteHeader(status)
				case "/layout-parsing":
					layoutRequests.Add(1)
					w.WriteHeader(http.StatusMethodNotAllowed)
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)

			ok, reason := pingPaddleOCRVL(server.URL, server.Client())
			if ok || !strings.Contains(reason, fmt.Sprintf("状态 %d", status)) {
				t.Fatalf("pingPaddleOCRVL() = (%v, %q), want status failure", ok, reason)
			}
			if got := layoutRequests.Load(); got != 0 {
				t.Errorf("GET /layout-parsing requests = %d, want 0", got)
			}
		})
	}
}

func TestPingPaddleOCRVLReportsHealthTransportFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	client := server.Client()
	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test endpoint: %v", err)
	}
	endpoint.User = url.UserPassword("health-user", "health-password")
	endpoint.RawQuery = "api_key=health-secret"
	server.Close()

	ok, reason := pingPaddleOCRVL(endpoint.String(), client)
	if ok || !strings.Contains(reason, "服务不可达") {
		t.Fatalf("pingPaddleOCRVL() = (%v, %q), want transport failure", ok, reason)
	}
	for _, leaked := range []string{"health-user", "health-password", "health-secret", "api_key"} {
		if strings.Contains(reason, leaked) {
			t.Errorf("pingPaddleOCRVL() reason leaked %q: %q", leaked, reason)
		}
	}
}

func TestPaddleOCRVLReaderReadReturnsMarkdownAndUsesRequestContext(t *testing.T) {
	allowPaddleOCRVLLoopbackTest(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/layout-parsing" {
			t.Errorf("request = %s %s, want POST /layout-parsing", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode request payload: %v", err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if payload["fileType"] != float64(0) {
			t.Errorf("fileType = %#v, want PDF code 0", payload["fileType"])
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"errorCode":0,"errorMsg":"","result":{"layoutParsingResults":[{"markdown":{"text":"# First","images":{}}},{"markdown":{"text":"Second","images":{}}}]}}`)
	}))
	t.Cleanup(server.Close)

	var logOutput bytes.Buffer
	testLogger := logrus.New()
	testLogger.SetOutput(&logOutput)
	testLogger.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})
	ctx := context.WithValue(
		context.Background(),
		types.LoggerContextKey,
		logrus.NewEntry(testLogger).WithField("document_process", "doc-process-7"),
	)

	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": server.URL})
	result, err := reader.Read(ctx, &types.ReadRequest{
		FileContent: []byte("sensitive success bytes"),
		FileName:    "success.pdf",
		FileType:    "pdf",
		RequestID:   "req-success",
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if result.MarkdownContent != "# First\n\nSecond" {
		t.Fatalf("MarkdownContent = %q, want merged page markdown", result.MarkdownContent)
	}

	logs := logOutput.String()
	for _, want := range []string{"document_process=doc-process-7", "request_id=req-success", "Parsed successfully"} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs missing %q:\n%s", want, logs)
		}
	}
	if strings.Contains(logs, "sensitive success bytes") {
		t.Fatalf("logs leaked file content:\n%s", logs)
	}
}

func TestPaddleOCRVLReaderReadDoesNotApplyDiagnosticLimitToSuccess(t *testing.T) {
	allowPaddleOCRVLLoopbackTest(t)
	largeMarkdown := strings.Repeat("successful page content ", 4096)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]interface{}{
			"errorCode": 0,
			"errorMsg":  "",
			"result": map[string]interface{}{
				"layoutParsingResults": []interface{}{
					map[string]interface{}{
						"markdown": map[string]interface{}{
							"text":   largeMarkdown,
							"images": map[string]string{},
						},
					},
				},
			},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": server.URL})
	result, err := reader.Read(context.Background(), &types.ReadRequest{
		FileContent: []byte("success body"),
		FileName:    "large-success.pdf",
		FileType:    "pdf",
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if result.MarkdownContent != largeMarkdown {
		t.Fatalf("MarkdownContent length = %d, want %d", len(result.MarkdownContent), len(largeMarkdown))
	}
}

func TestPaddleOCRVLReaderReadReportsSanitizedBoundedHTTPError(t *testing.T) {
	allowPaddleOCRVLLoopbackTest(t)
	const fileContent = "sensitive document bytes"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := "upstream unavailable openai_api_key=body-secret " +
			strings.Repeat("x", 2048) + " response-tail-marker"
		http.Error(w, body, http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test endpoint: %v", err)
	}
	endpoint.User = url.UserPassword("service-user", "endpoint-password")
	endpoint.Path = "/tenant"
	endpoint.RawQuery = "api_key=endpoint-secret"

	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": endpoint.String()})
	_, err = reader.Read(context.Background(), &types.ReadRequest{
		FileContent: []byte(fileContent),
		FileName:    "failure.pdf",
		FileType:    "pdf",
		RequestID:   "req-http-error",
	})
	if err == nil {
		t.Fatal("Read() error = nil, want HTTP status failure")
	}

	got := err.Error()
	for _, want := range []string{
		"PaddleOCR-VL API status 502",
		"upstream unavailable",
		"openai_api_key=***",
		"endpoint=\"" + server.URL + "/tenant\"",
		"file=\"failure.pdf\"",
		"request_id=\"req-http-error\"",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Read() error %q missing %q", got, want)
		}
	}
	for _, leaked := range []string{
		"endpoint-password", "endpoint-secret", "body-secret", "response-tail-marker", fileContent, "\n",
	} {
		if strings.Contains(got, leaked) {
			t.Errorf("Read() error leaked %q: %q", leaked, got)
		}
	}
	if len(got) > 1600 {
		t.Errorf("Read() error length = %d, want bounded diagnostic", len(got))
	}
}

func TestPaddleOCRVLReaderReadRedactsPrefixedFileContent(t *testing.T) {
	allowPaddleOCRVLLoopbackTest(t)
	const fileContent = "confidential document body 0123456789 abcdefghijklmnopqrstuvwxyz"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "parse failed: "+fileContent, http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": server.URL})
	_, err := reader.Read(context.Background(), &types.ReadRequest{
		FileContent: []byte(fileContent),
		FileName:    "prefixed-content.pdf",
		FileType:    "pdf",
		RequestID:   "req-prefixed-content",
	})
	if err == nil {
		t.Fatal("Read() error = nil, want HTTP status failure")
	}
	if strings.Contains(err.Error(), fileContent) || strings.Contains(err.Error(), "confidential document body") {
		t.Fatalf("Read() error leaked uploaded file content: %q", err)
	}
	if !strings.Contains(err.Error(), "[redacted file content]") {
		t.Errorf("Read() error %q missing file-content redaction marker", err)
	}
}

func TestPaddleOCRVLReaderReadRedactsTransportEndpointCredentials(t *testing.T) {
	allowPaddleOCRVLLoopbackTest(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test endpoint: %v", err)
	}
	server.Close()

	endpoint.User = url.UserPassword("transport-user", "transport-password")
	endpoint.Path = "/tenant"
	endpoint.RawQuery = "client_secret=transport-secret"
	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": endpoint.String()})
	_, err = reader.Read(context.Background(), &types.ReadRequest{
		FileContent: []byte("transport body"),
		FileName:    "transport.pdf",
		FileType:    "pdf",
		RequestID:   "req-transport",
	})
	if err == nil {
		t.Fatal("Read() error = nil, want transport failure")
	}

	got := err.Error()
	for _, want := range []string{
		"PaddleOCR-VL layout-parsing",
		"endpoint=\"" + server.URL + "/tenant\"",
		"request_id=\"req-transport\"",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Read() error %q missing %q", got, want)
		}
	}
	for _, leaked := range []string{
		"transport-user", "transport-password", "transport-secret", "client_secret",
	} {
		if strings.Contains(got, leaked) {
			t.Errorf("Read() transport error leaked %q: %q", leaked, got)
		}
	}
}

func TestPaddleOCRVLReaderReadReportsSanitizedPaddleError(t *testing.T) {
	allowPaddleOCRVLLoopbackTest(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"errorCode":72001,"errorMsg":"pipeline out of memory\nretry later client_secret=service-secret Authorization: Basic dXNlcjpwYXNz","result":{"layoutParsingResults":[]}}`)
	}))
	t.Cleanup(server.Close)

	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": server.URL})
	_, err := reader.Read(context.Background(), &types.ReadRequest{
		FileContent: []byte("sensitive error-code bytes"),
		FileName:    "error-code.pdf",
		FileType:    "pdf",
		RequestID:   "req-error-code",
	})
	if err == nil {
		t.Fatal("Read() error = nil, want Paddle errorCode failure")
	}

	got := err.Error()
	for _, want := range []string{
		"PaddleOCR-VL error 72001", "pipeline out of memory retry later", "client_secret=***", "Authorization=***",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Read() error %q missing %q", got, want)
		}
	}
	for _, leaked := range []string{"service-secret", "dXNlcjpwYXNz", "\n", "sensitive error-code bytes"} {
		if strings.Contains(got, leaked) {
			t.Errorf("Read() error leaked %q: %q", leaked, got)
		}
	}
}

func TestPaddleOCRVLReaderReadRedactsStandaloneAuthorizationSchemes(t *testing.T) {
	allowPaddleOCRVLLoopbackTest(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "authentication failed: Basic dXNlcjpwYXNz Bearer sk-standalone-secret", http.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)

	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": server.URL})
	_, err := reader.Read(context.Background(), &types.ReadRequest{
		FileContent: []byte("ordinary document content"),
		FileName:    "auth-failure.pdf",
		FileType:    "pdf",
	})
	if err == nil {
		t.Fatal("Read() error = nil, want authentication failure")
	}
	for _, leaked := range []string{"dXNlcjpwYXNz", "sk-standalone-secret"} {
		if strings.Contains(err.Error(), leaked) {
			t.Errorf("Read() error leaked standalone credential %q: %q", leaked, err)
		}
	}
}

func TestPaddleOCRVLReaderReadRedactsNonSampledBase64FileContent(t *testing.T) {
	allowPaddleOCRVLLoopbackTest(t)
	fileContent := make([]byte, 512)
	for index := range fileContent {
		fileContent[index] = byte((index*31 + 7) % 251)
	}
	encodedFragment := base64.StdEncoding.EncodeToString(fileContent[73:137])
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "decoder echo: "+encodedFragment, http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": server.URL})
	_, err := reader.Read(context.Background(), &types.ReadRequest{
		FileContent: fileContent,
		FileName:    "encoded-echo.pdf",
		FileType:    "pdf",
	})
	if err == nil {
		t.Fatal("Read() error = nil, want decoder failure")
	}
	if strings.Contains(err.Error(), encodedFragment) {
		t.Fatalf("Read() error leaked non-sampled base64 file content: %q", err)
	}
	if !strings.Contains(err.Error(), "[redacted file content]") {
		t.Errorf("Read() error %q missing file-content redaction marker", err)
	}
}

func TestPaddleOCRVLReaderReadRedactsShortFileContent(t *testing.T) {
	allowPaddleOCRVLLoopbackTest(t)
	const fileContent = "s3cr3t"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "parse failed: "+fileContent, http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": server.URL})
	_, err := reader.Read(context.Background(), &types.ReadRequest{
		FileContent: []byte(fileContent),
		FileName:    "short.pdf",
		FileType:    "pdf",
	})
	if err == nil {
		t.Fatal("Read() error = nil, want parse failure")
	}
	if strings.Contains(err.Error(), fileContent) {
		t.Fatalf("Read() error leaked short uploaded file content: %q", err)
	}
}

func TestPaddleOCRVLReaderReadRejectsOpaqueEndpointWithoutLoggingCredentials(t *testing.T) {
	const endpoint = "https:opaque-user:opaque-password@service.invalid/path?api_key=opaque-secret"
	var logOutput bytes.Buffer
	testLogger := logrus.New()
	testLogger.SetOutput(&logOutput)
	testLogger.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})
	ctx := context.WithValue(context.Background(), types.LoggerContextKey, logrus.NewEntry(testLogger))

	reader := NewPaddleOCRVLReader(map[string]string{"paddleocr_vl_endpoint": endpoint})
	_, err := reader.Read(ctx, &types.ReadRequest{
		FileContent: []byte("opaque endpoint body"),
		FileName:    "opaque.pdf",
		FileType:    "pdf",
	})
	if err == nil {
		t.Fatal("Read() error = nil, want invalid endpoint")
	}
	combined := err.Error() + "\n" + logOutput.String()
	if !strings.Contains(combined, "<invalid endpoint>") {
		t.Errorf("diagnostic = %q, want fixed invalid endpoint marker", combined)
	}
	for _, leaked := range []string{"opaque-user", "opaque-password", "opaque-secret", "api_key"} {
		if strings.Contains(combined, leaked) {
			t.Errorf("diagnostic leaked opaque endpoint credential %q: %q", leaked, combined)
		}
	}
}
