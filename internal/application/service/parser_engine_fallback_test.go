package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type parserFallbackTestReader struct {
	mu                  sync.Mutex
	calls               []string
	selectedFailures    int
	selectedResultError string
	selectedContextErr  error
	selectedNil         bool
	selectedEmpty       bool
	defaultEmpty        bool
	defaultResultError  string
	cancelSelected      bool
}

func (r *parserFallbackTestReader) Read(ctx context.Context, req *types.ReadRequest) (*types.ReadResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, req.ParserEngine)
	if req.ParserEngine == "remote_custom" {
		if r.selectedContextErr != nil {
			return nil, r.selectedContextErr
		}
		if r.cancelSelected {
			return nil, context.Canceled
		}
		if r.selectedFailures > 0 {
			r.selectedFailures--
			return nil, errors.New("selected parser unavailable")
		}
		if r.selectedResultError != "" {
			return &types.ReadResult{Error: r.selectedResultError}, nil
		}
		if r.selectedNil {
			return nil, nil
		}
		if r.selectedEmpty {
			return &types.ReadResult{Metadata: map[string]string{"parser": "remote_custom"}}, nil
		}
		return &types.ReadResult{MarkdownContent: "selected"}, nil
	}
	if r.defaultResultError != "" {
		return &types.ReadResult{Error: r.defaultResultError}, nil
	}
	if r.defaultEmpty {
		return &types.ReadResult{Metadata: map[string]string{"parser": "builtin"}}, nil
	}
	return &types.ReadResult{MarkdownContent: "default"}, nil
}

type parserFallbackTenantService struct {
	interfaces.TenantService
	mu    sync.Mutex
	calls int
}

func (s *parserFallbackTenantService) GetWeKnoraCloudCredentials(context.Context) *types.WeKnoraCloudCredentials {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return nil
}

func (s *parserFallbackTenantService) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestParserEngineFallback_ReaderDeadlineFallsBackAndStartsCooldown(t *testing.T) {
	reader := &parserFallbackTestReader{selectedContextErr: context.DeadlineExceeded}
	svc := &knowledgeService{documentReader: reader}

	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "deadline.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{"remote_custom", ""}, reader.recordedCalls(), "a reader-owned deadline is an engine failure")

	reader.selectedContextErr = nil
	reader.resetCalls()
	result, activeEngine, readerFound, err = svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "next.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{""}, reader.recordedCalls(), "the failed engine must remain cooling down")
}

func (r *parserFallbackTestReader) Reconnect(string) error { return nil }
func (r *parserFallbackTestReader) IsConnected() bool      { return true }
func (r *parserFallbackTestReader) ListEngines(context.Context, map[string]string) ([]types.ParserEngineInfo, error) {
	return nil, nil
}

func (r *parserFallbackTestReader) resetCalls() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

func (r *parserFallbackTestReader) recordedCalls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func TestParserEngineFallback_CoolsDownAndRetriesOnNextRequest(t *testing.T) {
	now := time.Date(2026, 8, 8, 5, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	reader := &parserFallbackTestReader{selectedFailures: 1}
	svc := &knowledgeService{documentReader: reader}
	svc.parserEngineFallback.now = func() time.Time { return now }
	overrides := map[string]string{"endpoint": "https://parser.example/v1"}

	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, overrides, &types.ReadRequest{FileName: "large.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{"remote_custom", ""}, reader.recordedCalls())

	reader.resetCalls()
	now = now.Add(4*time.Minute + 59*time.Second)
	result, activeEngine, readerFound, err = svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, overrides, &types.ReadRequest{FileName: "second.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{""}, reader.recordedCalls(), "cooldown must bypass the selected parser")

	reader.resetCalls()
	now = now.Add(time.Second)
	result, activeEngine, readerFound, err = svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, overrides, &types.ReadRequest{FileName: "retry.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "selected", result.MarkdownContent)
	require.Equal(t, "remote_custom", activeEngine)
	require.Equal(t, []string{"remote_custom"}, reader.recordedCalls(), "the next request after five minutes must probe the selected parser")

	reader.resetCalls()
	result, activeEngine, readerFound, err = svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, overrides, &types.ReadRequest{FileName: "restored.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "selected", result.MarkdownContent)
	require.Equal(t, "remote_custom", activeEngine)
	require.Equal(t, []string{"remote_custom"}, reader.recordedCalls(), "a successful probe restores the selected parser")
}

func TestParserEngineFallback_ConfigChangeBypassesOldCooldown(t *testing.T) {
	now := time.Date(2026, 8, 8, 5, 0, 0, 0, time.UTC)
	reader := &parserFallbackTestReader{selectedFailures: 1}
	svc := &knowledgeService{documentReader: reader}
	svc.parserEngineFallback.now = func() time.Time { return now }

	_, _, _, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false,
		map[string]string{"endpoint": "https://old.example/v1"}, &types.ReadRequest{FileName: "first.pdf"},
	)
	require.NoError(t, err)

	reader.resetCalls()
	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false,
		map[string]string{"endpoint": "https://new.example/v1"}, &types.ReadRequest{FileName: "second.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "selected", result.MarkdownContent)
	require.Equal(t, "remote_custom", activeEngine)
	require.Equal(t, []string{"remote_custom"}, reader.recordedCalls())
}

func TestParserEngineFallback_ResultErrorUsesDefaultParser(t *testing.T) {
	reader := &parserFallbackTestReader{selectedResultError: "selected parser endpoint is not configured"}
	svc := &knowledgeService{documentReader: reader}

	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "result-error.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{"remote_custom", ""}, reader.recordedCalls())
}

func TestParserEngineFallback_UnavailableSelectedResolverUsesDefaultParser(t *testing.T) {
	reader := &parserFallbackTestReader{}
	svc := &knowledgeService{
		documentReader: reader,
		tenantService:  &parserFallbackTenantService{},
	}

	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), docparser.WeKnoraCloudEngineName, "pdf", false, nil,
		&types.ReadRequest{FileName: "unavailable.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{""}, reader.recordedCalls(), "unavailable selected resolver must use only the default reader")
}

func TestParserEngineFallback_ParentCancellationDuringResolverDoesNotFallbackOrCooldown(t *testing.T) {
	reader := &parserFallbackTestReader{}
	tenantService := &parserFallbackTenantService{}
	svc := &knowledgeService{documentReader: reader, tenantService: tenantService}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		ctx, docparser.WeKnoraCloudEngineName, "pdf", false, nil,
		&types.ReadRequest{FileName: "cancelled-resolver.pdf"},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	require.True(t, readerFound, "caller must classify cancellation as a reader error, not missing configuration")
	require.Equal(t, docparser.WeKnoraCloudEngineName, activeEngine)
	require.Empty(t, reader.recordedCalls(), "parent cancellation must not invoke the default parser")
	require.Equal(t, 1, tenantService.callCount())

	result, activeEngine, readerFound, err = svc.readWithParserEngineFallback(
		context.Background(), docparser.WeKnoraCloudEngineName, "pdf", false, nil,
		&types.ReadRequest{FileName: "next-resolver.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, 2, tenantService.callCount(), "next request must probe the selected resolver again")
}

func TestParserEngineFallback_NilSelectedResultUsesDefaultParser(t *testing.T) {
	reader := &parserFallbackTestReader{selectedNil: true}
	svc := &knowledgeService{documentReader: reader}

	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "nil.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{"remote_custom", ""}, reader.recordedCalls())
}

func TestParserEngineFallback_ReaderCancellationFallsBackWhenParentIsLive(t *testing.T) {
	reader := &parserFallbackTestReader{selectedContextErr: context.Canceled}
	svc := &knowledgeService{documentReader: reader}

	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "reader-cancel.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{"remote_custom", ""}, reader.recordedCalls())

	reader.selectedContextErr = nil
	reader.resetCalls()
	result, activeEngine, readerFound, err = svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "cooldown.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{""}, reader.recordedCalls(), "reader-owned cancellation must start cooldown")
}

func TestParserEngineFallback_DefaultResultErrorRemainsResultContract(t *testing.T) {
	reader := &parserFallbackTestReader{
		selectedFailures:   1,
		defaultResultError: "default parser rejected document",
	}
	svc := &knowledgeService{documentReader: reader}

	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "default-error.pdf"},
	)
	require.NoError(t, err, "default ReadResult.Error must remain in the result contract")
	require.True(t, readerFound)
	require.Equal(t, "default parser rejected document", result.Error)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{"remote_custom", ""}, reader.recordedCalls())
}

func TestParserEngineFallback_MetadataOnlySelectedResultUsesDefaultParser(t *testing.T) {
	reader := &parserFallbackTestReader{selectedEmpty: true}
	svc := &knowledgeService{documentReader: reader}

	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "empty.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "default", result.MarkdownContent)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{"remote_custom", ""}, reader.recordedCalls())
}

func TestParserEngineFallback_MetadataOnlyDefaultResultReturnsError(t *testing.T) {
	reader := &parserFallbackTestReader{selectedFailures: 1, defaultEmpty: true}
	svc := &knowledgeService{documentReader: reader}

	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "empty-default.pdf"},
	)
	require.EqualError(t, err, "default document parser returned empty content")
	require.True(t, readerFound)
	require.NotNil(t, result)
	require.Empty(t, activeEngine)
	require.Equal(t, []string{"remote_custom", ""}, reader.recordedCalls())
}

func TestParserEngineFallback_TaskCancellationDoesNotStartCooldown(t *testing.T) {
	now := time.Date(2026, 8, 8, 5, 0, 0, 0, time.UTC)
	reader := &parserFallbackTestReader{cancelSelected: true}
	svc := &knowledgeService{documentReader: reader}
	svc.parserEngineFallback.now = func() time.Time { return now }

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		ctx, "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "cancelled.pdf"},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, readerFound)
	require.Equal(t, "remote_custom", activeEngine)
	require.Equal(t, []string{"remote_custom"}, reader.recordedCalls(), "cancellation must not invoke the default parser")

	reader.cancelSelected = false
	reader.resetCalls()
	result, activeEngine, readerFound, err := svc.readWithParserEngineFallback(
		context.Background(), "remote_custom", "pdf", false, nil, &types.ReadRequest{FileName: "next.pdf"},
	)
	require.NoError(t, err)
	require.True(t, readerFound)
	require.Equal(t, "selected", result.MarkdownContent)
	require.Equal(t, "remote_custom", activeEngine)
	require.Equal(t, []string{"remote_custom"}, reader.recordedCalls())
}
