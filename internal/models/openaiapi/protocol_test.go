package openaiapi

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type recordingProtocolStore struct {
	mu                    sync.Mutex
	failNext              bool
	protocolSaves         int
	responsesFieldSaves   int
	chatRequestShapeSaves int
}

type blockingProtocolStore struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	saves   int
	once    sync.Once
}

func newBlockingProtocolStore() *blockingProtocolStore {
	return &blockingProtocolStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingProtocolStore) Load(context.Context, string) (Protocol, bool, error) {
	return "", false, nil
}

func (s *blockingProtocolStore) Save(ctx context.Context, _ string, _ Protocol) error {
	s.mu.Lock()
	s.saves++
	s.mu.Unlock()
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockingProtocolStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

func (s *recordingProtocolStore) Load(context.Context, string) (Protocol, bool, error) {
	return "", false, nil
}

func (s *recordingProtocolStore) Save(context.Context, string, Protocol) error {
	return s.record(&s.protocolSaves)
}

func (s *recordingProtocolStore) LoadResponsesSyncMaxOutputUnsupported(
	context.Context, string,
) (bool, error) {
	return false, nil
}

func (s *recordingProtocolStore) SaveResponsesSyncMaxOutputUnsupported(
	context.Context, string,
) error {
	return s.record(&s.responsesFieldSaves)
}

func (s *recordingProtocolStore) LoadChatMaxCompletionNeutral(
	context.Context, string,
) (bool, error) {
	return false, nil
}

func (s *recordingProtocolStore) SaveChatMaxCompletionNeutral(context.Context, string) error {
	return s.record(&s.chatRequestShapeSaves)
}

func (s *recordingProtocolStore) record(counter *int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	*counter = *counter + 1
	if s.failNext {
		s.failNext = false
		return errors.New("forced persistence failure")
	}
	return nil
}

func (s *recordingProtocolStore) counts() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.protocolSaves, s.responsesFieldSaves, s.chatRequestShapeSaves
}

func TestProtocolCacheKeyTracksSavedModelConfiguration(t *testing.T) {
	base := "https://example.test/v1"
	first := ProtocolCacheKey("model-id", base, map[string]any{
		"model":   "model-a",
		"api_key": "secret-a",
	})
	same := ProtocolCacheKey("model-id", base, map[string]any{
		"model":   "model-a",
		"api_key": "secret-a",
	})
	changed := ProtocolCacheKey("model-id", base, map[string]any{
		"model":   "model-a",
		"api_key": "secret-b",
	})
	require.Equal(t, first, same)
	require.NotEqual(t, first, changed)
	require.Contains(t, first, "model:v4:", "protocol policy changes must invalidate legacy decisions")
	require.NotContains(t, first, "secret")

	MarkProtocolSuccess(first, ProtocolChatCompletions)
	require.Equal(t, ProtocolChatCompletions, PreferredProtocol(same))
	require.Equal(t, ProtocolResponses, PreferredProtocol(changed))
}

func TestSavedModelConfigFingerprintTracksEveryRequestAffectingSurface(t *testing.T) {
	base := SavedModelConfig{
		Provider:      "openai",
		InterfaceType: "openai",
		APIVersion:    "2026-07-01-preview",
		ExtraConfig:   map[string]any{"reasoning_effort": "high", "vendor_flag": "a"},
		Headers:       map[string]string{"X-Tenant": "one"},
		Auth:          map[string]string{"api_key": "secret", "app_id": "app"},
	}
	first := SavedModelConfigFingerprint(base)
	same := SavedModelConfigFingerprint(base)
	require.Equal(t, first, same)
	require.Len(t, first, sha256.Size*2)
	require.NotContains(t, first, "secret")
	reordered := SavedModelConfig{
		Provider:      "openai",
		InterfaceType: "openai",
		APIVersion:    "2026-07-01-preview",
		ExtraConfig:   map[string]any{"vendor_flag": "a", "reasoning_effort": "high"},
		Headers:       map[string]string{"X-Tenant": "one"},
		Auth:          map[string]string{"app_id": "app", "api_key": "secret"},
	}
	require.Equal(t, first, SavedModelConfigFingerprint(reordered), "map insertion order must not affect the fingerprint")

	mutations := []SavedModelConfig{
		{Provider: "deepseek", InterfaceType: base.InterfaceType, APIVersion: base.APIVersion, ExtraConfig: base.ExtraConfig, Headers: base.Headers, Auth: base.Auth},
		{Provider: base.Provider, InterfaceType: "responses", APIVersion: base.APIVersion, ExtraConfig: base.ExtraConfig, Headers: base.Headers, Auth: base.Auth},
		{Provider: base.Provider, InterfaceType: base.InterfaceType, APIVersion: "2027-01-01", ExtraConfig: base.ExtraConfig, Headers: base.Headers, Auth: base.Auth},
		{Provider: base.Provider, InterfaceType: base.InterfaceType, APIVersion: base.APIVersion, ExtraConfig: map[string]any{"reasoning_effort": "high", "vendor_flag": "b"}, Headers: base.Headers, Auth: base.Auth},
		{Provider: base.Provider, InterfaceType: base.InterfaceType, APIVersion: base.APIVersion, ExtraConfig: base.ExtraConfig, Headers: map[string]string{"X-Tenant": "two"}, Auth: base.Auth},
		{Provider: base.Provider, InterfaceType: base.InterfaceType, APIVersion: base.APIVersion, ExtraConfig: base.ExtraConfig, Headers: base.Headers, Auth: map[string]string{"api_key": "rotated", "app_id": "app"}},
	}
	for _, mutation := range mutations {
		require.NotEqual(t, first, SavedModelConfigFingerprint(mutation))
	}
}

func TestChatRequestShapeCapabilityTracksConfigurationFingerprint(t *testing.T) {
	clearProtocolMemoryForTest()
	t.Cleanup(clearProtocolMemoryForTest)

	first := ProtocolCacheKey("model-id", "https://example.test/v1", map[string]any{
		"model":       "vision-alias",
		"temperature": 0.1,
	})
	same := ProtocolCacheKey("model-id", "https://example.test/v1/chat/completions", map[string]any{
		"model":       "vision-alias",
		"temperature": 0.1,
	})
	changed := ProtocolCacheKey("model-id", "https://example.test/v1", map[string]any{
		"model":       "vision-alias",
		"temperature": 0.2,
	})

	require.Equal(t, ChatRequestShapeDefault, PreferredChatRequestShape(first))
	MarkChatRequestShapeSuccess(first, ChatRequestShapeMaxCompletionNeutral)
	require.Equal(t, ChatRequestShapeMaxCompletionNeutral, PreferredChatRequestShape(same))
	require.Equal(t, ChatRequestShapeDefault, PreferredChatRequestShape(changed))
}

func TestResponsesMaxOutputProbeLetsFollowersOmitWithoutPersistingVerdict(t *testing.T) {
	SetProtocolStore(nil)
	clearProtocolMemoryForTest()
	t.Cleanup(clearProtocolMemoryForTest)
	key := ProtocolCacheKey("model-id", "https://example.test/v1", map[string]any{"model": "alias"})

	leaderOmit, releaseLeader, err := BeginResponsesSyncMaxOutputProbe(context.Background(), key)
	require.NoError(t, err)
	require.False(t, leaderOmit)

	started := time.Now()
	followerOmit, releaseFollower, err := BeginResponsesSyncMaxOutputProbe(context.Background(), key)
	require.NoError(t, err)
	require.True(t, followerOmit)
	require.Less(t, time.Since(started), 100*time.Millisecond, "followers must not wait behind a long probe")
	releaseFollower()
	require.True(t, ResponsesSyncMaxOutputTokensSupported(key), "a follower success cannot persist unsupported")

	rejectionEpoch := MarkResponsesSyncMaxOutputTokensRejectedPending(key)
	ClearResponsesSyncMaxOutputTokensRejectedPending(key, rejectionEpoch)
	nextLeaderOmit, releaseNextLeader, err := BeginResponsesSyncMaxOutputProbe(context.Background(), key)
	require.NoError(t, err)
	require.False(t, nextLeaderOmit, "an inconclusive retry must permit an immediate new probe")

	// Releasing the older epoch must not clear ownership of the new leader.
	releaseLeader()
	followerAfterStaleRelease, releaseFollowerAfterStale, err := BeginResponsesSyncMaxOutputProbe(context.Background(), key)
	require.NoError(t, err)
	require.True(t, followerAfterStaleRelease)
	releaseFollowerAfterStale()
	releaseNextLeader()
}

func TestResponsesMaxOutputMaskedRejectionCannotOverrideFieldSuccess(t *testing.T) {
	SetProtocolStore(nil)
	clearProtocolMemoryForTest()
	t.Cleanup(clearProtocolMemoryForTest)
	key := ProtocolCacheKey("model-id", "https://example.test/v1", map[string]any{"model": "alias"})

	MarkResponsesSyncMaxOutputTokensSupported(key)
	rejectionEpoch := MarkResponsesSyncMaxOutputTokensRejectedPending(key)
	// A concurrent request carrying the documented field succeeds after the
	// masked upstream_error was observed but before its no-field retry settles.
	MarkResponsesSyncMaxOutputTokensSupported(key)

	persisted := ConfirmResponsesSyncMaxOutputTokensUnsupported(key, rejectionEpoch, false)
	require.False(t, persisted)
	require.True(t, ResponsesSyncMaxOutputTokensSupported(key))

	omit, release, err := BeginResponsesSyncMaxOutputProbe(context.Background(), key)
	require.NoError(t, err)
	require.False(t, omit, "field success must win over an ambiguous concurrent rejection")
	release()
}

func TestResponsesMaxOutputStaleClearCannotEraseNewEpoch(t *testing.T) {
	SetProtocolStore(nil)
	clearProtocolMemoryForTest()
	t.Cleanup(clearProtocolMemoryForTest)
	key := ProtocolCacheKey("model-id", "https://example.test/v1", map[string]any{"model": "alias"})

	staleEpoch := MarkResponsesSyncMaxOutputTokensRejectedPending(key)
	currentEpoch := MarkResponsesSyncMaxOutputTokensRejectedPending(key)
	ClearResponsesSyncMaxOutputTokensRejectedPending(key, staleEpoch)

	omit, release, err := BeginResponsesSyncMaxOutputProbe(context.Background(), key)
	require.NoError(t, err)
	require.True(t, omit, "a stale failure must not clear the current rejection epoch")
	release()

	ClearResponsesSyncMaxOutputTokensRejectedPending(key, currentEpoch)
	omit, release, err = BeginResponsesSyncMaxOutputProbe(context.Background(), key)
	require.NoError(t, err)
	require.False(t, omit, "clearing the current epoch must permit a fresh field probe")
	release()
}

func TestResponsesMaxOutputStaleConfirmCannotEraseNewEpoch(t *testing.T) {
	SetProtocolStore(nil)
	clearProtocolMemoryForTest()
	t.Cleanup(clearProtocolMemoryForTest)
	key := ProtocolCacheKey("model-id", "https://example.test/v1", map[string]any{"model": "alias"})

	staleEpoch := MarkResponsesSyncMaxOutputTokensRejectedPending(key)
	currentEpoch := MarkResponsesSyncMaxOutputTokensRejectedPending(key)
	require.False(t, ConfirmResponsesSyncMaxOutputTokensUnsupported(key, staleEpoch, false))

	omit, release, err := BeginResponsesSyncMaxOutputProbe(context.Background(), key)
	require.NoError(t, err)
	require.True(t, omit, "a stale success must not clear the current rejection epoch")
	release()

	ClearResponsesSyncMaxOutputTokensRejectedPending(key, currentEpoch)
}

func TestResponsesMaxOutputMaskedRejectionDoesNotCommitWithAnotherFieldRequestInFlight(t *testing.T) {
	SetProtocolStore(nil)
	clearProtocolMemoryForTest()
	t.Cleanup(clearProtocolMemoryForTest)
	key := ProtocolCacheKey("model-id", "https://example.test/v1", map[string]any{"model": "alias"})
	normalizedKey := normalizeProtocolCacheLookupKey(key)
	state := responsesMaxOutputStateFor(normalizedKey)

	state.mu.Lock()
	state.epoch = 7
	state.probeInFlight = true
	state.fieldInFlight = 2
	state.supportedEpoch = 0
	responsesMaxOutputRejectedPending.Store(normalizedKey, struct{}{})
	state.mu.Unlock()

	persisted := ConfirmResponsesSyncMaxOutputTokensUnsupported(key, 7, false)
	require.False(t, persisted)
	require.True(t, ResponsesSyncMaxOutputTokensSupported(key), "an in-flight field request must keep masked rejection provisional")
}

func TestChatRequestShapeRetryRequiresExplicitFieldOrSamplingRejection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{name: "max tokens rejected", status: http.StatusBadRequest, body: `{"error":{"message":"Unsupported parameter: max_tokens"}}`, want: true},
		{name: "use max completion tokens", status: http.StatusUnprocessableEntity, body: `{"detail":"Please use max_completion_tokens instead of max_tokens"}`, want: true},
		{name: "temperature rejected", status: http.StatusBadRequest, body: `{"detail":"temperature is not supported for this model"}`, want: true},
		{name: "opaque 400", status: http.StatusBadRequest, body: `{"error":{"type":"upstream_error"}}`, want: false},
		{name: "timeout", status: 0, body: "context deadline exceeded", want: false},
		{name: "http2", status: 0, body: "http2: client connection lost", want: false},
		{name: "503", status: http.StatusServiceUnavailable, body: `{"detail":"Unsupported parameter: max_tokens"}`, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := NewProtocolHTTPError(ProtocolChatCompletions, tc.status, tc.body)
			require.Equal(t, tc.want, ShouldRetryChatWithMaxCompletionNeutral(tc.status, err))
		})
	}
}

func TestProtocolFallbackOnlyRecognizesEndpointOrExplicitFormatErrors(t *testing.T) {
	require.True(t, ShouldTryAlternateProtocol(http.StatusNotFound, nil))
	require.True(t, ShouldTryAlternateProtocol(
		http.StatusBadRequest,
		NewProtocolHTTPError(ProtocolResponses, http.StatusBadRequest, "unknown parameter: input"),
	))
	require.True(t, ShouldTryAlternateProtocol(
		http.StatusUnprocessableEntity,
		NewProtocolHTTPError(ProtocolResponses, http.StatusUnprocessableEntity, "unsupported request format"),
	))
	require.False(t, ShouldTryAlternateProtocol(
		http.StatusBadRequest,
		NewProtocolHTTPError(ProtocolResponses, http.StatusBadRequest, "invalid prompt"),
	))
	require.False(t, ShouldTryAlternateProtocol(
		http.StatusTooManyRequests,
		NewProtocolHTTPError(ProtocolResponses, http.StatusTooManyRequests, "rate limited"),
	))
	require.False(t, ShouldTryAlternateForDecision(
		ProtocolDecision{Protocol: ProtocolResponses, Known: true},
		http.StatusUnprocessableEntity,
		NewProtocolHTTPError(ProtocolResponses, http.StatusUnprocessableEntity, "unsupported request format"),
	), "a known Responses endpoint must not be flipped by 400/422")
	require.True(t, ShouldTryAlternateForDecision(
		ProtocolDecision{Protocol: ProtocolChatCompletions, Known: true},
		http.StatusUnprocessableEntity,
		NewProtocolHTTPError(ProtocolChatCompletions, http.StatusUnprocessableEntity, "unsupported request format"),
	), "a known Chat endpoint may re-probe Responses on proven incompatibility")
}

func TestMaxOutputTokensUnsupportedRequiresExactFieldRejection(t *testing.T) {
	require.True(t, IsMaxOutputTokensUnsupported(
		http.StatusBadRequest,
		NewProtocolHTTPError(ProtocolResponses, http.StatusBadRequest,
			`{"detail":"Unsupported parameter: max_output_tokens"}`),
	))
	require.False(t, IsMaxOutputTokensUnsupported(
		http.StatusBadRequest,
		NewProtocolHTTPError(ProtocolResponses, http.StatusBadRequest,
			`{"detail":"Unsupported parameter: input"}`),
	))
	require.False(t, IsMaxOutputTokensUnsupported(
		http.StatusServiceUnavailable,
		NewProtocolHTTPError(ProtocolResponses, http.StatusServiceUnavailable,
			`{"detail":"Unsupported parameter: max_output_tokens"}`),
	))
}

func TestOpaqueOrTransientResponsesFailureNeverProvesAlternateProtocol(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "opaque upstream 400", status: http.StatusBadRequest, body: `{"error":{"message":"Upstream request failed","type":"upstream_error"}}`},
		{name: "opaque upstream 422", status: http.StatusUnprocessableEntity, body: `{"error":{"message":"Upstream request failed","type":"upstream_error"}}`},
		{name: "temporary 503", status: http.StatusServiceUnavailable, body: `{"error":{"message":"Service temporarily unavailable","type":"api_error"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := NewProtocolHTTPError(ProtocolResponses, tc.status, tc.body)
			require.False(t, ShouldTryAlternateForDecision(
				ProtocolDecision{Protocol: ProtocolResponses, Known: false}, tc.status, err,
			), "an unknown Responses probe still requires explicit endpoint/format evidence")
			require.False(t, ShouldTryAlternateForDecision(
				ProtocolDecision{Protocol: ProtocolResponses, Known: true}, tc.status, err,
			), "a cached Responses decision must not flip on a transient/opaque upstream failure")
		})
	}
}

func TestTransportFailuresNeverProveAlternateProtocol(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		errors.New("http2: client connection lost"),
	} {
		for _, known := range []bool{false, true} {
			require.False(t, ShouldTryAlternateForDecision(
				ProtocolDecision{Protocol: ProtocolResponses, Known: known}, 0, err,
			))
		}
	}
}

func TestResponsesMaxOutputRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{name: "explicit 400", status: http.StatusBadRequest, body: `{"detail":"Unsupported parameter: max_output_tokens"}`, want: true},
		{name: "explicit 422", status: http.StatusUnprocessableEntity, body: `{"detail":"Unsupported parameter: max_output_tokens"}`, want: true},
		{name: "masked upstream 400", status: http.StatusBadRequest, body: `{"error":{"message":"Upstream request failed","type":"upstream_error"}}`, want: true},
		{name: "masked upstream 422", status: http.StatusUnprocessableEntity, body: `{"error":{"message":"Upstream request failed","type":"upstream_error"}}`, want: true},
		{name: "masked upstream top-level detail", status: http.StatusBadRequest, body: `{"error":{"message":"Upstream request failed","type":"upstream_error"},"detail":"Unsupported parameter: input"}`, want: false},
		{name: "masked upstream inner code", status: http.StatusBadRequest, body: `{"error":{"message":"Upstream request failed","type":"upstream_error","code":"other_validation"}}`, want: false},
		{name: "masked upstream case change", status: http.StatusBadRequest, body: `{"error":{"message":"upstream request failed","type":"upstream_error"}}`, want: false},
		{name: "masked upstream trailing JSON", status: http.StatusBadRequest, body: `{"error":{"message":"Upstream request failed","type":"upstream_error"}} {}`, want: false},
		{name: "opaque 400", status: http.StatusBadRequest, body: `{"error":{"type":"upstream_error"}}`, want: false},
		{name: "opaque 422", status: http.StatusUnprocessableEntity, body: `{"error":{"type":"upstream_error"}}`, want: false},
		{name: "503", status: http.StatusServiceUnavailable, body: `{"error":{"message":"Upstream request failed","type":"upstream_error"}}`, want: false},
		{name: "other field", status: http.StatusBadRequest, body: `{"detail":"Unsupported parameter: input"}`, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := NewProtocolHTTPError(ProtocolResponses, tc.status, tc.body)
			require.Equal(t, tc.want, ShouldRetryResponsesWithoutMaxOutputTokens(tc.status, err))
		})
	}
}

func TestRedisProtocolStoreSurvivesMemoryReset(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	SetProtocolStore(NewRedisProtocolStore(client))
	t.Cleanup(func() {
		SetProtocolStore(nil)
		clearProtocolMemoryForTest()
	})

	key := ProtocolCacheKey("persisted-model", "https://example.test/v1", map[string]any{"model": "gpt-test"})
	MarkProtocolSuccess(key, ProtocolChatCompletions)
	require.Equal(t, ProtocolChatCompletions, serverProtocolValue(t, context.Background(), client, key))

	clearProtocolMemoryForTest() // simulate a fresh process using the same Redis
	decision := ResolveProtocol(key)
	require.True(t, decision.Known)
	require.Equal(t, ProtocolChatCompletions, decision.Protocol)
}

func TestRedisResponsesSyncMaxOutputCapabilitySurvivesMemoryReset(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	SetProtocolStore(NewRedisProtocolStore(client))
	t.Cleanup(func() {
		SetProtocolStore(nil)
		clearProtocolMemoryForTest()
	})

	key := ProtocolCacheKey("persisted-capability", "https://example.test/v1", map[string]any{"model": "gpt-test"})
	require.True(t, ResponsesSyncMaxOutputTokensSupported(key))
	MarkResponsesSyncMaxOutputTokensUnsupported(key)
	require.False(t, ResponsesSyncMaxOutputTokensSupported(key))

	clearProtocolMemoryForTest()
	require.False(t, ResponsesSyncMaxOutputTokensSupported(key), "Redis must preserve the saved-model capability")

	changed := ProtocolCacheKey("persisted-capability", "https://example.test/v1", map[string]any{"model": "gpt-test-2"})
	require.True(t, ResponsesSyncMaxOutputTokensSupported(changed), "editing model config must trigger a fresh capability probe")
}

func TestRedisChatRequestShapeCapabilitySurvivesMemoryReset(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	SetProtocolStore(NewRedisProtocolStore(client))
	t.Cleanup(func() {
		SetProtocolStore(nil)
		clearProtocolMemoryForTest()
	})

	key := ProtocolCacheKey("persisted-chat-shape", "https://example.test/v1", map[string]any{"model": "alias"})
	require.Equal(t, ChatRequestShapeDefault, PreferredChatRequestShape(key))
	MarkChatRequestShapeSuccess(key, ChatRequestShapeMaxCompletionNeutral)
	require.Equal(t, ChatRequestShapeMaxCompletionNeutral, PreferredChatRequestShape(key))

	clearProtocolMemoryForTest()
	require.Equal(t, ChatRequestShapeMaxCompletionNeutral, PreferredChatRequestShape(key))

	changed := ProtocolCacheKey("persisted-chat-shape", "https://example.test/v1", map[string]any{"model": "renamed-alias"})
	require.Equal(t, ChatRequestShapeDefault, PreferredChatRequestShape(changed))
}

func TestRedisCapabilitiesRetryPersistenceAfterInitialSaveFailure(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	SetProtocolStore(NewRedisProtocolStore(client))
	t.Cleanup(func() {
		SetProtocolStore(nil)
		clearProtocolMemoryForTest()
	})

	t.Run("protocol", func(t *testing.T) {
		key := ProtocolCacheKey("retry-protocol", "https://example.test/v1", nil)
		server.SetError("ERR forced first save failure")
		MarkProtocolSuccess(key, ProtocolChatCompletions)
		server.SetError("")
		MarkProtocolSuccess(key, ProtocolChatCompletions)
		clearProtocolMemoryForTest()
		decision := ResolveProtocol(key)
		require.True(t, decision.Known)
		require.Equal(t, ProtocolChatCompletions, decision.Protocol)
	})

	t.Run("responses sync field capability", func(t *testing.T) {
		key := ProtocolCacheKey("retry-responses-capability", "https://example.test/v1", nil)
		server.SetError("ERR forced first save failure")
		MarkResponsesSyncMaxOutputTokensUnsupported(key)
		server.SetError("")
		MarkResponsesSyncMaxOutputTokensUnsupported(key)
		clearProtocolMemoryForTest()
		require.False(t, ResponsesSyncMaxOutputTokensSupported(key))
	})

	t.Run("chat request shape", func(t *testing.T) {
		key := ProtocolCacheKey("retry-chat-shape", "https://example.test/v1", nil)
		server.SetError("ERR forced first save failure")
		MarkChatRequestShapeSuccess(key, ChatRequestShapeMaxCompletionNeutral)
		server.SetError("")
		MarkChatRequestShapeSuccess(key, ChatRequestShapeMaxCompletionNeutral)
		clearProtocolMemoryForTest()
		require.Equal(t, ChatRequestShapeMaxCompletionNeutral, PreferredChatRequestShape(key))
	})
}

func TestSuccessfulObservationsRetryFailedPersistenceThenStopWriting(t *testing.T) {
	store := &recordingProtocolStore{}
	SetProtocolStore(store)
	t.Cleanup(func() {
		SetProtocolStore(nil)
		clearProtocolMemoryForTest()
	})

	protocolKey := ProtocolCacheKey("write-once-protocol", "https://example.test/v1", nil)
	store.failNext = true
	MarkProtocolSuccess(protocolKey, ProtocolResponses)
	MarkProtocolSuccess(protocolKey, ProtocolResponses)
	MarkProtocolSuccess(protocolKey, ProtocolResponses)

	responsesKey := ProtocolCacheKey("write-once-responses", "https://example.test/v1", nil)
	store.failNext = true
	MarkResponsesSyncMaxOutputTokensUnsupported(responsesKey)
	MarkResponsesSyncMaxOutputTokensUnsupported(responsesKey)
	MarkResponsesSyncMaxOutputTokensUnsupported(responsesKey)

	chatKey := ProtocolCacheKey("write-once-chat", "https://example.test/v1", nil)
	store.failNext = true
	MarkChatRequestShapeSuccess(chatKey, ChatRequestShapeMaxCompletionNeutral)
	MarkChatRequestShapeSuccess(chatKey, ChatRequestShapeMaxCompletionNeutral)
	MarkChatRequestShapeSuccess(chatKey, ChatRequestShapeMaxCompletionNeutral)

	protocolSaves, responsesSaves, chatSaves := store.counts()
	require.Equal(t, 2, protocolSaves, "the first failed save must retry once, then stop")
	require.Equal(t, 2, responsesSaves, "the first failed save must retry once, then stop")
	require.Equal(t, 2, chatSaves, "the first failed save must retry once, then stop")
}

func TestConcurrentSuccessfulObservationsPersistOnce(t *testing.T) {
	store := &recordingProtocolStore{}
	SetProtocolStore(store)
	t.Cleanup(func() {
		SetProtocolStore(nil)
		clearProtocolMemoryForTest()
	})
	key := ProtocolCacheKey("concurrent-write-once", "https://example.test/v1", nil)

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			MarkProtocolSuccess(key, ProtocolResponses)
		}()
	}
	wg.Wait()

	protocolSaves, _, _ := store.counts()
	require.Equal(t, 1, protocolSaves)
}

func TestProtocolStoreGenerationDoesNotReuseSavedSlot(t *testing.T) {
	first := &recordingProtocolStore{}
	second := &recordingProtocolStore{}
	SetProtocolStore(first)
	t.Cleanup(func() {
		SetProtocolStore(nil)
		clearProtocolMemoryForTest()
	})
	key := ProtocolCacheKey("store-generation", "https://example.test/v1", nil)

	MarkProtocolSuccess(key, ProtocolResponses)
	SetProtocolStore(second)
	MarkProtocolSuccess(key, ProtocolResponses)

	firstSaves, _, _ := first.counts()
	secondSaves, _, _ := second.counts()
	require.Equal(t, 1, firstSaves)
	require.Equal(t, 1, secondSaves, "a new store generation must perform its own first save")
}

func TestProtocolStoreGenerationIsolatesConcurrentOldStoreSave(t *testing.T) {
	oldStore := newBlockingProtocolStore()
	newStore := &recordingProtocolStore{}
	SetProtocolStore(oldStore)
	t.Cleanup(func() {
		SetProtocolStore(nil)
		clearProtocolMemoryForTest()
	})
	key := ProtocolCacheKey("concurrent-store-generation", "https://example.test/v1", nil)

	oldDone := make(chan struct{})
	go func() {
		defer close(oldDone)
		MarkProtocolSuccess(key, ProtocolChatCompletions)
	}()
	select {
	case <-oldStore.started:
	case <-time.After(time.Second):
		t.Fatal("old store save did not start")
	}

	SetProtocolStore(newStore)
	MarkProtocolSuccess(key, ProtocolResponses)
	close(oldStore.release)
	select {
	case <-oldDone:
	case <-time.After(time.Second):
		t.Fatal("old store save did not finish")
	}

	MarkProtocolSuccess(key, ProtocolResponses)
	newSaves, _, _ := newStore.counts()
	require.Equal(t, 1, oldStore.count())
	require.Equal(t, 1, newSaves, "old-generation completion must not suppress or reopen the new-generation slot")
	require.Equal(t, ProtocolResponses, ResolveProtocol(key).Protocol)
}

func serverProtocolValue(t *testing.T, ctx context.Context, client *redis.Client, cacheKey string) Protocol {
	t.Helper()
	value, err := client.Get(ctx, redisProtocolKey(cacheKey)).Result()
	require.NoError(t, err)
	return Protocol(value)
}

func TestProtocolStoreFailureFallsOpenToMemory(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	SetProtocolStore(NewRedisProtocolStore(client))
	t.Cleanup(func() {
		SetProtocolStore(nil)
		clearProtocolMemoryForTest()
	})

	key := ProtocolCacheKey("memory-only", "https://example.test/v1", nil)
	decision := ResolveProtocol(key)
	require.False(t, decision.Known)
	require.Equal(t, ProtocolResponses, decision.Protocol)

	MarkProtocolSuccess(key, ProtocolResponses)
	decision = ResolveProtocol(key)
	require.True(t, decision.Known)
	require.Equal(t, ProtocolResponses, decision.Protocol)
}

func TestAcquireProtocolProbeHonorsWaitingContext(t *testing.T) {
	key := ProtocolCacheKey("probe-context", "https://example.test/v1", nil)
	release, err := AcquireProtocolProbe(context.Background(), key)
	require.NoError(t, err)
	defer release()

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = AcquireProtocolProbe(waitCtx, key)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
