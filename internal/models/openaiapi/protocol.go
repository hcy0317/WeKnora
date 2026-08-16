package openaiapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Protocol identifies the two OpenAI-compatible generation endpoints that
// WeKnora can negotiate at runtime.
type Protocol string

const (
	ProtocolResponses       Protocol = "responses"
	ProtocolChatCompletions Protocol = "chat_completions"
)

var (
	protocolCache                     sync.Map
	responsesSyncMaxOutputUnsupported sync.Map
	chatRequestShapeCache             sync.Map
	storeMu                           sync.RWMutex
	protocolStore                     ProtocolStore
	protocolStoreGeneration           uint64
	persistenceSlots                  sync.Map
	probeLocks                        sync.Map
	responsesMaxOutputStates          sync.Map
	responsesMaxOutputRejectedPending sync.Map
	responsesMaxOutputSupportedSeen   sync.Map
)

type responsesMaxOutputState struct {
	mu             sync.Mutex
	epoch          uint64
	probeInFlight  bool
	fieldInFlight  int
	supportedEpoch uint64
}

type persistenceKey struct {
	storeGeneration uint64
	kind            string
	cacheKey        string
}

type persistenceSlot struct {
	mu    sync.Mutex
	value string
	saved bool
}

const (
	protocolStoreTimeout       = 250 * time.Millisecond
	protocolProbeWaitTimeout   = 250 * time.Millisecond
	protocolDecisionCacheEpoch = "v4"
	persistenceKindProtocol    = "protocol"
	persistenceKindResponses   = "responses_sync_max_output_unsupported"
	persistenceKindChatShape   = "chat_max_completion_neutral"
)

// SavedModelConfig is the common request-affecting configuration boundary for
// Chat, remote VLM, and WeKnora Cloud protocol decisions. Secrets remain only
// hash inputs and are never retained in the returned fingerprint.
type SavedModelConfig struct {
	Provider      string            `json:"provider"`
	InterfaceType string            `json:"interface_type"`
	APIVersion    string            `json:"api_version"`
	ExtraConfig   any               `json:"extra_config"`
	Headers       map[string]string `json:"headers"`
	Auth          map[string]string `json:"auth"`
}

func SavedModelConfigFingerprint(config SavedModelConfig) string {
	encoded, err := json.Marshal(config)
	if err != nil {
		encoded = []byte(fmt.Sprintf("%#v", config))
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

// ProtocolDecision distinguishes an unprobed model from a model that has
// successfully completed a request on a particular protocol.
type ProtocolDecision struct {
	Protocol Protocol
	Known    bool
}

// ProtocolStore persists successful protocol decisions across processes.
// Implementations must treat backend failures as cache misses: protocol
// negotiation is an availability optimization and must never block model use.
type ProtocolStore interface {
	Load(ctx context.Context, cacheKey string) (Protocol, bool, error)
	Save(ctx context.Context, cacheKey string, protocol Protocol) error
}

// ResponsesCapabilityStore is an optional extension implemented by shared
// protocol stores. Capabilities use the same saved-model cache key as endpoint
// negotiation, so editing the configuration automatically triggers a fresh
// probe.
type ResponsesCapabilityStore interface {
	LoadResponsesSyncMaxOutputUnsupported(ctx context.Context, cacheKey string) (bool, error)
	SaveResponsesSyncMaxOutputUnsupported(ctx context.Context, cacheKey string) error
}

// ChatRequestShapeStore is an optional persistent capability extension. Only
// the proven alternate shape is stored; the documented Chat Completions shape
// remains the default for every new saved-model configuration.
type ChatRequestShapeStore interface {
	LoadChatMaxCompletionNeutral(ctx context.Context, cacheKey string) (bool, error)
	SaveChatMaxCompletionNeutral(ctx context.Context, cacheKey string) error
}

// ChatRequestShape selects fields for the same /chat/completions endpoint.
// It is intentionally independent of model and provider names.
type ChatRequestShape string

const (
	ChatRequestShapeDefault              ChatRequestShape = "default"
	ChatRequestShapeMaxCompletionNeutral ChatRequestShape = "max_completion_neutral"
)

// SetProtocolStore installs the shared persistent backend. A nil store selects
// the process-local Lite-mode fallback.
func SetProtocolStore(store ProtocolStore) {
	storeMu.Lock()
	protocolStore = store
	protocolStoreGeneration++
	storeMu.Unlock()
}

func currentProtocolStore() (ProtocolStore, uint64) {
	storeMu.RLock()
	defer storeMu.RUnlock()
	return protocolStore, protocolStoreGeneration
}

func markPersisted(storeGeneration uint64, kind, cacheKey, value string) {
	key := persistenceKey{storeGeneration: storeGeneration, kind: kind, cacheKey: cacheKey}
	entry, _ := persistenceSlots.LoadOrStore(key, &persistenceSlot{})
	slot := entry.(*persistenceSlot)
	slot.mu.Lock()
	slot.value = value
	slot.saved = true
	slot.mu.Unlock()
}

func persistObservation(
	storeGeneration uint64,
	kind string,
	cacheKey string,
	value string,
	save func(context.Context) error,
) {
	key := persistenceKey{storeGeneration: storeGeneration, kind: kind, cacheKey: cacheKey}
	entry, _ := persistenceSlots.LoadOrStore(key, &persistenceSlot{})
	slot := entry.(*persistenceSlot)
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.saved && slot.value == value {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), protocolStoreTimeout)
	err := save(ctx)
	cancel()
	if err == nil {
		slot.value = value
		slot.saved = true
	}
}

// ProtocolCacheKey binds a negotiated protocol to one saved model
// configuration without retaining credentials or headers in memory as plain
// text. Reconstructing the same saved model yields the same key; changing any
// serialized configuration field yields a fresh key and a fresh probe.
func ProtocolCacheKey(modelID, baseURL string, configuration any) string {
	payload := struct {
		ModelID       string `json:"model_id"`
		BaseURL       string `json:"base_url"`
		Configuration any    `json:"configuration"`
	}{
		ModelID:       strings.TrimSpace(modelID),
		BaseURL:       NormalizeBaseURL(baseURL),
		Configuration: configuration,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		encoded = []byte(payload.ModelID + "\x00" + payload.BaseURL + "\x00" + fmt.Sprintf("%T", configuration))
	}
	digest := sha256.Sum256(encoded)
	// v4 invalidates decisions written before the shared saved-model
	// configuration fingerprint covered all request-affecting fields.
	return "model:" + protocolDecisionCacheEpoch + ":" + hex.EncodeToString(digest[:])
}

// NormalizeBaseURL accepts either a conventional /v1 base URL or a full
// generation endpoint. Keeping this normalization endpoint-based avoids model
// and provider allowlists while still producing one stable cache key.
func NormalizeBaseURL(baseURL string) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	lower := strings.ToLower(baseURL)
	for _, suffix := range []string{"/chat/completions", "/responses"} {
		if strings.HasSuffix(lower, suffix) {
			return strings.TrimRight(baseURL[:len(baseURL)-len(suffix)], "/")
		}
	}
	return baseURL
}

func Endpoint(baseURL string, protocol Protocol) string {
	baseURL = NormalizeBaseURL(baseURL)
	if protocol == ProtocolChatCompletions {
		return baseURL + "/chat/completions"
	}
	return baseURL + "/responses"
}

// PreferredProtocol returns the saved model's last successful decision,
// otherwise Responses. Decisions do not expire on a timer: a changed model
// configuration produces a different ProtocolCacheKey, while a later endpoint
// or request-format incompatibility flips the existing key immediately.
func PreferredProtocol(cacheKey string) Protocol {
	return ResolveProtocol(cacheKey).Protocol
}

// ResolveProtocol returns the last fully successful protocol decision. On a
// persistent-store failure it fails open to the process-local cache/default.
func ResolveProtocol(cacheKey string) ProtocolDecision {
	key := normalizeProtocolCacheLookupKey(cacheKey)
	if cached, ok := protocolCache.Load(key); ok {
		return ProtocolDecision{Protocol: cached.(Protocol), Known: true}
	}
	store, storeGeneration := currentProtocolStore()
	if store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), protocolStoreTimeout)
		protocol, found, err := store.Load(ctx, key)
		cancel()
		if err == nil && found && validProtocol(protocol) {
			protocolCache.Store(key, protocol)
			markPersisted(storeGeneration, persistenceKindProtocol, key, string(protocol))
			return ProtocolDecision{Protocol: protocol, Known: true}
		}
	}
	return ProtocolDecision{Protocol: ProtocolResponses, Known: false}
}

func AlternateProtocol(protocol Protocol) Protocol {
	if protocol == ProtocolChatCompletions {
		return ProtocolResponses
	}
	return ProtocolChatCompletions
}

func MarkProtocolSuccess(cacheKey string, protocol Protocol) {
	if !validProtocol(protocol) {
		return
	}
	key := normalizeProtocolCacheLookupKey(cacheKey)
	protocolCache.Store(key, protocol)
	store, storeGeneration := currentProtocolStore()
	if store != nil {
		persistObservation(storeGeneration, persistenceKindProtocol, key, string(protocol), func(ctx context.Context) error {
			return store.Save(ctx, key, protocol)
		})
	}
}

// MarkProtocolUnsupported immediately flips the endpoint to the alternate
// protocol. This is also the re-probe path when a previously cached protocol
// later starts returning endpoint-level errors.
func MarkProtocolUnsupported(cacheKey string, protocol Protocol) Protocol {
	return AlternateProtocol(protocol)
}

// ResponsesSyncMaxOutputTokensSupported reports whether a non-streaming
// Responses request may include max_output_tokens. Unknown configurations
// default to the standard API shape and are downgraded only after the provider
// explicitly rejects that exact field and a same-endpoint retry succeeds.
func ResponsesSyncMaxOutputTokensSupported(cacheKey string) bool {
	key := normalizeProtocolCacheLookupKey(cacheKey)
	if _, rejected := responsesMaxOutputRejectedPending.Load(key); rejected {
		return false
	}
	if _, unsupported := responsesSyncMaxOutputUnsupported.Load(key); unsupported {
		return false
	}
	store, storeGeneration := currentProtocolStore()
	capabilityStore, ok := store.(ResponsesCapabilityStore)
	if !ok {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), protocolStoreTimeout)
	unsupported, err := capabilityStore.LoadResponsesSyncMaxOutputUnsupported(ctx, key)
	cancel()
	if err != nil || !unsupported {
		return true
	}
	responsesSyncMaxOutputUnsupported.Store(key, struct{}{})
	markPersisted(storeGeneration, persistenceKindResponses, key, "1")
	return false
}

// MarkResponsesSyncMaxOutputTokensUnsupported persists only a proven negative
// capability. Streaming remains unaffected because the observed incompatibility
// belongs to the provider's synchronous Responses path.
func MarkResponsesSyncMaxOutputTokensUnsupported(cacheKey string) {
	key := normalizeProtocolCacheLookupKey(cacheKey)
	state := responsesMaxOutputStateFor(key)
	state.mu.Lock()
	state.epoch++
	commitResponsesSyncMaxOutputUnsupportedLocked(key, state)
	state.mu.Unlock()
	persistResponsesSyncMaxOutputUnsupported(key)
}

func commitResponsesSyncMaxOutputUnsupportedLocked(key string, state *responsesMaxOutputState) {
	state.probeInFlight = false
	state.supportedEpoch = 0
	responsesMaxOutputRejectedPending.Delete(key)
	responsesMaxOutputSupportedSeen.Delete(key)
	responsesSyncMaxOutputUnsupported.Store(key, struct{}{})
}

func persistResponsesSyncMaxOutputUnsupported(key string) {
	store, storeGeneration := currentProtocolStore()
	capabilityStore, ok := store.(ResponsesCapabilityStore)
	if !ok {
		return
	}
	persistObservation(storeGeneration, persistenceKindResponses, key, "1", func(ctx context.Context) error {
		return capabilityStore.SaveResponsesSyncMaxOutputUnsupported(ctx, key)
	})
}

// MarkResponsesSyncMaxOutputTokensSupported avoids re-serializing later calls
// after the documented field has completed successfully in this process.
func MarkResponsesSyncMaxOutputTokensSupported(cacheKey string) {
	key := normalizeProtocolCacheLookupKey(cacheKey)
	state := responsesMaxOutputStateFor(key)
	state.mu.Lock()
	if _, unsupported := responsesSyncMaxOutputUnsupported.Load(key); unsupported {
		state.probeInFlight = false
		state.mu.Unlock()
		return
	}
	state.epoch++
	state.supportedEpoch = state.epoch
	state.probeInFlight = false
	responsesMaxOutputSupportedSeen.Store(key, struct{}{})
	responsesMaxOutputRejectedPending.Delete(key)
	state.mu.Unlock()
}

// MarkResponsesSyncMaxOutputTokensRejectedPending exposes a first request's
// explicit field rejection to concurrent callers before its no-field retry has
// completed. The observation is process-local and is persisted only after a
// no-field request succeeds.
func MarkResponsesSyncMaxOutputTokensRejectedPending(cacheKey string) uint64 {
	key := normalizeProtocolCacheLookupKey(cacheKey)
	responsesMaxOutputRejectedPending.Store(key, struct{}{})
	state := responsesMaxOutputStateFor(key)
	state.mu.Lock()
	state.epoch++
	state.probeInFlight = true
	epoch := state.epoch
	state.mu.Unlock()
	return epoch
}

func ClearResponsesSyncMaxOutputTokensRejectedPending(cacheKey string, rejectionEpoch uint64) {
	key := normalizeProtocolCacheLookupKey(cacheKey)
	state := responsesMaxOutputStateFor(key)
	state.mu.Lock()
	if state.epoch != rejectionEpoch {
		state.mu.Unlock()
		return
	}
	state.epoch++
	state.probeInFlight = false
	responsesMaxOutputRejectedPending.Delete(key)
	state.mu.Unlock()
}

// ConfirmResponsesSyncMaxOutputTokensUnsupported persists a successful
// no-field retry only when the rejection still belongs to the current
// capability epoch. A masked upstream_error cannot override any observed field
// success; an explicit max_output_tokens rejection may, because it is direct
// schema evidence that the upstream shape changed.
func ConfirmResponsesSyncMaxOutputTokensUnsupported(
	cacheKey string,
	rejectionEpoch uint64,
	explicit bool,
) bool {
	key := normalizeProtocolCacheLookupKey(cacheKey)
	state := responsesMaxOutputStateFor(key)
	state.mu.Lock()
	if state.epoch != rejectionEpoch {
		state.mu.Unlock()
		return false
	}
	if !explicit && (state.supportedEpoch > 0 || state.fieldInFlight > 1) {
		state.epoch++
		state.probeInFlight = false
		responsesMaxOutputRejectedPending.Delete(key)
		state.mu.Unlock()
		return false
	}
	state.epoch++
	commitResponsesSyncMaxOutputUnsupportedLocked(key, state)
	state.mu.Unlock()
	persistResponsesSyncMaxOutputUnsupported(key)
	return true
}

// PreferredChatRequestShape returns the only alternate shape that has been
// proven by a successful same-endpoint retry for this exact configuration.
func PreferredChatRequestShape(cacheKey string) ChatRequestShape {
	key := normalizeProtocolCacheLookupKey(cacheKey)
	if _, ok := chatRequestShapeCache.Load(key); ok {
		return ChatRequestShapeMaxCompletionNeutral
	}
	store, storeGeneration := currentProtocolStore()
	capabilityStore, ok := store.(ChatRequestShapeStore)
	if !ok {
		return ChatRequestShapeDefault
	}
	ctx, cancel := context.WithTimeout(context.Background(), protocolStoreTimeout)
	modern, err := capabilityStore.LoadChatMaxCompletionNeutral(ctx, key)
	cancel()
	if err != nil || !modern {
		return ChatRequestShapeDefault
	}
	chatRequestShapeCache.Store(key, struct{}{})
	markPersisted(storeGeneration, persistenceKindChatShape, key, string(ChatRequestShapeMaxCompletionNeutral))
	return ChatRequestShapeMaxCompletionNeutral
}

// MarkChatRequestShapeSuccess caches only a successfully observed alternate
// shape. A rejected retry, timeout, opaque error, or 5xx is never cached.
func MarkChatRequestShapeSuccess(cacheKey string, shape ChatRequestShape) {
	if shape != ChatRequestShapeMaxCompletionNeutral {
		return
	}
	key := normalizeProtocolCacheLookupKey(cacheKey)
	chatRequestShapeCache.Store(key, struct{}{})
	store, storeGeneration := currentProtocolStore()
	capabilityStore, ok := store.(ChatRequestShapeStore)
	if !ok {
		return
	}
	persistObservation(
		storeGeneration,
		persistenceKindChatShape,
		key,
		string(ChatRequestShapeMaxCompletionNeutral),
		func(ctx context.Context) error {
			return capabilityStore.SaveChatMaxCompletionNeutral(ctx, key)
		},
	)
}

// BuildChatRequestWithShape serializes a Chat request using either the
// documented/default fields or the one bounded alternate shape. The alternate
// moves the output budget to max_completion_tokens and removes sampling
// controls that reasoning-style endpoints explicitly reject.
func BuildChatRequestWithShape(request any, shape ChatRequestShape) (map[string]any, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal Chat Completions request: %w", err)
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		return nil, fmt.Errorf("decode Chat Completions request: %w", err)
	}
	if shape != ChatRequestShapeMaxCompletionNeutral {
		return body, nil
	}
	if _, exists := body["max_completion_tokens"]; !exists {
		if maxTokens, ok := body["max_tokens"]; ok {
			body["max_completion_tokens"] = maxTokens
		}
	}
	delete(body, "max_tokens")
	delete(body, "temperature")
	delete(body, "top_p")
	delete(body, "frequency_penalty")
	delete(body, "presence_penalty")
	return body, nil
}

func validProtocol(protocol Protocol) bool {
	return protocol == ProtocolResponses || protocol == ProtocolChatCompletions
}

type protocolProbeLock struct {
	token chan struct{}
}

// AcquireProtocolProbe serializes only the initial unknown-protocol request for
// one saved model. Waiting is context-aware and bounded: if the first upstream
// request stalls before response headers, later business requests continue
// with the default Responses preference instead of being swallowed behind a
// 12-18 minute mutex wait. Once a successful decision exists, callers skip
// this path entirely.
func AcquireProtocolProbe(ctx context.Context, cacheKey string) (func(), error) {
	key := normalizeProtocolCacheLookupKey(cacheKey)
	candidate := &protocolProbeLock{token: make(chan struct{}, 1)}
	candidate.token <- struct{}{}
	entry, _ := probeLocks.LoadOrStore(key, candidate)
	lock := entry.(*protocolProbeLock)
	timer := time.NewTimer(protocolProbeWaitTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return func() {}, nil
	case <-lock.token:
		return func() { lock.token <- struct{}{} }, nil
	}
}

// BeginResponsesSyncMaxOutputProbe elects one unknown request to carry the
// documented max_output_tokens field. Concurrent followers never wait for a
// long generation: while the leader is in flight they immediately omit the
// field, but their success does not persist a capability verdict. Only the
// leader may mark support, or persist rejection after its same-endpoint
// no-field retry succeeds.
func BeginResponsesSyncMaxOutputProbe(
	ctx context.Context,
	cacheKey string,
) (omitMaxOutputTokens bool, release func(), err error) {
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	key := normalizeProtocolCacheLookupKey(cacheKey)
	if !ResponsesSyncMaxOutputTokensSupported(cacheKey) {
		return true, func() {}, nil
	}
	state := responsesMaxOutputStateFor(key)
	state.mu.Lock()
	if _, rejected := responsesMaxOutputRejectedPending.Load(key); rejected {
		state.mu.Unlock()
		return true, func() {}, nil
	}
	if _, unsupported := responsesSyncMaxOutputUnsupported.Load(key); unsupported {
		state.mu.Unlock()
		return true, func() {}, nil
	}
	if state.probeInFlight {
		state.mu.Unlock()
		return true, func() {}, nil
	}
	if _, supported := responsesMaxOutputSupportedSeen.Load(key); supported || state.supportedEpoch > 0 {
		state.fieldInFlight++
		state.mu.Unlock()
		return false, responsesMaxOutputFieldRelease(state, 0, false), nil
	}
	state.epoch++
	epoch := state.epoch
	state.probeInFlight = true
	state.fieldInFlight++
	state.mu.Unlock()
	return false, responsesMaxOutputFieldRelease(state, epoch, true), nil
}

func responsesMaxOutputFieldRelease(
	state *responsesMaxOutputState,
	probeEpoch uint64,
	ownsProbe bool,
) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			state.mu.Lock()
			if state.fieldInFlight > 0 {
				state.fieldInFlight--
			}
			if ownsProbe && state.epoch == probeEpoch {
				state.probeInFlight = false
			}
			state.mu.Unlock()
		})
	}
}

func responsesMaxOutputStateFor(key string) *responsesMaxOutputState {
	state, _ := responsesMaxOutputStates.LoadOrStore(key, &responsesMaxOutputState{})
	return state.(*responsesMaxOutputState)
}

func normalizeProtocolCacheLookupKey(cacheKey string) string {
	cacheKey = strings.TrimSpace(cacheKey)
	if strings.HasPrefix(cacheKey, "model:") {
		return cacheKey
	}
	return NormalizeBaseURL(cacheKey)
}

// IsEndpointUnsupported deliberately follows sub2api's conservative rule:
// only 404 and 405 prove that the selected protocol endpoint does not exist.
// Validation, auth, rate-limit, and 5xx errors must not silently replay a
// potentially billable request on another endpoint.
func IsEndpointUnsupported(statusCode int) bool {
	return statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed
}

// ProtocolHTTPError preserves the upstream response body for conservative
// schema-error detection without making every caller parse formatted strings.
type ProtocolHTTPError struct {
	Protocol   Protocol
	StatusCode int
	Body       string
}

func (e *ProtocolHTTPError) Error() string {
	return fmt.Sprintf(
		"%s API request failed with status %d: %s",
		e.Protocol, e.StatusCode, strings.TrimSpace(e.Body),
	)
}

func NewProtocolHTTPError(protocol Protocol, statusCode int, body string) error {
	return &ProtocolHTTPError{Protocol: protocol, StatusCode: statusCode, Body: body}
}

// IsProtocolFormatError recognizes only explicit request-schema or endpoint
// capability errors. Generic 400s, auth, limits, safety rejections, and 5xx
// failures do not qualify for replay on the alternate protocol.
func IsProtocolFormatError(statusCode int, err error) bool {
	if statusCode != http.StatusBadRequest && statusCode != http.StatusUnprocessableEntity {
		return false
	}
	body := ""
	var protocolErr *ProtocolHTTPError
	if errors.As(err, &protocolErr) {
		body = protocolErr.Body
	} else if err != nil {
		body = err.Error()
	}
	body = strings.ToLower(body)
	for _, marker := range []string{
		"unknown parameter",
		"unknown_parameter",
		"unsupported parameter",
		"unsupported_parameter",
		"unrecognized request argument",
		"unrecognized_request_argument",
		"unknown field",
		"unrecognized field",
		"unsupported field",
		"invalid request schema",
		"invalid_request_schema",
		"unsupported request format",
		"does not support responses",
		"doesn't support responses",
		"not support responses",
		"not supported maxtokens",
		"please use maxcompletiontokens",
		"please use max_completion_tokens",
	} {
		if strings.Contains(body, marker) {
			return true
		}
	}
	return false
}

// IsMaxOutputTokensUnsupported recognizes only an explicit rejection of the
// Responses max_output_tokens field. Callers handle it before generic
// format-error fallback so a partial Responses implementation is retried on
// /responses rather than mislabeled as Chat-Completions-only.
func IsMaxOutputTokensUnsupported(statusCode int, err error) bool {
	if statusCode != http.StatusBadRequest && statusCode != http.StatusUnprocessableEntity {
		return false
	}
	body := ""
	var protocolErr *ProtocolHTTPError
	if errors.As(err, &protocolErr) {
		body = protocolErr.Body
	} else if err != nil {
		body = err.Error()
	}
	body = strings.ToLower(body)
	if !strings.Contains(body, "max_output_tokens") {
		return false
	}
	for _, marker := range []string{
		"unsupported parameter",
		"unsupported_parameter",
		"unknown parameter",
		"unknown_parameter",
		"unrecognized request argument",
		"unrecognized_request_argument",
		"unknown field",
		"unrecognized field",
		"unsupported field",
	} {
		if strings.Contains(body, marker) {
			return true
		}
	}
	return false
}

// ShouldRetryResponsesWithoutMaxOutputTokens recognizes the one bounded
// synchronous Responses downgrade. Some gateways hide the upstream field
// rejection and return only their exact generic upstream_error envelope, so
// that envelope is eligible for the same-endpoint retry as well. The caller
// must separately prove that the request body actually contained
// max_output_tokens and cache the downgrade only after the retry succeeds.
func ShouldRetryResponsesWithoutMaxOutputTokens(statusCode int, err error) bool {
	if IsMaxOutputTokensUnsupported(statusCode, err) {
		return true
	}
	if statusCode != http.StatusBadRequest && statusCode != http.StatusUnprocessableEntity {
		return false
	}

	var protocolErr *ProtocolHTTPError
	if !errors.As(err, &protocolErr) {
		return false
	}
	var envelope map[string]json.RawMessage
	if json.Unmarshal([]byte(protocolErr.Body), &envelope) != nil || len(envelope) != 1 {
		return false
	}
	errorBody, ok := envelope["error"]
	if !ok {
		return false
	}
	var upstreamError map[string]json.RawMessage
	if json.Unmarshal(errorBody, &upstreamError) != nil || len(upstreamError) != 2 {
		return false
	}
	messageBody, hasMessage := upstreamError["message"]
	typeBody, hasType := upstreamError["type"]
	if !hasMessage || !hasType {
		return false
	}
	var message, errorType string
	if json.Unmarshal(messageBody, &message) != nil || json.Unmarshal(typeBody, &errorType) != nil {
		return false
	}
	return strings.TrimSpace(message) == "Upstream request failed" &&
		strings.TrimSpace(errorType) == "upstream_error"
}

// ShouldRetryChatWithMaxCompletionNeutral recognizes only explicit 400/422
// evidence that the documented Chat fields or sampling controls are
// incompatible. It never treats opaque, transport, timeout, or 5xx failures as
// capability evidence.
func ShouldRetryChatWithMaxCompletionNeutral(statusCode int, err error) bool {
	if statusCode != http.StatusBadRequest && statusCode != http.StatusUnprocessableEntity {
		return false
	}
	body := ""
	var protocolErr *ProtocolHTTPError
	if errors.As(err, &protocolErr) {
		body = protocolErr.Body
	} else if err != nil {
		body = err.Error()
	}
	body = strings.ToLower(body)
	fieldMentioned := false
	for _, field := range []string{
		"max_tokens", "max tokens", "temperature", "top_p", "top p",
		"frequency_penalty", "presence_penalty",
	} {
		if strings.Contains(body, field) {
			fieldMentioned = true
			break
		}
	}
	if !fieldMentioned {
		return false
	}
	for _, marker := range []string{
		"unsupported", "not supported", "does not support", "doesn't support",
		"unknown parameter", "unknown_parameter", "unrecognized request argument",
		"unknown field", "unrecognized field", "please use max_completion_tokens",
		"use max_completion_tokens",
	} {
		if strings.Contains(body, marker) {
			return true
		}
	}
	return false
}

// ShouldTryAlternateForDecision permits replay only when the response proves
// endpoint absence or request-format incompatibility. Opaque and transient
// failures are never evidence that Chat Completions is the correct endpoint.
func ShouldTryAlternateForDecision(decision ProtocolDecision, statusCode int, err error) bool {
	if decision.Known && decision.Protocol == ProtocolResponses &&
		(statusCode == http.StatusBadRequest || statusCode == http.StatusUnprocessableEntity) {
		return false
	}
	return ShouldTryAlternateProtocol(statusCode, err)
}

func ShouldTryAlternateProtocol(statusCode int, err error) bool {
	return IsEndpointUnsupported(statusCode) || IsProtocolFormatError(statusCode, err)
}

func clearProtocolMemoryForTest() {
	protocolCache.Range(func(key, _ any) bool {
		protocolCache.Delete(key)
		return true
	})
	responsesSyncMaxOutputUnsupported.Range(func(key, _ any) bool {
		responsesSyncMaxOutputUnsupported.Delete(key)
		return true
	})
	responsesMaxOutputRejectedPending.Range(func(key, _ any) bool {
		responsesMaxOutputRejectedPending.Delete(key)
		return true
	})
	responsesMaxOutputStates.Range(func(key, _ any) bool {
		responsesMaxOutputStates.Delete(key)
		return true
	})
	responsesMaxOutputSupportedSeen.Range(func(key, _ any) bool {
		responsesMaxOutputSupportedSeen.Delete(key)
		return true
	})
	chatRequestShapeCache.Range(func(key, _ any) bool {
		chatRequestShapeCache.Delete(key)
		return true
	})
}
