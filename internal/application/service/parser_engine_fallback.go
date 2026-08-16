package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

const parserEngineFallbackCooldown = 5 * time.Minute

type parserEngineFallbackEntry struct {
	retryAfter time.Time
	probing    bool
}

// parserEngineFallbackTracker is a request-triggered circuit breaker for
// explicitly selected parser engines. It does not start goroutines or alter
// worker concurrency: the first request after retryAfter probes the selected
// engine, while requests arriving during cooldown (or during that probe) use
// the default parser path.
type parserEngineFallbackTracker struct {
	mu      sync.Mutex
	entries map[string]parserEngineFallbackEntry
	now     func() time.Time
}

type parserEngineFallbackDecision struct {
	key            string
	selectedEngine string
	activeEngine   string
	retryAfter     time.Time
	probe          bool
	cooling        bool
}

func (t *parserEngineFallbackTracker) currentTime() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

func isExplicitParserEngine(engine string) bool {
	engine = strings.ToLower(strings.TrimSpace(engine))
	return engine != "" && engine != "builtin" && engine != docparser.SimpleEngineName
}

func parserEngineFallbackKey(engine string, overrides map[string]string) string {
	engine = strings.ToLower(strings.TrimSpace(engine))
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	h := sha256.New()
	_, _ = h.Write([]byte(engine))
	for _, key := range keys {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(overrides[key]))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (t *parserEngineFallbackTracker) choose(engine string, overrides map[string]string) parserEngineFallbackDecision {
	engine = strings.TrimSpace(engine)
	decision := parserEngineFallbackDecision{
		selectedEngine: engine,
		activeEngine:   engine,
	}
	if !isExplicitParserEngine(engine) {
		return decision
	}

	decision.key = parserEngineFallbackKey(engine, overrides)
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, exists := t.entries[decision.key]
	if !exists {
		return decision
	}

	now := t.currentTime()
	if entry.probing || now.Before(entry.retryAfter) {
		decision.activeEngine = ""
		decision.retryAfter = entry.retryAfter
		decision.cooling = true
		return decision
	}

	entry.probing = true
	t.entries[decision.key] = entry
	decision.probe = true
	return decision
}

func (t *parserEngineFallbackTracker) recordFailure(decision parserEngineFallbackDecision) time.Time {
	if decision.key == "" {
		return time.Time{}
	}
	retryAfter := t.currentTime().Add(parserEngineFallbackCooldown)
	t.mu.Lock()
	if t.entries == nil {
		t.entries = make(map[string]parserEngineFallbackEntry)
	}
	t.entries[decision.key] = parserEngineFallbackEntry{retryAfter: retryAfter}
	t.mu.Unlock()
	return retryAfter
}

func (t *parserEngineFallbackTracker) recordSuccess(decision parserEngineFallbackDecision) {
	if decision.key == "" {
		return
	}
	t.mu.Lock()
	delete(t.entries, decision.key)
	t.mu.Unlock()
}

func (t *parserEngineFallbackTracker) cancelProbe(decision parserEngineFallbackDecision) {
	if decision.key == "" || !decision.probe {
		return
	}
	t.mu.Lock()
	delete(t.entries, decision.key)
	t.mu.Unlock()
}

func parserFailureReason(result *types.ReadResult, err error) string {
	if err != nil {
		return err.Error()
	}
	if result == nil {
		return "parser returned no result"
	}
	if strings.TrimSpace(result.Error) != "" {
		return result.Error
	}
	return "parser failed"
}

func hasSemanticParserContent(result *types.ReadResult) bool {
	if result == nil {
		return false
	}
	return strings.TrimSpace(result.MarkdownContent) != "" ||
		len(result.ImageRefs) > 0 ||
		result.IsAudio ||
		len(result.AudioData) > 0
}

func validateParserReadResult(result *types.ReadResult, err error, parserName string) error {
	if err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("%s returned no result", parserName)
	}
	if strings.TrimSpace(result.Error) != "" {
		return nil
	}
	if !hasSemanticParserContent(result) {
		return fmt.Errorf("%s returned empty content", parserName)
	}
	return nil
}

// readWithParserEngineFallback calls an explicitly selected parser first and
// falls back to the normal default route when that engine is unavailable. A
// failed selection enters a five-minute cooldown. Retry is request-triggered;
// no background polling or extra parsing concurrency is introduced.
func (s *knowledgeService) readWithParserEngineFallback(
	ctx context.Context,
	selectedEngine string,
	fileType string,
	isURL bool,
	overrides map[string]string,
	req *types.ReadRequest,
) (*types.ReadResult, string, bool, error) {
	decision := s.parserEngineFallback.choose(selectedEngine, overrides)
	activeEngine := decision.activeEngine
	if decision.cooling {
		logger.Warnf(ctx,
			"[convert] selected parser engine=%q is cooling down until %s; using default parser",
			decision.selectedEngine, decision.retryAfter.Format(time.RFC3339))
	}

	reader := s.resolveDocReader(ctx, activeEngine, fileType, isURL, overrides)
	if reader == nil && ctx.Err() != nil {
		s.parserEngineFallback.cancelProbe(decision)
		return nil, activeEngine, true, ctx.Err()
	}
	if reader == nil && activeEngine == decision.selectedEngine && decision.key != "" {
		retryAfter := s.parserEngineFallback.recordFailure(decision)
		logger.Warnf(ctx,
			"[convert] selected parser engine=%q is unavailable; using default parser and retrying after %s",
			decision.selectedEngine, retryAfter.Format(time.RFC3339))
		activeEngine = ""
		reader = s.resolveDocReader(ctx, activeEngine, fileType, isURL, overrides)
	}
	if reader == nil {
		if ctx.Err() != nil {
			s.parserEngineFallback.cancelProbe(decision)
			return nil, activeEngine, true, ctx.Err()
		}
		return nil, activeEngine, false, nil
	}

	req.ParserEngine = activeEngine
	result, err := s.callDocReaderWithTimeout(ctx, reader, req)
	selectedAttempt := decision.key != "" && activeEngine == decision.selectedEngine
	parserName := "document parser"
	if activeEngine == "" {
		parserName = "default document parser"
	}
	err = validateParserReadResult(result, err, parserName)
	if !selectedAttempt {
		return result, activeEngine, true, err
	}
	if err == nil && strings.TrimSpace(result.Error) == "" {
		s.parserEngineFallback.recordSuccess(decision)
		return result, activeEngine, true, nil
	}

	// A cancelled/expired document task is not evidence that the parser engine
	// is unavailable. Preserve the original cancellation and leave the next
	// request free to try the selected engine again.
	if ctx.Err() != nil {
		s.parserEngineFallback.cancelProbe(decision)
		return result, activeEngine, true, err
	}

	reason := parserFailureReason(result, err)
	retryAfter := s.parserEngineFallback.recordFailure(decision)
	logger.Warnf(ctx,
		"[convert] selected parser engine=%q failed: %s; using default parser and retrying selected engine after %s",
		decision.selectedEngine, reason, retryAfter.Format(time.RFC3339))

	defaultReader := s.resolveDocReader(ctx, "", fileType, isURL, overrides)
	if defaultReader == nil {
		return result, activeEngine, true, err
	}
	req.ParserEngine = ""
	fallbackResult, fallbackErr := s.callDocReaderWithTimeout(ctx, defaultReader, req)
	fallbackErr = validateParserReadResult(fallbackResult, fallbackErr, "default document parser")
	return fallbackResult, "", true, fallbackErr
}
