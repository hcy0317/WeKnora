package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	wikiGenerationLeaseGrace  = 5 * time.Minute
	wikiGenerationFallbackTTL = 30 * 24 * time.Hour
)

type wikiGenerationScope struct {
	TenantID        uint64
	KnowledgeBaseID string
	WorkRevision    string
	// RecoveryRevision is present only for an explicit partial-repair attempt.
	// Generated base fragments remain reusable; only ambiguous or terminal
	// fragments fork into this attempt-scoped paid-call budget.
	RecoveryRevision string
	RuntimeSnapshot  string
}

type wikiGenerationScopeContextKey struct{}

func withWikiGenerationScope(ctx context.Context, scope wikiGenerationScope) context.Context {
	return context.WithValue(ctx, wikiGenerationScopeContextKey{}, scope)
}

func wikiGenerationScopeFromContext(ctx context.Context) (wikiGenerationScope, bool) {
	if ctx == nil {
		return wikiGenerationScope{}, false
	}
	scope, ok := ctx.Value(wikiGenerationScopeContextKey{}).(wikiGenerationScope)
	return scope, ok && scope.TenantID > 0 && scope.KnowledgeBaseID != "" && scope.WorkRevision != ""
}

type wikiGenerationFallback struct {
	CallID string `json:"call_id"`
	Output string `json:"output"`
}

type wikiGenerationLedger struct {
	store             interfaces.WikiGenerationFragmentStore
	redis             *redis.Client
	candidate         *types.WikiGenerationFragment
	recoveryCandidate *types.WikiGenerationFragment
}

func (s *wikiIngestService) prepareWikiGenerationLedger(
	ctx context.Context,
	purpose string,
	requestJSON []byte,
	chatModel chat.Chat,
) (*wikiGenerationLedger, error) {
	scope, ok := wikiGenerationScopeFromContext(ctx)
	if !ok {
		return nil, nil
	}
	store, ok := s.wikiService.(interfaces.WikiGenerationFragmentStore)
	if !ok || store == nil {
		return nil, nil
	}
	promptDigest := wikiCheckpointDigest(string(requestJSON))
	modelID := ""
	if chatModel != nil {
		modelID = chatModel.GetModelID()
	}
	modelSnapshot := wikiCheckpointDigest(scope.RuntimeSnapshot, modelID)
	fragmentKey := wikiCheckpointDigest(purpose, promptDigest)
	candidateForRevision := func(workRevision string) *types.WikiGenerationFragment {
		fragmentID := wikiCheckpointDigest(
			strconv.FormatUint(scope.TenantID, 10), scope.KnowledgeBaseID,
			workRevision, purpose, fragmentKey, promptDigest, modelSnapshot,
		)
		return &types.WikiGenerationFragment{
			FragmentID: fragmentID, TenantID: scope.TenantID,
			KnowledgeBaseID: scope.KnowledgeBaseID, WorkRevision: workRevision,
			Purpose: purpose, FragmentKey: fragmentKey, PromptDigest: promptDigest,
			ModelSnapshot: modelSnapshot, State: types.WikiGenerationFragmentReady,
		}
	}
	primary := candidateForRevision(scope.WorkRevision)
	var recovery *types.WikiGenerationFragment
	if scope.RecoveryRevision != "" && scope.RecoveryRevision != scope.WorkRevision {
		recovery = candidateForRevision(scope.RecoveryRevision)
	}
	return &wikiGenerationLedger{
		store:     store,
		redis:     s.redisClient,
		candidate: primary, recoveryCandidate: recovery,
	}, nil
}

func (l *wikiGenerationLedger) fallbackKey() string {
	return "wiki:generation:fragment:" + l.candidate.FragmentID
}

func (l *wikiGenerationLedger) recoverFallback(
	ctx context.Context, expected *types.WikiGenerationFragment,
) (*types.WikiGenerationFragment, bool, error) {
	if l == nil || l.redis == nil {
		return nil, false, nil
	}
	raw, err := l.redis.Get(ctx, l.fallbackKey()).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		logger.Warnf(ctx, "wiki generation: read Redis output fallback failed for %s: %v", l.candidate.FragmentID, err)
		return nil, false, nil
	}
	var fallback wikiGenerationFallback
	if err := json.Unmarshal(raw, &fallback); err != nil || fallback.CallID == "" || fallback.Output == "" ||
		expected == nil || fallback.CallID != expected.CallID {
		return nil, false, fmt.Errorf("restore wiki generation Redis fallback: invalid payload")
	}
	if err := l.store.CompleteWikiGenerationFragment(
		ctx, l.candidate.FragmentID, fallback.CallID, fallback.Output,
	); err != nil {
		return nil, false, fmt.Errorf("restore wiki generation Redis fallback: %w", err)
	}
	if err := l.redis.Del(ctx, l.fallbackKey()).Err(); err != nil {
		logger.Warnf(ctx, "wiki generation: delete promoted Redis fallback failed for %s: %v", l.candidate.FragmentID, err)
	}
	copy := *expected
	copy.State = types.WikiGenerationFragmentGenerated
	copy.CallID = ""
	copy.LeaseUntil = nil
	copy.Output = fallback.Output
	return &copy, true, nil
}

func (l *wikiGenerationLedger) reserve(
	ctx context.Context, attemptTimeout time.Duration,
) (*types.WikiGenerationFragment, bool, error) {
	reserveCandidate := func(candidate *types.WikiGenerationFragment) (*types.WikiGenerationFragment, bool, error) {
		callID := uuid.NewString()
		return l.store.ReserveWikiGenerationFragment(
			ctx, candidate, callID, time.Now().Add(attemptTimeout+wikiGenerationLeaseGrace), wikiLLMMaxAttempts,
		)
	}
	primary := l.candidate
	fragment, granted, err := reserveCandidate(primary)
	if err != nil {
		return nil, false, fmt.Errorf("reserve wiki generation fragment: %w", err)
	}
	l.candidate = primary
	if !granted && l.recoveryCandidate != nil &&
		(fragment.State == types.WikiGenerationFragmentAmbiguous ||
			fragment.State == types.WikiGenerationFragmentTerminal) {
		l.candidate = l.recoveryCandidate
		fragment, granted, err = reserveCandidate(l.recoveryCandidate)
		if err != nil {
			return nil, false, fmt.Errorf("reserve wiki recovery fragment: %w", err)
		}
	}
	if granted {
		return fragment, true, nil
	}
	switch fragment.State {
	case types.WikiGenerationFragmentGenerated, types.WikiGenerationFragmentSucceeded:
		if fragment.Output == "" {
			return nil, false, newWikiGenerationError(
				WikiGenerationErrorPersistence,
				errors.New("durable wiki generation fragment has empty output"),
			)
		}
		return fragment, false, nil
	case types.WikiGenerationFragmentTerminal:
		return nil, false, newWikiGenerationError(
			WikiGenerationErrorBudgetExhausted,
			fmt.Errorf("wiki generation fragment exhausted %d paid-call attempts", fragment.Attempts),
		)
	case types.WikiGenerationFragmentAmbiguous:
		return nil, false, newWikiGenerationError(
			WikiGenerationErrorAmbiguousCall,
			errors.New("wiki generation call outcome is ambiguous; refusing another paid call"),
		)
	case types.WikiGenerationFragmentCalling:
		if restored, ok, err := l.recoverFallback(ctx, fragment); err != nil || ok {
			return restored, false, err
		}
		return nil, false, newWikiGenerationError(
			WikiGenerationErrorTransientTransport,
			errors.New("wiki generation fragment is owned by another live call"),
		)
	default:
		return nil, false, fmt.Errorf("wiki generation fragment returned unexpected state %q", fragment.State)
	}
}

func (l *wikiGenerationLedger) complete(
	ctx context.Context, fragment *types.WikiGenerationFragment, output string,
) error {
	if l == nil || fragment == nil {
		return nil
	}
	if err := l.store.CompleteWikiGenerationFragment(ctx, fragment.FragmentID, fragment.CallID, output); err == nil {
		if l.redis != nil {
			_ = l.redis.Del(ctx, l.fallbackKey()).Err()
		}
		return nil
	} else {
		persistErr := err
		fallbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if l.redis != nil {
			encoded, marshalErr := json.Marshal(wikiGenerationFallback{CallID: fragment.CallID, Output: output})
			if marshalErr == nil {
				if redisErr := l.redis.Set(fallbackCtx, l.fallbackKey(), encoded, wikiGenerationFallbackTTL).Err(); redisErr == nil {
					logger.Warnf(ctx,
						"wiki generation: DB output checkpoint failed for %s; continuing from exact Redis fallback: %v",
						fragment.FragmentID, persistErr,
					)
					return nil
				} else {
					persistErr = errors.Join(persistErr, redisErr)
				}
			} else {
				persistErr = errors.Join(persistErr, marshalErr)
			}
		}
		_ = l.store.MarkWikiGenerationFragmentAmbiguous(
			fallbackCtx, fragment.FragmentID, fragment.CallID, persistErr.Error(),
		)
		logger.Errorf(ctx, "wiki generation: DB and Redis output persistence both failed for %s: %v", fragment.FragmentID, persistErr)
		return newWikiGenerationError(WikiGenerationErrorAmbiguousCall,
			fmt.Errorf("persist wiki generation output: %w", persistErr))
	}
}

func (l *wikiGenerationLedger) settleFailure(
	ctx context.Context,
	fragment *types.WikiGenerationFragment,
	callErr error,
) error {
	if l == nil || fragment == nil || fragment.CallID == "" {
		return nil
	}
	class := wikiGenerationErrorClassOf(classifyWikiGenerationError(ctx, callErr))
	switch class {
	case WikiGenerationErrorTransientTransport:
		terminal := fragment.Attempts >= wikiLLMMaxAttempts
		if err := l.store.ReleaseWikiGenerationFragment(
			ctx, fragment.FragmentID, fragment.CallID, callErr.Error(), terminal,
		); err != nil {
			return newWikiGenerationError(WikiGenerationErrorAmbiguousCall,
				fmt.Errorf("release wiki generation owner after transport failure: %w", err))
		}
		if terminal {
			return newWikiGenerationError(WikiGenerationErrorBudgetExhausted,
				fmt.Errorf("wiki generation fragment exhausted %d paid-call attempts: %w", fragment.Attempts, callErr))
		}
		return nil
	case WikiGenerationErrorCancelled, WikiGenerationErrorAmbiguousCall:
		if err := l.store.MarkWikiGenerationFragmentAmbiguous(
			context.WithoutCancel(ctx), fragment.FragmentID, fragment.CallID, callErr.Error(),
		); err != nil {
			return newWikiGenerationError(WikiGenerationErrorAmbiguousCall,
				fmt.Errorf("mark wiki generation call ambiguous: %w", err))
		}
		return nil
	default:
		if err := l.store.ReleaseWikiGenerationFragment(
			ctx, fragment.FragmentID, fragment.CallID, callErr.Error(), true,
		); err != nil {
			return newWikiGenerationError(WikiGenerationErrorAmbiguousCall,
				fmt.Errorf("terminate wiki generation fragment: %w", err))
		}
		return nil
	}
}

func (s *wikiIngestService) markWikiGenerationFragmentsSucceeded(ctx context.Context, workRevision string) error {
	if workRevision == "" {
		return nil
	}
	store, ok := s.wikiService.(interfaces.WikiGenerationFragmentStore)
	if !ok || store == nil {
		return nil
	}
	fragments, err := store.ListWikiGenerationFragments(ctx, workRevision)
	if err != nil {
		return err
	}
	for i := range fragments {
		fragment := &fragments[i]
		if fragment.State != types.WikiGenerationFragmentCalling {
			continue
		}
		if s.redisClient == nil {
			return fmt.Errorf("settle wiki generation fragment %s: calling output fallback is unavailable", fragment.FragmentID)
		}
		key := "wiki:generation:fragment:" + fragment.FragmentID
		raw, redisErr := s.redisClient.Get(ctx, key).Bytes()
		if redisErr != nil {
			return fmt.Errorf("settle wiki generation fragment %s from Redis: %w", fragment.FragmentID, redisErr)
		}
		var fallback wikiGenerationFallback
		if err := json.Unmarshal(raw, &fallback); err != nil || fallback.CallID != fragment.CallID || fallback.Output == "" {
			return fmt.Errorf("settle wiki generation fragment %s: invalid owner-bound Redis fallback", fragment.FragmentID)
		}
		if err := store.CompleteWikiGenerationFragment(
			ctx, fragment.FragmentID, fallback.CallID, fallback.Output,
		); err != nil {
			return fmt.Errorf("promote wiki generation fragment %s: %w", fragment.FragmentID, err)
		}
		if err := s.redisClient.Del(ctx, key).Err(); err != nil {
			logger.Warnf(ctx, "wiki generation: delete settled Redis fallback failed for %s: %v", fragment.FragmentID, err)
		}
	}
	return store.MarkWikiGenerationFragmentsSucceeded(ctx, workRevision)
}

func (s *wikiIngestService) markWikiGenerationScopeSucceeded(
	ctx context.Context, scope wikiGenerationScope,
) error {
	if err := s.markWikiGenerationFragmentsSucceeded(ctx, scope.WorkRevision); err != nil {
		return err
	}
	if scope.RecoveryRevision == "" || scope.RecoveryRevision == scope.WorkRevision {
		return nil
	}
	return s.markWikiGenerationFragmentsSucceeded(ctx, scope.RecoveryRevision)
}
