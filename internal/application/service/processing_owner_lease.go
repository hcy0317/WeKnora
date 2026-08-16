package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

const processingOwnerLeaseKeyPrefix = "weknora:processing-owner:"

var processingOwnerLeaseRenewInterval = 20 * time.Second

const wikiProcessingOwnerLeaseTTL = 90 * time.Second
const processingWorkerLeaseTTL = 90 * time.Second

const checkProcessingOwnerLeaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return 1
end
return 0
`

const renewProcessingOwnerLeaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`

const releaseProcessingOwnerLeaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`

type processingOwnerLease struct {
	client        *redis.Client
	key           string
	value         string
	ttl           time.Duration
	renewInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	stop          chan struct{}
	once          sync.Once

	errMu sync.RWMutex
	err   error
}

func processingOwnerLeaseKey(ref types.ProcessingOwnerRef) string {
	return fmt.Sprintf("%s%d:%s:%d:%s", processingOwnerLeaseKeyPrefix,
		ref.TenantID, ref.KnowledgeID, ref.Attempt, ref.Name)
}

func tryAcquireProcessingOwnerLease(
	ctx context.Context,
	client *redis.Client,
	ref types.ProcessingOwnerRef,
	owner types.TaskClaimOwner,
	ttl time.Duration,
) (*processingOwnerLease, bool, error) {
	if client == nil {
		return nil, false, errors.New("processing owner lease: Redis is unavailable")
	}
	if !ref.Valid() || !owner.Valid() || ttl <= 0 {
		return nil, false, errors.New("processing owner lease: valid ref, owner and ttl are required")
	}
	valueBytes, err := json.Marshal(owner)
	if err != nil {
		return nil, false, fmt.Errorf("marshal processing owner lease: %w", err)
	}
	key := processingOwnerLeaseKey(ref)
	acquired, err := client.SetNX(ctx, key, string(valueBytes), ttl).Result()
	if err != nil {
		return nil, false, fmt.Errorf("acquire processing owner lease: %w", err)
	}
	if !acquired {
		return nil, false, nil
	}
	ownerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	lease := &processingOwnerLease{
		client: client, key: key, value: string(valueBytes), ttl: ttl,
		renewInterval: processingOwnerLeaseRenewInterval,
		ctx:           ownerCtx, cancel: cancel, stop: make(chan struct{}),
	}
	go lease.renewLoop()
	return lease, true, nil
}

func inspectProcessingOwnerLease(
	ctx context.Context, client *redis.Client, ref types.ProcessingOwnerRef,
) (*types.ProcessingOwnerLeaseSnapshot, error) {
	if client == nil {
		return nil, errors.New("inspect processing owner lease: Redis is unavailable")
	}
	if !ref.Valid() {
		return nil, errors.New("inspect processing owner lease: valid owner ref is required")
	}
	value, err := client.Get(ctx, processingOwnerLeaseKey(ref)).Result()
	if errors.Is(err, redis.Nil) {
		return &types.ProcessingOwnerLeaseSnapshot{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect processing owner lease: %w", err)
	}
	ttl, err := client.PTTL(ctx, processingOwnerLeaseKey(ref)).Result()
	if err != nil {
		return nil, fmt.Errorf("inspect processing owner lease ttl: %w", err)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("inspect processing owner lease: active key has invalid ttl %s", ttl)
	}
	var owner types.TaskClaimOwner
	if err := json.Unmarshal([]byte(value), &owner); err != nil || !owner.Valid() {
		return nil, fmt.Errorf("inspect processing owner lease: malformed owner value")
	}
	return &types.ProcessingOwnerLeaseSnapshot{Active: true, Owner: owner, TTL: ttl}, nil
}

func (l *processingOwnerLease) renewLoop() {
	interval := l.renewInterval
	if max := l.ttl / 3; max > 0 && interval > max {
		interval = max
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			result, err := l.client.Eval(l.ctx, renewProcessingOwnerLeaseScript,
				[]string{l.key}, l.value, l.ttl.Milliseconds()).Int64()
			if err != nil || result != 1 {
				if err == nil {
					err = errors.New("processing owner lease ownership changed")
				}
				l.errMu.Lock()
				l.err = err
				l.errMu.Unlock()
				l.cancel()
				return
			}
		}
	}
}

func (l *processingOwnerLease) Context() context.Context { return l.ctx }

func (l *processingOwnerLease) Err() error {
	l.errMu.RLock()
	defer l.errMu.RUnlock()
	return l.err
}

func (l *processingOwnerLease) Release(ctx context.Context) error {
	var releaseErr error
	l.once.Do(func() {
		close(l.stop)
		result, err := l.client.Eval(ctx, releaseProcessingOwnerLeaseScript,
			[]string{l.key}, l.value).Int64()
		if err != nil {
			releaseErr = fmt.Errorf("release processing owner lease: %w", err)
		} else if result != 1 {
			releaseErr = errors.New("release processing owner lease: ownership changed")
		}
		l.cancel()
	})
	return releaseErr
}

// processingWorkerLease fences one concrete delivery of a normal post-process
// worker. The stable ref coordinates duplicate deliveries, the immutable
// owner token prevents an older delivery from publishing after ownership has
// moved to a successor, and the bound tracker rejects commits after a newer
// processing attempt becomes authoritative.
type processingWorkerLease struct {
	lease          *processingOwnerLease
	ctx            context.Context
	cancel         context.CancelFunc
	stop           func() bool
	attemptRef     types.ProcessingOwnerRef
	attemptTracker SpanTracker
}

func acquireProcessingWorkerLease(
	ctx context.Context,
	client *redis.Client,
	ref types.ProcessingOwnerRef,
	owner types.TaskClaimOwner,
) (*processingWorkerLease, error) {
	lease, acquired, err := tryAcquireProcessingOwnerLease(ctx, client, ref, owner, processingWorkerLeaseTTL)
	if err != nil {
		return nil, fmt.Errorf("processing worker lease: %w", err)
	}
	if !acquired {
		return nil, fmt.Errorf("processing worker lease: logical owner already has an active delivery")
	}
	workCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lease.Context(), cancel)
	workCtx = withProcessingOwnerContext(workCtx, lease.Context())
	return &processingWorkerLease{lease: lease, ctx: workCtx, cancel: cancel, stop: stop}, nil
}

func acquireTaskProcessingWorkerLease(
	ctx context.Context,
	client *redis.Client,
	ref types.ProcessingOwnerRef,
	fallbackTaskID string,
	tracker SpanTracker,
) (context.Context, *processingWorkerLease, error) {
	taskID, ok := asynq.GetTaskID(ctx)
	if !ok || taskID == "" {
		taskID = fallbackTaskID
	}
	if taskID == "" {
		return ctx, nil, errors.New("processing worker lease: concrete task id is unavailable")
	}
	lease, err := acquireProcessingWorkerLease(ctx, client, ref,
		types.TaskClaimOwner{Token: uuid.NewString(), TaskID: taskID})
	if err != nil {
		return ctx, nil, err
	}
	lease.attemptRef = ref
	lease.attemptTracker = tracker
	return lease.Context(), lease, nil
}

func (l *processingWorkerLease) Context() context.Context {
	return l.ctx
}

// Check is the fail-closed commit fence. It reads the current Redis owner
// immediately before a durable write, so a model call that ignored context
// cancellation cannot publish after this delivery lost ownership.
func (l *processingWorkerLease) Check(ctx context.Context) error {
	if err := l.lease.Err(); err != nil {
		return fmt.Errorf("processing worker lease lost: %w", err)
	}
	result, err := l.lease.client.Eval(ctx, checkProcessingOwnerLeaseScript,
		[]string{l.lease.key}, l.lease.value).Int64()
	if err != nil {
		return fmt.Errorf("check processing worker lease: %w", err)
	}
	if result != 1 {
		return errors.New("processing worker lease lost: ownership changed")
	}
	return nil
}

func (l *processingWorkerLease) CommitFence() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.Check(ctx); err != nil {
		return err
	}
	if l.attemptRef.Attempt <= 0 || l.attemptRef.KnowledgeID == "" {
		return nil
	}
	if l.attemptTracker == nil {
		return errors.New("processing worker lease: latest attempt fence is unavailable")
	}
	superseded, err := attemptSupersededStrict(ctx, l.attemptTracker,
		l.attemptRef.KnowledgeID, l.attemptRef.Attempt)
	if err != nil {
		return fmt.Errorf("processing worker lease: check latest attempt: %w", err)
	}
	if superseded {
		return fmt.Errorf("processing worker lease: attempt %d is superseded",
			l.attemptRef.Attempt)
	}
	return nil
}

type attemptCommitGuardTracker interface {
	WithAttemptCommitGuard(context.Context, string, int, func(context.Context) error) error
}

// CommitWithFence executes one durable external mutation while the source
// attempt is serialized against OpenAttempt. The Redis owner is re-read only
// after the repository guard is held, closing the prior check-then-write
// window for a worker that lost either delivery ownership or attempt authority.
func (l *processingWorkerLease) CommitWithFence(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return errors.New("processing worker lease: commit callback is required")
	}
	if l.attemptRef.Attempt <= 0 || l.attemptRef.KnowledgeID == "" {
		return errors.New("processing worker lease: attempt fence is unavailable")
	}
	if l.attemptTracker == nil {
		return errors.New("processing worker lease: latest attempt fence is unavailable")
	}
	guard, ok := l.attemptTracker.(attemptCommitGuardTracker)
	if !ok {
		return errors.New("processing worker lease: write-side attempt guard is unavailable")
	}
	return guard.WithAttemptCommitGuard(ctx, l.attemptRef.KnowledgeID, l.attemptRef.Attempt, func(guardedCtx context.Context) error {
		if err := l.Check(guardedCtx); err != nil {
			return err
		}
		return fn(guardedCtx)
	})
}

func (l *processingWorkerLease) Release() {
	if l.stop != nil {
		l.stop()
	}
	if l.cancel != nil {
		l.cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = l.lease.Release(ctx)
}

type processingOwnerContextKey struct{}

func withProcessingOwnerContext(ctx, ownerCtx context.Context) context.Context {
	return context.WithValue(ctx, processingOwnerContextKey{}, ownerCtx)
}

func detachedProcessingOwnerContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	base, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	ownerCtx, _ := ctx.Value(processingOwnerContextKey{}).(context.Context)
	if ownerCtx == nil {
		return base, cancel
	}
	stop := context.AfterFunc(ownerCtx, cancel)
	return base, func() {
		stop()
		cancel()
	}
}

type wikiOwnerGuard struct {
	ctx                context.Context
	cancel             context.CancelFunc
	stop               chan struct{}
	once               sync.Once
	repo               interfaces.TaskPendingOpsClaimLease
	ids                []int64
	owner              types.TaskClaimOwner
	leases             []*processingOwnerLease
	claimRenewInterval time.Duration
	errMu              sync.RWMutex
	err                error

	claimMu       sync.Mutex
	claimsStopped bool
}

func (s *wikiIngestService) acquireWikiOwnerGuard(
	ctx context.Context,
	tenantID uint64,
	ops []WikiPendingOp,
	ids []int64,
	owner types.TaskClaimOwner,
) (*wikiOwnerGuard, error) {
	repo, ok := s.pendingRepo.(interfaces.TaskPendingOpsClaimLease)
	if !ok {
		return nil, errors.New("wiki owner guard: owner-safe pending repository is unavailable")
	}
	ownerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	guard := &wikiOwnerGuard{
		ctx: ownerCtx, cancel: cancel, stop: make(chan struct{}), repo: repo,
		ids: append([]int64(nil), ids...), owner: owner,
		claimRenewInterval: processingOwnerLeaseRenewInterval,
	}
	seen := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		if op.KnowledgeID == "" || op.Attempt <= 0 {
			continue
		}
		ref := types.ProcessingOwnerRef{
			TenantID: tenantID, KnowledgeID: op.KnowledgeID,
			Attempt: op.Attempt, Name: "postprocess.wiki",
		}
		key := processingOwnerLeaseKey(ref)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		lease, acquired, err := tryAcquireProcessingOwnerLease(ownerCtx, s.redisClient, ref, owner, wikiProcessingOwnerLeaseTTL)
		if err != nil || !acquired {
			guard.Release()
			if err != nil {
				return nil, fmt.Errorf("wiki owner guard: %w", err)
			}
			return nil, fmt.Errorf("wiki owner guard: logical owner already has an active lease knowledge=%s attempt=%d", op.KnowledgeID, op.Attempt)
		}
		guard.leases = append(guard.leases, lease)
		go func(l *processingOwnerLease) {
			<-l.Context().Done()
			if err := l.Err(); err != nil {
				guard.fail(err)
			}
		}(lease)
	}
	go guard.renewClaims()
	return guard, nil
}

func (g *wikiOwnerGuard) renewClaims() {
	ticker := time.NewTicker(g.claimRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.claimMu.Lock()
			if g.claimsStopped {
				g.claimMu.Unlock()
				return
			}
			err := g.repo.RenewClaims(g.ctx, g.ids, g.owner)
			g.claimMu.Unlock()
			if err != nil {
				g.fail(fmt.Errorf("renew Wiki durable claim heartbeat: %w", err))
				return
			}
		}
	}
}

func (g *wikiOwnerGuard) StopClaimRenewal() {
	g.claimMu.Lock()
	g.claimsStopped = true
	g.claimMu.Unlock()
}

func (g *wikiOwnerGuard) fail(err error) {
	g.errMu.Lock()
	if g.err == nil {
		g.err = err
	}
	g.errMu.Unlock()
	g.cancel()
}

func (g *wikiOwnerGuard) Err() error {
	g.errMu.RLock()
	defer g.errMu.RUnlock()
	return g.err
}

func (g *wikiOwnerGuard) WorkContext(parent context.Context) (context.Context, context.CancelFunc) {
	workCtx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(g.ctx, cancel)
	workCtx = withProcessingOwnerContext(workCtx, g.ctx)
	return workCtx, func() {
		stop()
		cancel()
	}
}

func (g *wikiOwnerGuard) Release() {
	g.once.Do(func() {
		close(g.stop)
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		for _, lease := range g.leases {
			_ = lease.Release(releaseCtx)
		}
		g.cancel()
	})
}
