package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/google/uuid"
)

const (
	knowledgeProcessingLeaseRenewal   = 20 * time.Second
	knowledgeProcessingLeasePoll      = 250 * time.Millisecond
	knowledgeProcessingLeaseSafety    = 35 * time.Minute
	knowledgeProcessingLeaseKeyPrefix = "weknora:knowledge-processing-lease:"
)

const renewKnowledgeProcessingLeaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`

const releaseKnowledgeProcessingLeaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`

type localKnowledgeProcessingLease struct {
	semaphore chan struct{}
	refs      int
}

var localKnowledgeProcessingLeases = struct {
	sync.Mutex
	entries map[string]*localKnowledgeProcessingLease
}{entries: make(map[string]*localKnowledgeProcessingLease)}

func knowledgeProcessingLeaseKey(tenantID uint64, knowledgeID string) string {
	return fmt.Sprintf("%s%d:%s", knowledgeProcessingLeaseKeyPrefix, tenantID, knowledgeID)
}

func (s *knowledgeService) knowledgeProcessingLeaseTTL() time.Duration {
	// The lease must not expire while an outer document task plus one bounded
	// 30-minute parser/index call can still be unwinding after cancellation.
	// Renewal keeps the common case fresh; this longer expiry is the fencing
	// safety net when Redis becomes unavailable mid-call.
	return config.DocumentProcessTimeout(s.config) + knowledgeProcessingLeaseSafety
}

// acquireKnowledgeProcessingLease serializes every worker that may delete or
// publish resources for one knowledge row. HTTP submission deliberately does
// not take this lease: it may open a newer attempt immediately, while the new
// worker waits for the older delivery to observe that it was superseded and
// leave the critical section.
func (s *knowledgeService) acquireKnowledgeProcessingLease(
	ctx context.Context, tenantID uint64, knowledgeID string,
) (context.Context, func(), error) {
	if knowledgeID == "" {
		return nil, nil, fmt.Errorf("knowledge processing lease: knowledge id is required")
	}
	key := knowledgeProcessingLeaseKey(tenantID, knowledgeID)
	if s.redisClient == nil {
		return acquireLocalKnowledgeProcessingLease(ctx, key)
	}

	token := uuid.NewString()
	leaseTTL := s.knowledgeProcessingLeaseTTL()
	ticker := time.NewTicker(knowledgeProcessingLeasePoll)
	defer ticker.Stop()
	for {
		acquired, err := s.redisClient.SetNX(ctx, key, token, leaseTTL).Result()
		if err != nil {
			return nil, nil, fmt.Errorf("acquire knowledge processing lease: %w", err)
		}
		if acquired {
			break
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-ticker.C:
		}
	}

	leaseCtx, cancel := context.WithCancel(ctx)
	stopRenewal := make(chan struct{})
	go func() {
		ticker := time.NewTicker(knowledgeProcessingLeaseRenewal)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenewal:
				return
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				result, err := s.redisClient.Eval(
					leaseCtx,
					renewKnowledgeProcessingLeaseScript,
					[]string{key},
					token,
					leaseTTL.Milliseconds(),
				).Int64()
				if err != nil || result != 1 {
					logger.Warnf(leaseCtx,
						"Knowledge processing lease lost key=%s: result=%d err=%v", key, result, err)
					cancel()
					return
				}
			}
		}
	}()

	var once sync.Once
	release := func() {
		once.Do(func() {
			close(stopRenewal)
			cancel()
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer releaseCancel()
			if _, err := s.redisClient.Eval(
				releaseCtx,
				releaseKnowledgeProcessingLeaseScript,
				[]string{key},
				token,
			).Result(); err != nil {
				logger.Warnf(releaseCtx, "Release knowledge processing lease failed key=%s: %v", key, err)
			}
		})
	}
	return leaseCtx, release, nil
}

func acquireLocalKnowledgeProcessingLease(
	ctx context.Context, key string,
) (context.Context, func(), error) {
	localKnowledgeProcessingLeases.Lock()
	entry := localKnowledgeProcessingLeases.entries[key]
	if entry == nil {
		entry = &localKnowledgeProcessingLease{semaphore: make(chan struct{}, 1)}
		localKnowledgeProcessingLeases.entries[key] = entry
	}
	entry.refs++
	localKnowledgeProcessingLeases.Unlock()

	select {
	case entry.semaphore <- struct{}{}:
	case <-ctx.Done():
		releaseLocalKnowledgeProcessingLeaseRef(key, entry, false)
		return nil, nil, ctx.Err()
	}

	leaseCtx, cancel := context.WithCancel(ctx)
	var once sync.Once
	release := func() {
		once.Do(func() {
			cancel()
			releaseLocalKnowledgeProcessingLeaseRef(key, entry, true)
		})
	}
	return leaseCtx, release, nil
}

func releaseLocalKnowledgeProcessingLeaseRef(
	key string, entry *localKnowledgeProcessingLease, held bool,
) {
	if held {
		<-entry.semaphore
	}
	localKnowledgeProcessingLeases.Lock()
	defer localKnowledgeProcessingLeases.Unlock()
	entry.refs--
	if entry.refs == 0 && len(entry.semaphore) == 0 && localKnowledgeProcessingLeases.entries[key] == entry {
		delete(localKnowledgeProcessingLeases.entries, key)
	}
}
