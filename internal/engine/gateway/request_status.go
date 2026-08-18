package gateway

import (
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

type RequestState string

const (
	RequestWaiting   RequestState = "waiting"
	RequestProxying  RequestState = "proxying"
	RequestCompleted RequestState = "completed"
	RequestFailed    RequestState = "failed"
)

type RequestStatus struct {
	RequestID          string          `json:"request_id"`
	Group              lifecycle.Group `json:"group"`
	State              RequestState    `json:"state"`
	Phase              string          `json:"phase"`
	ElapsedMS          int64           `json:"elapsed_ms"`
	ColdStart          bool            `json:"cold_start,omitempty"`
	ControllerDegraded bool            `json:"controller_degraded,omitempty"`
	BackendID          string          `json:"backend_id,omitempty"`
	ErrorCode          string          `json:"error_code,omitempty"`
	StartedAt          time.Time       `json:"started_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

type requestStatusStore struct {
	mu        sync.Mutex
	statuses  map[string]RequestStatus
	retention time.Duration
	max       int
	now       func() time.Time
}

func newRequestStatusStore(retention time.Duration, max int) *requestStatusStore {
	return &requestStatusStore{
		statuses:  make(map[string]RequestStatus),
		retention: retention,
		max:       max,
		now:       time.Now,
	}
}

func (s *requestStatusStore) start(requestID string, group lifecycle.Group) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	s.pruneLocked(now)
	s.statuses[requestID] = RequestStatus{
		RequestID: requestID,
		Group:     group,
		State:     RequestWaiting,
		Phase:     "acquiring_lease",
		StartedAt: now,
		UpdatedAt: now,
	}
	s.pruneOverflowLocked()
}

func (s *requestStatusStore) proxying(requestID string, lease lifecycle.Lease, controllerDegraded bool) {
	s.update(requestID, func(status *RequestStatus) {
		status.State = RequestProxying
		status.Phase = "proxying"
		status.ColdStart = lease.ColdStart
		status.ControllerDegraded = controllerDegraded
		status.BackendID = lease.Backend.ID
		status.ErrorCode = ""
	})
}

func (s *requestStatusStore) complete(requestID string) {
	s.update(requestID, func(status *RequestStatus) {
		status.State = RequestCompleted
		status.Phase = "completed"
		status.ErrorCode = ""
	})
}

func (s *requestStatusStore) fail(requestID, errorCode string) {
	s.update(requestID, func(status *RequestStatus) {
		status.State = RequestFailed
		status.Phase = "failed"
		status.ErrorCode = errorCode
	})
}

func (s *requestStatusStore) update(requestID string, mutate func(*RequestStatus)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, ok := s.statuses[requestID]
	if !ok {
		return
	}
	mutate(&status)
	status.UpdatedAt = s.now().UTC()
	status.ElapsedMS = max(status.UpdatedAt.Sub(status.StartedAt).Milliseconds(), 0)
	s.statuses[requestID] = status
}

func (s *requestStatusStore) get(requestID string) (RequestStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	s.pruneLocked(now)
	status, ok := s.statuses[requestID]
	if !ok {
		return RequestStatus{}, false
	}
	if status.State == RequestWaiting || status.State == RequestProxying {
		status.ElapsedMS = max(now.Sub(status.StartedAt).Milliseconds(), 0)
	}
	return status, true
}

func (s *requestStatusStore) pruneLocked(now time.Time) {
	for requestID, status := range s.statuses {
		if status.State != RequestWaiting && status.State != RequestProxying && now.Sub(status.UpdatedAt) > s.retention {
			delete(s.statuses, requestID)
		}
	}
}

func (s *requestStatusStore) pruneOverflowLocked() {
	for len(s.statuses) > s.max {
		var oldestID string
		var oldest time.Time
		for requestID, status := range s.statuses {
			if status.State == RequestWaiting || status.State == RequestProxying {
				continue
			}
			if oldestID == "" || status.UpdatedAt.Before(oldest) {
				oldestID = requestID
				oldest = status.UpdatedAt
			}
		}
		if oldestID == "" {
			return
		}
		delete(s.statuses, oldestID)
	}
}
