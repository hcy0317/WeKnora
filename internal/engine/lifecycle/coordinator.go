package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type State string

const (
	StateStopped  State = "stopped"
	StateStarting State = "starting"
	StateReady    State = "ready"
	StateBusy     State = "busy"
	StateDraining State = "draining"
	StateStopping State = "stopping"
	StateFailed   State = "failed"
)

type FailureKind string

const (
	FailureStartTimeout   FailureKind = "start_timeout"
	FailureStartFailed    FailureKind = "start_failed"
	FailureCooldownActive FailureKind = "cooldown_active"
)

type Failure struct {
	Kind       FailureKind
	Group      Group
	Err        error
	RetryAfter time.Duration
}

func (e *Failure) Error() string {
	return fmt.Sprintf("engine group %s: %s: %v", e.Group, e.Kind, e.Err)
}

func (e *Failure) Unwrap() error { return e.Err }

type Backend struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type Runtime interface {
	Start(ctx context.Context, group Group) (Backend, error)
	Stop(ctx context.Context, group Group) error
}

type StartRequest struct {
	Group      Group
	GPUAllowed bool
}

type AdmissionRuntime interface {
	StartWithAdmission(ctx context.Context, request StartRequest) (Backend, error)
}

type DrainingRuntime interface {
	Drain(ctx context.Context, group Group) error
}

type AcquireRequest struct {
	RequestID string `json:"request_id"`
	GatewayID string `json:"gateway_id"`
	Purpose   string `json:"purpose"`
}

type ShadowLease struct {
	ID              string `json:"lease_id"`
	RequestID       string `json:"request_id"`
	Group           Group  `json:"group"`
	Purpose         string `json:"purpose"`
	ControllerEpoch uint64 `json:"controller_epoch"`
}

type GatewayReconcile struct {
	GatewayID      string        `json:"gateway_id"`
	GatewayEpoch   uint64        `json:"gateway_epoch"`
	ActiveLeaseIDs []string      `json:"active_lease_ids"`
	ShadowLeases   []ShadowLease `json:"shadow_leases"`
}

type Lease struct {
	ID              string        `json:"lease_id"`
	RequestID       string        `json:"request_id"`
	GatewayID       string        `json:"gateway_id"`
	Purpose         string        `json:"purpose"`
	Group           Group         `json:"group"`
	Backend         Backend       `json:"backend"`
	ControllerEpoch uint64        `json:"controller_epoch"`
	CatalogRevision uint64        `json:"catalog_revision"`
	ColdStart       bool          `json:"cold_start"`
	Waited          time.Duration `json:"waited_ns"`
}

type GroupSnapshot struct {
	Group   Group  `json:"group"`
	State   State  `json:"state"`
	Epoch   uint64 `json:"epoch"`
	Pending int    `json:"pending"`
	Active  int    `json:"active"`
	Suspect int    `json:"suspect"`
	Shadow  int    `json:"shadow"`
}

type startAttempt struct {
	done    chan struct{}
	backend Backend
	err     error
}

type groupCoordinator struct {
	mu            sync.Mutex
	state         State
	epoch         uint64
	pending       int
	active        map[string]Lease
	suspect       map[string]Lease
	shadow        map[string]Lease
	backend       Backend
	start         *startAttempt
	stopDone      chan struct{}
	idleSince     *time.Time
	cooldownUntil *time.Time
}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type CoordinatorOption func(*Coordinator)

func WithClock(clock Clock) CoordinatorOption {
	return func(coordinator *Coordinator) {
		if clock != nil {
			coordinator.clock = clock
		}
	}
}

type Coordinator struct {
	configMu    sync.RWMutex
	config      Config
	runtime     Runtime
	clock       Clock
	groups      map[Group]*groupCoordinator
	leaseGroups sync.Map
	nextID      atomic.Uint64
	gpuAllowed  atomic.Bool
}

func NewCoordinator(config Config, runtime Runtime, options ...CoordinatorOption) (*Coordinator, error) {
	if runtime == nil {
		return nil, errors.New("engine lifecycle runtime is required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	clonedConfig, err := cloneConfig(config)
	if err != nil {
		return nil, err
	}

	coordinator := &Coordinator{
		config:  clonedConfig,
		runtime: runtime,
		clock:   realClock{},
		groups:  make(map[Group]*groupCoordinator, len(managedGroups)),
	}
	coordinator.gpuAllowed.Store(true)
	for _, option := range options {
		option(coordinator)
	}
	for _, group := range managedGroups {
		coordinator.groups[group] = &groupCoordinator{
			state:   StateStopped,
			active:  make(map[string]Lease),
			suspect: make(map[string]Lease),
			shadow:  make(map[string]Lease),
		}
	}
	return coordinator, nil
}

func (c *Coordinator) Acquire(ctx context.Context, group Group, request AcquireRequest) (Lease, error) {
	groupState, ok := c.groups[group]
	if !ok {
		return Lease{}, fmt.Errorf("unknown engine group %q", group)
	}
	config := c.currentConfig()
	policy, err := config.PolicyFor(group)
	if err != nil {
		return Lease{}, err
	}
	startedAt := c.clock.Now()
	coldStart := false

	for {
		groupState.mu.Lock()
		if groupState.state == StateFailed && groupState.cooldownUntil != nil {
			retryAfter := groupState.cooldownUntil.Sub(c.clock.Now())
			if retryAfter > 0 {
				groupState.mu.Unlock()
				return Lease{}, &Failure{
					Kind:       FailureCooldownActive,
					Group:      group,
					Err:        errors.New("start failure cooldown is active"),
					RetryAfter: retryAfter,
				}
			}
			groupState.cooldownUntil = nil
			groupState.state = StateStopped
		}
		if groupState.state == StateReady || groupState.state == StateBusy || groupState.state == StateDraining {
			waited := time.Duration(0)
			if coldStart {
				waited = c.clock.Now().Sub(startedAt)
			}
			lease := c.issueLeaseLocked(groupState, group, request, coldStart, waited)
			groupState.mu.Unlock()
			return lease, nil
		}

		if groupState.state == StateStopping {
			stopDone := groupState.stopDone
			groupState.pending++
			groupState.mu.Unlock()
			select {
			case <-stopDone:
				groupState.mu.Lock()
				groupState.pending--
				groupState.mu.Unlock()
				coldStart = true
				continue
			case <-ctx.Done():
				groupState.mu.Lock()
				groupState.pending--
				groupState.mu.Unlock()
				return Lease{}, ctx.Err()
			}
		}

		attempt := groupState.start
		leader := false
		if groupState.state != StateStarting || attempt == nil {
			attempt = &startAttempt{done: make(chan struct{})}
			groupState.start = attempt
			groupState.state = StateStarting
			groupState.epoch++
			leader = true
		}
		groupState.pending++
		groupState.mu.Unlock()
		coldStart = true

		if leader {
			c.runStart(group, groupState, attempt, policy)
		}

		select {
		case <-attempt.done:
			groupState.mu.Lock()
			groupState.pending--
			if attempt.err != nil {
				groupState.mu.Unlock()
				return Lease{}, attempt.err
			}
			lease := c.issueLeaseLocked(
				groupState,
				group,
				request,
				true,
				c.clock.Now().Sub(startedAt),
			)
			groupState.mu.Unlock()
			return lease, nil
		case <-ctx.Done():
			groupState.mu.Lock()
			groupState.pending--
			groupState.mu.Unlock()
			return Lease{}, ctx.Err()
		}
	}
}

func (c *Coordinator) runStart(
	group Group,
	groupState *groupCoordinator,
	attempt *startAttempt,
	policy Policy,
) {
	startContext, cancel := context.WithTimeout(context.Background(), policy.StartupTimeout)
	defer cancel()

	backend, err := c.startRuntime(startContext, group)
	if err != nil {
		kind := FailureStartFailed
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(startContext.Err(), context.DeadlineExceeded) {
			kind = FailureStartTimeout
		}
		err = &Failure{Kind: kind, Group: group, Err: err}
	}

	groupState.mu.Lock()
	attempt.backend = backend
	attempt.err = err
	if err != nil {
		groupState.state = StateFailed
		cooldownUntil := c.clock.Now().Add(policy.FailureCooldown)
		groupState.cooldownUntil = &cooldownUntil
	} else {
		groupState.backend = backend
		groupState.state = StateReady
		groupState.cooldownUntil = nil
	}
	close(attempt.done)
	groupState.mu.Unlock()
}

func (c *Coordinator) startRuntime(ctx context.Context, group Group) (Backend, error) {
	if runtime, ok := c.runtime.(AdmissionRuntime); ok {
		return runtime.StartWithAdmission(ctx, StartRequest{
			Group:      group,
			GPUAllowed: group != GroupReranker || c.gpuAllowed.Load(),
		})
	}
	return c.runtime.Start(ctx, group)
}

func (c *Coordinator) SetGPUAdmission(allowed bool) {
	c.gpuAllowed.Store(allowed)
}

func (c *Coordinator) issueLeaseLocked(
	groupState *groupCoordinator,
	group Group,
	request AcquireRequest,
	coldStart bool,
	waited time.Duration,
) Lease {
	lease := Lease{
		ID:              fmt.Sprintf("lease-%d", c.nextID.Add(1)),
		RequestID:       request.RequestID,
		GatewayID:       request.GatewayID,
		Purpose:         request.Purpose,
		Group:           group,
		Backend:         groupState.backend,
		ControllerEpoch: groupState.epoch,
		CatalogRevision: c.currentConfig().Revision,
		ColdStart:       coldStart,
		Waited:          waited,
	}
	groupState.active[lease.ID] = lease
	groupState.idleSince = nil
	groupState.state = StateBusy
	c.leaseGroups.Store(lease.ID, group)
	return lease
}

type ReleaseReason string

const (
	ReleaseCompleted        ReleaseReason = "completed"
	ReleaseClientDisconnect ReleaseReason = "client_disconnect"
	ReleaseUpstreamError    ReleaseReason = "upstream_error"
)

func (c *Coordinator) Release(leaseID string, _ ReleaseReason) error {
	value, ok := c.leaseGroups.Load(leaseID)
	if !ok {
		return nil
	}
	group := value.(Group)
	groupState := c.groups[group]

	groupState.mu.Lock()
	_, active := groupState.active[leaseID]
	_, suspect := groupState.suspect[leaseID]
	_, shadow := groupState.shadow[leaseID]
	if active || suspect || shadow {
		delete(groupState.active, leaseID)
		delete(groupState.suspect, leaseID)
		delete(groupState.shadow, leaseID)
		if groupState.leaseCountLocked() == 0 && groupState.pending == 0 {
			now := c.clock.Now()
			groupState.idleSince = &now
			groupState.state = StateReady
		}
	}
	groupState.mu.Unlock()
	c.leaseGroups.Delete(leaseID)
	return nil
}

func (c *Coordinator) MarkSuspect(leaseID string) error {
	value, ok := c.leaseGroups.Load(leaseID)
	if !ok {
		return fmt.Errorf("unknown lease %q", leaseID)
	}
	group := value.(Group)
	groupState := c.groups[group]
	groupState.mu.Lock()
	defer groupState.mu.Unlock()
	lease, active := groupState.active[leaseID]
	if !active {
		if _, alreadySuspect := groupState.suspect[leaseID]; alreadySuspect {
			return nil
		}
		return fmt.Errorf("lease %q is not active", leaseID)
	}
	delete(groupState.active, leaseID)
	groupState.suspect[leaseID] = lease
	groupState.idleSince = nil
	groupState.state = StateBusy
	return nil
}

func (c *Coordinator) Renew(leaseID string) error {
	value, ok := c.leaseGroups.Load(leaseID)
	if !ok {
		return fmt.Errorf("unknown lease %q", leaseID)
	}
	group := value.(Group)
	groupState := c.groups[group]
	groupState.mu.Lock()
	defer groupState.mu.Unlock()
	if _, active := groupState.active[leaseID]; active {
		return nil
	}
	if lease, suspect := groupState.suspect[leaseID]; suspect {
		delete(groupState.suspect, leaseID)
		groupState.active[leaseID] = lease
		groupState.idleSince = nil
		groupState.state = StateBusy
		return nil
	}
	if lease, shadow := groupState.shadow[leaseID]; shadow {
		delete(groupState.shadow, leaseID)
		groupState.active[leaseID] = lease
		groupState.idleSince = nil
		groupState.state = StateBusy
		return nil
	}
	return fmt.Errorf("unknown lease %q", leaseID)
}

func (c *Coordinator) ReconcileGateway(reconcile GatewayReconcile) error {
	if reconcile.GatewayID == "" {
		return errors.New("gateway_id is required")
	}
	activeIDs := make(map[string]struct{}, len(reconcile.ActiveLeaseIDs))
	for _, leaseID := range reconcile.ActiveLeaseIDs {
		activeIDs[leaseID] = struct{}{}
	}
	shadowByID := make(map[string]ShadowLease, len(reconcile.ShadowLeases))
	for _, shadow := range reconcile.ShadowLeases {
		if !isManagedGroup(shadow.Group) {
			return fmt.Errorf("unknown engine group %q in shadow lease %q", shadow.Group, shadow.ID)
		}
		if shadow.ID == "" {
			return errors.New("shadow lease id is required")
		}
		shadowByID[shadow.ID] = shadow
	}

	for _, group := range managedGroups {
		groupState := c.groups[group]
		groupState.mu.Lock()

		for leaseID, lease := range groupState.suspect {
			if lease.GatewayID != reconcile.GatewayID {
				continue
			}
			if _, active := activeIDs[leaseID]; active {
				delete(groupState.suspect, leaseID)
				groupState.active[leaseID] = lease
				continue
			}
			if _, shadow := shadowByID[leaseID]; shadow {
				delete(groupState.suspect, leaseID)
				groupState.shadow[leaseID] = lease
				continue
			}
			delete(groupState.suspect, leaseID)
			c.leaseGroups.Delete(leaseID)
		}

		for leaseID, lease := range groupState.shadow {
			if lease.GatewayID != reconcile.GatewayID {
				continue
			}
			if _, active := activeIDs[leaseID]; active {
				delete(groupState.shadow, leaseID)
				groupState.active[leaseID] = lease
				continue
			}
			if _, shadow := shadowByID[leaseID]; shadow {
				continue
			}
			delete(groupState.shadow, leaseID)
			c.leaseGroups.Delete(leaseID)
		}

		for leaseID, shadow := range shadowByID {
			if shadow.Group != group {
				continue
			}
			if _, knownActive := groupState.active[leaseID]; knownActive {
				continue
			}
			if _, knownShadow := groupState.shadow[leaseID]; knownShadow {
				continue
			}
			lease := Lease{
				ID:              shadow.ID,
				RequestID:       shadow.RequestID,
				GatewayID:       reconcile.GatewayID,
				Purpose:         shadow.Purpose,
				Group:           shadow.Group,
				Backend:         groupState.backend,
				ControllerEpoch: shadow.ControllerEpoch,
				CatalogRevision: c.currentConfig().Revision,
			}
			groupState.shadow[leaseID] = lease
			c.leaseGroups.Store(leaseID, group)
		}

		if groupState.leaseCountLocked() == 0 && groupState.pending == 0 &&
			(groupState.state == StateBusy || groupState.state == StateReady) {
			now := c.clock.Now()
			groupState.idleSince = &now
			groupState.state = StateReady
		} else if groupState.leaseCountLocked() > 0 {
			groupState.idleSince = nil
			groupState.state = StateBusy
		}
		groupState.mu.Unlock()
	}

	return nil
}

func (g *groupCoordinator) leaseCountLocked() int {
	return len(g.active) + len(g.suspect) + len(g.shadow)
}

func (c *Coordinator) SweepIdle(ctx context.Context) error {
	for _, group := range managedGroups {
		policy, err := c.currentConfig().PolicyFor(group)
		if err != nil {
			return err
		}
		if policy.Mode == ModeAlwaysOn {
			continue
		}

		groupState := c.groups[group]
		groupState.mu.Lock()
		if groupState.state != StateReady || groupState.idleSince == nil ||
			groupState.leaseCountLocked() != 0 || groupState.pending != 0 ||
			c.clock.Now().Sub(*groupState.idleSince) < policy.IdleTimeout {
			groupState.mu.Unlock()
			continue
		}
		groupState.state = StateDraining
		groupState.epoch++
		groupState.mu.Unlock()

		if runtime, ok := c.runtime.(DrainingRuntime); ok {
			if err := runtime.Drain(ctx, group); err != nil {
				groupState.mu.Lock()
				if groupState.state == StateDraining {
					groupState.state = StateReady
				}
				groupState.mu.Unlock()
				return fmt.Errorf("drain engine group %s: %w", group, err)
			}
		}

		groupState.mu.Lock()
		if groupState.state != StateDraining || groupState.leaseCountLocked() != 0 || groupState.pending != 0 {
			groupState.mu.Unlock()
			continue
		}
		groupState.state = StateStopping
		groupState.stopDone = make(chan struct{})
		groupState.mu.Unlock()

		if err := c.runtime.Stop(ctx, group); err != nil {
			groupState.mu.Lock()
			groupState.state = StateFailed
			close(groupState.stopDone)
			groupState.stopDone = nil
			groupState.mu.Unlock()
			return fmt.Errorf("stop engine group %s: %w", group, err)
		}

		groupState.mu.Lock()
		groupState.state = StateStopped
		groupState.backend = Backend{}
		groupState.idleSince = nil
		close(groupState.stopDone)
		groupState.stopDone = nil
		groupState.mu.Unlock()
	}
	return nil
}

func (c *Coordinator) ApplyConfig(config Config) error {
	if err := config.Validate(); err != nil {
		return err
	}
	cloned, err := cloneConfig(config)
	if err != nil {
		return err
	}
	c.configMu.Lock()
	defer c.configMu.Unlock()
	if cloned.Revision <= c.config.Revision {
		return fmt.Errorf("config revision must increase: current=%d candidate=%d", c.config.Revision, cloned.Revision)
	}
	if cloned.SchemaVersion != c.config.SchemaVersion ||
		!reflect.DeepEqual(cloned.Catalog, c.config.Catalog) ||
		!reflect.DeepEqual(cloned.Controller, c.config.Controller) {
		return errors.New("schema_version, catalog, and controller cannot change at runtime")
	}
	c.config = cloned
	return nil
}

func (c *Coordinator) EnsureAlwaysOn(ctx context.Context) error {
	config := c.currentConfig()
	for _, group := range managedGroups {
		groupConfig := config.Groups[group]
		if groupConfig.Mode != ModeAlwaysOn {
			continue
		}
		snapshot, err := c.Snapshot(group)
		if err != nil {
			return err
		}
		if snapshot.State == StateReady || snapshot.State == StateBusy {
			continue
		}
		lease, err := c.Acquire(ctx, group, AcquireRequest{
			RequestID: "controller-ensure-" + string(group),
			GatewayID: "controller",
			Purpose:   "ensure_always_on",
		})
		if err != nil {
			return fmt.Errorf("ensure always-on engine group %s: %w", group, err)
		}
		if err := c.Release(lease.ID, ReleaseCompleted); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) currentConfig() Config {
	c.configMu.RLock()
	defer c.configMu.RUnlock()
	return c.config
}

func cloneConfig(config Config) (Config, error) {
	encoded, err := encodeConfig(config)
	if err != nil {
		return Config{}, err
	}
	cloned, err := DecodeConfig(bytes.NewReader(encoded))
	if err != nil {
		return Config{}, err
	}
	return *cloned, nil
}

func (c *Coordinator) Snapshot(group Group) (GroupSnapshot, error) {
	groupState, ok := c.groups[group]
	if !ok {
		return GroupSnapshot{}, fmt.Errorf("unknown engine group %q", group)
	}
	groupState.mu.Lock()
	defer groupState.mu.Unlock()
	return GroupSnapshot{
		Group:   group,
		State:   groupState.state,
		Epoch:   groupState.epoch,
		Pending: groupState.pending,
		Active:  len(groupState.active),
		Suspect: len(groupState.suspect),
		Shadow:  len(groupState.shadow),
	}, nil
}
