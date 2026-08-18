package lifecycle

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingRuntime struct {
	starts       atomic.Int32
	startEntered chan struct{}
	allowReady   chan struct{}
	enteredOnce  sync.Once
}

func (r *blockingRuntime) Start(ctx context.Context, _ Group) (Backend, error) {
	r.starts.Add(1)
	r.enteredOnce.Do(func() { close(r.startEntered) })
	select {
	case <-r.allowReady:
		return Backend{ID: "primary", URL: "http://engine:8080"}, nil
	case <-ctx.Done():
		return Backend{}, ctx.Err()
	}
}

func (r *blockingRuntime) Stop(context.Context, Group) error { return nil }

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type countingRuntime struct {
	starts atomic.Int32
	stops  atomic.Int32
}

type blockingStopRuntime struct {
	starts      atomic.Int32
	stops       atomic.Int32
	stopEntered chan struct{}
	allowStop   chan struct{}
	enteredOnce sync.Once
}

type failOnceRuntime struct {
	starts atomic.Int32
}

func (r *failOnceRuntime) Start(context.Context, Group) (Backend, error) {
	if r.starts.Add(1) == 1 {
		return Backend{}, fmt.Errorf("probe failed")
	}
	return Backend{ID: "primary", URL: "http://engine:8080"}, nil
}

func (r *failOnceRuntime) Stop(context.Context, Group) error { return nil }

type admissionRuntime struct {
	mu              sync.Mutex
	request         StartRequest
	stoppedBackends []string
}

type failFirstBackendStopRuntime struct {
	admissionRuntime
	stopAttempts atomic.Int32
}

func (r *failFirstBackendStopRuntime) StopBackend(
	ctx context.Context,
	group Group,
	backendID string,
) error {
	if r.stopAttempts.Add(1) == 1 {
		return fmt.Errorf("docker stop unavailable")
	}
	return r.admissionRuntime.StopBackend(ctx, group, backendID)
}

func (r *admissionRuntime) Start(context.Context, Group) (Backend, error) {
	return Backend{ID: "gpu", URL: "http://reranker-gpu:8000"}, nil
}

func (r *admissionRuntime) StartWithAdmission(_ context.Context, request StartRequest) (Backend, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.request = request
	if request.GPUAllowed {
		return Backend{ID: "gpu", URL: "http://reranker-gpu:8000"}, nil
	}
	return Backend{ID: "cpu", URL: "http://reranker-cpu:8000"}, nil
}

func (r *admissionRuntime) Stop(context.Context, Group) error { return nil }

func (r *admissionRuntime) StopBackend(_ context.Context, _ Group, backendID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stoppedBackends = append(r.stoppedBackends, backendID)
	return nil
}

type blockingDrainRuntime struct {
	starts       atomic.Int32
	stops        atomic.Int32
	drainEntered chan struct{}
	allowDrain   chan struct{}
	enteredOnce  sync.Once
}

func (r *blockingDrainRuntime) Start(context.Context, Group) (Backend, error) {
	r.starts.Add(1)
	return Backend{ID: "primary", URL: "http://engine:8080"}, nil
}

func (r *blockingDrainRuntime) Drain(ctx context.Context, _ Group) error {
	r.enteredOnce.Do(func() { close(r.drainEntered) })
	select {
	case <-r.allowDrain:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *blockingDrainRuntime) Stop(context.Context, Group) error {
	r.stops.Add(1)
	return nil
}

func (r *blockingStopRuntime) Start(context.Context, Group) (Backend, error) {
	r.starts.Add(1)
	return Backend{ID: "primary", URL: "http://engine:8080"}, nil
}

func (r *blockingStopRuntime) Stop(ctx context.Context, _ Group) error {
	r.stops.Add(1)
	r.enteredOnce.Do(func() { close(r.stopEntered) })
	select {
	case <-r.allowStop:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *countingRuntime) Start(context.Context, Group) (Backend, error) {
	r.starts.Add(1)
	return Backend{ID: "primary", URL: "http://engine:8080"}, nil
}

func (r *countingRuntime) Stop(context.Context, Group) error {
	r.stops.Add(1)
	return nil
}

func TestCoordinatorSharesColdStartAcrossPendingRequests(t *testing.T) {
	t.Parallel()

	runtime := &blockingRuntime{
		startEntered: make(chan struct{}),
		allowReady:   make(chan struct{}),
	}
	coordinator, err := NewCoordinator(testConfig(), runtime)
	require.NoError(t, err)

	const requestCount = 8
	results := make(chan Lease, requestCount)
	errors := make(chan error, requestCount)
	startTogether := make(chan struct{})
	for i := range requestCount {
		go func(index int) {
			<-startTogether
			lease, acquireErr := coordinator.Acquire(context.Background(), GroupASR, AcquireRequest{
				RequestID: fmt.Sprintf("request-%d", index),
				GatewayID: "gateway-1",
				Purpose:   "transcribe",
			})
			results <- lease
			errors <- acquireErr
		}(i)
	}
	close(startTogether)

	select {
	case <-runtime.startEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime start was not called")
	}
	require.Eventually(t, func() bool {
		snapshot, snapshotErr := coordinator.Snapshot(GroupASR)
		return snapshotErr == nil && snapshot.State == StateStarting && snapshot.Pending == requestCount
	}, 2*time.Second, 10*time.Millisecond)
	close(runtime.allowReady)

	leaseIDs := make(map[string]struct{}, requestCount)
	for range requestCount {
		require.NoError(t, <-errors)
		lease := <-results
		require.True(t, lease.ColdStart)
		require.Equal(t, GroupASR, lease.Group)
		require.Equal(t, "primary", lease.Backend.ID)
		leaseIDs[lease.ID] = struct{}{}
	}
	require.Len(t, leaseIDs, requestCount)
	require.Equal(t, int32(1), runtime.starts.Load())

	snapshot, err := coordinator.Snapshot(GroupASR)
	require.NoError(t, err)
	require.Equal(t, StateBusy, snapshot.State)
	require.Zero(t, snapshot.Pending)
	require.Equal(t, requestCount, snapshot.Active)
}

func TestCoordinatorStopsOnDemandGroupAfterFullIdlePeriod(t *testing.T) {
	t.Parallel()

	clock := &manualClock{now: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)}
	runtime := &countingRuntime{}
	coordinator, err := NewCoordinator(testConfig(), runtime, WithClock(clock))
	require.NoError(t, err)

	lease, err := coordinator.Acquire(context.Background(), GroupASR, AcquireRequest{
		RequestID: "request-idle",
		GatewayID: "gateway-1",
		Purpose:   "transcribe",
	})
	require.NoError(t, err)
	require.NoError(t, coordinator.Release(lease.ID, ReleaseCompleted))

	clock.Advance(9 * time.Minute)
	require.NoError(t, coordinator.SweepIdle(context.Background()))
	require.Zero(t, runtime.stops.Load())
	snapshot, err := coordinator.Snapshot(GroupASR)
	require.NoError(t, err)
	require.Equal(t, StateReady, snapshot.State)

	clock.Advance(time.Minute)
	require.NoError(t, coordinator.SweepIdle(context.Background()))
	require.Equal(t, int32(1), runtime.stops.Load())
	snapshot, err = coordinator.Snapshot(GroupASR)
	require.NoError(t, err)
	require.Equal(t, StateStopped, snapshot.State)
}

func TestCoordinatorQueuesAcquireBehindIrreversibleStop(t *testing.T) {
	t.Parallel()

	clock := &manualClock{now: time.Date(2026, 8, 18, 13, 0, 0, 0, time.UTC)}
	runtime := &blockingStopRuntime{
		stopEntered: make(chan struct{}),
		allowStop:   make(chan struct{}),
	}
	coordinator, err := NewCoordinator(testConfig(), runtime, WithClock(clock))
	require.NoError(t, err)

	lease, err := coordinator.Acquire(context.Background(), GroupASR, AcquireRequest{
		RequestID: "before-stop",
		GatewayID: "gateway-1",
		Purpose:   "transcribe",
	})
	require.NoError(t, err)
	require.NoError(t, coordinator.Release(lease.ID, ReleaseCompleted))
	clock.Advance(10 * time.Minute)

	sweepDone := make(chan error, 1)
	go func() { sweepDone <- coordinator.SweepIdle(context.Background()) }()
	select {
	case <-runtime.stopEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime stop was not called")
	}

	acquireDone := make(chan error, 1)
	go func() {
		_, acquireErr := coordinator.Acquire(context.Background(), GroupASR, AcquireRequest{
			RequestID: "during-stop",
			GatewayID: "gateway-1",
			Purpose:   "transcribe",
		})
		acquireDone <- acquireErr
	}()
	require.Eventually(t, func() bool {
		snapshot, snapshotErr := coordinator.Snapshot(GroupASR)
		return snapshotErr == nil && snapshot.State == StateStopping && snapshot.Pending == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), runtime.starts.Load(), "start must not overlap an in-flight stop")
	select {
	case acquireErr := <-acquireDone:
		t.Fatalf("acquire returned before stop completed: %v", acquireErr)
	case <-time.After(50 * time.Millisecond):
	}

	close(runtime.allowStop)
	require.NoError(t, <-sweepDone)
	require.NoError(t, <-acquireDone)
	require.Equal(t, int32(2), runtime.starts.Load())

	snapshot, err := coordinator.Snapshot(GroupASR)
	require.NoError(t, err)
	require.Equal(t, StateBusy, snapshot.State)
	require.Zero(t, snapshot.Pending)
	require.Equal(t, 1, snapshot.Active)
}

func TestCoordinatorAppliesCooldownOnlyAfterRealStartFailure(t *testing.T) {
	t.Parallel()

	clock := &manualClock{now: time.Date(2026, 8, 18, 14, 0, 0, 0, time.UTC)}
	runtime := &failOnceRuntime{}
	coordinator, err := NewCoordinator(testConfig(), runtime, WithClock(clock))
	require.NoError(t, err)

	_, err = coordinator.Acquire(context.Background(), GroupPaddleOCR, AcquireRequest{
		RequestID: "failed-start",
		GatewayID: "gateway-1",
		Purpose:   "parse",
	})
	var failure *Failure
	require.ErrorAs(t, err, &failure)
	require.Equal(t, FailureStartFailed, failure.Kind)

	_, err = coordinator.Acquire(context.Background(), GroupPaddleOCR, AcquireRequest{
		RequestID: "during-cooldown",
		GatewayID: "gateway-1",
		Purpose:   "parse",
	})
	require.ErrorAs(t, err, &failure)
	require.Equal(t, FailureCooldownActive, failure.Kind)
	require.Equal(t, 5*time.Minute, failure.RetryAfter)
	require.Equal(t, int32(1), runtime.starts.Load())

	clock.Advance(5 * time.Minute)
	lease, err := coordinator.Acquire(context.Background(), GroupPaddleOCR, AcquireRequest{
		RequestID: "after-cooldown",
		GatewayID: "gateway-1",
		Purpose:   "parse",
	})
	require.NoError(t, err)
	require.True(t, lease.ColdStart)
	require.Equal(t, int32(2), runtime.starts.Load())
}

func TestCoordinatorNeverStopsWhileLeaseIsSuspect(t *testing.T) {
	t.Parallel()

	clock := &manualClock{now: time.Date(2026, 8, 18, 15, 0, 0, 0, time.UTC)}
	runtime := &countingRuntime{}
	coordinator, err := NewCoordinator(testConfig(), runtime, WithClock(clock))
	require.NoError(t, err)

	lease, err := coordinator.Acquire(context.Background(), GroupReranker, AcquireRequest{
		RequestID: "rerank-long-request",
		GatewayID: "gateway-1",
		Purpose:   "rerank",
	})
	require.NoError(t, err)
	require.NoError(t, coordinator.MarkSuspect(lease.ID))

	clock.Advance(60 * time.Minute)
	require.NoError(t, coordinator.SweepIdle(context.Background()))
	require.Zero(t, runtime.stops.Load())

	snapshot, err := coordinator.Snapshot(GroupReranker)
	require.NoError(t, err)
	require.Equal(t, StateBusy, snapshot.State)
	require.Zero(t, snapshot.Active)
	require.Equal(t, 1, snapshot.Suspect)
}

func TestCoordinatorRestartsFullIdlePeriodAfterGatewayReconcile(t *testing.T) {
	t.Parallel()

	clock := &manualClock{now: time.Date(2026, 8, 18, 16, 0, 0, 0, time.UTC)}
	runtime := &countingRuntime{}
	coordinator, err := NewCoordinator(testConfig(), runtime, WithClock(clock))
	require.NoError(t, err)

	lease, err := coordinator.Acquire(context.Background(), GroupReranker, AcquireRequest{
		RequestID: "request-before-partition",
		GatewayID: "gateway-1",
		Purpose:   "rerank",
	})
	require.NoError(t, err)
	require.NoError(t, coordinator.MarkSuspect(lease.ID))
	clock.Advance(30 * time.Minute)

	require.NoError(t, coordinator.ReconcileGateway(GatewayReconcile{
		GatewayID:      "gateway-1",
		GatewayEpoch:   2,
		ActiveLeaseIDs: nil,
		ShadowLeases:   nil,
	}))
	snapshot, err := coordinator.Snapshot(GroupReranker)
	require.NoError(t, err)
	require.Equal(t, StateReady, snapshot.State)
	require.Zero(t, snapshot.Suspect)

	clock.Advance(9 * time.Minute)
	require.NoError(t, coordinator.SweepIdle(context.Background()))
	require.Zero(t, runtime.stops.Load())
	clock.Advance(time.Minute)
	require.NoError(t, coordinator.SweepIdle(context.Background()))
	require.Equal(t, int32(1), runtime.stops.Load())
}

func TestCoordinatorReconcileRemovesCompletedActiveLeaseMissingFromGatewayLedger(t *testing.T) {
	t.Parallel()

	clock := &manualClock{now: time.Date(2026, 8, 18, 16, 30, 0, 0, time.UTC)}
	coordinator, err := NewCoordinator(testConfig(), &countingRuntime{}, WithClock(clock))
	require.NoError(t, err)
	_, err = coordinator.Acquire(context.Background(), GroupASR, AcquireRequest{
		RequestID: "release-lost-during-partition",
		GatewayID: "gateway-1",
		Purpose:   "transcribe",
	})
	require.NoError(t, err)

	require.NoError(t, coordinator.ReconcileGateway(GatewayReconcile{
		GatewayID:      "gateway-1",
		GatewayEpoch:   3,
		ActiveLeaseIDs: nil,
		ShadowLeases:   nil,
	}))
	snapshot, err := coordinator.Snapshot(GroupASR)
	require.NoError(t, err)
	require.Equal(t, StateReady, snapshot.State)
	require.Zero(t, snapshot.Active)
}

func TestCoordinatorClosesOnlyNewRerankerGPUAdmission(t *testing.T) {
	t.Parallel()

	runtime := &admissionRuntime{}
	coordinator, err := NewCoordinator(testConfig(), runtime)
	require.NoError(t, err)
	coordinator.SetGPUAdmission(false)

	lease, err := coordinator.Acquire(context.Background(), GroupReranker, AcquireRequest{
		RequestID: "gpu-pressure-request",
		GatewayID: "gateway-1",
		Purpose:   "rerank",
	})
	require.NoError(t, err)
	require.False(t, runtime.request.GPUAllowed)
	require.Equal(t, GroupReranker, runtime.request.Group)
	require.Equal(t, "cpu", lease.Backend.ID)
	require.True(t, lease.ColdStart)
}

func TestCoordinatorDrainsExistingGPULeaseAndRoutesNewRerankerLeaseToCPU(t *testing.T) {
	t.Parallel()

	runtime := &admissionRuntime{}
	coordinator, err := NewCoordinator(testConfig(), runtime)
	require.NoError(t, err)

	gpuLease, err := coordinator.Acquire(context.Background(), GroupReranker, AcquireRequest{
		RequestID: "gpu-before-pressure", GatewayID: "gateway-1", Purpose: "rerank",
	})
	require.NoError(t, err)
	require.Equal(t, "gpu", gpuLease.Backend.ID)

	coordinator.SetGPUAdmission(false)
	cpuLease, err := coordinator.Acquire(context.Background(), GroupReranker, AcquireRequest{
		RequestID: "cpu-during-pressure", GatewayID: "gateway-1", Purpose: "rerank",
	})
	require.NoError(t, err)
	require.Equal(t, "cpu", cpuLease.Backend.ID)
	require.NotEqual(t, gpuLease.ID, cpuLease.ID)

	require.NoError(t, coordinator.Release(gpuLease.ID, ReleaseCompleted))
	require.NoError(t, coordinator.SweepGPUAdmission(context.Background()))
	require.Equal(t, []string{"gpu"}, runtime.stoppedBackends)

	snapshot, err := coordinator.Snapshot(GroupReranker)
	require.NoError(t, err)
	require.Equal(t, StateBusy, snapshot.State)
	require.Equal(t, 1, snapshot.Active)
}

func TestCoordinatorRestoresAlwaysOnRerankerToGPUAfterPressureRecovery(t *testing.T) {
	t.Parallel()

	config := testConfig()
	reranker := config.Groups[GroupReranker]
	reranker.Mode = ModeAlwaysOn
	config.Groups[GroupReranker] = reranker
	runtime := &admissionRuntime{}
	coordinator, err := NewCoordinator(config, runtime)
	require.NoError(t, err)

	coordinator.SetGPUAdmission(false)
	require.NoError(t, coordinator.EnsureAlwaysOn(context.Background()))

	coordinator.SetGPUAdmission(true)
	require.NoError(t, coordinator.SweepGPUAdmission(context.Background()))
	require.Equal(t, []string{"cpu"}, runtime.stoppedBackends)
	require.NoError(t, coordinator.EnsureAlwaysOn(context.Background()))

	lease, err := coordinator.Acquire(context.Background(), GroupReranker, AcquireRequest{
		RequestID: "gpu-after-recovery", GatewayID: "gateway-1", Purpose: "rerank",
	})
	require.NoError(t, err)
	require.Equal(t, "gpu", lease.Backend.ID)
	require.False(t, lease.ColdStart)
}

func TestCoordinatorKeepsFailedGPUStopForCooldownBoundedRetry(t *testing.T) {
	t.Parallel()

	clock := &manualClock{now: time.Date(2026, 8, 18, 18, 0, 0, 0, time.UTC)}
	runtime := &failFirstBackendStopRuntime{}
	coordinator, err := NewCoordinator(testConfig(), runtime, WithClock(clock))
	require.NoError(t, err)

	lease, err := coordinator.Acquire(context.Background(), GroupReranker, AcquireRequest{
		RequestID: "gpu-before-stop-failure", GatewayID: "gateway-1", Purpose: "rerank",
	})
	require.NoError(t, err)
	coordinator.SetGPUAdmission(false)
	require.NoError(t, coordinator.Release(lease.ID, ReleaseCompleted))
	require.ErrorContains(t, coordinator.SweepGPUAdmission(context.Background()), "docker stop unavailable")

	require.NoError(t, coordinator.SweepGPUAdmission(context.Background()))
	require.Equal(t, int32(1), runtime.stopAttempts.Load(), "retry must respect failure cooldown")
	clock.Advance(5 * time.Minute)
	require.NoError(t, coordinator.SweepGPUAdmission(context.Background()))
	require.Equal(t, int32(2), runtime.stopAttempts.Load())
	require.Equal(t, []string{"gpu"}, runtime.stoppedBackends)
}

func TestCoordinatorRejectsNewPaddleLeaseUnderPressureWithoutInterruptingActiveLease(t *testing.T) {
	t.Parallel()

	runtime := &admissionRuntime{}
	coordinator, err := NewCoordinator(testConfig(), runtime)
	require.NoError(t, err)
	lease, err := coordinator.Acquire(context.Background(), GroupPaddleOCR, AcquireRequest{
		RequestID: "paddle-before-pressure", GatewayID: "gateway-1", Purpose: "parse",
	})
	require.NoError(t, err)

	coordinator.SetGroupGPUAdmission(GroupPaddleOCR, false)
	_, err = coordinator.Acquire(context.Background(), GroupPaddleOCR, AcquireRequest{
		RequestID: "paddle-during-pressure", GatewayID: "gateway-1", Purpose: "parse",
	})
	var failure *Failure
	require.ErrorAs(t, err, &failure)
	require.Equal(t, FailureGPUAdmissionClosed, failure.Kind)

	snapshot, snapshotErr := coordinator.Snapshot(GroupPaddleOCR)
	require.NoError(t, snapshotErr)
	require.Equal(t, 1, snapshot.Active)
	require.NoError(t, coordinator.Release(lease.ID, ReleaseCompleted))
	require.NoError(t, coordinator.SweepGPUAdmission(context.Background()))
	require.Equal(t, []string{"gpu"}, runtime.stoppedBackends)
}

func TestCoordinatorCancelsReversibleDrainForNewAcquire(t *testing.T) {
	t.Parallel()

	clock := &manualClock{now: time.Date(2026, 8, 18, 17, 0, 0, 0, time.UTC)}
	runtime := &blockingDrainRuntime{
		drainEntered: make(chan struct{}),
		allowDrain:   make(chan struct{}),
	}
	coordinator, err := NewCoordinator(testConfig(), runtime, WithClock(clock))
	require.NoError(t, err)

	lease, err := coordinator.Acquire(context.Background(), GroupASR, AcquireRequest{
		RequestID: "before-drain",
		GatewayID: "gateway-1",
		Purpose:   "transcribe",
	})
	require.NoError(t, err)
	require.NoError(t, coordinator.Release(lease.ID, ReleaseCompleted))
	clock.Advance(10 * time.Minute)

	sweepDone := make(chan error, 1)
	go func() { sweepDone <- coordinator.SweepIdle(context.Background()) }()
	select {
	case <-runtime.drainEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("drain phase was not entered")
	}
	snapshot, err := coordinator.Snapshot(GroupASR)
	require.NoError(t, err)
	require.Equal(t, StateDraining, snapshot.State)

	newLease, err := coordinator.Acquire(context.Background(), GroupASR, AcquireRequest{
		RequestID: "during-drain",
		GatewayID: "gateway-1",
		Purpose:   "transcribe",
	})
	require.NoError(t, err)
	require.False(t, newLease.ColdStart)
	require.Equal(t, int32(1), runtime.starts.Load())
	close(runtime.allowDrain)
	require.NoError(t, <-sweepDone)
	require.Zero(t, runtime.stops.Load())

	snapshot, err = coordinator.Snapshot(GroupASR)
	require.NoError(t, err)
	require.Equal(t, StateBusy, snapshot.State)
	require.Equal(t, 1, snapshot.Active)
}

func TestCoordinatorAppliesUpdatedPolicyWithoutRestart(t *testing.T) {
	t.Parallel()

	clock := &manualClock{now: time.Date(2026, 8, 18, 18, 0, 0, 0, time.UTC)}
	runtime := &countingRuntime{}
	coordinator, err := NewCoordinator(testConfig(), runtime, WithClock(clock))
	require.NoError(t, err)

	lease, err := coordinator.Acquire(context.Background(), GroupASR, AcquireRequest{
		RequestID: "before-config-update",
		GatewayID: "gateway-1",
		Purpose:   "transcribe",
	})
	require.NoError(t, err)
	require.NoError(t, coordinator.Release(lease.ID, ReleaseCompleted))

	updated := testConfig()
	updated.Revision = 2
	idleMinutes := 1
	asr := updated.Groups[GroupASR]
	asr.IdleMinutes = &idleMinutes
	updated.Groups[GroupASR] = asr
	require.NoError(t, coordinator.ApplyConfig(updated))

	clock.Advance(time.Minute)
	require.NoError(t, coordinator.SweepIdle(context.Background()))
	require.Equal(t, int32(1), runtime.stops.Load())
}

func TestCoordinatorEnsuresAlwaysOnGroupWithoutHoldingSyntheticLease(t *testing.T) {
	t.Parallel()

	config := testConfig()
	asr := config.Groups[GroupASR]
	asr.Mode = ModeAlwaysOn
	config.Groups[GroupASR] = asr
	runtime := &countingRuntime{}
	coordinator, err := NewCoordinator(config, runtime)
	require.NoError(t, err)

	require.NoError(t, coordinator.EnsureAlwaysOn(context.Background()))
	require.Equal(t, int32(1), runtime.starts.Load())
	snapshot, err := coordinator.Snapshot(GroupASR)
	require.NoError(t, err)
	require.Equal(t, StateReady, snapshot.State)
	require.Zero(t, snapshot.Active)
}

func testConfig() Config {
	return Config{
		SchemaVersion: CurrentSchemaVersion,
		Revision:      1,
		Defaults: DefaultsConfig{
			IdleMinutes:            10,
			StartupTimeoutSeconds:  120,
			FailureCooldownMinutes: 5,
		},
		Groups: map[Group]GroupConfig{
			GroupPaddleOCR: {Mode: ModeOnDemand},
			GroupASR:       {Mode: ModeOnDemand},
			GroupReranker:  {Mode: ModeOnDemand},
		},
	}
}
