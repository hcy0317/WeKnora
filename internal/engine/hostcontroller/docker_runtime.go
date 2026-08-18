package hostcontroller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

var ErrObserveOnly = errors.New("controller is observe-only")

type CommandRunner interface {
	Run(ctx context.Context, arguments ...string) (string, error)
}

type DockerRuntimeOption func(*DockerRuntime)

func WithHealthPollInterval(interval time.Duration) DockerRuntimeOption {
	return func(runtime *DockerRuntime) {
		if interval > 0 {
			runtime.healthPollInterval = interval
		}
	}
}

func WithObserveOnly(observeOnly bool) DockerRuntimeOption {
	return func(runtime *DockerRuntime) {
		runtime.observeOnly = observeOnly
	}
}

func WithActuationGate(gate ActuationGate) DockerRuntimeOption {
	return func(runtime *DockerRuntime) {
		runtime.actuationGate = gate
	}
}

type DockerRuntime struct {
	catalog            lifecycle.CatalogConfig
	runner             CommandRunner
	healthPollInterval time.Duration
	observeOnly        bool
	actuationGate      ActuationGate
}

func NewDockerRuntime(
	catalog lifecycle.CatalogConfig,
	runner CommandRunner,
	options ...DockerRuntimeOption,
) (*DockerRuntime, error) {
	if runner == nil {
		return nil, errors.New("docker command runner is required")
	}
	if err := catalog.Validate(); err != nil {
		return nil, err
	}
	runtime := &DockerRuntime{
		catalog:            catalog,
		runner:             runner,
		healthPollInterval: time.Second,
	}
	for _, option := range options {
		option(runtime)
	}
	return runtime, nil
}

func (r *DockerRuntime) Start(ctx context.Context, group lifecycle.Group) (lifecycle.Backend, error) {
	return r.StartWithAdmission(ctx, lifecycle.StartRequest{Group: group, GPUAllowed: true})
}

func (r *DockerRuntime) StartWithAdmission(
	ctx context.Context,
	request lifecycle.StartRequest,
) (lifecycle.Backend, error) {
	if r.observeOnly || r.actuationGate == nil {
		return r.startWithAdmission(ctx, request)
	}
	var backend lifecycle.Backend
	err := r.actuationGate.WithOwnership(ctx, func() error {
		var err error
		backend, err = r.startWithAdmission(ctx, request)
		return err
	})
	return backend, err
}

func (r *DockerRuntime) startWithAdmission(
	ctx context.Context,
	request lifecycle.StartRequest,
) (lifecycle.Backend, error) {
	group, ok := r.catalog.Groups[request.Group]
	if !ok {
		return lifecycle.Backend{}, fmt.Errorf("unknown engine group %q", request.Group)
	}
	var failures []error
	for _, backend := range group.Backends {
		if backend.GPU && !request.GPUAllowed {
			continue
		}
		if err := r.startBackend(ctx, backend); err != nil {
			failures = append(failures, fmt.Errorf("backend %s: %w", backend.ID, err))
			continue
		}
		return lifecycle.Backend{ID: backend.ID, URL: backend.Upstream}, nil
	}
	if len(failures) == 0 {
		return lifecycle.Backend{}, fmt.Errorf("engine group %s has no admissible backend", request.Group)
	}
	return lifecycle.Backend{}, fmt.Errorf("start engine group %s: %w", request.Group, errors.Join(failures...))
}

func (r *DockerRuntime) startBackend(ctx context.Context, backend lifecycle.CatalogBackend) error {
	started := make([]string, 0, len(backend.Containers))
	for _, container := range backend.Containers {
		if !r.observeOnly {
			if _, err := r.runner.Run(ctx, "start", container); err != nil {
				r.stopStarted(context.Background(), started)
				return fmt.Errorf("start container %s: %w", container, err)
			}
			started = append(started, container)
		}
		if err := r.waitHealthy(ctx, container); err != nil {
			if !r.observeOnly {
				r.stopStarted(context.Background(), started)
			}
			return err
		}
	}
	return nil
}

func (r *DockerRuntime) waitHealthy(ctx context.Context, container string) error {
	ticker := time.NewTicker(r.healthPollInterval)
	defer ticker.Stop()
	for {
		state, err := r.inspectState(ctx, container)
		if err == nil && state.Running && state.Health != nil && state.Health.Status == "healthy" {
			return nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("wait for container %s health: %v: %w", container, err, ctx.Err())
			}
			return fmt.Errorf("wait for container %s health: status=%s health=%s: %w",
				container, state.Status, state.healthStatus(), ctx.Err())
		case <-ticker.C:
		}
	}
}

type containerState struct {
	Status  string `json:"Status"`
	Running bool   `json:"Running"`
	Health  *struct {
		Status string `json:"Status"`
	} `json:"Health"`
}

func (s containerState) healthStatus() string {
	if s.Health == nil {
		return "missing"
	}
	return s.Health.Status
}

func (r *DockerRuntime) inspectState(ctx context.Context, container string) (containerState, error) {
	output, err := r.runner.Run(ctx, "inspect", "--format", "{{json .State}}", container)
	if err != nil {
		return containerState{}, err
	}
	var state containerState
	if err := json.Unmarshal([]byte(output), &state); err != nil {
		return containerState{}, fmt.Errorf("decode container %s state: %w", container, err)
	}
	return state, nil
}

func (r *DockerRuntime) stopStarted(ctx context.Context, containers []string) {
	for index := len(containers) - 1; index >= 0; index-- {
		_, _ = r.runner.Run(ctx, "stop", "--time", "20", containers[index])
	}
}

func (r *DockerRuntime) Stop(ctx context.Context, group lifecycle.Group) error {
	if r.observeOnly {
		return ErrObserveOnly
	}
	if r.actuationGate != nil {
		return r.actuationGate.WithOwnership(ctx, func() error {
			return r.stop(ctx, group)
		})
	}
	return r.stop(ctx, group)
}

func (r *DockerRuntime) stop(ctx context.Context, group lifecycle.Group) error {
	catalogGroup, ok := r.catalog.Groups[group]
	if !ok {
		return fmt.Errorf("unknown engine group %q", group)
	}
	seen := make(map[string]struct{})
	var failures []error
	for backendIndex := len(catalogGroup.Backends) - 1; backendIndex >= 0; backendIndex-- {
		containers := catalogGroup.Backends[backendIndex].Containers
		for containerIndex := len(containers) - 1; containerIndex >= 0; containerIndex-- {
			container := containers[containerIndex]
			if _, duplicate := seen[container]; duplicate {
				continue
			}
			seen[container] = struct{}{}
			if _, err := r.runner.Run(ctx, "stop", "--time", "20", container); err != nil {
				failures = append(failures, fmt.Errorf("stop container %s: %w", container, err))
			}
		}
	}
	return errors.Join(failures...)
}

func (r *DockerRuntime) StopBackend(ctx context.Context, group lifecycle.Group, backendID string) error {
	if r.observeOnly {
		return ErrObserveOnly
	}
	action := func() error { return r.stopBackend(ctx, group, backendID) }
	if r.actuationGate != nil {
		return r.actuationGate.WithOwnership(ctx, action)
	}
	return action()
}

func (r *DockerRuntime) stopBackend(ctx context.Context, group lifecycle.Group, backendID string) error {
	catalogGroup, ok := r.catalog.Groups[group]
	if !ok {
		return fmt.Errorf("unknown engine group %q", group)
	}
	for _, backend := range catalogGroup.Backends {
		if backend.ID != backendID {
			continue
		}
		var failures []error
		for index := len(backend.Containers) - 1; index >= 0; index-- {
			container := backend.Containers[index]
			if _, err := r.runner.Run(ctx, "stop", "--time", "20", container); err != nil {
				failures = append(failures, fmt.Errorf("stop container %s: %w", container, err))
			}
		}
		return errors.Join(failures...)
	}
	return fmt.Errorf("unknown engine backend %q for group %s", backendID, group)
}
