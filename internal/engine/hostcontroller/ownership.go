package hostcontroller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

var ErrOwnerConflict = errors.New("engine Docker owner is already active")
var ErrOwnerMismatch = errors.New("engine Docker owner mismatch")

type Ownership interface {
	Close() error
}

type ActuationGate interface {
	WithOwnership(context.Context, func() error) error
}

type ownerFileActuationGate struct {
	mutexName     string
	ownerPath     string
	expectedOwner string
}

func NewOwnerFileActuationGate(mutexName, ownerPath, expectedOwner string) (ActuationGate, error) {
	if mutexName == "" {
		return nil, errors.New("engine owner mutex is required")
	}
	if ownerPath == "" {
		return nil, errors.New("engine owner state path is required")
	}
	if expectedOwner != "legacy" && expectedOwner != "controller" {
		return nil, fmt.Errorf("unsupported engine Docker owner %q", expectedOwner)
	}
	return &ownerFileActuationGate{
		mutexName:     mutexName,
		ownerPath:     ownerPath,
		expectedOwner: expectedOwner,
	}, nil
}

func (g *ownerFileActuationGate) WithOwnership(ctx context.Context, action func() error) (err error) {
	if action == nil {
		return errors.New("engine actuation action is required")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ownership, err := AcquireOwnership(g.mutexName)
	if err != nil {
		return fmt.Errorf("acquire engine Docker owner interlock: %w", err)
	}
	defer func() {
		if closeErr := ownership.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("release engine Docker owner interlock: %w", closeErr))
		}
	}()

	owner, err := readOwnerState(g.ownerPath)
	if err != nil {
		return err
	}
	if owner != g.expectedOwner {
		return fmt.Errorf("%w: expected=%s actual=%s", ErrOwnerMismatch, g.expectedOwner, owner)
	}
	return action()
}

func readOwnerState(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open engine Docker owner state: %w", err)
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, 65))
	if err != nil {
		return "", fmt.Errorf("read engine Docker owner state: %w", err)
	}
	if len(contents) > 64 {
		return "", errors.New("engine Docker owner state is too large")
	}
	owner := strings.TrimSpace(string(contents))
	if owner != "legacy" && owner != "controller" {
		return "", fmt.Errorf("invalid engine Docker owner state %q", owner)
	}
	return owner, nil
}
