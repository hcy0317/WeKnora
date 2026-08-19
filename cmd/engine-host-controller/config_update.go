package main

import (
	"fmt"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

func setControllerObserveOnly(configPath string, observeOnly bool) (*lifecycle.Config, error) {
	store := lifecycle.NewConfigStore(configPath)
	current, err := store.Load()
	if err != nil {
		return nil, err
	}
	if current.Controller.ObserveOnly == observeOnly {
		return current, nil
	}
	candidate := *current
	candidate.Controller.ObserveOnly = observeOnly
	updated, err := store.Update(current.Revision, candidate)
	if err != nil {
		return nil, fmt.Errorf("update controller observe-only mode: %w", err)
	}
	return updated, nil
}
