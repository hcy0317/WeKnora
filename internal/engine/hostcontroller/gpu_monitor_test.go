package hostcontroller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
	"github.com/stretchr/testify/require"
)

type failingGPUProbe struct{}

func (failingGPUProbe) Sample(context.Context) (GPUSample, error) {
	return GPUSample{}, errors.New("probe unavailable")
}

type recordingGPUAdmissionTarget struct {
	values map[lifecycle.Group]bool
}

func (t *recordingGPUAdmissionTarget) SetGroupGPUAdmission(group lifecycle.Group, allowed bool) {
	t.values[group] = allowed
}

func TestGPUAdmissionPolicyAppliesSustainHysteresisAndCooldown(t *testing.T) {
	t.Parallel()

	config := lifecycle.GPUAdmissionConfig{
		Enabled:                    true,
		PollSeconds:                5,
		SustainSeconds:             10,
		CooldownSeconds:            30,
		RerankerEnableBelowPercent: 70,
		RerankerDisableAtPercent:   85,
		RerankerMinimumFreeMiB:     3500,
		RerankerCriticalFreeMiB:    2048,
		PaddleEnableBelowPercent:   60,
		PaddleDisableAtPercent:     92,
		PaddleMinimumFreeMiB:       7000,
		PaddleCriticalFreeMiB:      1024,
	}
	policy := newGPUAdmissionPolicy(config)
	t0 := time.Date(2026, 8, 18, 17, 30, 0, 0, time.UTC)
	high := GPUSample{UsedPercent: 95, FreeMiB: 900}

	decision := policy.Evaluate(high, t0)
	require.True(t, decision.PaddleAllowed)
	require.True(t, decision.RerankerAllowed)
	decision = policy.Evaluate(high, t0.Add(11*time.Second))
	require.False(t, decision.PaddleAllowed)
	require.False(t, decision.RerankerAllowed)

	low := GPUSample{UsedPercent: 30, FreeMiB: 12000}
	decision = policy.Evaluate(low, t0.Add(25*time.Second))
	require.False(t, decision.PaddleAllowed, "cooldown must keep Paddle closed")
	require.False(t, decision.RerankerAllowed, "cooldown must keep reranker closed")
	decision = policy.Evaluate(low, t0.Add(42*time.Second))
	require.False(t, decision.PaddleAllowed, "recovery must also sustain")
	decision = policy.Evaluate(low, t0.Add(53*time.Second))
	require.True(t, decision.PaddleAllowed)
	require.True(t, decision.RerankerAllowed)
}

func TestParseNvidiaSMISampleRejectsMalformedOutput(t *testing.T) {
	t.Parallel()

	sample, err := parseNvidiaSMISample("16376, 11864, 4185, 3")
	require.NoError(t, err)
	require.Equal(t, 16376, sample.TotalMiB)
	require.Equal(t, 4185, sample.FreeMiB)
	require.InDelta(t, 72.4, sample.UsedPercent, 0.1)

	_, err = parseNvidiaSMISample("not,gpu,data")
	require.Error(t, err)
}

func TestGPUMonitorFailsClosedWhenProbeIsUnavailable(t *testing.T) {
	t.Parallel()

	target := &recordingGPUAdmissionTarget{values: make(map[lifecycle.Group]bool)}
	monitor := &GPUMonitor{
		probe:  failingGPUProbe{},
		policy: newGPUAdmissionPolicy(lifecycle.GPUAdmissionConfig{}),
		target: target,
	}
	err := monitor.Refresh(context.Background())
	require.ErrorContains(t, err, "probe unavailable")
	require.False(t, target.values[lifecycle.GroupPaddleOCR])
	require.False(t, target.values[lifecycle.GroupReranker])
}
