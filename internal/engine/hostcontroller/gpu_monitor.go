package hostcontroller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

type GPUSample struct {
	TotalMiB           int
	UsedMiB            int
	FreeMiB            int
	UtilizationPercent int
	UsedPercent        float64
}

type GPUAdmissionDecision struct {
	PaddleAllowed   bool
	RerankerAllowed bool
}

type admissionWindow struct {
	allowed       bool
	highSince     *time.Time
	recoverySince *time.Time
	disabledUntil time.Time
}

type gpuAdmissionPolicy struct {
	config   lifecycle.GPUAdmissionConfig
	paddle   admissionWindow
	reranker admissionWindow
}

func newGPUAdmissionPolicy(config lifecycle.GPUAdmissionConfig) *gpuAdmissionPolicy {
	return &gpuAdmissionPolicy{
		config:   config,
		paddle:   admissionWindow{allowed: true},
		reranker: admissionWindow{allowed: true},
	}
}

func (p *gpuAdmissionPolicy) Evaluate(sample GPUSample, now time.Time) GPUAdmissionDecision {
	p.updateWindow(
		&p.paddle,
		now,
		sample.UsedPercent >= float64(p.config.PaddleDisableAtPercent) || sample.FreeMiB < p.config.PaddleCriticalFreeMiB,
		sample.UsedPercent <= float64(p.config.PaddleEnableBelowPercent) && sample.FreeMiB >= p.config.PaddleMinimumFreeMiB,
	)
	p.updateWindow(
		&p.reranker,
		now,
		sample.UsedPercent >= float64(p.config.RerankerDisableAtPercent) || sample.FreeMiB < p.config.RerankerCriticalFreeMiB,
		sample.UsedPercent <= float64(p.config.RerankerEnableBelowPercent) && sample.FreeMiB >= p.config.RerankerMinimumFreeMiB,
	)
	return GPUAdmissionDecision{
		PaddleAllowed:   p.paddle.allowed,
		RerankerAllowed: p.reranker.allowed,
	}
}

func (p *gpuAdmissionPolicy) updateWindow(window *admissionWindow, now time.Time, high, recovered bool) {
	sustain := time.Duration(p.config.SustainSeconds) * time.Second
	if window.allowed {
		window.recoverySince = nil
		if !high {
			window.highSince = nil
			return
		}
		if window.highSince == nil {
			started := now
			window.highSince = &started
			return
		}
		if now.Sub(*window.highSince) >= sustain {
			window.allowed = false
			window.highSince = nil
			window.disabledUntil = now.Add(time.Duration(p.config.CooldownSeconds) * time.Second)
		}
		return
	}

	window.highSince = nil
	if now.Before(window.disabledUntil) || !recovered {
		window.recoverySince = nil
		return
	}
	if window.recoverySince == nil {
		started := now
		window.recoverySince = &started
		return
	}
	if now.Sub(*window.recoverySince) >= sustain {
		window.allowed = true
		window.recoverySince = nil
	}
}

type gpuProbe interface {
	Sample(context.Context) (GPUSample, error)
}

type nvidiaSMIProbe struct {
	executable string
}

func (p nvidiaSMIProbe) Sample(ctx context.Context) (GPUSample, error) {
	command := exec.CommandContext(
		ctx,
		p.executable,
		"--query-gpu=memory.total,memory.used,memory.free,utilization.gpu",
		"--format=csv,noheader,nounits",
	)
	output, err := command.Output()
	if err != nil {
		return GPUSample{}, fmt.Errorf("query GPU pressure: %w", err)
	}
	return parseNvidiaSMISample(string(output))
}

func parseNvidiaSMISample(output string) (GPUSample, error) {
	line := strings.TrimSpace(strings.Split(output, "\n")[0])
	parts := strings.Split(line, ",")
	if len(parts) != 4 {
		return GPUSample{}, errors.New("nvidia-smi returned an unexpected field count")
	}
	values := make([]int, len(parts))
	for index, part := range parts {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || value < 0 {
			return GPUSample{}, fmt.Errorf("parse nvidia-smi field %d", index)
		}
		values[index] = value
	}
	if values[0] == 0 || values[1] > values[0] || values[2] > values[0] {
		return GPUSample{}, errors.New("nvidia-smi returned invalid memory totals")
	}
	return GPUSample{
		TotalMiB:           values[0],
		UsedMiB:            values[1],
		FreeMiB:            values[2],
		UtilizationPercent: values[3],
		UsedPercent:        float64(values[1]) * 100 / float64(values[0]),
	}, nil
}

type gpuAdmissionTarget interface {
	SetGroupGPUAdmission(lifecycle.Group, bool)
}

type GPUMonitor struct {
	config lifecycle.GPUAdmissionConfig
	probe  gpuProbe
	policy *gpuAdmissionPolicy
	target gpuAdmissionTarget
}

func NewGPUMonitor(config lifecycle.GPUAdmissionConfig, target gpuAdmissionTarget) (*GPUMonitor, error) {
	if !config.Enabled {
		return nil, nil
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if target == nil {
		return nil, errors.New("GPU admission target is required")
	}
	return &GPUMonitor{
		config: config,
		probe:  nvidiaSMIProbe{executable: config.NvidiaSMIExecutable},
		policy: newGPUAdmissionPolicy(config),
		target: target,
	}, nil
}

func (m *GPUMonitor) Refresh(ctx context.Context) error {
	sample, err := m.probe.Sample(ctx)
	if err != nil {
		m.target.SetGroupGPUAdmission(lifecycle.GroupPaddleOCR, false)
		m.target.SetGroupGPUAdmission(lifecycle.GroupReranker, false)
		return err
	}
	decision := m.policy.Evaluate(sample, time.Now())
	m.target.SetGroupGPUAdmission(lifecycle.GroupPaddleOCR, decision.PaddleAllowed)
	m.target.SetGroupGPUAdmission(lifecycle.GroupReranker, decision.RerankerAllowed)
	return nil
}

func (m *GPUMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.config.PollSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.Refresh(ctx); err != nil {
				log.Printf("engine GPU admission failed closed: %v", err)
			}
		}
	}
}
