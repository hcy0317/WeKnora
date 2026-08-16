package langfuse

import (
	"context"
	"time"
)

// GenerationProgress is a low-volume transport milestone for a streaming
// generation. It deliberately records no prompt or partial model content:
// business data keeps its existing atomic "completed + validated" write
// boundary while the processing timeline can still distinguish dispatch,
// response-header, and first-event stalls.
type GenerationProgress struct {
	State     string
	Protocol  string
	Endpoint  string
	EventType string
	At        time.Time
}

type generationProgressObserver func(GenerationProgress)
type generationProgressObserverKey struct{}

// WithGenerationProgressObserver installs the observer used by model
// transports to report coarse streaming milestones to the active Generation.
func WithGenerationProgressObserver(
	ctx context.Context,
	observer func(GenerationProgress),
) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, generationProgressObserverKey{}, generationProgressObserver(observer))
}

// ReportGenerationProgress is a no-op when the call is not wrapped by the
// Langfuse generation decorator.
func ReportGenerationProgress(ctx context.Context, progress GenerationProgress) {
	if ctx == nil {
		return
	}
	observer, _ := ctx.Value(generationProgressObserverKey{}).(generationProgressObserver)
	if observer == nil {
		return
	}
	if progress.At.IsZero() {
		progress.At = time.Now()
	}
	observer(progress)
}
