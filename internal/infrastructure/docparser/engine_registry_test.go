package docparser

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type countingAvailabilityEngine struct {
	checks int
}

func (e *countingAvailabilityEngine) Name() string            { return PaddleOCRVLEngineName }
func (e *countingAvailabilityEngine) Description() string     { return "test paddle" }
func (e *countingAvailabilityEngine) FileTypes(bool) []string { return []string{"pdf"} }
func (e *countingAvailabilityEngine) CheckAvailable(bool, map[string]string) (bool, string) {
	e.checks++
	return false, "probe should have been skipped"
}
func (e *countingAvailabilityEngine) NewReader(context.Context, ReaderDeps) (interfaces.DocReader, error) {
	return nil, nil
}

func TestListAllEnginesBuiltinIncludesHTML(t *testing.T) {
	engines := ListAllEngines(true, nil, nil)
	for _, engine := range engines {
		if engine.Name != "builtin" {
			continue
		}
		if !engine.Available {
			t.Fatalf("builtin engine is unavailable: %s", engine.UnavailableReason)
		}

		fileTypes := make(map[string]bool, len(engine.FileTypes))
		for _, fileType := range engine.FileTypes {
			fileTypes[fileType] = true
		}
		for _, want := range []string{"html", "htm"} {
			if !fileTypes[want] {
				t.Errorf("builtin engine file types do not include %q: %v", want, engine.FileTypes)
			}
		}
		return
	}

	t.Fatal("builtin engine not found")
}

func TestListAllEnginesPassiveOverrideSkipsAvailabilityProbe(t *testing.T) {
	original := localEngines
	t.Cleanup(func() { localEngines = original })
	engine := &countingAvailabilityEngine{}
	localEngines = []EngineRegistration{engine}

	options := EngineListOptions{PassiveAvailability: map[string]types.ParserEngineInfo{
		PaddleOCRVLEngineName: {
			Available: true,
			State:     types.ParserEngineStateStandby,
		},
	}}
	for i := 0; i < 10; i++ {
		engines := ListAllEnginesWithOptions(false, nil, nil, options)
		if len(engines) != 1 {
			t.Fatalf("list %d returned %d engines, want 1", i, len(engines))
		}
		if !engines[0].Available || engines[0].State != types.ParserEngineStateStandby {
			t.Fatalf("list %d returned availability=%v state=%q, want true/standby", i, engines[0].Available, engines[0].State)
		}
	}
	if engine.checks != 0 {
		t.Fatalf("passive listing ran %d availability probes, want 0", engine.checks)
	}
}
