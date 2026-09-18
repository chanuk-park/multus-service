package probe

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/boanlab/multus-service/internal/model"
	"github.com/boanlab/multus-service/internal/obs"
)

type scriptedProber struct {
	mu      sync.Mutex
	results map[string]bool // scope key -> success
	calls   map[string]int
}

func newScripted() *scriptedProber {
	return &scriptedProber{results: map[string]bool{}, calls: map[string]int{}}
}

func (p *scriptedProber) set(key string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.results[key] = ok
}

func (p *scriptedProber) Probe(_ context.Context, s Spec) Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[s.ScopeKey]++
	return Result{Success: p.results[s.ScopeKey], ObservedAt: time.Now()}
}

func (p *scriptedProber) callsFor(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[key]
}

func (p *scriptedProber) Retain(map[string]bool) {}
func (p *scriptedProber) Close()                 {}

func recorder(t *testing.T) *obs.Recorder {
	t.Helper()
	r, err := obs.NewRecorder("")
	if err != nil {
		t.Fatalf("recorder: %v", err)
	}
	return r
}

func TestManagerOmitsUnknownRatherThanGuessing(t *testing.T) {
	// A key that has not reached a verdict produces no PathHealth at all. The
	// controller already treats a missing report as not-ready; claiming a
	// measured failure that never happened would corrupt the evaluation.
	p := newScripted()
	m := NewManager(p, time.Hour, 3, 2, "node-1", recorder(t))
	m.SetTargets([]Spec{{ScopeKey: "a", Scope: model.ScopeEndpoint}})
	if got := m.States(); len(got) != 0 {
		t.Errorf("states = %v, want none before any sample", got)
	}
}

func TestManagerAppliesHysteresisPerScopeKey(t *testing.T) {
	p := newScripted()
	p.set("a", true)
	p.set("b", true)
	m := NewManager(p, time.Hour, 2, 2, "node-1", recorder(t))
	m.SetTargets([]Spec{
		{ScopeKey: "a", Scope: model.ScopeEndpoint},
		{ScopeKey: "b", Scope: model.ScopeEndpoint},
	})
	ctx := context.Background()
	m.round(ctx)
	m.round(ctx)
	if len(m.States()) != 2 {
		t.Fatalf("states = %v, want both healthy", m.States())
	}

	// Only 'a' fails. Endpoint scope keeps the two independent.
	p.set("a", false)
	m.round(ctx)
	m.round(ctx)
	byKey := map[string]bool{}
	for _, s := range m.States() {
		byKey[s.ScopeID] = s.PathReady
	}
	if byKey["a"] {
		t.Error("a should be down after two failures with k=2")
	}
	if !byKey["b"] {
		t.Error("b must be unaffected by a's failure")
	}
}

func TestOneSharedKeyMeansOneProbeAndOneMachine(t *testing.T) {
	// The whole of Node-scope aggregation: N attachments collapse to one key,
	// so one probe runs and one state machine decides for all of them.
	p := newScripted()
	p.set("domain", true)
	m := NewManager(p, time.Hour, 2, 2, "node-1", recorder(t))
	m.SetTargets([]Spec{{ScopeKey: "domain", Scope: model.ScopeNode, NAD: "oai/n2"}})

	ctx := context.Background()
	m.round(ctx)
	m.round(ctx)
	if p.callsFor("domain") != 2 {
		t.Errorf("probe calls = %d, want one per round", p.callsFor("domain"))
	}
	states := m.States()
	if len(states) != 1 || states[0].Scope != model.ScopeNode {
		t.Fatalf("states = %+v, want a single Node-scope entry", states)
	}
}

func TestChangedIsReportedOnceAndCleared(t *testing.T) {
	p := newScripted()
	p.set("a", true)
	m := NewManager(p, time.Hour, 1, 1, "node-1", recorder(t))
	m.SetTargets([]Spec{{ScopeKey: "a", Scope: model.ScopeEndpoint}})

	m.round(context.Background())
	if ch := m.TakeChanged(); !ch["a"] {
		t.Error("the first verdict must be reported as a change")
	}
	if ch := m.TakeChanged(); len(ch) != 0 {
		t.Error("changed set must be cleared once taken")
	}
	m.round(context.Background())
	if ch := m.TakeChanged(); len(ch) != 0 {
		t.Error("a steady state is not a change")
	}
}

func TestDroppedTargetForgetsItsVerdict(t *testing.T) {
	// A returning attachment starts from Unknown. Inheriting a verdict formed
	// about a namespace that no longer exists would be worse than no verdict.
	p := newScripted()
	p.set("a", true)
	m := NewManager(p, time.Hour, 1, 1, "node-1", recorder(t))
	m.SetTargets([]Spec{{ScopeKey: "a", Scope: model.ScopeEndpoint}})
	m.round(context.Background())
	if len(m.States()) != 1 {
		t.Fatal("expected a verdict")
	}
	m.SetTargets(nil)
	m.SetTargets([]Spec{{ScopeKey: "a", Scope: model.ScopeEndpoint}})
	if got := m.States(); len(got) != 0 {
		t.Errorf("states = %v, want none after the target was dropped and re-added", got)
	}
}
