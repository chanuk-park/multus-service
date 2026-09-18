package probe

import (
	"testing"

	"github.com/boanlab/multus-service/internal/model"
)

func fail() Result    { return Result{Success: false, ErrorKind: ErrTimeout} }
func success() Result { return Result{Success: true} }

func TestStartsUnknownNotHealthy(t *testing.T) {
	// Nothing has been observed. That is not the same as healthy, and the
	// manager omits an Unknown key rather than publishing a guess.
	m := NewMachine(3, 2)
	if m.State() != model.StateUnknown {
		t.Errorf("state = %v, want Unknown", m.State())
	}
}

func TestFailureThresholdDebouncesASingleLoss(t *testing.T) {
	m := NewMachine(3, 2)
	m.Observe(success())
	m.Observe(success())
	if m.State() != model.StateHealthy {
		t.Fatalf("state = %v, want Healthy", m.State())
	}
	// One lost packet is not a path failure.
	if _, _, changed := m.Observe(fail()); changed {
		t.Error("a single failure must not move the state")
	}
	if _, _, changed := m.Observe(fail()); changed {
		t.Error("two failures must not move the state with k=3")
	}
	from, to, changed := m.Observe(fail())
	if !changed || from != model.StateHealthy || to != model.StateUnhealthy {
		t.Errorf("third failure: from=%v to=%v changed=%v", from, to, changed)
	}
}

func TestSuccessResetsTheFailureStreak(t *testing.T) {
	// Flapping below the threshold must not accumulate into a transition.
	m := NewMachine(3, 2)
	m.Observe(success())
	m.Observe(success())
	for i := 0; i < 10; i++ {
		m.Observe(fail())
		m.Observe(fail())
		m.Observe(success())
	}
	if m.State() != model.StateHealthy {
		t.Errorf("state = %v, want Healthy: two failures never reach k=3", m.State())
	}
}

func TestRecoveryNeedsTheSuccessThreshold(t *testing.T) {
	m := NewMachine(1, 3)
	m.Observe(fail())
	if m.State() != model.StateUnhealthy {
		t.Fatalf("state = %v, want Unhealthy", m.State())
	}
	m.Observe(success())
	m.Observe(success())
	if m.State() != model.StateUnhealthy {
		t.Error("two successes must not restore a path that needs three")
	}
	if _, to, changed := m.Observe(success()); !changed || to != model.StateHealthy {
		t.Errorf("third success: to=%v changed=%v", to, changed)
	}
}

func TestThresholdsAreIndependent(t *testing.T) {
	// How eager to withdraw and how eager to restore are different questions.
	m := NewMachine(1, 5)
	m.Observe(success())
	m.Observe(success())
	m.Observe(success())
	m.Observe(success())
	m.Observe(success())
	if m.State() != model.StateHealthy {
		t.Fatalf("state = %v, want Healthy", m.State())
	}
	if _, to, _ := m.Observe(fail()); to != model.StateUnhealthy {
		t.Error("k=1 must withdraw on the first failure")
	}
}

func TestZeroThresholdsAreClampedNotZero(t *testing.T) {
	m := NewMachine(0, 0)
	if m.FailureThreshold != 1 || m.SuccessThreshold != 1 {
		t.Errorf("thresholds = %d/%d, want 1/1", m.FailureThreshold, m.SuccessThreshold)
	}
}

func TestSocketKeyDistinguishesSource(t *testing.T) {
	// One socket per (netns, source, interface): a Pod recreate changes the
	// netns path, and reusing the old socket would probe a dead namespace.
	a := Spec{NetnsPath: "/var/run/netns/a", SourceIP: "10.0.0.1", Interface: "net1"}
	for _, b := range []Spec{
		{NetnsPath: "/var/run/netns/b", SourceIP: "10.0.0.1", Interface: "net1"},
		{NetnsPath: "/var/run/netns/a", SourceIP: "10.0.0.2", Interface: "net1"},
		{NetnsPath: "/var/run/netns/a", SourceIP: "10.0.0.1", Interface: "net2"},
	} {
		if SocketKey(a) == SocketKey(b) {
			t.Errorf("socket key collides: %+v vs %+v", a, b)
		}
	}
}
