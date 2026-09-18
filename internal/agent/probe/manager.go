package probe

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/boanlab/multus-service/internal/model"
	"github.com/boanlab/multus-service/internal/obs"
)

// Manager runs the probe loop and owns one state machine per scope key.
//
// Under Endpoint scope a key is an attachment, so there is one probe and one
// machine each. Under Node scope a key is a (node, NAD) domain: one probe, one
// machine, and every attachment in the domain reads the same answer. That is
// the whole of the aggregation -- it lives here rather than in the report
// format, so the two scopes differ only in how targets are keyed.
type Manager struct {
	Prober           Prober
	Interval         time.Duration
	FailureThreshold int
	SuccessThreshold int
	NodeName         string
	Events           *obs.Recorder

	mu       sync.Mutex
	targets  map[string]Spec
	machines map[string]*Machine
	changed  map[string]bool

	// inFlight keeps one round at a time. A round waits for its slowest probe,
	// and a probe that fails by timing out takes the full timeout -- which can
	// exceed the interval. Blocking the ticker would then silently stretch the
	// sampling period, and the hysteresis thresholds are expressed in samples,
	// so the stretch would show up as an unexplained delay in the results.
	inFlight atomic.Bool

	wake chan struct{}
}

// NewManager returns a manager with no targets.
func NewManager(p Prober, interval time.Duration, failureThreshold, successThreshold int,
	nodeName string, events *obs.Recorder) *Manager {
	return &Manager{
		Prober:           p,
		Interval:         interval,
		FailureThreshold: failureThreshold,
		SuccessThreshold: successThreshold,
		NodeName:         nodeName,
		Events:           events,
		targets:          map[string]Spec{},
		machines:         map[string]*Machine{},
		changed:          map[string]bool{},
		wake:             make(chan struct{}, 1),
	}
}

// SetTargets replaces the probe set. A key that disappears takes its state
// machine with it, so a returning attachment starts from Unknown rather than
// inheriting a verdict formed about a namespace that no longer exists.
func (m *Manager) SetTargets(specs []Spec) {
	m.mu.Lock()
	defer m.mu.Unlock()

	next := make(map[string]Spec, len(specs))
	for _, s := range specs {
		next[s.ScopeKey] = s
	}
	for k := range m.machines {
		if _, ok := next[k]; !ok {
			delete(m.machines, k)
			delete(m.changed, k)
		}
	}
	for k := range next {
		if _, ok := m.machines[k]; !ok {
			m.machines[k] = NewMachine(m.FailureThreshold, m.SuccessThreshold)
		}
	}
	m.targets = next
}

// Wake fires when a path state changed, so the agent can report it immediately
// instead of waiting for its next sweep.
func (m *Manager) Wake() <-chan struct{} { return m.wake }

// States returns the current debounced path state for every target.
//
// A key whose machine has not yet reached a verdict is omitted entirely rather
// than reported as false: the controller already treats a missing report as
// not-ready, and claiming to have measured a failure that was never observed
// would corrupt the evaluation.
func (m *Manager) States() []model.PathHealth {
	m.mu.Lock()
	defer m.mu.Unlock()

	keys := make([]string, 0, len(m.targets))
	for k := range m.targets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	now := time.Now()
	out := make([]model.PathHealth, 0, len(keys))
	for _, k := range keys {
		mc := m.machines[k]
		if mc == nil || mc.State() == model.StateUnknown {
			continue
		}
		s := m.targets[k]
		out = append(out, model.PathHealth{
			Scope:      s.Scope,
			ScopeID:    k,
			NodeName:   m.NodeName,
			NAD:        s.NAD,
			Target:     s.Target,
			PathReady:  mc.State() == model.StateHealthy,
			ObservedAt: now,
		})
	}
	return out
}

// TakeChanged returns and clears the scope keys whose state moved.
func (m *Manager) TakeChanged() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.changed
	m.changed = map[string]bool{}
	return out
}

// Start runs the probe loop until ctx is cancelled.
func (m *Manager) Start(ctx context.Context) error {
	lg := log.FromContext(ctx).WithName("probe")
	interval := m.Interval
	if interval <= 0 {
		interval = time.Second
	}
	m.Events.Emit("probe_loop_started",
		"node", m.NodeName, "interval_ms", interval.Milliseconds(),
		"failure_threshold", m.FailureThreshold, "success_threshold", m.SuccessThreshold)
	lg.Info("starting", "interval", interval,
		"failureThreshold", m.FailureThreshold, "successThreshold", m.SuccessThreshold)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer m.Prober.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if !m.inFlight.CompareAndSwap(false, true) {
				// The previous round is still waiting on a slow probe. Skipping
				// is recorded rather than hidden: it means the effective
				// sampling period is longer than --probe-interval, which
				// changes how long the failure thresholds take to trip.
				m.Events.Emit("probe_round_skipped", "node", m.NodeName)
				continue
			}
			go func() {
				defer m.inFlight.Store(false)
				m.round(ctx)
			}()
		}
	}
}

func (m *Manager) round(ctx context.Context) {
	m.mu.Lock()
	specs := make([]Spec, 0, len(m.targets))
	for _, s := range m.targets {
		specs = append(specs, s)
	}
	m.mu.Unlock()

	if len(specs) == 0 {
		m.Prober.Retain(map[string]bool{})
		return
	}

	keep := make(map[string]bool, len(specs))
	for _, s := range specs {
		keep[SocketKey(s)] = true
	}
	m.Prober.Retain(keep)

	// Probes are independent and each can block for its whole timeout, so one
	// slow target must not delay the rest of the round.
	var wg sync.WaitGroup
	results := make([]Result, len(specs))
	for i := range specs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = m.Prober.Probe(ctx, specs[i])
		}(i)
	}
	wg.Wait()

	anyChanged := false
	for i, s := range specs {
		r := results[i]

		// Every sample is recorded, not just the transitions. The gap between
		// the first failed probe and the state change is the hysteresis cost,
		// and it can only be measured if both are in the stream.
		m.Events.Emit("path_probe",
			"scope_id", s.ScopeKey, "scope", string(s.Scope),
			"attachment_id", s.AttachmentID, "pod", s.PodName,
			"interface", s.Interface, "source_ip", s.SourceIP,
			"target", s.Target, "nad", s.NAD,
			"success", r.Success, "rtt_ms", float64(r.RTT)/1e6,
			"error_kind", string(r.ErrorKind))

		m.mu.Lock()
		mc := m.machines[s.ScopeKey]
		if mc == nil {
			m.mu.Unlock()
			continue
		}
		from, to, changed := mc.Observe(r)
		succ, fail := mc.Streak()
		if changed {
			m.changed[s.ScopeKey] = true
		}
		m.mu.Unlock()

		if changed {
			anyChanged = true
			m.Events.Emit("path_state_changed",
				"scope_id", s.ScopeKey, "scope", string(s.Scope),
				"attachment_id", s.AttachmentID, "pod", s.PodName,
				"interface", s.Interface, "source_ip", s.SourceIP, "target", s.Target,
				"from", string(from), "to", string(to),
				"consecutive_success", succ, "consecutive_failure", fail)
		}
	}

	if anyChanged {
		select {
		case m.wake <- struct{}{}:
		default:
		}
	}
}
