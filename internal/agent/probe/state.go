package probe

import "github.com/boanlab/multus-service/internal/model"

// Machine debounces probe samples into a path state.
//
// Thresholds exist because a single lost packet is not a path failure, and
// because a path that has just come back should be observed twice before
// traffic is steered onto it again. The two counts are separate on purpose: how
// eager to withdraw and how eager to restore are different questions, and the
// evaluation varies them independently.
//
// The state machine is deliberately not part of the prober. Under Node scope
// one machine serves a whole (node, NAD) domain, so every attachment in it
// transitions at the same instant rather than drifting apart on its own
// counters.
type Machine struct {
	FailureThreshold int
	SuccessThreshold int

	state              model.State
	consecutiveSuccess int
	consecutiveFailure int
	samples            int
}

// NewMachine starts in Unknown: nothing has been observed, which is not the
// same as unhealthy and must not be published as healthy either.
func NewMachine(failureThreshold, successThreshold int) *Machine {
	if failureThreshold < 1 {
		failureThreshold = 1
	}
	if successThreshold < 1 {
		successThreshold = 1
	}
	return &Machine{
		FailureThreshold: failureThreshold,
		SuccessThreshold: successThreshold,
		state:            model.StateUnknown,
	}
}

// State reports the debounced state.
func (m *Machine) State() model.State { return m.state }

// Samples reports how many probe results have been observed.
func (m *Machine) Samples() int { return m.samples }

// Streak reports the current consecutive success and failure counts.
func (m *Machine) Streak() (success, failure int) {
	return m.consecutiveSuccess, m.consecutiveFailure
}

// Observe folds in one sample and reports whether the state moved.
func (m *Machine) Observe(r Result) (from, to model.State, changed bool) {
	from = m.state
	m.samples++

	if r.Success {
		m.consecutiveSuccess++
		m.consecutiveFailure = 0
		if m.state != model.StateHealthy && m.consecutiveSuccess >= m.SuccessThreshold {
			m.state = model.StateHealthy
		}
	} else {
		m.consecutiveFailure++
		m.consecutiveSuccess = 0
		if m.state != model.StateUnhealthy && m.consecutiveFailure >= m.FailureThreshold {
			m.state = model.StateUnhealthy
		}
	}
	return from, m.state, from != m.state
}
