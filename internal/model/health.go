package model

import "time"

// HealthState is the controller's internal view of an attachment.
//
// EndpointSlice can only express ready true/false, but the controller needs a
// third value: "no fresh report". A dead agent and a failing path both end up
// as ready=false, yet they are different events and are recorded as such.
type HealthState string

const (
	HealthHealthy   HealthState = "Healthy"
	HealthUnhealthy HealthState = "Unhealthy"
	HealthUnknown   HealthState = "Unknown"
)

// Report is one health observation from a node agent.
type Report struct {
	AttachmentID string
	PodUID       string
	Namespace    string
	PodName      string
	NAD          string
	Interface    string
	IP           string

	// LocalReady is observed from inside the Pod netns: the interface exists,
	// carries the expected address, and the link is usable. All three are
	// needed -- bringing a link down leaves the IPv4 address in place, so
	// address state alone reports a dead interface as healthy.
	LocalReady bool

	// PathReady is the result of an active probe. netlink cannot supply it:
	// an underlay blackhole changes no local kernel state at all.
	PathReady bool

	ObservedAt time.Time
}
