package model

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// ProbeScope says how a path result was obtained. It has to survive into the
// data: a Node-scope result rests on the assumption that the host path
// represents the Pod path, which is false for macvlan and ipvlan toward
// same-node endpoints.
type ProbeScope string

const (
	ScopeEndpoint ProbeScope = "Endpoint"
	ScopeNode     ProbeScope = "Node"
)

// State is the controller's view of one half of an attachment's health.
//
// EndpointSlice can only express ready true/false, but the controller needs a
// third value. A dead agent and a failing probe both drive ready=false, yet
// they are different events and the evaluation must tell them apart.
type State string

const (
	StateHealthy   State = "Healthy"
	StateUnhealthy State = "Unhealthy"
	StateUnknown   State = "Unknown"
)

// PathDomainID identifies a Node-scope probe domain: one shared probe, one
// shared hysteresis, for every attachment of one NAD on one node.
func PathDomainID(nodeName, nad string) string {
	h := sha256.New()
	h.Write([]byte(nodeName))
	h.Write([]byte{0})
	h.Write([]byte(nad))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// LocalHealth is one attachment's interface state, observed from inside its
// Pod netns. Cardinality is always one per attachment.
type LocalHealth struct {
	AttachmentID string
	PodUID       string
	Namespace    string
	PodName      string
	NAD          string
	Interface    string
	IP           string
	NodeName     string

	// The three checks are kept separate because they fail differently. Taking
	// a link down leaves the IPv4 address in place, so AddressPresent alone
	// reports a dead interface as healthy.
	InterfaceExists bool
	AddressPresent  bool
	LinkUsable      bool

	ObservedAt time.Time
}

// Ready is the agent's verdict for this attachment's local state.
func (l LocalHealth) Ready() bool {
	return l.InterfaceExists && l.AddressPresent && l.LinkUsable
}

// PathHealth is an active probe result. Cardinality depends on scope: one per
// attachment under Endpoint scope, one per (node, NAD) under Node scope.
type PathHealth struct {
	Scope ProbeScope
	// ScopeID is an attachment ID under Endpoint scope and a PathDomainID under
	// Node scope. The controller joins on it, so a Node-scope result is stored
	// once and read by every attachment in the domain.
	ScopeID   string
	NodeName  string
	NAD       string
	Target    string
	PathReady bool

	ObservedAt time.Time
}

// PathKey returns the key an attachment reads its path state under.
func (a Attachment) PathKey(scope ProbeScope) string {
	if scope == ScopeNode {
		return PathDomainID(a.NodeName, a.NAD)
	}
	return a.ID()
}

// ReportOrigin identifies which agent process produced a report and where it
// sat in that process's stream.
//
// Ordering and freshness deliberately avoid comparing a node's clock with the
// controller's. A restarted agent gets a new instance id, and sequence numbers
// are monotonic only within one instance, so the pair orders reports without a
// shared clock. ObservedAt stays on the report for measurement only.
type ReportOrigin struct {
	NodeName      string
	AgentInstance string
	Sequence      uint64
}
