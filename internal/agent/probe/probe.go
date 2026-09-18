// Package probe measures reachability from an endpoint's secondary network
// context to a configured health target.
//
// What a path probe means is deliberately narrow:
//
//	P(e) = reachability from the endpoint's secondary source
//	       to the health target declared on its NAD
//
// It does not mean every client can reach the endpoint. A partial partition, or
// connectivity that differs per client, is outside what one probe can say. The
// value of the measurement comes from where it is sourced -- inside the Pod
// netns, bound to the secondary address -- not from the target being special.
//
// The probe never decides readiness. It produces samples; a separate state
// machine applies hysteresis; only then does a PathHealth exist. Keeping those
// apart is what lets Node scope share one state machine across a whole
// (node, NAD) domain instead of running one per attachment.
package probe

import (
	"context"
	"time"

	"github.com/boanlab/multus-service/internal/model"
)

// ErrorKind classifies a failed sample. It is carried into the event stream so
// a campaign can tell a blackhole apart from an unreachable next hop.
type ErrorKind string

const (
	ErrNone        ErrorKind = ""
	ErrTimeout     ErrorKind = "timeout"
	ErrUnreachable ErrorKind = "unreachable"
	ErrSocket      ErrorKind = "socket"
	ErrSend        ErrorKind = "send"
)

// Result is one probe sample. It deliberately carries no notion of readiness.
type Result struct {
	Success    bool
	RTT        time.Duration
	ErrorKind  ErrorKind
	Detail     string
	ObservedAt time.Time
}

// Spec says what to probe and from where.
//
// Source and Interface are both bound explicitly. Entering the Pod netns and
// relying on its routing table to pick the secondary interface would usually
// work, and "usually" is not a correctness argument: a default route, a second
// attachment, or a policy rule could send the probe out of a different
// interface and the result would still look like a healthy secondary path.
type Spec struct {
	// ScopeKey is the attachment ID under Endpoint scope and the (node, NAD)
	// domain ID under Node scope.
	ScopeKey string
	Scope    model.ProbeScope

	NAD          string
	AttachmentID string
	PodName      string

	// NetnsPath is empty for a Node-scope probe, which runs in the host netns.
	NetnsPath string
	Interface string
	SourceIP  string
	Target    string

	Timeout time.Duration
}

// Prober takes one sample.
type Prober interface {
	Probe(ctx context.Context, s Spec) Result
	// Retain closes any cached resources whose key is no longer wanted.
	Retain(keys map[string]bool)
	Close()
}
