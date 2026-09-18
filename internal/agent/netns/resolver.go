// Package netns resolves a Pod UID to the network namespace of its sandbox.
//
// The resolution deliberately never involves an IP address. host-local IPAM
// allocates per node, so one NAD subnet shared across nodes hands the same
// address to Pods on different nodes -- observed on the bench cluster, not
// hypothesised. A PodUID -> IP -> netns lookup would therefore be ambiguous
// exactly when it matters most.
//
// The path taken instead is:
//
//	Pod UID -> CRI PodSandbox -> sandbox netns -> network-status.interface
//
// The Resolver interface exists so the runtime-specific part stays one file
// deep and the observation logic above it never learns about containerd.
package netns

import (
	"context"
	"errors"
)

// ErrNotFound means no ready sandbox exists for the Pod right now. It is an
// ordinary transient state during startup and teardown, not a failure.
var ErrNotFound = errors.New("no sandbox for pod")

// Handle locates one sandbox's network namespace.
type Handle struct {
	PodUID    string
	SandboxID string
	// Path is the netns file to enter, e.g. /var/run/netns/cni-<uuid> or
	// /proc/<pid>/ns/net.
	Path string
	// PID is the sandbox process, kept so a caller can fall back or diagnose.
	PID int
}

// Resolver maps a Pod UID to its sandbox network namespace.
type Resolver interface {
	Resolve(ctx context.Context, podUID string) (Handle, error)
}
