package controller

import (
	"github.com/boanlab/multus-service/internal/model"
)

// Readiness is the published ready value plus the reason behind it. The reason
// is what makes the event stream diagnosable after the fact.
type Readiness struct {
	Ready  bool
	Reason string
	Health model.HealthState
}

// ComputeReady decides whether one attachment may appear in DNS.
//
//	Ready = PodReady AND LocalReady AND PathReady AND report is fresh
//
// Every term is required, and each covers a blind spot the others do not:
// PodReady covers the workload, LocalReady covers the interface inside the Pod
// netns, PathReady covers reachability, and freshness covers the agent itself.
//
// Until a node agent exists, no report is ever fresh, so every endpoint stays
// ready=false. That is the correct Phase 1 behaviour, not a placeholder: an
// address nobody has checked must not be advertised.
func ComputeReady(a model.Attachment, hs *HealthStore) Readiness {
	state := hs.State(a.ID())

	if !a.PodReady {
		return Readiness{Ready: false, Reason: "PodNotReady", Health: state}
	}

	rep, fresh := hs.Get(a.ID())
	if !fresh {
		if rep.AttachmentID == "" {
			return Readiness{Ready: false, Reason: "NoHealthReport", Health: state}
		}
		return Readiness{Ready: false, Reason: "HealthReportStale", Health: state}
	}
	if !rep.LocalReady {
		return Readiness{Ready: false, Reason: "LocalNotReady", Health: state}
	}
	if !rep.PathReady {
		return Readiness{Ready: false, Reason: "PathNotReady", Health: state}
	}
	return Readiness{Ready: true, Reason: "Healthy", Health: state}
}
