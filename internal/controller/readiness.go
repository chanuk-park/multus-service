package controller

import (
	"github.com/boanlab/multus-service/internal/model"
)

// Readiness is the published ready value plus why. The reason is what makes the
// event stream diagnosable after the fact, and it keeps the two halves of the
// health state visible rather than collapsed into one boolean.
type Readiness struct {
	AttachmentID string

	Ready  bool
	Reason string

	Local model.State
	Path  model.State
}

// ComputeReady decides whether one attachment may appear in DNS.
//
//	Ready = PodReady
//	     && LocalFresh && LocalReady
//	     && PathFresh  && PathReady
//
// The five terms cover five different blind spots, and none implies another:
// PodReady covers the workload but rides the primary interface; LocalReady
// covers the secondary interface but only its local kernel state; PathReady
// covers reachability, which no local state reflects; and the two freshness
// terms cover the agent itself, so a dead agent is not read as healthy.
//
// Path state is looked up under the scope's key. Under Node scope that is the
// (node, NAD) domain, so one shared probe result serves every attachment in the
// domain -- read here rather than copied into each report.
func ComputeReady(a model.Attachment, scope model.ProbeScope, hs *HealthStore) Readiness {
	pathKey := a.PathKey(scope)
	r := Readiness{
		AttachmentID: a.ID(),
		Local:        hs.LocalState(a.ID()),
		Path:         hs.PathState(pathKey),
	}

	if !a.PodReady {
		r.Reason = "PodNotReady"
		return r
	}

	local, fresh := hs.Local(a.ID())
	if !fresh {
		if local.AttachmentID == "" {
			r.Reason = "NoLocalReport"
		} else {
			r.Reason = "LocalReportStale"
		}
		return r
	}
	if !local.Ready() {
		switch {
		case !local.InterfaceExists:
			r.Reason = "InterfaceMissing"
		case !local.LinkUsable:
			r.Reason = "LinkDown"
		default:
			r.Reason = "AddressMissing"
		}
		return r
	}

	path, fresh := hs.Path(pathKey)
	if !fresh {
		if path.ScopeID == "" {
			r.Reason = "NoPathReport"
		} else {
			r.Reason = "PathReportStale"
		}
		return r
	}
	if !path.PathReady {
		r.Reason = "PathNotReady"
		return r
	}

	r.Ready = true
	r.Reason = "Healthy"
	return r
}
