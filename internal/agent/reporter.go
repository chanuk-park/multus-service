package agent

import (
	"context"

	"github.com/boanlab/multus-service/internal/model"
	"github.com/boanlab/multus-service/internal/obs"
)

// Sink receives observations. Phase 2 writes them to the measurement stream;
// Phase 3 swaps in the gRPC client without touching the observation loop.
type Sink interface {
	Local(ctx context.Context, r model.LocalHealth) error
}

// LogSink writes reports to the JSONL event stream.
type LogSink struct{ Events *obs.Recorder }

// Local emits one observation.
func (s LogSink) Local(_ context.Context, r model.LocalHealth) error {
	s.Events.Emit("local_health",
		"attachment_id", r.AttachmentID,
		"pod_uid", r.PodUID,
		"namespace", r.Namespace,
		"pod", r.PodName,
		"nad", r.NAD,
		"interface", r.Interface,
		"ip", r.IP,
		"node", r.NodeName,
		"local_ready", r.Ready(),
		"interface_exists", r.InterfaceExists,
		"address_present", r.AddressPresent,
		"link_usable", r.LinkUsable,
		"observed_at", r.ObservedAt.UnixNano())
	return nil
}
