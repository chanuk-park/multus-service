// Package agent measures the network state of secondary attachments on one node.
//
// It decides nothing. Readiness also depends on Pod state and on freshness of
// the reports themselves, neither of which an agent can judge, so the verdict
// is computed centrally and the agent only reports what it saw.
package agent

import (
	"context"
	"errors"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/boanlab/multus-service/internal/agent/local"
	agentnetns "github.com/boanlab/multus-service/internal/agent/netns"
	"github.com/boanlab/multus-service/internal/attach"
	"github.com/boanlab/multus-service/internal/model"
	"github.com/boanlab/multus-service/internal/multus"
	"github.com/boanlab/multus-service/internal/obs"
)

// Agent observes every secondary attachment belonging to a managed Service
// whose Pod runs on this node.
type Agent struct {
	Client   client.Reader
	NodeName string
	Resolver agentnetns.Resolver
	Monitor  *local.Monitor
	Sink     Sink
	Events   *obs.Recorder

	// Resync bounds how long a missed netlink event can go unnoticed, and
	// doubles as the report heartbeat that keeps the controller's freshness
	// check satisfied while nothing changes.
	Resync time.Duration

	mu   sync.Mutex
	prev map[string]bool // attachment ID -> last reported local_ready
	seen map[string]bool // attachment IDs observed in the last sweep
}

// Start runs until the context is cancelled. It satisfies manager.Runnable.
func (a *Agent) Start(ctx context.Context) error {
	lg := log.FromContext(ctx).WithName("agent")
	a.prev = map[string]bool{}
	a.seen = map[string]bool{}

	a.Events.Emit("agent_started", "node", a.NodeName, "resync_ms", a.Resync.Milliseconds())
	lg.Info("starting", "node", a.NodeName, "resync", a.Resync)

	ticker := time.NewTicker(a.Resync)
	defer ticker.Stop()
	defer a.Monitor.Close()

	a.sweep(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			a.sweep(ctx)
		case <-a.Monitor.Events():
			// A namespace changed. Drain anything else already queued so one
			// sweep covers the whole burst.
			drain(a.Monitor.Events())
			a.sweep(ctx)
		}
	}
}

func drain(ch <-chan string) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// sweep rebuilds the attachment set from the API cache, resolves each Pod's
// netns, inspects the interfaces and reports.
func (a *Agent) sweep(ctx context.Context) {
	lg := log.FromContext(ctx).WithName("agent")

	atts, err := a.targets(ctx)
	if err != nil {
		lg.Error(err, "building attachment set")
		return
	}

	// One netns entry per (Pod, interface): a dual-stack attachment is two
	// Attachments sharing an interface, and re-reading it twice would be waste.
	type nsKey struct{ podUID, iface string }
	inspected := map[nsKey]local.Link{}
	handles := map[string]agentnetns.Handle{}

	live := map[string]bool{}
	now := time.Now()

	for _, at := range atts {
		uid := string(at.PodUID)

		h, ok := handles[uid]
		if !ok {
			h, err = a.Resolver.Resolve(ctx, uid)
			if err != nil {
				if !errors.Is(err, agentnetns.ErrNotFound) {
					lg.V(1).Info("resolving netns", "pod", at.PodName, "err", err.Error())
				}
				// No sandbox yet, or it just went away. Report nothing rather
				// than reporting a guess: the controller already treats a
				// missing report as not-ready.
				continue
			}
			handles[uid] = h
			if err := a.Monitor.Ensure(uid, h.Path); err != nil {
				lg.V(1).Info("subscribing to netns", "pod", at.PodName, "err", err.Error())
			}
		}

		key := nsKey{uid, at.Interface}
		link, ok := inspected[key]
		if !ok {
			link, err = local.Inspect(h.Path, at.Interface)
			if err != nil {
				lg.V(1).Info("inspecting interface", "pod", at.PodName, "iface", at.Interface, "err", err.Error())
				continue
			}
			inspected[key] = link
		}

		r := model.LocalHealth{
			AttachmentID:    at.ID(),
			PodUID:          uid,
			Namespace:       at.PodNamespace,
			PodName:         at.PodName,
			NAD:             at.NAD,
			Interface:       at.Interface,
			IP:              at.IP,
			NodeName:        a.NodeName,
			InterfaceExists: link.Exists,
			AddressPresent:  link.Has(at.IP),
			LinkUsable:      link.Exists && link.Usable,
			ObservedAt:      now,
		}
		live[r.AttachmentID] = true

		a.transition(r, h)
		if err := a.Sink.Local(ctx, r); err != nil {
			lg.V(1).Info("reporting", "attachment", r.AttachmentID, "err", err.Error())
		}
	}

	a.retire(live, handles)
}

// transition emits the anchor timestamps the evaluation measures detection
// latency from. A report every resync is enough for freshness; a transition is
// what marks the moment the fault became visible.
func (a *Agent) transition(r model.LocalHealth, h agentnetns.Handle) {
	a.mu.Lock()
	was, known := a.prev[r.AttachmentID]
	a.prev[r.AttachmentID] = r.Ready()
	a.mu.Unlock()

	if !known {
		a.Events.Emit("attachment_observed",
			"attachment_id", r.AttachmentID, "pod", r.PodName, "ip", r.IP,
			"interface", r.Interface, "netns", h.Path, "sandbox", h.SandboxID,
			"local_ready", r.Ready())
		return
	}
	if was == r.Ready() {
		return
	}
	event := "recovery_detected"
	if !r.Ready() {
		event = "failure_detected"
	}
	a.Events.Emit(event,
		"attachment_id", r.AttachmentID, "pod", r.PodName, "ip", r.IP,
		"interface", r.Interface,
		"interface_exists", r.InterfaceExists,
		"address_present", r.AddressPresent,
		"link_usable", r.LinkUsable)
}

// retire forgets attachments that vanished, so a stale entry cannot be revived
// by a late event and so the netns cache does not grow without bound.
func (a *Agent) retire(live map[string]bool, handles map[string]agentnetns.Handle) {
	a.mu.Lock()
	gone := make([]string, 0)
	for id := range a.prev {
		if !live[id] {
			gone = append(gone, id)
		}
	}
	for _, id := range gone {
		delete(a.prev, id)
	}
	a.mu.Unlock()

	for _, id := range gone {
		a.Events.Emit("attachment_retired", "attachment_id", id)
	}

	if c, ok := a.Resolver.(*agentnetns.Cache); ok {
		for _, uid := range a.Monitor.Watching() {
			if _, still := handles[uid]; !still {
				a.Monitor.Forget(uid)
				c.Forget(uid)
			}
		}
	}
}

// targets returns every attachment on this node that a managed Service claims.
func (a *Agent) targets(ctx context.Context) ([]model.Attachment, error) {
	var svcs corev1.ServiceList
	if err := a.Client.List(ctx, &svcs); err != nil {
		return nil, err
	}

	// The cache is already restricted to this node, so this lists node Pods.
	var pods corev1.PodList
	if err := a.Client.List(ctx, &pods); err != nil {
		return nil, err
	}

	var out []model.Attachment
	for i := range svcs.Items {
		svc := &svcs.Items[i]
		if !attach.Managed(svc) || attach.Validate(svc) != nil {
			continue
		}
		nad, err := attach.NAD(svc)
		if err != nil {
			continue
		}
		sel, err := attach.Selector(svc)
		if err != nil {
			continue
		}
		for j := range pods.Items {
			pod := &pods.Items[j]
			if pod.Namespace != svc.Namespace || !attach.Eligible(pod) {
				continue
			}
			if !sel.Matches(labelsOf(pod)) {
				continue
			}
			atts, err := attach.FromPod(pod, nad)
			if err != nil {
				// ErrNoAttachment and ErrAmbiguous are both handled by not
				// reporting. The controller raises the events; the agent stays
				// quiet so one misconfiguration is not reported twice per node.
				if !errors.Is(err, multus.ErrNoAttachment) && !errors.Is(err, multus.ErrAmbiguous) {
					log.FromContext(ctx).V(1).Info("deriving attachment", "pod", pod.Name, "err", err.Error())
				}
				continue
			}
			out = append(out, atts...)
		}
	}
	return out, nil
}

// SetupWithManager registers the agent as a managed runnable.
func (a *Agent) SetupWithManager(mgr ctrl.Manager) error {
	return mgr.Add(a)
}
