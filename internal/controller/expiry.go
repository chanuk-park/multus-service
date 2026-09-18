package controller

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/boanlab/multus-service/internal/obs"
)

// ExpirySweeper turns the passage of time into an event.
//
// An entry going stale means the agent stopped talking, which drives the
// endpoint to not-ready exactly like a reported failure does -- but it is a
// different event, and nothing else would notice it: no report arrives to
// trigger a reconcile, so without this the endpoint would keep its last
// published value until some unrelated change came along.
type ExpirySweeper struct {
	Store    *HealthStore
	Registry *Registry
	Events   *obs.Recorder
	Notify   func(types.NamespacedName)
	Interval time.Duration
}

// Start runs until ctx is cancelled. It satisfies manager.Runnable.
func (e *ExpirySweeper) Start(ctx context.Context) error {
	interval := e.Interval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	pruneEvery := int(30 * time.Second / interval)
	if pruneEvery < 1 {
		pruneEvery = 1
	}
	n := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for _, x := range e.Store.SweepExpired() {
				e.Events.Emit("health_expired",
					"kind", x.Kind, "id", x.ID, "node", x.Node,
					"ttl_ms", e.Store.TTL().Milliseconds())
				var owners []types.NamespacedName
				if x.Kind == "local" {
					if _, o, ok := e.Registry.Attachment(x.ID); ok {
						owners = o
					}
				} else if o, ok := e.Registry.PathKey(x.ID); ok {
					owners = o
				}
				for _, o := range owners {
					e.Notify(o)
				}
			}
			if n++; n >= pruneEvery {
				n = 0
				if l, p := e.Store.Prune(); l+p > 0 {
					log.FromContext(ctx).V(1).Info("pruned dead health entries", "local", l, "path", p)
				}
			}
		}
	}
}
