package agent

import (
	"context"

	"github.com/boanlab/multus-service/internal/model"
)

// Snapshot is one sweep's worth of observations: the node's complete current
// state, plus which attachments just changed.
//
// Both halves matter. The full set is what makes health a renewable lease
// rather than an event log -- an agent that only spoke on change would go
// silent after a controller restart, leaving healthy endpoints at Unknown
// forever. The changed set is what lets a real failure be sent immediately
// instead of waiting for the next refresh.
type Snapshot struct {
	Locals  []model.LocalHealth
	Paths   []model.PathHealth
	Changed map[string]bool
	Retired []string
}

// Sink delivers observations to the controller.
type Sink interface {
	Publish(ctx context.Context, s Snapshot) error
}

// NopSink discards everything. Used when no controller address is configured,
// which keeps the agent runnable for local observation experiments.
type NopSink struct{}

// Publish does nothing.
func (NopSink) Publish(context.Context, Snapshot) error { return nil }
