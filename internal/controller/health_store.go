package controller

import (
	"errors"
	"sync"
	"time"

	"github.com/boanlab/multus-service/internal/model"
)

// Report rejection reasons. They are values rather than strings because the
// event stream reports them and the E2E asserts on them.
var (
	// ErrStaleInstance means the report came from an agent process that a newer
	// one has superseded. A restarted agent gets a new instance id, so this is
	// how an in-flight report from the old process is discarded.
	ErrStaleInstance = errors.New("report from a superseded agent instance")
	// ErrOutOfOrder means the sequence number did not advance. A gRPC stream
	// orders its own messages, but a reconnect can leave two streams briefly
	// overlapping.
	ErrOutOfOrder = errors.New("report sequence did not advance")
)

type localEntry struct {
	Report     model.LocalHealth
	Origin     model.ReportOrigin
	AcceptedAt time.Time
	expired    bool
}

type pathEntry struct {
	Report     model.PathHealth
	Origin     model.ReportOrigin
	AcceptedAt time.Time
	expired    bool
}

// HealthStore holds the two kinds of observation separately, because they have
// different cardinality.
//
//	local  keyed by attachment ID   -- always one per attachment
//	path   keyed by scope ID        -- attachment ID under Endpoint scope,
//	                                   (node, NAD) domain ID under Node scope
//
// Keeping them apart is what lets one Node-scope probe result serve every
// attachment in its domain without being copied per Pod.
//
// Freshness is measured from AcceptedAt -- the controller's own clock at the
// moment it took the report -- never from the agent's observed_at. A node whose
// clock is wrong must not be able to make a stale report look current, nor a
// current one look stale.
type HealthStore struct {
	mu  sync.RWMutex
	ttl time.Duration

	local map[string]localEntry
	path  map[string]pathEntry

	// nodeInstance is the agent process currently authoritative for each node.
	// The most recent stream to open wins, which is what makes an agent restart
	// invalidate anything still in flight from the old process.
	nodeInstance map[string]string

	now func() time.Time
}

// NewHealthStore returns a store that treats reports older than ttl as stale.
func NewHealthStore(ttl time.Duration) *HealthStore {
	return &HealthStore{
		ttl:          ttl,
		local:        map[string]localEntry{},
		path:         map[string]pathEntry{},
		nodeInstance: map[string]string{},
		now:          time.Now,
	}
}

// TTL reports the freshness window.
func (s *HealthStore) TTL() time.Duration { return s.ttl }

// AdoptInstance makes instance the authoritative agent for node and returns the
// instance it replaced, if any.
func (s *HealthStore) AdoptInstance(node, instance string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.nodeInstance[node]
	s.nodeInstance[node] = instance
	return prev
}

func (s *HealthStore) currentLocked(o model.ReportOrigin) bool {
	cur, ok := s.nodeInstance[o.NodeName]
	return ok && cur == o.AgentInstance
}

// AcceptLocal stores one local observation.
func (s *HealthStore) AcceptLocal(o model.ReportOrigin, r model.LocalHealth) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.currentLocked(o) {
		return ErrStaleInstance
	}
	if cur, ok := s.local[r.AttachmentID]; ok &&
		cur.Origin.AgentInstance == o.AgentInstance && o.Sequence <= cur.Origin.Sequence {
		return ErrOutOfOrder
	}
	s.local[r.AttachmentID] = localEntry{Report: r, Origin: o, AcceptedAt: s.now()}
	return nil
}

// AcceptPath stores one probe result.
func (s *HealthStore) AcceptPath(o model.ReportOrigin, r model.PathHealth) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.currentLocked(o) {
		return ErrStaleInstance
	}
	if cur, ok := s.path[r.ScopeID]; ok &&
		cur.Origin.AgentInstance == o.AgentInstance && o.Sequence <= cur.Origin.Sequence {
		return ErrOutOfOrder
	}
	s.path[r.ScopeID] = pathEntry{Report: r, Origin: o, AcceptedAt: s.now()}
	return nil
}

// ApplySnapshot replaces everything held for one node in a single step.
//
// A partial apply would be visible: the controller would reconcile against the
// entries that had arrived and publish the rest as not ready until the remainder
// landed. Committing atomically is what keeps a controller restart from
// flapping readiness across a node's endpoints.
func (s *HealthStore) ApplySnapshot(o model.ReportOrigin, locals []model.LocalHealth, paths []model.PathHealth) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.currentLocked(o) {
		return ErrStaleInstance
	}
	now := s.now()

	for id, e := range s.local {
		if e.Report.NodeName == o.NodeName {
			delete(s.local, id)
		}
	}
	for id, e := range s.path {
		if e.Report.NodeName == o.NodeName {
			delete(s.path, id)
		}
	}
	for _, r := range locals {
		s.local[r.AttachmentID] = localEntry{Report: r, Origin: o, AcceptedAt: now}
	}
	for _, r := range paths {
		s.path[r.ScopeID] = pathEntry{Report: r, Origin: o, AcceptedAt: now}
	}
	return nil
}

// Local returns the observation and whether it is still fresh.
func (s *HealthStore) Local(attachmentID string) (model.LocalHealth, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.local[attachmentID]
	if !ok {
		return model.LocalHealth{}, false
	}
	return e.Report, s.now().Sub(e.AcceptedAt) <= s.ttl
}

// Path returns the probe result for a scope key and whether it is still fresh.
func (s *HealthStore) Path(scopeID string) (model.PathHealth, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.path[scopeID]
	if !ok {
		return model.PathHealth{}, false
	}
	return e.Report, s.now().Sub(e.AcceptedAt) <= s.ttl
}

// LocalState collapses a local observation into the tri-state view.
func (s *HealthStore) LocalState(attachmentID string) model.State {
	r, fresh := s.Local(attachmentID)
	if !fresh {
		return model.StateUnknown
	}
	if r.Ready() {
		return model.StateHealthy
	}
	return model.StateUnhealthy
}

// PathState collapses a probe result into the tri-state view.
func (s *HealthStore) PathState(scopeID string) model.State {
	r, fresh := s.Path(scopeID)
	if !fresh {
		return model.StateUnknown
	}
	if r.PathReady {
		return model.StateHealthy
	}
	return model.StateUnhealthy
}

// ForgetLocal drops an attachment's local state. Path state is left alone:
// under Node scope it is shared, so one departing attachment must not
// invalidate the domain.
func (s *HealthStore) ForgetLocal(attachmentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.local, attachmentID)
}

// Expired is one entry that has just crossed the freshness window.
type Expired struct {
	Kind string // "local" or "path"
	ID   string
	Node string
}

// SweepExpired reports entries that have just gone stale, once each. A stale
// entry means the agent stopped talking; that is a different event from an
// agent reporting a failure, and the evaluation needs to tell them apart.
func (s *HealthStore) SweepExpired() []Expired {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var out []Expired
	for id, e := range s.local {
		if !e.expired && now.Sub(e.AcceptedAt) > s.ttl {
			e.expired = true
			s.local[id] = e
			out = append(out, Expired{"local", id, e.Report.NodeName})
		}
	}
	for id, e := range s.path {
		if !e.expired && now.Sub(e.AcceptedAt) > s.ttl {
			e.expired = true
			s.path[id] = e
			out = append(out, Expired{"path", id, e.Report.NodeName})
		}
	}
	return out
}

// Prune removes entries stale for more than twice the TTL, so a long-lived
// controller does not accumulate dead attachments and dead domains.
func (s *HealthStore) Prune() (locals, paths int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cut := s.now().Add(-2 * s.ttl)
	for id, e := range s.local {
		if e.AcceptedAt.Before(cut) {
			delete(s.local, id)
			locals++
		}
	}
	for id, e := range s.path {
		if e.AcceptedAt.Before(cut) {
			delete(s.path, id)
			paths++
		}
	}
	return locals, paths
}

// Len reports how many local and path observations are tracked.
func (s *HealthStore) Len() (locals, paths int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.local), len(s.path)
}
