package controller

import (
	"sync"
	"time"

	"github.com/boanlab/multus-service/internal/model"
)

// HealthStore holds the two kinds of observation separately, because they have
// different cardinality.
//
//	local  keyed by attachment ID          -- always one per attachment
//	path   keyed by scope ID               -- attachment ID under Endpoint scope,
//	                                          (node, NAD) domain ID under Node scope
//
// Keeping them apart is what lets one Node-scope probe result serve every
// attachment in its domain without being copied per Pod.
type HealthStore struct {
	mu    sync.RWMutex
	ttl   time.Duration
	local map[string]model.LocalHealth
	path  map[string]model.PathHealth

	now func() time.Time // injectable for tests
}

// NewHealthStore returns a store that treats reports older than ttl as stale.
func NewHealthStore(ttl time.Duration) *HealthStore {
	return &HealthStore{
		ttl:   ttl,
		local: make(map[string]model.LocalHealth),
		path:  make(map[string]model.PathHealth),
		now:   time.Now,
	}
}

// PutLocal stores a local observation, ignoring one older than what is held.
func (s *HealthStore) PutLocal(r model.LocalHealth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.local[r.AttachmentID]; ok && cur.ObservedAt.After(r.ObservedAt) {
		return
	}
	s.local[r.AttachmentID] = r
}

// PutPath stores a path result, ignoring one older than what is held.
func (s *HealthStore) PutPath(r model.PathHealth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.path[r.ScopeID]; ok && cur.ObservedAt.After(r.ObservedAt) {
		return
	}
	s.path[r.ScopeID] = r
}

// Local returns the observation and whether it is still fresh.
func (s *HealthStore) Local(attachmentID string) (model.LocalHealth, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.local[attachmentID]
	if !ok {
		return model.LocalHealth{}, false
	}
	return r, s.now().Sub(r.ObservedAt) <= s.ttl
}

// Path returns the probe result for a scope key and whether it is still fresh.
func (s *HealthStore) Path(scopeID string) (model.PathHealth, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.path[scopeID]
	if !ok {
		return model.PathHealth{}, false
	}
	return r, s.now().Sub(r.ObservedAt) <= s.ttl
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

// PathState collapses a path result into the tri-state view.
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

// ForgetLocal drops an attachment's local state, e.g. once its Pod is gone.
// Path state is left alone: under Node scope it is shared, so one departing
// attachment must not invalidate the domain.
func (s *HealthStore) ForgetLocal(attachmentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.local, attachmentID)
}

// Prune removes observations stale for more than twice the TTL, so a long-lived
// controller does not accumulate dead attachments and dead domains.
func (s *HealthStore) Prune() (locals, paths int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cut := s.now().Add(-2 * s.ttl)
	for id, r := range s.local {
		if r.ObservedAt.Before(cut) {
			delete(s.local, id)
			locals++
		}
	}
	for id, r := range s.path {
		if r.ObservedAt.Before(cut) {
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
