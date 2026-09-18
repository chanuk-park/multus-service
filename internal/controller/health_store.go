package controller

import (
	"sync"
	"time"

	"github.com/boanlab/multus-service/internal/model"
)

// HealthStore keeps the most recent report per attachment and ages them out.
//
// Freshness is the mechanism that separates "the path is down" from "the agent
// is gone". Both drive the endpoint to ready=false, but only the first is a
// network failure, and the evaluation needs to tell them apart.
type HealthStore struct {
	mu  sync.RWMutex
	ttl time.Duration
	m   map[string]model.Report

	now func() time.Time // injectable for tests
}

// NewHealthStore returns a store that treats reports older than ttl as stale.
func NewHealthStore(ttl time.Duration) *HealthStore {
	return &HealthStore{
		ttl: ttl,
		m:   make(map[string]model.Report),
		now: time.Now,
	}
}

// Put stores a report, ignoring one that is older than what is already held.
func (s *HealthStore) Put(r model.Report) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.m[r.AttachmentID]; ok && cur.ObservedAt.After(r.ObservedAt) {
		return
	}
	s.m[r.AttachmentID] = r
}

// Get returns the report and whether it is still fresh.
func (s *HealthStore) Get(id string) (model.Report, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.m[id]
	if !ok {
		return model.Report{}, false
	}
	return r, s.now().Sub(r.ObservedAt) <= s.ttl
}

// State collapses a report into the controller's tri-state view.
func (s *HealthStore) State(id string) model.HealthState {
	r, fresh := s.Get(id)
	if !fresh {
		return model.HealthUnknown
	}
	if r.LocalReady && r.PathReady {
		return model.HealthHealthy
	}
	return model.HealthUnhealthy
}

// Forget drops an attachment, e.g. once its Pod is gone.
func (s *HealthStore) Forget(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, id)
}

// Prune removes reports that have been stale for more than twice the TTL, so a
// long-lived controller does not accumulate dead attachments.
func (s *HealthStore) Prune() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	cut := s.now().Add(-2 * s.ttl)
	n := 0
	for id, r := range s.m {
		if r.ObservedAt.Before(cut) {
			delete(s.m, id)
			n++
		}
	}
	return n
}

// Len reports how many attachments are currently tracked.
func (s *HealthStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}
