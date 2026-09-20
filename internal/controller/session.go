package controller

import (
	"context"
	"sync"
	"sync/atomic"
)

// SessionManager cancels the streams belonging to an agent when that agent's
// authority is withdrawn.
//
// TokenReview alone does not close an already-open stream: a Pod-bound token
// stops authenticating the moment its Pod is deleted, but a bidirectional
// stream authenticated earlier keeps running. Binding each stream to its agent
// Pod UID, and cancelling on deregistration, is what makes the Kubernetes
// token-lifecycle guarantee actually govern the live connection.
type SessionManager struct {
	mu       sync.Mutex
	streams  map[string]map[uint64]context.CancelFunc // podUID -> streamID -> cancel
	streamID atomic.Uint64
}

// NewSessionManager returns an empty manager.
func NewSessionManager() *SessionManager {
	return &SessionManager{streams: map[string]map[uint64]context.CancelFunc{}}
}

// Register derives a cancellable context bound to the agent's Pod UID and
// returns it with a release function to call when the stream ends.
func (m *SessionManager) Register(parent context.Context, podUID string) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	id := m.streamID.Add(1)

	m.mu.Lock()
	if m.streams[podUID] == nil {
		m.streams[podUID] = map[uint64]context.CancelFunc{}
	}
	m.streams[podUID][id] = cancel
	m.mu.Unlock()

	return ctx, func() {
		m.mu.Lock()
		if s, ok := m.streams[podUID]; ok {
			delete(s, id)
			if len(s) == 0 {
				delete(m.streams, podUID)
			}
		}
		m.mu.Unlock()
		cancel()
	}
}

// Revoke cancels every stream currently bound to the Pod UID.
func (m *SessionManager) Revoke(podUID string) int {
	m.mu.Lock()
	cancels := make([]context.CancelFunc, 0)
	if s, ok := m.streams[podUID]; ok {
		for _, c := range s {
			cancels = append(cancels, c)
		}
		delete(m.streams, podUID)
	}
	m.mu.Unlock()
	for _, c := range cancels {
		c()
	}
	return len(cancels)
}

// Active reports how many Pod UIDs have at least one live stream.
func (m *SessionManager) Active() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.streams)
}
