package controller

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"

	"github.com/boanlab/multus-service/internal/model"
)

// Registry is the controller's record of what exists, and the reason an agent
// report cannot invent anything.
//
// The attachment set is derived from Service + Pod + network-status, here, by
// the reconciler. An agent that reports an attachment id the registry has never
// heard of is rejected rather than believed: health reports change the state of
// something known, they are not a discovery channel. The same holds for
// Node-scope path domains, which only exist because some attachment placed one
// there.
type Registry struct {
	mu sync.RWMutex

	byService map[types.NamespacedName]serviceEntry

	// derived
	attachments map[string]model.Attachment
	attOwners   map[string][]types.NamespacedName
	pathOwners  map[string][]types.NamespacedName
	pathNodes   map[string]map[string]bool // path key -> set of nodes it covers
}

type serviceEntry struct {
	atts  []model.Attachment
	scope model.ProbeScope
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		byService:   map[types.NamespacedName]serviceEntry{},
		attachments: map[string]model.Attachment{},
		attOwners:   map[string][]types.NamespacedName{},
		pathOwners:  map[string][]types.NamespacedName{},
		pathNodes:   map[string]map[string]bool{},
	}
}

// SetService replaces everything the registry holds for one Service.
func (r *Registry) SetService(key types.NamespacedName, atts []model.Attachment, scope model.ProbeScope) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byService[key] = serviceEntry{atts: atts, scope: scope}
	r.rebuildLocked()
}

// RemoveService drops a Service, e.g. once it is deleted or stops being managed.
func (r *Registry) RemoveService(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byService, key)
	r.rebuildLocked()
}

func (r *Registry) rebuildLocked() {
	atts := make(map[string]model.Attachment)
	attOwners := make(map[string][]types.NamespacedName)
	pathOwners := make(map[string][]types.NamespacedName)
	pathNodes := make(map[string]map[string]bool)

	for key, e := range r.byService {
		for _, a := range e.atts {
			id := a.ID()
			atts[id] = a
			attOwners[id] = appendUnique(attOwners[id], key)
			pk := a.PathKey(e.scope)
			pathOwners[pk] = appendUnique(pathOwners[pk], key)
			if pathNodes[pk] == nil {
				pathNodes[pk] = map[string]bool{}
			}
			pathNodes[pk][a.NodeName] = true
		}
	}
	r.attachments, r.attOwners, r.pathOwners, r.pathNodes = atts, attOwners, pathOwners, pathNodes
}

func appendUnique(list []types.NamespacedName, key types.NamespacedName) []types.NamespacedName {
	for _, k := range list {
		if k == key {
			return list
		}
	}
	return append(list, key)
}

// Attachment returns the attachment with this id and the Services that publish
// it. The bool is false when nothing in the cluster claims that id.
func (r *Registry) Attachment(id string) (model.Attachment, []types.NamespacedName, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.attachments[id]
	if !ok {
		return model.Attachment{}, nil, false
	}
	return a, append([]types.NamespacedName(nil), r.attOwners[id]...), true
}

// PathKey reports whether any attachment currently reads path state under this
// key, and which Services are affected when it changes.
func (r *Registry) PathKey(key string) ([]types.NamespacedName, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	owners, ok := r.pathOwners[key]
	if !ok {
		return nil, false
	}
	return append([]types.NamespacedName(nil), owners...), true
}

// PathKeyOnNode reports whether the given node is the sole node behind a path
// key -- i.e. whether that node is authorized to report path evidence for it.
//
// An Endpoint-scope key covers exactly one attachment, so exactly one node. A
// Node-scope key is (node, NAD) by construction, so also one node. A key that
// somehow spans nodes authorizes none of them, which is the safe answer.
func (r *Registry) PathKeyOnNode(pathKey, node string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := r.pathNodes[pathKey]
	return len(nodes) == 1 && nodes[node]
}

// Len reports how many distinct attachments and path keys are known.
func (r *Registry) Len() (attachments, pathKeys int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.attachments), len(r.pathOwners)
}
