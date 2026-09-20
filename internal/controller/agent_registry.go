package controller

import (
	"context"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/boanlab/multus-service/internal/obs"
)

// AgentInfo is what the controller knows about one running agent Pod.
type AgentInfo struct {
	PodUID         string
	PodName        string
	Namespace      string
	ServiceAccount string
	NodeName       string
}

// AgentRegistry tracks the currently-running agent Pods.
//
// It exists because "holds a valid Pod-bound token" is not the same as "is a
// node agent": an ordinary Pod on the same ServiceAccount would authenticate
// identically. Membership here is the second half of producer authorization,
// and it also supplies the authoritative node -- Pod.spec.nodeName keyed by the
// validated Pod UID.
//
// Removal drives session revocation: when an agent Pod disappears, any stream
// still open under its identity is cancelled, so a deleted agent's evidence
// stops flowing even if the process (or an attacker holding its old token)
// keeps the connection open.
type AgentRegistry struct {
	mu      sync.RWMutex
	byUID   map[string]AgentInfo
	byKey   map[types.NamespacedName]string // pod nsName -> current uid
	onRemLk sync.RWMutex
	onRem   func(podUID string)
}

// NewAgentRegistry returns an empty registry.
func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{
		byUID: map[string]AgentInfo{},
		byKey: map[types.NamespacedName]string{},
	}
}

// OnRemove registers the hook fired when an agent Pod leaves the registry.
func (r *AgentRegistry) OnRemove(f func(podUID string)) {
	r.onRemLk.Lock()
	defer r.onRemLk.Unlock()
	r.onRem = f
}

// ByUID looks up an agent by its Pod UID.
func (r *AgentRegistry) ByUID(uid string) (AgentInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.byUID[uid]
	return info, ok
}

// Len reports how many agents are tracked.
func (r *AgentRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byUID)
}

func (r *AgentRegistry) upsert(key types.NamespacedName, info AgentInfo) (replaced string) {
	r.mu.Lock()
	prev, had := r.byKey[key]
	if had && prev != info.PodUID {
		delete(r.byUID, prev)
		replaced = prev
	}
	r.byKey[key] = info.PodUID
	r.byUID[info.PodUID] = info
	r.mu.Unlock()
	return replaced
}

func (r *AgentRegistry) remove(key types.NamespacedName) (removed string) {
	r.mu.Lock()
	uid, ok := r.byKey[key]
	if ok {
		delete(r.byKey, key)
		delete(r.byUID, uid)
		removed = uid
	}
	r.mu.Unlock()
	return removed
}

func (r *AgentRegistry) fireRemove(uid string) {
	if uid == "" {
		return
	}
	r.onRemLk.RLock()
	f := r.onRem
	r.onRemLk.RUnlock()
	if f != nil {
		f(uid)
	}
}

// AgentPodReconciler keeps the AgentRegistry in step with the agent Pods.
type AgentPodReconciler struct {
	client.Client
	Registry *AgentRegistry
	Events   *obs.Recorder
	Label    string // pod label identifying agents, key=value
	LabelKey string
	LabelVal string
	AgentSA  string
	AgentsNS string
}

// Reconcile upserts or removes one agent Pod.
func (r *AgentPodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		// Gone (or being deleted): drop it and revoke its session.
		if uid := r.Registry.remove(req.NamespacedName); uid != "" {
			r.Events.Emit("agent_deregistered", "pod", req.Name, "namespace", req.Namespace, "pod_uid", uid)
			r.Registry.fireRemove(uid)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// A terminating Pod is no longer a valid reporter, even before the object
	// disappears, so treat it as removed.
	if pod.DeletionTimestamp != nil {
		if uid := r.Registry.remove(req.NamespacedName); uid != "" {
			r.Events.Emit("agent_deregistered", "pod", pod.Name, "namespace", pod.Namespace, "pod_uid", uid)
			r.Registry.fireRemove(uid)
		}
		return ctrl.Result{}, nil
	}

	info := AgentInfo{
		PodUID:         string(pod.UID),
		PodName:        pod.Name,
		Namespace:      pod.Namespace,
		ServiceAccount: pod.Spec.ServiceAccountName,
		NodeName:       pod.Spec.NodeName,
	}
	// nodeName is empty until scheduled; without it there is no authority to
	// grant, so wait for a later event.
	if info.NodeName == "" {
		return ctrl.Result{}, nil
	}
	if replaced := r.Registry.upsert(req.NamespacedName, info); replaced != "" {
		// The Pod was recreated with a new UID; the old identity must lose its
		// session so a stream held open under it cannot outlive the Pod.
		r.Events.Emit("agent_replaced", "pod", pod.Name, "old_uid", replaced, "new_uid", info.PodUID)
		r.Registry.fireRemove(replaced)
	}
	r.Events.Emit("agent_registered",
		"pod", pod.Name, "namespace", pod.Namespace, "pod_uid", info.PodUID, "node", info.NodeName)
	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler, filtered to agent Pods.
func (r *AgentPodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	agentPods := predicate.NewPredicateFuncs(func(o client.Object) bool {
		return o.GetLabels()[r.LabelKey] == r.LabelVal
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("agent-registry").
		For(&corev1.Pod{}, builder.WithPredicates(agentPods)).
		Complete(r)
}
