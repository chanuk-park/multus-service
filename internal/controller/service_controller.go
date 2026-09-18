// Package controller owns the Kubernetes objects: it discovers secondary
// addresses from Pod annotations and publishes them as EndpointSlices.
//
// It deliberately does not measure anything. Network state comes from the node
// agents; this half only decides what the API should say.
package controller

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/boanlab/multus-service/internal/attach"
	"github.com/boanlab/multus-service/internal/model"
	"github.com/boanlab/multus-service/internal/multus"
	"github.com/boanlab/multus-service/internal/obs"
)

// ServiceReconciler publishes secondary addresses for annotated Services.
type ServiceReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Health   *HealthStore
	Events   *obs.Recorder

	// Registry is the authority an agent report is checked against. The
	// reconciler is the only writer: attachments come from Service + Pod +
	// network-status, never from what an agent claims to see.
	Registry *Registry

	// HealthEvents re-enqueues a Service when its health input changed.
	// Readiness would otherwise only move on the next Service or Pod event.
	HealthEvents chan event.TypedGenericEvent[*corev1.Service]
}

// Reconcile brings the owned EndpointSlices in line with the Pods currently
// matching the Service's workload selector.
func (r *ServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	var svc corev1.Service
	if err := r.Get(ctx, req.NamespacedName, &svc); err != nil {
		if apierrors.IsNotFound(err) {
			// Owned slices carry an ownerReference to the Service, so deletion
			// is handled by garbage collection. The registry is not, so a
			// deleted Service must stop vouching for its attachments.
			r.Registry.RemoveService(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !attach.Managed(&svc) {
		r.Registry.RemoveService(req.NamespacedName)
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "annotation removed")
	}

	// Probe configuration belongs to the NAD. Honouring it here would break the
	// assumption a shared Node-scope path domain rests on: two Services on one
	// NAD could then name different health targets while sharing one path state.
	if stray := attach.ProbeConfigOnService(&svc); len(stray) > 0 {
		r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "ProbeConfigOnService",
			"%v belong on the NetworkAttachmentDefinition, not on a Service; ignoring them. "+
				"Probe settings are per-network because a Node-scope path domain is shared by every "+
				"attachment of one NAD on one node", stray)
	}

	if err := validateService(&svc); err != nil {
		r.Recorder.Event(&svc, corev1.EventTypeWarning, "InvalidService", err.Error())
		r.Events.Emit("service_invalid",
			"namespace", svc.Namespace, "service", svc.Name, "error", err.Error())
		// Refuse to publish anything for a Service that breaks the invariants.
		r.Registry.RemoveService(req.NamespacedName)
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "invalid service")
	}

	nad, err := attach.NAD(&svc)
	if err != nil {
		r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "InvalidNetwork",
			"annotation %s: %v", AnnotationNetwork, err)
		r.Registry.RemoveService(req.NamespacedName)
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "invalid network annotation")
	}

	sel, err := attach.Selector(&svc)
	if err != nil {
		r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "InvalidSelector", "%v", err)
		r.Registry.RemoveService(req.NamespacedName)
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "unusable workload selector")
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(svc.Namespace),
		client.MatchingLabelsSelector{Selector: sel},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list pods: %w", err)
	}

	atts := make([]model.Attachment, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !attach.Eligible(pod) {
			continue
		}
		found, err := attach.FromPod(pod, nad)
		if err != nil {
			switch {
			case errors.Is(err, multus.ErrNoAttachment):
				// Normal while CNI ADD has not completed, and normal for a Pod
				// that simply does not carry this network.
			case errors.Is(err, multus.ErrAmbiguous):
				r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "AmbiguousAttachment",
					"pod %s attaches %s more than once (%v); skipping it rather than guessing an interface",
					pod.Name, nad, err)
				r.Events.Emit("attachment_ambiguous",
					"namespace", svc.Namespace, "service", svc.Name,
					"pod", pod.Name, "nad", nad, "error", err.Error())
			default:
				r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "BadNetworkStatus",
					"pod %s: %v", pod.Name, err)
				lg.Error(err, "deriving attachment", "pod", pod.Name)
			}
			continue
		}
		for _, a := range found {
			atts = append(atts, a)
			r.Events.Emit("attachment_discovered",
				"namespace", svc.Namespace, "service", svc.Name,
				"pod", pod.Name, "pod_uid", string(pod.UID),
				"nad", nad, "interface", a.Interface, "ip", a.IP,
				"attachment_id", a.ID(), "pod_ready", a.PodReady)
		}
	}

	// Probe scope is Endpoint for now. Phase 6 resolves it per NAD via
	// attach.ParseProbeScope; until a Node-scope probe exists there is nothing
	// for a Node key to read, and Endpoint scope is the one that holds without
	// a topology assumption.
	scope := model.ScopeEndpoint

	// Register before publishing: an agent may report the moment a slice
	// appears, and a report for an attachment the registry has not yet seen
	// would be rejected.
	r.Registry.SetService(req.NamespacedName, atts, scope)

	want, readiness, warnings := desiredSlices(&svc, nad, scope, atts, r.Health)
	for _, w := range warnings {
		r.Recorder.Event(&svc, corev1.EventTypeWarning, w.Reason, w.Message)
		r.Events.Emit("attachment_warning",
			"namespace", svc.Namespace, "service", svc.Name,
			"reason", w.Reason, "message", w.Message)
	}

	ops, err := r.applySlices(ctx, &svc, want)
	for _, op := range ops {
		if op.Op == "nochange" {
			continue
		}
		r.Events.Emit("slice_patched",
			"namespace", svc.Namespace, "service", svc.Name,
			"slice", op.Name, "op", op.Op,
			"endpoints", op.Total, "ready", op.ReadyCount)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	readyCount := 0
	for _, rd := range readiness {
		if rd.Ready {
			readyCount++
		}
	}
	lg.V(1).Info("reconciled", "service", svc.Name, "nad", nad,
		"attachments", len(atts), "ready", readyCount)

	return ctrl.Result{}, nil
}

// validateService enforces the invariants; see attach.Validate for the reasons.
func validateService(svc *corev1.Service) error { return attach.Validate(svc) }

// dropOwnedSlices removes every slice this controller owns for the Service,
// used when the Service stops being managed or stops being valid.
func (r *ServiceReconciler) dropOwnedSlices(ctx context.Context, svc *corev1.Service, why string) error {
	var owned discoveryv1.EndpointSliceList
	if err := r.List(ctx, &owned,
		client.InNamespace(svc.Namespace),
		client.MatchingLabels{
			discoveryv1.LabelServiceName: svc.Name,
			discoveryv1.LabelManagedBy:   ManagedBy,
		},
	); err != nil {
		return err
	}
	for i := range owned.Items {
		s := &owned.Items[i]
		if err := r.Delete(ctx, s); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		r.Events.Emit("slice_patched",
			"namespace", svc.Namespace, "service", svc.Name,
			"slice", s.Name, "op", "delete", "reason", why)
	}
	return nil
}

// Enqueue asks for a Service to be reconciled because its health input moved.
// It never blocks: a full channel already means a reconcile is pending, and
// reconcile re-reads everything anyway.
func (r *ServiceReconciler) Enqueue(key types.NamespacedName) {
	if r.HealthEvents == nil {
		return
	}
	select {
	case r.HealthEvents <- event.TypedGenericEvent[*corev1.Service]{
		Object: &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: key.Name, Namespace: key.Namespace,
		}},
	}:
	default:
	}
}

// SetupWithManager wires the Service, owned-slice, Pod and health watches.
func (r *ServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("secondary-service").
		For(&corev1.Service{}).
		Owns(&discoveryv1.EndpointSlice{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.servicesForPod)).
		WatchesRawSource(source.Channel(r.HealthEvents,
			&handler.TypedEnqueueRequestForObject[*corev1.Service]{})).
		Complete(r)
}
