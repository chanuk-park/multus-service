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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

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
}

// Reconcile brings the owned EndpointSlices in line with the Pods currently
// matching the Service's workload selector.
func (r *ServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	var svc corev1.Service
	if err := r.Get(ctx, req.NamespacedName, &svc); err != nil {
		// Owned slices carry an ownerReference to the Service, so deletion is
		// handled by garbage collection rather than here.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !attach.Managed(&svc) {
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "annotation removed")
	}

	if err := validateService(&svc); err != nil {
		r.Recorder.Event(&svc, corev1.EventTypeWarning, "InvalidService", err.Error())
		r.Events.Emit("service_invalid",
			"namespace", svc.Namespace, "service", svc.Name, "error", err.Error())
		// Refuse to publish anything for a Service that breaks the invariants.
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "invalid service")
	}

	nad, err := attach.NAD(&svc)
	if err != nil {
		r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "InvalidNetwork",
			"annotation %s: %v", AnnotationNetwork, err)
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "invalid network annotation")
	}

	sel, err := attach.Selector(&svc)
	if err != nil {
		r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "InvalidSelector", "%v", err)
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

	// Probe scope is Endpoint for now. Phase 6 resolves it per NAD from the
	// secondary-service.boanlab.io/probe-scope annotation; until a Node-scope
	// probe exists there is nothing for a Node key to read, and Endpoint scope
	// is the one that is correct without a topology assumption.
	scope := model.ScopeEndpoint

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

// SetupWithManager wires the Service, owned-slice and Pod watches.
func (r *ServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("secondary-service").
		For(&corev1.Service{}).
		Owns(&discoveryv1.EndpointSlice{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.servicesForPod)).
		Complete(r)
}
