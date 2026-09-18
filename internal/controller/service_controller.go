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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

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

	nadRef, managed := svc.Annotations[AnnotationNetwork]
	if !managed {
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "annotation removed")
	}

	if err := validateService(&svc); err != nil {
		r.Recorder.Event(&svc, corev1.EventTypeWarning, "InvalidService", err.Error())
		r.Events.Emit("service_invalid",
			"namespace", svc.Namespace, "service", svc.Name, "error", err.Error())
		// Refuse to publish anything for a Service that breaks the invariants.
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "invalid service")
	}

	nad, err := multus.CanonicalNAD(nadRef, svc.Namespace)
	if err != nil {
		r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "InvalidNetwork",
			"annotation %s: %v", AnnotationNetwork, err)
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "invalid network annotation")
	}

	selRaw, ok := svc.Annotations[AnnotationSelector]
	if !ok || selRaw == "" {
		r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "MissingSelector",
			"annotation %s is required; an empty selector would match every Pod", AnnotationSelector)
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "missing workload selector")
	}
	sel, err := labels.Parse(selRaw)
	if err != nil {
		r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "InvalidSelector",
			"annotation %s=%q: %v", AnnotationSelector, selRaw, err)
		return ctrl.Result{}, r.dropOwnedSlices(ctx, &svc, "invalid workload selector")
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
		if !eligible(pod) {
			continue
		}
		list, err := multus.Parse(pod.Annotations[multus.StatusAnnotation])
		if err != nil {
			r.Recorder.Eventf(&svc, corev1.EventTypeWarning, "BadNetworkStatus",
				"pod %s: %v", pod.Name, err)
			continue
		}
		entry, err := multus.Select(list, nad)
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
				lg.Error(err, "selecting attachment", "pod", pod.Name)
			}
			continue
		}

		ready := podReady(pod)
		for _, ip := range entry.Addresses() {
			a := model.Attachment{
				PodUID:       pod.UID,
				PodName:      pod.Name,
				PodNamespace: pod.Namespace,
				NodeName:     pod.Spec.NodeName,
				NAD:          nad,
				Interface:    entry.Interface,
				IP:           ip,
				PodReady:     ready,
			}
			atts = append(atts, a)
			r.Events.Emit("attachment_discovered",
				"namespace", svc.Namespace, "service", svc.Name,
				"pod", pod.Name, "pod_uid", string(pod.UID),
				"nad", nad, "interface", entry.Interface, "ip", ip,
				"attachment_id", a.ID(), "pod_ready", ready)
		}
	}

	want, readiness, warnings := desiredSlices(&svc, nad, atts, r.Health)
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

// validateService enforces the two invariants the whole design rests on.
//
// A selector would hand the Service to the built-in EndpointSlice controller,
// which creates its own slice from primary Pod IPs; CoreDNS merges every slice
// carrying the same service-name label, so the primary address would appear in
// the answer next to the secondary one. A ClusterIP would put kube-proxy in the
// path, which cannot forward to addresses it knows nothing about.
func validateService(svc *corev1.Service) error {
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		return fmt.Errorf("service must be headless (clusterIP: None), got %q", svc.Spec.ClusterIP)
	}
	if len(svc.Spec.Selector) > 0 {
		return errors.New("service must not set spec.selector; " +
			"the built-in EndpointSlice controller would publish primary Pod IPs alongside the secondary ones. " +
			"Use the " + AnnotationSelector + " annotation instead")
	}
	return nil
}

// eligible filters out Pods that must not appear in a slice.
func eligible(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return false
	}
	return true
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

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
