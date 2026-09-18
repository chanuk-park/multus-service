package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/boanlab/multus-service/internal/attach"
)

// servicesForPod maps a Pod event to the Services that may publish it.
//
// The mapping cannot use spec.selector, because managed Services deliberately
// have none. It reads the workload-selector annotation instead, which is why
// this is a map function rather than an owner reference.
func (r *ServiceReconciler) servicesForPod(ctx context.Context, obj client.Object) []ctrl.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	var svcs corev1.ServiceList
	if err := r.List(ctx, &svcs, client.InNamespace(pod.Namespace)); err != nil {
		log.FromContext(ctx).Error(err, "listing services for pod", "pod", pod.Name)
		return nil
	}

	podLabels := labels.Set(pod.Labels)
	var reqs []ctrl.Request
	for i := range svcs.Items {
		svc := &svcs.Items[i]
		if !attach.Managed(svc) {
			continue
		}
		sel, err := attach.Selector(svc)
		if err != nil {
			continue
		}
		if !sel.Matches(podLabels) {
			continue
		}
		reqs = append(reqs, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: svc.Namespace, Name: svc.Name},
		})
	}
	return reqs
}
