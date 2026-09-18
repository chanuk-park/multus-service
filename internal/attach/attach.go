// Package attach derives publishable attachments from a Service and the Pods it
// selects. Both the controller and the node agent need exactly this mapping, so
// it lives in one place rather than being implemented twice and drifting.
package attach

import (
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/boanlab/multus-service/internal/model"
	"github.com/boanlab/multus-service/internal/multus"
)

const (
	// AnnotationNetwork names the NetworkAttachmentDefinition whose addresses
	// this Service publishes. A bare name resolves in the Service's namespace.
	AnnotationNetwork = "secondary-service.boanlab.io/network"
	// AnnotationSelector is the label selector choosing the workload Pods.
	// It is an annotation and not spec.selector because a real selector would
	// hand the Service to the built-in EndpointSlice controller, which
	// publishes primary Pod IPs that CoreDNS then merges into the same answer.
	AnnotationSelector = "secondary-service.boanlab.io/workload-selector"

	// ManagedBy marks EndpointSlices this project owns.
	ManagedBy = "secondary-service.boanlab.io"
)

// Managed reports whether the Service opts into secondary publication.
func Managed(svc *corev1.Service) bool {
	_, ok := svc.Annotations[AnnotationNetwork]
	return ok
}

// Validate enforces the two invariants the design rests on.
//
// A selector would hand the Service to the built-in EndpointSlice controller,
// which creates its own slice from primary Pod IPs; CoreDNS merges every slice
// carrying the same service-name label, so the primary address would appear in
// the answer beside the secondary one. A ClusterIP would put kube-proxy in the
// path, and kube-proxy cannot forward to addresses it knows nothing about.
func Validate(svc *corev1.Service) error {
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

// NAD returns the canonical "ns/name" of the Service's network.
func NAD(svc *corev1.Service) (string, error) {
	return multus.CanonicalNAD(svc.Annotations[AnnotationNetwork], svc.Namespace)
}

// Selector parses the workload selector. An absent selector is an error rather
// than a default, because an empty selector matches every Pod in the namespace.
func Selector(svc *corev1.Service) (labels.Selector, error) {
	raw, ok := svc.Annotations[AnnotationSelector]
	if !ok || raw == "" {
		return nil, fmt.Errorf("annotation %s is required; an empty selector would match every Pod", AnnotationSelector)
	}
	return labels.Parse(raw)
}

// Eligible filters out Pods that must not appear in a slice.
func Eligible(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return false
	}
	return true
}

// PodReady reads the Pod's Ready condition. It rides the primary interface and
// therefore says nothing about any secondary address.
func PodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// FromPod expands one Pod's attachment to the named NAD into one Attachment per
// address. A dual-stack attachment yields two, because the v4 and v6 paths fail
// independently and are tracked independently.
//
// Errors are multus.ErrNoAttachment (ordinary: CNI ADD has not completed, or
// the Pod simply does not carry this network) and multus.ErrAmbiguous (the Pod
// attached the network more than once; the caller must skip it rather than
// guess an interface).
func FromPod(pod *corev1.Pod, nad string) ([]model.Attachment, error) {
	list, err := multus.Parse(pod.Annotations[multus.StatusAnnotation])
	if err != nil {
		return nil, err
	}
	entry, err := multus.Select(list, nad)
	if err != nil {
		return nil, err
	}

	ready := PodReady(pod)
	addrs := entry.Addresses()
	out := make([]model.Attachment, 0, len(addrs))
	for _, ip := range addrs {
		out = append(out, model.Attachment{
			PodUID:       pod.UID,
			PodName:      pod.Name,
			PodNamespace: pod.Namespace,
			NodeName:     pod.Spec.NodeName,
			NAD:          nad,
			Interface:    entry.Interface,
			IP:           ip,
			PodReady:     ready,
		})
	}
	return out, nil
}
