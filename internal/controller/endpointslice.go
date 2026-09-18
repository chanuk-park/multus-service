package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/boanlab/multus-service/internal/attach"
	"github.com/boanlab/multus-service/internal/model"
)

// The annotation contract lives in internal/attach, shared with the node agent
// so the two can never disagree about which Pods a Service claims.
const (
	AnnotationNetwork  = attach.AnnotationNetwork
	AnnotationSelector = attach.AnnotationSelector
	// ManagedBy marks slices this controller owns. The built-in controller only
	// touches slices carrying its own value, so the two never collide.
	ManagedBy = attach.ManagedBy
)

const (
	// sliceNetworkAnnotation records which NAD a slice came from. The NAD name
	// contains "/", so it cannot be a label value.
	sliceNetworkAnnotation = "secondary-service.boanlab.io/network"

	// maxEndpointsPerSlice is the API's hard limit, measured: 1001 endpoints is
	// rejected with "Too many: must have at most 1000 items".
	maxEndpointsPerSlice = 1000
)

// warning is a problem worth surfacing on the Service. Reason becomes the
// Kubernetes Event reason, so it must stay stable enough to filter on.
type warning struct {
	Reason  string
	Message string
}

// sliceName is deterministic so reconciliation never orphans a slice.
func sliceName(svcName string, at discoveryv1.AddressType) string {
	return fmt.Sprintf("%s-secondary-%s", svcName, strings.ToLower(string(at)))
}

// servicePorts translates Service ports into EndpointSlice ports.
//
// There is no kube-proxy in this path -- the Service is headless, so nothing
// rewrites the destination. The port in the slice is the port the client
// actually connects to and the port CoreDNS publishes in SRV records.
func servicePorts(svc *corev1.Service) ([]discoveryv1.EndpointPort, []warning) {
	out := make([]discoveryv1.EndpointPort, 0, len(svc.Spec.Ports))
	var warnings []warning
	for _, p := range svc.Spec.Ports {
		port := p.Port
		if p.TargetPort.Type == 0 && p.TargetPort.IntValue() > 0 {
			port = int32(p.TargetPort.IntValue())
		} else if p.TargetPort.Type == 1 && p.TargetPort.StrVal != "" {
			// A named targetPort would need a per-Pod container lookup to
			// resolve. Fall back to the Service port and say so, rather than
			// silently publishing a port nothing listens on.
			warnings = append(warnings, warning{"UnresolvedTargetPort", fmt.Sprintf(
				"port %q uses named targetPort %q; publishing service port %d instead",
				p.Name, p.TargetPort.StrVal, p.Port)})
		}
		proto := p.Protocol
		if proto == "" {
			proto = corev1.ProtocolTCP
		}
		ep := discoveryv1.EndpointPort{
			Port:     ptr.To(port),
			Protocol: ptr.To(proto),
		}
		if p.Name != "" {
			ep.Name = ptr.To(p.Name)
		} else {
			ep.Name = ptr.To("")
		}
		out = append(out, ep)
	}
	return out, warnings
}

// desiredSlices builds the complete set of slices for a Service, one per
// address family. Families are kept apart because CoreDNS keys A and AAAA off
// addressType; mixing them would be rejected by the API anyway.
func desiredSlices(
	svc *corev1.Service,
	nad string,
	scope model.ProbeScope,
	atts []model.Attachment,
	hs *HealthStore,
) (map[string]*discoveryv1.EndpointSlice, []Readiness, []warning) {

	ports, warnings := servicePorts(svc)

	byFamily := map[discoveryv1.AddressType][]model.Attachment{}
	for _, a := range atts {
		at := a.AddressType()
		byFamily[at] = append(byFamily[at], a)
	}

	out := make(map[string]*discoveryv1.EndpointSlice, len(byFamily))
	readiness := make([]Readiness, 0, len(atts))

	for at, list := range byFamily {
		// Deterministic order: the slice contents must not churn just because
		// the Pod lister returned a different order. PodUID breaks ties so that
		// duplicate addresses resolve the same way on every reconcile.
		sort.Slice(list, func(i, j int) bool {
			if list[i].IP != list[j].IP {
				return list[i].IP < list[j].IP
			}
			return list[i].PodUID < list[j].PodUID
		})

		// Two Pods claiming one address is a real and easy misconfiguration:
		// host-local IPAM allocates per node, so one NAD subnet shared across
		// nodes hands the same address to Pods on different nodes.
		//
		// Every claimant is withdrawn, not all-but-one. Picking a winner is
		// deterministic but not correct: DNS carries the address alone, so a
		// client has no way to reach the Pod the controller chose, and
		// targetRef plays no part in forwarding. Both Pods still own the
		// address in the data plane whatever the slice says. The only honest
		// answer is to publish neither and say so.
		list, conflicts := dropConflictingAddresses(list)
		dupIPs := make([]string, 0, len(conflicts))
		for ip := range conflicts {
			dupIPs = append(dupIPs, ip)
		}
		sort.Strings(dupIPs)
		for _, ip := range dupIPs {
			warnings = append(warnings, warning{"DuplicateAddress", fmt.Sprintf(
				"address %s is claimed by %d Pods (%s); none of them is published. "+
					"DNS carries the address alone, so no choice among them is reachable on purpose. "+
					"Per-node IPAM on a subnet shared across nodes is the usual cause",
				ip, len(conflicts[ip]), strings.Join(conflicts[ip], ", "))})
		}

		if len(list) > maxEndpointsPerSlice {
			warnings = append(warnings, warning{"SliceLimit", fmt.Sprintf(
				"%d %s attachments exceed the %d-endpoint slice limit; publishing the first %d by address",
				len(list), at, maxEndpointsPerSlice, maxEndpointsPerSlice)})
			list = list[:maxEndpointsPerSlice]
		}

		eps := make([]discoveryv1.Endpoint, 0, len(list))
		for _, a := range list {
			r := ComputeReady(a, scope, hs)
			readiness = append(readiness, r)
			ep := discoveryv1.Endpoint{
				Addresses: []string{a.IP},
				Conditions: discoveryv1.EndpointConditions{
					// ready is always set explicitly. A nil ready is read as
					// healthy by CoreDNS, so omitting it while the state is
					// unknown would advertise an unchecked address.
					Ready:       ptr.To(r.Ready),
					Serving:     ptr.To(r.Ready),
					Terminating: ptr.To(false),
				},
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: a.PodNamespace,
					Name:      a.PodName,
					UID:       a.PodUID,
				},
			}
			if h := a.Hostname(); h != "" {
				ep.Hostname = ptr.To(h)
			}
			if a.NodeName != "" {
				ep.NodeName = ptr.To(a.NodeName)
			}
			eps = append(eps, ep)
		}

		name := sliceName(svc.Name, at)
		out[name] = &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: svc.Namespace,
				Labels: map[string]string{
					discoveryv1.LabelServiceName: svc.Name,
					discoveryv1.LabelManagedBy:   ManagedBy,
				},
				Annotations: map[string]string{
					sliceNetworkAnnotation: nad,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "v1",
					Kind:       "Service",
					Name:       svc.Name,
					UID:        svc.UID,
					Controller: ptr.To(true),
				}},
			},
			AddressType: at,
			Ports:       ports,
			Endpoints:   eps,
		}
	}
	return out, readiness, warnings
}

// dropConflictingAddresses removes every attachment whose address is claimed
// more than once, and reports the claimants per address.
//
// Input must already be sorted by (IP, PodUID) so the reported order is stable.
func dropConflictingAddresses(list []model.Attachment) ([]model.Attachment, map[string][]string) {
	out := make([]model.Attachment, 0, len(list))
	var conflicts map[string][]string
	for i := 0; i < len(list); {
		j := i
		for j < len(list) && list[j].IP == list[i].IP {
			j++
		}
		if j-i == 1 {
			out = append(out, list[i])
		} else {
			if conflicts == nil {
				conflicts = map[string][]string{}
			}
			pods := make([]string, 0, j-i)
			for _, a := range list[i:j] {
				pods = append(pods, a.PodName)
			}
			conflicts[list[i].IP] = pods
		}
		i = j
	}
	return out, conflicts
}

// comparable projections -- only the fields this controller manages are
// compared, so server-side defaulting never triggers a write loop.

type epKey struct {
	addr        string
	ready       bool
	serving     bool
	terminating bool
	hostname    string
	nodeName    string
	targetUID   types.UID
	targetName  string
}

type portKey struct {
	name  string
	proto string
	port  int32
}

func projectEndpoints(s *discoveryv1.EndpointSlice) []epKey {
	out := make([]epKey, 0, len(s.Endpoints))
	for _, e := range s.Endpoints {
		k := epKey{
			ready:       ptr.Deref(e.Conditions.Ready, false),
			serving:     ptr.Deref(e.Conditions.Serving, false),
			terminating: ptr.Deref(e.Conditions.Terminating, false),
			hostname:    ptr.Deref(e.Hostname, ""),
			nodeName:    ptr.Deref(e.NodeName, ""),
		}
		if len(e.Addresses) > 0 {
			k.addr = e.Addresses[0]
		}
		if e.TargetRef != nil {
			k.targetUID = e.TargetRef.UID
			k.targetName = e.TargetRef.Name
		}
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].addr < out[j].addr })
	return out
}

func projectPorts(s *discoveryv1.EndpointSlice) []portKey {
	out := make([]portKey, 0, len(s.Ports))
	for _, p := range s.Ports {
		out = append(out, portKey{
			name:  ptr.Deref(p.Name, ""),
			proto: string(ptr.Deref(p.Protocol, corev1.ProtocolTCP)),
			port:  ptr.Deref(p.Port, 0),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].name != out[j].name {
			return out[i].name < out[j].name
		}
		return out[i].port < out[j].port
	})
	return out
}

func sliceUpToDate(have, want *discoveryv1.EndpointSlice) bool {
	if have.AddressType != want.AddressType {
		return false
	}
	if have.Labels[discoveryv1.LabelServiceName] != want.Labels[discoveryv1.LabelServiceName] ||
		have.Labels[discoveryv1.LabelManagedBy] != want.Labels[discoveryv1.LabelManagedBy] ||
		have.Annotations[sliceNetworkAnnotation] != want.Annotations[sliceNetworkAnnotation] {
		return false
	}
	if !equalSlices(projectPorts(have), projectPorts(want)) {
		return false
	}
	return equalSlices(projectEndpoints(have), projectEndpoints(want))
}

func equalSlices[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// sliceOp describes what reconciliation did, for the event stream.
type sliceOp struct {
	Name       string
	Op         string // create | update | delete | nochange
	ReadyCount int
	Total      int
}

// applySlices makes the cluster match want, deleting any owned slice that is no
// longer desired.
func (r *ServiceReconciler) applySlices(
	ctx context.Context,
	svc *corev1.Service,
	want map[string]*discoveryv1.EndpointSlice,
) ([]sliceOp, error) {

	var existing discoveryv1.EndpointSliceList
	if err := r.List(ctx, &existing,
		client.InNamespace(svc.Namespace),
		client.MatchingLabels{
			discoveryv1.LabelServiceName: svc.Name,
			discoveryv1.LabelManagedBy:   ManagedBy,
		},
	); err != nil {
		return nil, fmt.Errorf("list owned slices: %w", err)
	}

	have := make(map[string]*discoveryv1.EndpointSlice, len(existing.Items))
	for i := range existing.Items {
		have[existing.Items[i].Name] = &existing.Items[i]
	}

	ops := make([]sliceOp, 0, len(want)+len(have))

	names := make([]string, 0, len(want))
	for n := range want {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		w := want[name]
		ready := 0
		for _, e := range w.Endpoints {
			if ptr.Deref(e.Conditions.Ready, false) {
				ready++
			}
		}
		cur, ok := have[name]
		switch {
		case !ok:
			if err := r.Create(ctx, w); err != nil && !apierrors.IsAlreadyExists(err) {
				return ops, fmt.Errorf("create slice %s: %w", name, err)
			}
			ops = append(ops, sliceOp{name, "create", ready, len(w.Endpoints)})
		case sliceUpToDate(cur, w):
			ops = append(ops, sliceOp{name, "nochange", ready, len(w.Endpoints)})
		default:
			upd := cur.DeepCopy()
			upd.AddressType = w.AddressType
			upd.Ports = w.Ports
			upd.Endpoints = w.Endpoints
			if upd.Labels == nil {
				upd.Labels = map[string]string{}
			}
			for k, v := range w.Labels {
				upd.Labels[k] = v
			}
			if upd.Annotations == nil {
				upd.Annotations = map[string]string{}
			}
			for k, v := range w.Annotations {
				upd.Annotations[k] = v
			}
			upd.OwnerReferences = w.OwnerReferences
			if err := r.Update(ctx, upd); err != nil {
				return ops, fmt.Errorf("update slice %s: %w", name, err)
			}
			ops = append(ops, sliceOp{name, "update", ready, len(w.Endpoints)})
		}
	}

	stale := make([]string, 0)
	for name := range have {
		if _, keep := want[name]; !keep {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	for _, name := range stale {
		if err := r.Delete(ctx, have[name]); err != nil && !apierrors.IsNotFound(err) {
			return ops, fmt.Errorf("delete slice %s: %w", name, err)
		}
		ops = append(ops, sliceOp{name, "delete", 0, 0})
	}

	return ops, nil
}
