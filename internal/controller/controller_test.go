package controller

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/boanlab/multus-service/internal/model"
)

func testService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "amf-n2", Namespace: "oai", UID: "svc-uid"},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Ports: []corev1.ServicePort{
				{Name: "n2", Port: 38412, Protocol: corev1.ProtocolTCP},
			},
		},
	}
}

func att(ip, iface, pod string, ready bool) model.Attachment {
	return model.Attachment{
		PodUID: types.UID("pod-uid-" + pod), PodName: pod, PodNamespace: "oai",
		NodeName: "node-1", NAD: "oai/n2-net", Interface: iface, IP: ip, PodReady: ready,
	}
}

func TestValidateServiceRejectsSelector(t *testing.T) {
	svc := testService()
	svc.Spec.Selector = map[string]string{"app": "oai-amf"}
	err := validateService(svc)
	if err == nil {
		t.Fatal("a Service with a selector must be rejected: the built-in " +
			"EndpointSlice controller would publish primary Pod IPs alongside ours")
	}
}

func TestValidateServiceRejectsClusterIP(t *testing.T) {
	svc := testService()
	svc.Spec.ClusterIP = "10.43.1.2"
	if err := validateService(svc); err == nil {
		t.Fatal("a Service with a ClusterIP must be rejected")
	}
}

func TestValidateServiceAccepts(t *testing.T) {
	if err := validateService(testService()); err != nil {
		t.Fatalf("valid service rejected: %v", err)
	}
}

func TestNewEndpointsStartNotReady(t *testing.T) {
	// The whole point of Phase 1: an address nobody has health-checked is
	// published as ready=false, even when the Pod itself is Ready.
	hs := NewHealthStore(15 * time.Second)
	want, readiness, _ := desiredSlices(testService(), "oai/n2-net",
		[]model.Attachment{att("10.100.50.249", "n2", "amf-0", true)}, hs)

	s := want["amf-n2-secondary-ipv4"]
	if s == nil {
		t.Fatalf("no IPv4 slice; got %v", keys(want))
	}
	if len(s.Endpoints) != 1 {
		t.Fatalf("want 1 endpoint, got %d", len(s.Endpoints))
	}
	got := s.Endpoints[0].Conditions.Ready
	if got == nil {
		t.Fatal("ready must never be nil: CoreDNS reads a nil ready as healthy")
	}
	if *got {
		t.Error("endpoint must not be ready without a fresh health report")
	}
	if readiness[0].Reason != "NoHealthReport" {
		t.Errorf("reason = %q, want NoHealthReport", readiness[0].Reason)
	}
}

func TestReadyRequiresEveryTerm(t *testing.T) {
	a := att("10.100.50.249", "n2", "amf-0", true)
	now := time.Now()

	cases := []struct {
		name       string
		podReady   bool
		report     *model.Report
		wantReady  bool
		wantReason string
	}{
		{"pod not ready", false, &model.Report{LocalReady: true, PathReady: true, ObservedAt: now}, false, "PodNotReady"},
		{"no report", true, nil, false, "NoHealthReport"},
		{"stale report", true, &model.Report{LocalReady: true, PathReady: true, ObservedAt: now.Add(-time.Hour)}, false, "HealthReportStale"},
		{"local down", true, &model.Report{LocalReady: false, PathReady: true, ObservedAt: now}, false, "LocalNotReady"},
		{"path down", true, &model.Report{LocalReady: true, PathReady: false, ObservedAt: now}, false, "PathNotReady"},
		{"all good", true, &model.Report{LocalReady: true, PathReady: true, ObservedAt: now}, true, "Healthy"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hs := NewHealthStore(15 * time.Second)
			a.PodReady = c.podReady
			if c.report != nil {
				r := *c.report
				r.AttachmentID = a.ID()
				hs.Put(r)
			}
			got := ComputeReady(a, hs)
			if got.Ready != c.wantReady || got.Reason != c.wantReason {
				t.Errorf("got (%v,%q), want (%v,%q)", got.Ready, got.Reason, c.wantReady, c.wantReason)
			}
		})
	}
}

func TestHealthStoreSeparatesStaleFromUnhealthy(t *testing.T) {
	// A dead agent and a failing path both drive ready=false, but they are
	// different events and the store must distinguish them.
	hs := NewHealthStore(10 * time.Second)
	now := time.Now()

	hs.Put(model.Report{AttachmentID: "fresh-bad", LocalReady: true, PathReady: false, ObservedAt: now})
	hs.Put(model.Report{AttachmentID: "stale-good", LocalReady: true, PathReady: true, ObservedAt: now.Add(-time.Minute)})

	if got := hs.State("fresh-bad"); got != model.HealthUnhealthy {
		t.Errorf("fresh failing report = %v, want Unhealthy", got)
	}
	if got := hs.State("stale-good"); got != model.HealthUnknown {
		t.Errorf("stale report = %v, want Unknown", got)
	}
	if got := hs.State("never-seen"); got != model.HealthUnknown {
		t.Errorf("missing report = %v, want Unknown", got)
	}
}

func TestHealthStoreIgnoresOutOfOrderReports(t *testing.T) {
	hs := NewHealthStore(time.Minute)
	now := time.Now()
	hs.Put(model.Report{AttachmentID: "a", PathReady: false, ObservedAt: now})
	hs.Put(model.Report{AttachmentID: "a", PathReady: true, ObservedAt: now.Add(-time.Second)})
	r, _ := hs.Get("a")
	if r.PathReady {
		t.Error("an older report overwrote a newer one")
	}
}

func TestDualStackSplitsIntoTwoSlices(t *testing.T) {
	hs := NewHealthStore(time.Minute)
	want, _, _ := desiredSlices(testService(), "oai/n2-net", []model.Attachment{
		att("10.100.61.10", "net1", "amf-0", true),
		att("fd00:61::10", "net1", "amf-0", true),
	}, hs)

	v4 := want["amf-n2-secondary-ipv4"]
	v6 := want["amf-n2-secondary-ipv6"]
	if v4 == nil || v6 == nil {
		t.Fatalf("want one slice per family, got %v", keys(want))
	}
	if v4.AddressType != discoveryv1.AddressTypeIPv4 || v6.AddressType != discoveryv1.AddressTypeIPv6 {
		t.Error("addressType must match the family")
	}
	if v4.Endpoints[0].Addresses[0] != "10.100.61.10" || v6.Endpoints[0].Addresses[0] != "fd00:61::10" {
		t.Error("addresses landed in the wrong slice")
	}
}

func TestAttachmentIDDistinguishesEverything(t *testing.T) {
	base := att("10.0.0.1", "net1", "p", true)
	variants := map[string]model.Attachment{
		"ip":        {PodUID: base.PodUID, NAD: base.NAD, Interface: base.Interface, IP: "10.0.0.2"},
		"interface": {PodUID: base.PodUID, NAD: base.NAD, Interface: "net2", IP: base.IP},
		"nad":       {PodUID: base.PodUID, NAD: "oai/other", Interface: base.Interface, IP: base.IP},
		"poduid":    {PodUID: "other", NAD: base.NAD, Interface: base.Interface, IP: base.IP},
	}
	for name, v := range variants {
		if v.ID() == base.ID() {
			t.Errorf("attachment ID collides when %s differs", name)
		}
	}
	if base.ID() != att("10.0.0.1", "net1", "p", false).ID() {
		t.Error("attachment ID must not depend on mutable readiness")
	}
}

func TestPortsUseTargetPortWhenNumeric(t *testing.T) {
	svc := testService()
	svc.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	ports, warn := servicePorts(svc)
	if len(warn) != 0 {
		t.Errorf("unexpected warnings: %v", warn)
	}
	if ptr.Deref(ports[0].Port, 0) != 9999 {
		t.Errorf("port = %d, want 9999", ptr.Deref(ports[0].Port, 0))
	}
}

func TestNamedTargetPortWarnsAndFallsBack(t *testing.T) {
	svc := testService()
	svc.Spec.Ports[0].TargetPort = intstr.FromString("n2-port")
	ports, warn := servicePorts(svc)
	if len(warn) == 0 {
		t.Error("a named targetPort cannot be resolved here and must be reported")
	}
	if ptr.Deref(ports[0].Port, 0) != 38412 {
		t.Errorf("port = %d, want the service port 38412", ptr.Deref(ports[0].Port, 0))
	}
}

func TestHostnameRejectsInvalidLabels(t *testing.T) {
	if got := att("10.0.0.1", "net1", "amf-abc-123", true).Hostname(); got != "amf-abc-123" {
		t.Errorf("hostname = %q, want the pod name", got)
	}
	for _, bad := range []string{"has.dot", "Upper", "-lead", "trail-", ""} {
		if got := att("10.0.0.1", "net1", bad, true).Hostname(); got != "" {
			t.Errorf("pod name %q must not become a hostname, got %q", bad, got)
		}
	}
}

func TestSliceUpToDateIgnoresOrder(t *testing.T) {
	hs := NewHealthStore(time.Minute)
	a := []model.Attachment{att("10.0.0.2", "net1", "b", true), att("10.0.0.1", "net1", "a", true)}
	b := []model.Attachment{att("10.0.0.1", "net1", "a", true), att("10.0.0.2", "net1", "b", true)}
	x, _, _ := desiredSlices(testService(), "oai/n2-net", a, hs)
	y, _, _ := desiredSlices(testService(), "oai/n2-net", b, hs)
	if !sliceUpToDate(x["amf-n2-secondary-ipv4"], y["amf-n2-secondary-ipv4"]) {
		t.Error("endpoint ordering must not cause a spurious update")
	}
}

func TestOwnerReferencePointsAtService(t *testing.T) {
	// Service deletion must cascade to the slices, so this is load-bearing.
	hs := NewHealthStore(time.Minute)
	want, _, _ := desiredSlices(testService(), "oai/n2-net",
		[]model.Attachment{att("10.0.0.1", "net1", "a", true)}, hs)
	or := want["amf-n2-secondary-ipv4"].OwnerReferences
	if len(or) != 1 || or[0].Kind != "Service" || or[0].UID != "svc-uid" || !ptr.Deref(or[0].Controller, false) {
		t.Errorf("ownerReferences = %+v", or)
	}
}

func TestManagedByLabelIsOurs(t *testing.T) {
	hs := NewHealthStore(time.Minute)
	want, _, _ := desiredSlices(testService(), "oai/n2-net",
		[]model.Attachment{att("10.0.0.1", "net1", "a", true)}, hs)
	got := want["amf-n2-secondary-ipv4"].Labels[discoveryv1.LabelManagedBy]
	if got != ManagedBy {
		t.Errorf("managed-by = %q, want %q", got, ManagedBy)
	}
}

func TestSliceLimitTruncatesAndWarns(t *testing.T) {
	hs := NewHealthStore(time.Minute)
	atts := make([]model.Attachment, 0, maxEndpointsPerSlice+5)
	for i := 0; i < maxEndpointsPerSlice+5; i++ {
		atts = append(atts, att(ipv4(i), "net1", "p", true))
	}
	want, _, warn := desiredSlices(testService(), "oai/n2-net", atts, hs)
	if len(warn) == 0 {
		t.Error("exceeding the 1000-endpoint API limit must be reported, not silently truncated")
	}
	if n := len(want["amf-n2-secondary-ipv4"].Endpoints); n != maxEndpointsPerSlice {
		t.Errorf("endpoints = %d, want %d", n, maxEndpointsPerSlice)
	}
}

func ipv4(i int) string {
	return "10.200." + itoa(i/250) + "." + itoa(i%250+1)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestDuplicateAddressIsDroppedAndReported(t *testing.T) {
	// host-local IPAM allocates per node, so the same NAD subnet hands out the
	// same address on every node. Publishing both would put a node-local
	// address into a cluster-wide DNS answer.
	hs := NewHealthStore(time.Minute)
	a := att("10.244.77.2", "net1", "amf-a", true)
	b := att("10.244.77.2", "net1", "amf-b", true)
	c := att("10.244.77.3", "net1", "amf-c", true)

	want, _, warn := desiredSlices(testService(), "oai/n2-net", []model.Attachment{b, a, c}, hs)
	s := want["amf-n2-secondary-ipv4"]
	if len(s.Endpoints) != 2 {
		t.Fatalf("endpoints = %d, want 2 (one per distinct address)", len(s.Endpoints))
	}
	if len(warn) == 0 {
		t.Error("a duplicate address must be reported, not silently merged")
	}
}

func TestDuplicateWinnerIsStable(t *testing.T) {
	hs := NewHealthStore(time.Minute)
	a := att("10.244.77.2", "net1", "amf-a", true)
	b := att("10.244.77.2", "net1", "amf-b", true)

	first, _, _ := desiredSlices(testService(), "oai/n2-net", []model.Attachment{a, b}, hs)
	second, _, _ := desiredSlices(testService(), "oai/n2-net", []model.Attachment{b, a}, hs)
	if !sliceUpToDate(first["amf-n2-secondary-ipv4"], second["amf-n2-secondary-ipv4"]) {
		t.Error("which duplicate wins must not depend on lister order, or reconcile will flap")
	}
}
