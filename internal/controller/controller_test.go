package controller

import (
	"testing"
	"time"

	"errors"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/boanlab/multus-service/internal/model"
)

const nadX = "oai/n2-net"

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
		NodeName: "node-1", NAD: nadX, Interface: iface, IP: ip, PodReady: ready,
	}
}

func build(t *testing.T, hs *HealthStore, atts ...model.Attachment) (map[string]*discoveryv1.EndpointSlice, []Readiness, []warning) {
	t.Helper()
	return desiredSlices(testService(), nadX, model.ScopeEndpoint, atts, hs)
}

// fakeClock drives the store's notion of "now". Freshness is measured from the
// moment the controller accepted a report, so tests move the controller's clock
// rather than backdating the agent's observed_at -- which is exactly the
// distinction the design relies on.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

const (
	testNode     = "node-1"
	testInstance = "inst-1"
)

var testSeq uint64

func newStore(ttl time.Duration) (*HealthStore, *fakeClock) {
	c := &fakeClock{t: time.Now()}
	hs := NewHealthStore(ttl)
	hs.now = c.now
	hs.AdoptInstance(testNode, testInstance)
	return hs, c
}

func origin() model.ReportOrigin {
	testSeq++
	return model.ReportOrigin{NodeName: testNode, AgentInstance: testInstance, Sequence: testSeq}
}

func putLocal(t *testing.T, hs *HealthStore, r model.LocalHealth) {
	t.Helper()
	r.NodeName = testNode
	if err := hs.AcceptLocal(origin(), r); err != nil {
		t.Fatalf("AcceptLocal: %v", err)
	}
}

func putPath(t *testing.T, hs *HealthStore, r model.PathHealth) {
	t.Helper()
	r.NodeName = testNode
	if err := hs.AcceptPath(origin(), r); err != nil {
		t.Fatalf("AcceptPath: %v", err)
	}
}

func healthy(a model.Attachment, at time.Time) model.LocalHealth {
	return model.LocalHealth{
		AttachmentID: a.ID(), InterfaceExists: true, AddressPresent: true,
		LinkUsable: true, ObservedAt: at,
	}
}

// ---------------------------------------------------------------- invariants

func TestValidateServiceRejectsSelector(t *testing.T) {
	svc := testService()
	svc.Spec.Selector = map[string]string{"app": "oai-amf"}
	if err := validateService(svc); err == nil {
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

// ---------------------------------------------------------------- readiness

func TestNewEndpointsStartNotReady(t *testing.T) {
	// An address nobody has health-checked is published ready=false, even when
	// the Pod itself is Ready. Discovery and availability are decoupled on
	// purpose: multus writes the address ~4.7s before the Pod is Ready.
	hs, _ := newStore(15 * time.Second)
	want, readiness, _ := build(t, hs, att("10.100.50.249", "n2", "amf-0", true))

	s := want["amf-n2-secondary-ipv4"]
	if s == nil {
		t.Fatalf("no IPv4 slice; got %v", keys(want))
	}
	got := s.Endpoints[0].Conditions.Ready
	if got == nil {
		t.Fatal("ready must never be nil: CoreDNS reads a nil ready as healthy")
	}
	if *got {
		t.Error("endpoint must not be ready without a fresh health report")
	}
	if readiness[0].Reason != "NoLocalReport" {
		t.Errorf("reason = %q, want NoLocalReport", readiness[0].Reason)
	}
}

func TestReadyRequiresEveryTerm(t *testing.T) {
	now := time.Now()
	a := att("10.100.50.249", "n2", "amf-0", true)
	good := healthy(a, now)
	goodPath := model.PathHealth{
		Scope: model.ScopeEndpoint, ScopeID: a.ID(), PathReady: true, ObservedAt: now,
	}

	cases := []struct {
		name       string
		podReady   bool
		local      *model.LocalHealth
		path       *model.PathHealth
		wantReady  bool
		wantReason string
	}{
		{"pod not ready", false, &good, &goodPath, false, "PodNotReady"},
		{"no local report", true, nil, &goodPath, false, "NoLocalReport"},
		{"interface gone", true, mut(good, func(l *model.LocalHealth) { l.InterfaceExists = false }), &goodPath, false, "InterfaceMissing"},
		{"link down", true, mut(good, func(l *model.LocalHealth) { l.LinkUsable = false }), &goodPath, false, "LinkDown"},
		{"address flushed", true, mut(good, func(l *model.LocalHealth) { l.AddressPresent = false }), &goodPath, false, "AddressMissing"},
		{"no path report", true, &good, nil, false, "NoPathReport"},
		{"path down", true, &good, mutPath(goodPath, func(p *model.PathHealth) { p.PathReady = false }), false, "PathNotReady"},
		{"all good", true, &good, &goodPath, true, "Healthy"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hs, _ := newStore(15 * time.Second)
			a.PodReady = c.podReady
			if c.local != nil {
				putLocal(t, hs, *c.local)
			}
			if c.path != nil {
				putPath(t, hs, *c.path)
			}
			got := ComputeReady(a, model.ScopeEndpoint, hs)
			if got.Ready != c.wantReady || got.Reason != c.wantReason {
				t.Errorf("got (%v,%q), want (%v,%q)", got.Ready, got.Reason, c.wantReady, c.wantReason)
			}
		})
	}
}

func TestStaleLocalReportBlocksReady(t *testing.T) {
	hs, clk := newStore(5 * time.Second)
	a := att("10.0.0.1", "net1", "amf-0", true)
	putLocal(t, hs, healthy(a, time.Now()))
	clk.advance(6 * time.Second)
	putPath(t, hs, model.PathHealth{Scope: model.ScopeEndpoint, ScopeID: a.ID(), PathReady: true})

	got := ComputeReady(a, model.ScopeEndpoint, hs)
	if got.Ready || got.Reason != "LocalReportStale" {
		t.Errorf("got (%v,%q), want (false,\"LocalReportStale\")", got.Ready, got.Reason)
	}
}

func TestStalePathReportBlocksReady(t *testing.T) {
	hs, clk := newStore(5 * time.Second)
	a := att("10.0.0.1", "net1", "amf-0", true)
	putPath(t, hs, model.PathHealth{Scope: model.ScopeEndpoint, ScopeID: a.ID(), PathReady: true})
	clk.advance(6 * time.Second)
	putLocal(t, hs, healthy(a, time.Now()))

	got := ComputeReady(a, model.ScopeEndpoint, hs)
	if got.Ready || got.Reason != "PathReportStale" {
		t.Errorf("got (%v,%q), want (false,\"PathReportStale\")", got.Ready, got.Reason)
	}
}

func TestLinkDownBeatsAddressPresent(t *testing.T) {
	// Measured: taking a link down deletes the IPv6 link-local address but
	// leaves the IPv4 address in place. An agent that only watched addresses
	// would report this dead interface as healthy.
	hs, _ := newStore(time.Minute)
	a := att("10.100.50.249", "n2", "amf-0", true)
	putLocal(t, hs, model.LocalHealth{
		AttachmentID: a.ID(), InterfaceExists: true, AddressPresent: true,
		LinkUsable: false, ObservedAt: time.Now(),
	})
	putPath(t, hs, model.PathHealth{Scope: model.ScopeEndpoint, ScopeID: a.ID(), PathReady: true, ObservedAt: time.Now()})
	if got := ComputeReady(a, model.ScopeEndpoint, hs); got.Ready || got.Reason != "LinkDown" {
		t.Errorf("got (%v,%q), want (false,\"LinkDown\")", got.Ready, got.Reason)
	}
}

// ---------------------------------------------------------------- health store

func TestStoreSeparatesStaleFromUnhealthy(t *testing.T) {
	// A dead agent and a failing probe both drive ready=false, but they are
	// different events and the store must tell them apart.
	hs, clk := newStore(10 * time.Second)

	putPath(t, hs, model.PathHealth{ScopeID: "stale-good", PathReady: true})
	clk.advance(11 * time.Second)
	putPath(t, hs, model.PathHealth{ScopeID: "fresh-bad", PathReady: false})

	if got := hs.PathState("fresh-bad"); got != model.StateUnhealthy {
		t.Errorf("fresh failing probe = %v, want Unhealthy", got)
	}
	if got := hs.PathState("stale-good"); got != model.StateUnknown {
		t.Errorf("stale probe = %v, want Unknown", got)
	}
	if got := hs.PathState("never-seen"); got != model.StateUnknown {
		t.Errorf("missing probe = %v, want Unknown", got)
	}
}

func TestFreshnessIgnoresTheAgentClock(t *testing.T) {
	// A node with a badly skewed clock must not be able to make a current
	// report look stale, nor a stale one look current. observed_at is carried
	// for measurement and never consulted for freshness.
	hs, _ := newStore(10 * time.Second)
	putLocal(t, hs, model.LocalHealth{
		AttachmentID: "skewed", InterfaceExists: true, AddressPresent: true, LinkUsable: true,
		ObservedAt: time.Now().Add(-72 * time.Hour),
	})
	if _, fresh := hs.Local("skewed"); !fresh {
		t.Error("a report the controller just accepted must be fresh regardless of observed_at")
	}
}

func TestStoreIgnoresOutOfOrderReports(t *testing.T) {
	// One gRPC stream orders its own messages, but a reconnect can leave two
	// streams briefly overlapping, so the sequence number decides.
	hs, _ := newStore(time.Minute)
	o := model.ReportOrigin{NodeName: testNode, AgentInstance: testInstance, Sequence: 10}
	if err := hs.AcceptPath(o, model.PathHealth{ScopeID: "a", PathReady: false, NodeName: testNode}); err != nil {
		t.Fatalf("AcceptPath: %v", err)
	}
	older := model.ReportOrigin{NodeName: testNode, AgentInstance: testInstance, Sequence: 9}
	if err := hs.AcceptPath(older, model.PathHealth{ScopeID: "a", PathReady: true, NodeName: testNode}); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("err = %v, want ErrOutOfOrder", err)
	}
	if r, _ := hs.Path("a"); r.PathReady {
		t.Error("an older report overwrote a newer one")
	}
}

func TestRestartedAgentSupersedesTheOldInstance(t *testing.T) {
	// The newest stream to open is authoritative for its node. Anything still
	// in flight from the previous process is discarded rather than applied.
	hs, _ := newStore(time.Minute)
	old := model.ReportOrigin{NodeName: testNode, AgentInstance: testInstance, Sequence: 5}
	if err := hs.AcceptLocal(old, model.LocalHealth{AttachmentID: "x", NodeName: testNode,
		InterfaceExists: true, AddressPresent: true, LinkUsable: true}); err != nil {
		t.Fatalf("AcceptLocal: %v", err)
	}

	if prev := hs.AdoptInstance(testNode, "inst-2"); prev != testInstance {
		t.Errorf("superseded = %q, want %q", prev, testInstance)
	}
	if err := hs.AcceptLocal(old, model.LocalHealth{AttachmentID: "x", NodeName: testNode}); !errors.Is(err, ErrStaleInstance) {
		t.Fatalf("err = %v, want ErrStaleInstance", err)
	}
	// The new instance starts its own sequence, so a low number must still land.
	fresh := model.ReportOrigin{NodeName: testNode, AgentInstance: "inst-2", Sequence: 1}
	if err := hs.AcceptLocal(fresh, model.LocalHealth{AttachmentID: "x", NodeName: testNode}); err != nil {
		t.Fatalf("a new instance must not be blocked by the old sequence: %v", err)
	}
}

func TestSnapshotReplacesNodeStateAtomically(t *testing.T) {
	// A partial apply would be visible: the controller would publish whatever
	// had not arrived yet as not ready. The commit happens in one step.
	hs, _ := newStore(time.Minute)
	putLocal(t, hs, model.LocalHealth{AttachmentID: "gone", InterfaceExists: true, AddressPresent: true, LinkUsable: true})
	putLocal(t, hs, model.LocalHealth{AttachmentID: "kept", InterfaceExists: true, AddressPresent: true, LinkUsable: true})

	o := model.ReportOrigin{NodeName: testNode, AgentInstance: testInstance, Sequence: 100}
	err := hs.ApplySnapshot(o, []model.LocalHealth{{
		AttachmentID: "kept", NodeName: testNode,
		InterfaceExists: true, AddressPresent: true, LinkUsable: true,
	}}, nil)
	if err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if _, ok := hs.Local("gone"); ok {
		t.Error("an attachment missing from the snapshot must be dropped")
	}
	if _, fresh := hs.Local("kept"); !fresh {
		t.Error("an attachment present in the snapshot must survive")
	}
}

func TestSnapshotOnlyTouchesItsOwnNode(t *testing.T) {
	hs, _ := newStore(time.Minute)
	hs.AdoptInstance("node-2", "inst-2")
	if err := hs.AcceptLocal(
		model.ReportOrigin{NodeName: "node-2", AgentInstance: "inst-2", Sequence: 1},
		model.LocalHealth{AttachmentID: "other-node", NodeName: "node-2",
			InterfaceExists: true, AddressPresent: true, LinkUsable: true}); err != nil {
		t.Fatalf("AcceptLocal: %v", err)
	}
	o := model.ReportOrigin{NodeName: testNode, AgentInstance: testInstance, Sequence: 100}
	if err := hs.ApplySnapshot(o, nil, nil); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if _, ok := hs.Local("other-node"); !ok {
		t.Error("one node's snapshot must not clear another node's state")
	}
}

func TestExpiryFiresOncePerEntry(t *testing.T) {
	hs, clk := newStore(5 * time.Second)
	putLocal(t, hs, model.LocalHealth{AttachmentID: "a", InterfaceExists: true, AddressPresent: true, LinkUsable: true})
	if got := hs.SweepExpired(); len(got) != 0 {
		t.Fatalf("nothing should be expired yet, got %v", got)
	}
	clk.advance(6 * time.Second)
	got := hs.SweepExpired()
	if len(got) != 1 || got[0].ID != "a" || got[0].Kind != "local" {
		t.Fatalf("expired = %v, want one local entry \"a\"", got)
	}
	if again := hs.SweepExpired(); len(again) != 0 {
		t.Error("an entry must only be reported expired once")
	}
}

func TestNodeScopeSharesOnePathResult(t *testing.T) {
	// The reason the two streams are split: under Node scope a single probe
	// result covers every attachment in the (node, NAD) domain. It is stored
	// once and read by each attachment, never copied per Pod.
	hs, _ := newStore(time.Minute)
	now := time.Now()

	pods := []model.Attachment{
		att("10.0.0.1", "net1", "a", true),
		att("10.0.0.2", "net1", "b", true),
		att("10.0.0.3", "net1", "c", true),
	}
	for _, p := range pods {
		putLocal(t, hs, healthy(p, now))
	}

	domain := model.PathDomainID("node-1", nadX)
	putPath(t, hs, model.PathHealth{
		Scope: model.ScopeNode, ScopeID: domain, NodeName: "node-1", NAD: nadX,
		PathReady: true, ObservedAt: now,
	})

	if _, paths := hs.Len(); paths != 1 {
		t.Fatalf("path entries = %d, want 1 shared entry for the whole domain", paths)
	}
	for _, p := range pods {
		if got := ComputeReady(p, model.ScopeNode, hs); !got.Ready {
			t.Errorf("%s not ready under a shared domain result: %s", p.PodName, got.Reason)
		}
	}

	// One shared transition moves every attachment at once -- no per-Pod skew.
	putPath(t, hs, model.PathHealth{
		Scope: model.ScopeNode, ScopeID: domain, NodeName: "node-1", NAD: nadX,
		PathReady: false, ObservedAt: now.Add(time.Second),
	})
	for _, p := range pods {
		if got := ComputeReady(p, model.ScopeNode, hs); got.Ready {
			t.Errorf("%s stayed ready after the shared domain went down", p.PodName)
		}
	}
}

func TestPathKeyDependsOnScope(t *testing.T) {
	a := att("10.0.0.1", "net1", "a", true)
	if a.PathKey(model.ScopeEndpoint) != a.ID() {
		t.Error("Endpoint scope must key path state per attachment")
	}
	if a.PathKey(model.ScopeNode) != model.PathDomainID(a.NodeName, a.NAD) {
		t.Error("Node scope must key path state per (node, NAD)")
	}
	b := att("10.0.0.2", "net1", "b", true)
	if a.PathKey(model.ScopeNode) != b.PathKey(model.ScopeNode) {
		t.Error("two attachments of one NAD on one node must share a domain")
	}
	if a.PathKey(model.ScopeEndpoint) == b.PathKey(model.ScopeEndpoint) {
		t.Error("Endpoint scope must not collapse distinct attachments")
	}
}

func TestForgettingOneAttachmentKeepsSharedPath(t *testing.T) {
	hs, _ := newStore(time.Minute)
	now := time.Now()
	a := att("10.0.0.1", "net1", "a", true)
	putLocal(t, hs, healthy(a, now))
	putPath(t, hs, model.PathHealth{
		Scope: model.ScopeNode, ScopeID: model.PathDomainID("node-1", nadX),
		PathReady: true, ObservedAt: now,
	})
	hs.ForgetLocal(a.ID())
	if _, paths := hs.Len(); paths != 1 {
		t.Error("a departing attachment must not invalidate a shared domain result")
	}
}

// ---------------------------------------------------------------- slices

func TestDualStackSplitsIntoTwoSlices(t *testing.T) {
	hs, _ := newStore(time.Minute)
	want, _, _ := build(t, hs,
		att("10.100.61.10", "net1", "amf-0", true),
		att("fd00:61::10", "net1", "amf-0", true),
	)
	v4, v6 := want["amf-n2-secondary-ipv4"], want["amf-n2-secondary-ipv6"]
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
	hs, _ := newStore(time.Minute)
	x, _, _ := build(t, hs, att("10.0.0.2", "net1", "b", true), att("10.0.0.1", "net1", "a", true))
	y, _, _ := build(t, hs, att("10.0.0.1", "net1", "a", true), att("10.0.0.2", "net1", "b", true))
	if !sliceUpToDate(x["amf-n2-secondary-ipv4"], y["amf-n2-secondary-ipv4"]) {
		t.Error("endpoint ordering must not cause a spurious update")
	}
}

func TestOwnerReferencePointsAtService(t *testing.T) {
	hs, _ := newStore(time.Minute)
	want, _, _ := build(t, hs, att("10.0.0.1", "net1", "a", true))
	or := want["amf-n2-secondary-ipv4"].OwnerReferences
	if len(or) != 1 || or[0].Kind != "Service" || or[0].UID != "svc-uid" || !ptr.Deref(or[0].Controller, false) {
		t.Errorf("ownerReferences = %+v", or)
	}
}

func TestManagedByLabelIsOurs(t *testing.T) {
	hs, _ := newStore(time.Minute)
	want, _, _ := build(t, hs, att("10.0.0.1", "net1", "a", true))
	if got := want["amf-n2-secondary-ipv4"].Labels[discoveryv1.LabelManagedBy]; got != ManagedBy {
		t.Errorf("managed-by = %q, want %q", got, ManagedBy)
	}
}

func TestSliceLimitTruncatesAndWarns(t *testing.T) {
	hs, _ := newStore(time.Minute)
	atts := make([]model.Attachment, 0, maxEndpointsPerSlice+5)
	for i := 0; i < maxEndpointsPerSlice+5; i++ {
		atts = append(atts, att(ipv4(i), "net1", "p"+itoa(i), true))
	}
	want, _, warn := build(t, hs, atts...)
	if !hasReason(warn, "SliceLimit") {
		t.Error("exceeding the 1000-endpoint API limit must be reported, not silently truncated")
	}
	if n := len(want["amf-n2-secondary-ipv4"].Endpoints); n != maxEndpointsPerSlice {
		t.Errorf("endpoints = %d, want %d", n, maxEndpointsPerSlice)
	}
}

// ------------------------------------------------- duplicate address conflict

func TestConflictingAddressExcludesEveryClaimant(t *testing.T) {
	// Picking a winner is deterministic but not correct. DNS carries the
	// address alone, so a client cannot be steered to the Pod the controller
	// chose, and both Pods still own the address in the data plane. Publishing
	// neither is the only honest answer.
	hs, _ := newStore(time.Minute)
	want, _, warn := build(t, hs,
		att("10.244.77.2", "net1", "amf-b", true),
		att("10.244.77.2", "net1", "amf-a", true),
		att("10.244.77.3", "net1", "amf-c", true),
	)
	s := want["amf-n2-secondary-ipv4"]
	if len(s.Endpoints) != 1 || s.Endpoints[0].Addresses[0] != "10.244.77.3" {
		t.Fatalf("only the unconflicted address may be published, got %v", addrsOf(s))
	}
	if !hasReason(warn, "DuplicateAddress") {
		t.Error("the conflict must be reported")
	}
}

func TestConflictWarningNamesEveryClaimant(t *testing.T) {
	hs, _ := newStore(time.Minute)
	_, _, warn := build(t, hs,
		att("10.244.77.2", "net1", "amf-a", true),
		att("10.244.77.2", "net1", "amf-b", true),
		att("10.244.77.2", "net1", "amf-c", true),
	)
	var msg string
	for _, w := range warn {
		if w.Reason == "DuplicateAddress" {
			msg = w.Message
		}
	}
	for _, pod := range []string{"amf-a", "amf-b", "amf-c"} {
		if !containsStr(msg, pod) {
			t.Errorf("warning should name %s, got %q", pod, msg)
		}
	}
}

func TestConflictOnOneFamilyLeavesTheOtherAlone(t *testing.T) {
	hs, _ := newStore(time.Minute)
	want, _, _ := build(t, hs,
		att("10.244.77.2", "net1", "amf-a", true),
		att("10.244.77.2", "net1", "amf-b", true),
		att("fd00::1", "net1", "amf-a", true),
	)
	if s := want["amf-n2-secondary-ipv4"]; s != nil && len(s.Endpoints) > 0 {
		t.Errorf("conflicted IPv4 address was published: %v", addrsOf(s))
	}
	if s := want["amf-n2-secondary-ipv6"]; s == nil || len(s.Endpoints) != 1 {
		t.Error("an unconflicted IPv6 address must still be published")
	}
}

// ---------------------------------------------------------------- helpers

func mut(l model.LocalHealth, f func(*model.LocalHealth)) *model.LocalHealth {
	f(&l)
	return &l
}

func mutPath(p model.PathHealth, f func(*model.PathHealth)) *model.PathHealth {
	f(&p)
	return &p
}

func hasReason(ws []warning, reason string) bool {
	for _, w := range ws {
		if w.Reason == reason {
			return true
		}
	}
	return false
}

func addrsOf(s *discoveryv1.EndpointSlice) []string {
	if s == nil {
		return nil
	}
	out := make([]string, 0, len(s.Endpoints))
	for _, e := range s.Endpoints {
		out = append(out, e.Addresses[0])
	}
	return out
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func ipv4(i int) string { return "10.200." + itoa(i/250) + "." + itoa(i%250+1) }

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
