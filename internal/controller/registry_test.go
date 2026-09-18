package controller

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/boanlab/multus-service/internal/attach"
	"github.com/boanlab/multus-service/internal/model"
)

var svcKey = types.NamespacedName{Namespace: "oai", Name: "amf-n2"}

func TestRegistryIsTheOnlyDiscoveryAuthority(t *testing.T) {
	// An agent report can change the health of something the controller already
	// derived from Service + Pod + network-status. It cannot bring an
	// attachment into existence, so an unknown id has to be unrecognisable.
	r := NewRegistry()
	a := att("10.0.0.1", "net1", "amf-0", true)
	r.SetService(svcKey, []model.Attachment{a}, model.ScopeEndpoint)

	if _, owners, ok := r.Attachment(a.ID()); !ok || len(owners) != 1 || owners[0] != svcKey {
		t.Fatalf("known attachment not resolvable: ok=%v owners=%v", ok, owners)
	}
	if _, _, ok := r.Attachment("0000000000000000"); ok {
		t.Error("an attachment the controller never derived must not be recognised")
	}
}

func TestRegistryForgetsADeletedService(t *testing.T) {
	r := NewRegistry()
	a := att("10.0.0.1", "net1", "amf-0", true)
	r.SetService(svcKey, []model.Attachment{a}, model.ScopeEndpoint)
	r.RemoveService(svcKey)
	if _, _, ok := r.Attachment(a.ID()); ok {
		t.Error("a deleted Service must stop vouching for its attachments")
	}
}

func TestRegistryPathKeyFollowsScope(t *testing.T) {
	r := NewRegistry()
	a := att("10.0.0.1", "net1", "amf-0", true)
	b := att("10.0.0.2", "net1", "amf-1", true)

	r.SetService(svcKey, []model.Attachment{a, b}, model.ScopeEndpoint)
	if _, pk := r.Len(); pk != 2 {
		t.Errorf("Endpoint scope path keys = %d, want 2", pk)
	}

	r.SetService(svcKey, []model.Attachment{a, b}, model.ScopeNode)
	if _, pk := r.Len(); pk != 1 {
		t.Errorf("Node scope path keys = %d, want 1 shared domain", pk)
	}
	owners, ok := r.PathKey(model.PathDomainID("node-1", nadX))
	if !ok || len(owners) != 1 {
		t.Errorf("shared domain not resolvable: ok=%v owners=%v", ok, owners)
	}
}

func TestRegistryReportsEveryOwningService(t *testing.T) {
	// Two Services may select the same Pods on the same NAD. A health change
	// has to re-enqueue both, or one of them keeps publishing a stale verdict.
	r := NewRegistry()
	other := types.NamespacedName{Namespace: "oai", Name: "amf-n2-alt"}
	a := att("10.0.0.1", "net1", "amf-0", true)
	r.SetService(svcKey, []model.Attachment{a}, model.ScopeEndpoint)
	r.SetService(other, []model.Attachment{a}, model.ScopeEndpoint)

	_, owners, ok := r.Attachment(a.ID())
	if !ok || len(owners) != 2 {
		t.Fatalf("owners = %v, want both Services", owners)
	}
}

func TestProbeConfigIsRejectedOnAService(t *testing.T) {
	// Probe settings live on the NAD. A shared Node-scope domain assumes every
	// attachment of one NAD on one node probes the same target; per-Service
	// settings would break that silently.
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: "amf-n2", Namespace: "oai",
		Annotations: map[string]string{
			attach.AnnotationNetwork: "n2-net",
			attach.NADProbeScope:     "node",
			attach.NADHealthTarget:   "10.0.0.1",
		},
	}}
	got := attach.ProbeConfigOnService(svc)
	if len(got) != 2 {
		t.Fatalf("stray probe annotations = %v, want both flagged", got)
	}
	clean := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{attach.AnnotationNetwork: "n2-net"},
	}}
	if got := attach.ProbeConfigOnService(clean); len(got) != 0 {
		t.Errorf("clean Service flagged: %v", got)
	}
}

func TestProbeScopeDefaultsToEndpoint(t *testing.T) {
	// Node scope rests on a topology assumption that is false for macvlan and
	// ipvlan toward same-node endpoints, so it must be asked for.
	for _, raw := range []string{"", "endpoint", "Endpoint"} {
		s, err := attach.ParseProbeScope(raw)
		if err != nil || s != model.ScopeEndpoint {
			t.Errorf("ParseProbeScope(%q) = %v, %v", raw, s, err)
		}
	}
	if s, err := attach.ParseProbeScope("node"); err != nil || s != model.ScopeNode {
		t.Errorf("ParseProbeScope(node) = %v, %v", s, err)
	}
	if _, err := attach.ParseProbeScope("cluster"); err == nil {
		t.Error("an unknown scope must be an error, not a silent default")
	}
}

func TestSharedDomainOneHysteresisOneTransition(t *testing.T) {
	// The point of splitting the two report kinds: a Node-scope domain holds
	// one result, so its attachments move together instead of drifting apart on
	// per-copy hysteresis.
	hs, _ := newStore(time.Minute)
	pods := []model.Attachment{
		att("10.0.0.1", "net1", "a", true),
		att("10.0.0.2", "net1", "b", true),
	}
	for _, p := range pods {
		putLocal(t, hs, healthy(p, time.Now()))
	}
	domain := model.PathDomainID("node-1", nadX)
	putPath(t, hs, model.PathHealth{Scope: model.ScopeNode, ScopeID: domain, NAD: nadX, PathReady: true})

	if _, paths := hs.Len(); paths != 1 {
		t.Fatalf("path entries = %d, want 1", paths)
	}
	for _, p := range pods {
		if r := ComputeReady(p, model.ScopeNode, hs); !r.Ready {
			t.Errorf("%s: %s", p.PodName, r.Reason)
		}
	}
}
