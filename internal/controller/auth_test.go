package controller

import (
	"context"
	"errors"
	"testing"

	authv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// fakeAPI answers TokenReview with a Pod-bound identity and SubjectAccessReview
// with the given decision, the two calls the authenticators make.
func fakeAPI(podUID, sa, claimedNode string, allowed bool) *fake.Clientset {
	c := fake.NewSimpleClientset()
	c.PrependReactor("create", "tokenreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		tr := a.(k8stesting.CreateAction).GetObject().(*authv1.TokenReview)
		tr.Status = authv1.TokenReviewStatus{
			Authenticated: true,
			Audiences:     tr.Spec.Audiences,
			User: authv1.UserInfo{
				Username: "system:serviceaccount:" + sa,
				Extra: map[string]authv1.ExtraValue{
					extraPodUID:   {podUID},
					extraPodName:  {"p"},
					extraNodeName: {claimedNode},
				},
			},
		}
		return true, tr, nil
	})
	c.PrependReactor("create", "subjectaccessreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		sar := a.(k8stesting.CreateAction).GetObject().(*authzv1.SubjectAccessReview)
		ra := sar.Spec.ResourceAttributes
		if ra == nil || ra.Group != ReportGroup || ra.Resource != ReportResource || ra.Verb != ReportVerb {
			return true, nil, errors.New("unexpected access review")
		}
		sar.Status.Allowed = allowed
		return true, sar, nil
	})
	return c
}

// A valid token for an identity RBAC does not grant is refused, even when the
// Pod would otherwise be a registered agent.
func TestFullModeRefusesIdentityWithoutRBAC(t *testing.T) {
	reg := NewAgentRegistry()
	reg.upsert(types.NamespacedName{Namespace: "ns", Name: "p"}, AgentInfo{PodUID: "u1", NodeName: "node-a"})
	a := &K8sAuthenticator{Client: fakeAPI("u1", "attacker:default", "node-a", false), Audience: "aud",
		Agents: reg, Authorize: true}
	if _, _, err := a.Authenticate(context.Background(), "tok"); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("want ErrNotAuthorized, got %v", err)
	}
}

// RBAC-granted but not a live agent Pod: refused. Granted and live: the node
// comes from the registry (Pod.spec.nodeName), not from the token's claim.
func TestFullModeNeedsLiveAgentAndBindsNodeFromPod(t *testing.T) {
	reg := NewAgentRegistry()
	a := &K8sAuthenticator{Client: fakeAPI("u1", "sys:agent", "node-claimed", true), Audience: "aud",
		Agents: reg, Authorize: true}
	if _, _, err := a.Authenticate(context.Background(), "tok"); !errors.Is(err, ErrNotAnAgent) {
		t.Fatalf("want ErrNotAnAgent, got %v", err)
	}
	reg.upsert(types.NamespacedName{Namespace: "ns", Name: "p"}, AgentInfo{PodUID: "u1", NodeName: "node-a"})
	ag, tm, err := a.Authenticate(context.Background(), "tok")
	if err != nil {
		t.Fatal(err)
	}
	if ag.NodeName != "node-a" {
		t.Fatalf("node from %q, want the Pod's node-a", ag.NodeName)
	}
	if tm.Total <= 0 {
		t.Fatal("timing not recorded")
	}
}

// The baseline accepts any Pod-bound token with the audience and trusts the
// token's node claim: an ordinary Pod on node-a becomes a producer for node-a.
func TestTokenModeAcceptsAnyPodOnItsNode(t *testing.T) {
	a := &TokenAuthenticator{Client: fakeAPI("u9", "attacker:default", "node-a", false), Audience: "aud"}
	ag, _, err := a.Authenticate(context.Background(), "tok")
	if err != nil {
		t.Fatal(err)
	}
	if ag.NodeName != "node-a" {
		t.Fatalf("node %q", ag.NodeName)
	}
}
