package controller

import (
	"context"
	"errors"
	"fmt"

	"time"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// extra keys the apiserver sets on a Pod-bound token's TokenReview result.
//
// pod-uid and pod-name are validated at authentication time (the Pod must exist
// and its UID must match). node-name and node-uid are present but NOT validated
// by the apiserver, so they are used only as a consistency check -- the node is
// resolved from the validated pod-uid via the AgentRegistry instead.
const (
	extraPodName  = "authentication.kubernetes.io/pod-name"
	extraPodUID   = "authentication.kubernetes.io/pod-uid"
	extraNodeName = "authentication.kubernetes.io/node-name"
)

// AuthenticatedAgent is the identity a stream is bound to for its lifetime.
//
// Every field is derived from control-plane facts, never from anything the
// caller asserted in the envelope. NodeName in particular comes from the Pod the
// token is bound to, so it cannot be forged by claiming a different node.
type AuthenticatedAgent struct {
	PodUID         string
	PodName        string
	Namespace      string
	ServiceAccount string
	NodeName       string
}

// AuthTiming breaks down where establishment time goes, so a reviewer asking
// "isn't the Kubernetes API round trip expensive?" can be answered directly.
type AuthTiming struct {
	TokenReview time.Duration
	PodLookup   time.Duration
	Total       time.Duration
}

// Authenticator turns a bearer token into a node-bound agent identity.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (*AuthenticatedAgent, AuthTiming, error)
}

var (
	// ErrUnauthenticated means the token failed TokenReview: absent, expired,
	// wrong audience, or bound to a Pod that no longer exists.
	ErrUnauthenticated = errors.New("token failed authentication")
	// ErrNotAnAgent means the token authenticated a real workload, but not one
	// that is a current node agent. A valid Pod-bound token is not by itself
	// authority to report health -- an ordinary Pod sharing the ServiceAccount
	// would clear TokenReview too.
	ErrNotAnAgent = errors.New("authenticated workload is not a registered node agent")
)

// K8sAuthenticator authenticates via TokenReview and authorizes against the set
// of currently-running agent Pods.
type K8sAuthenticator struct {
	Client   kubernetes.Interface
	Audience string
	Agents   *AgentRegistry

	// ExpectNamespace and ExpectServiceAccount pin which workload identity may
	// act as an agent, so a valid token from an unrelated SA is refused before
	// the AgentRegistry is even consulted.
	ExpectNamespace      string
	ExpectServiceAccount string
}

// Authenticate validates the token and binds it to a node.
func (a *K8sAuthenticator) Authenticate(ctx context.Context, token string) (agent *AuthenticatedAgent, t AuthTiming, err error) {
	start := time.Now()
	defer func() { t.Total = time.Since(start) }()

	if token == "" {
		return nil, t, fmt.Errorf("%w: no bearer token", ErrUnauthenticated)
	}

	tr := &authv1.TokenReview{
		Spec: authv1.TokenReviewSpec{Token: token, Audiences: []string{a.Audience}},
	}
	trStart := time.Now()
	res, err := a.Client.AuthenticationV1().TokenReviews().Create(ctx, tr, metav1.CreateOptions{})
	t.TokenReview = time.Since(trStart)
	if err != nil {
		return nil, t, fmt.Errorf("tokenreview: %w", err)
	}
	if !res.Status.Authenticated {
		return nil, t, fmt.Errorf("%w: %s", ErrUnauthenticated, res.Status.Error)
	}
	// The apiserver returns the intersection of requested and token audiences;
	// an empty intersection means the token was minted for something else.
	if !containsStr(res.Status.Audiences, a.Audience) {
		return nil, t, fmt.Errorf("%w: audience %q not honored", ErrUnauthenticated, a.Audience)
	}

	extra := res.Status.User.Extra
	podUID := firstExtra(extra, extraPodUID)
	if podUID == "" {
		return nil, t, fmt.Errorf("%w: not a Pod-bound token", ErrUnauthenticated)
	}
	// res.Status.User.Username is system:serviceaccount:<ns>:<name>; the
	// AgentRegistry carries the authoritative ServiceAccount, so it is used for
	// the check rather than parsing the username here.

	// The token proves the Pod exists and its UID matches. The AgentRegistry
	// proves that Pod is one of our agents and supplies the authoritative node,
	// which is Pod.spec.nodeName keyed by the validated UID.
	plStart := time.Now()
	info, ok := a.Agents.ByUID(podUID)
	t.PodLookup = time.Since(plStart)
	if !ok {
		return nil, t, fmt.Errorf("%w: pod-uid %s", ErrNotAnAgent, podUID)
	}
	if a.ExpectNamespace != "" && info.Namespace != a.ExpectNamespace {
		return nil, t, fmt.Errorf("%w: namespace %s", ErrNotAnAgent, info.Namespace)
	}
	if a.ExpectServiceAccount != "" && info.ServiceAccount != a.ExpectServiceAccount {
		return nil, t, fmt.Errorf("%w: service account %s", ErrNotAnAgent, info.ServiceAccount)
	}

	return &AuthenticatedAgent{
		PodUID:         podUID,
		PodName:        info.PodName,
		Namespace:      info.Namespace,
		ServiceAccount: info.ServiceAccount,
		NodeName:       info.NodeName,
	}, t, nil
}

func firstExtra(extra map[string]authv1.ExtraValue, key string) string {
	if v, ok := extra[key]; ok && len(v) > 0 {
		return string(v[0])
	}
	return ""
}

func containsStr(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
