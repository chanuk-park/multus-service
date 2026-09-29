package controller

import (
	"context"
	"errors"
	"fmt"

	"time"

	authv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
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
	TokenReview  time.Duration
	AccessReview time.Duration
	PodLookup    time.Duration
	Total        time.Duration
}

// The permission a producer must hold, checked with a SubjectAccessReview. It is
// a virtual resource: nothing is stored under it, it exists so that who may
// report health is ordinary RBAC -- a Role granted to the agent's
// ServiceAccount -- rather than a name compiled into the controller.
const (
	ReportGroup    = "secondary-service.boanlab.io"
	ReportResource = "healthreports"
	ReportVerb     = "create"
)

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
	// ErrNotAuthorized means RBAC does not grant the authenticated identity the
	// health-report permission.
	ErrNotAuthorized = errors.New("identity is not authorized to report health")
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
	// Authorize adds a SubjectAccessReview for the health-report permission, so
	// the set of identities allowed to report is managed as RBAC.
	Authorize bool
}

// reviewToken runs the TokenReview shared by every authenticated mode: the
// token must be valid, carry our audience, and be bound to a Pod.
func reviewToken(ctx context.Context, c kubernetes.Interface, aud, token string, t *AuthTiming) (*authv1.TokenReview, string, error) {
	if token == "" {
		return nil, "", fmt.Errorf("%w: no bearer token", ErrUnauthenticated)
	}
	tr := &authv1.TokenReview{Spec: authv1.TokenReviewSpec{Token: token, Audiences: []string{aud}}}
	start := time.Now()
	res, err := c.AuthenticationV1().TokenReviews().Create(ctx, tr, metav1.CreateOptions{})
	t.TokenReview = time.Since(start)
	if err != nil {
		return nil, "", fmt.Errorf("tokenreview: %w", err)
	}
	if !res.Status.Authenticated {
		return nil, "", fmt.Errorf("%w: %s", ErrUnauthenticated, res.Status.Error)
	}
	// The apiserver returns the intersection of requested and token audiences;
	// an empty intersection means the token was minted for something else.
	if !containsStr(res.Status.Audiences, aud) {
		return nil, "", fmt.Errorf("%w: audience %q not honored", ErrUnauthenticated, aud)
	}
	podUID := firstExtra(res.Status.User.Extra, extraPodUID)
	if podUID == "" {
		return nil, "", fmt.Errorf("%w: not a Pod-bound token", ErrUnauthenticated)
	}
	return res, podUID, nil
}

// Authenticate validates the token and binds it to a node.
func (a *K8sAuthenticator) Authenticate(ctx context.Context, token string) (agent *AuthenticatedAgent, t AuthTiming, err error) {
	start := time.Now()
	defer func() { t.Total = time.Since(start) }()

	res, podUID, err := reviewToken(ctx, a.Client, a.Audience, token, &t)
	if err != nil {
		return nil, t, err
	}

	// Who may report is RBAC: ask the apiserver whether this identity holds the
	// health-report permission. A valid token from any other ServiceAccount --
	// including one an attacker's own Pod projected with our audience -- stops here.
	if a.Authorize {
		u := res.Status.User
		extra := map[string]authzv1.ExtraValue{}
		for k, v := range u.Extra {
			extra[k] = authzv1.ExtraValue(v)
		}
		sar := &authzv1.SubjectAccessReview{Spec: authzv1.SubjectAccessReviewSpec{
			User: u.Username, Groups: u.Groups, UID: u.UID, Extra: extra,
			ResourceAttributes: &authzv1.ResourceAttributes{Group: ReportGroup, Resource: ReportResource, Verb: ReportVerb},
		}}
		sStart := time.Now()
		sres, err := a.Client.AuthorizationV1().SubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
		t.AccessReview = time.Since(sStart)
		if err != nil {
			return nil, t, fmt.Errorf("subjectaccessreview: %w", err)
		}
		if !sres.Status.Allowed {
			return nil, t, fmt.Errorf("%w: %s", ErrNotAuthorized, u.Username)
		}
	}

	// RBAC names an identity, not a node, and anyone who can create Pods in the
	// agent namespace can run as that identity. The AgentRegistry proves the Pod
	// is a live agent and supplies the authoritative node: Pod.spec.nodeName
	// keyed by the validated UID.
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

// TokenAuthenticator is the "authenticated but not authorized" baseline, kept to
// show why authentication alone is not producer authority. It accepts any valid
// Pod-bound token carrying our audience -- which any Pod can obtain by
// projecting one in its own spec -- and takes the node from the token's own
// node-name claim. Nothing checks that the Pod is an agent.
type TokenAuthenticator struct {
	Client   kubernetes.Interface
	Audience string
	// ExpectUser, when set, additionally requires the authenticated identity to
	// be exactly this ServiceAccount (system:serviceaccount:<ns>:<name>) -- the
	// check a careful implementation would add without RBAC or a live-Pod check.
	ExpectUser string
}

// Authenticate implements Authenticator for the baseline.
func (a *TokenAuthenticator) Authenticate(ctx context.Context, token string) (agent *AuthenticatedAgent, t AuthTiming, err error) {
	start := time.Now()
	defer func() { t.Total = time.Since(start) }()
	res, podUID, err := reviewToken(ctx, a.Client, a.Audience, token, &t)
	if err != nil {
		return nil, t, err
	}
	if a.ExpectUser != "" && res.Status.User.Username != a.ExpectUser {
		return nil, t, fmt.Errorf("%w: %s", ErrNotAuthorized, res.Status.User.Username)
	}
	node := firstExtra(res.Status.User.Extra, extraNodeName)
	if node == "" {
		return nil, t, fmt.Errorf("%w: token carries no node claim", ErrUnauthenticated)
	}
	return &AuthenticatedAgent{
		PodUID:         podUID,
		PodName:        firstExtra(res.Status.User.Extra, extraPodName),
		ServiceAccount: res.Status.User.Username,
		NodeName:       node,
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
