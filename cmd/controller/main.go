// Command controller runs the Discovery/Ready controller.
//
// It owns Kubernetes state only: it watches Services, Pods and its own
// EndpointSlices, and publishes the secondary addresses it finds. Network
// measurement belongs to the node agents, which report in over gRPC.
package main

import (
	"flag"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/boanlab/multus-service/internal/controller"
	"github.com/boanlab/multus-service/internal/obs"
)

var scheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = discoveryv1.AddToScheme(scheme)
}

func main() {
	var (
		metricsAddr string
		probeAddr   string
		leaderElect bool
		eventsFile  string
		healthTTL   time.Duration
		healthAddr  string
		healthAud   string
		agentNS     string
		agentSA     string
		agentLabel  string
		tlsDir      string
		requireAuth bool
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "liveness/readiness endpoint")
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election")
	flag.StringVar(&eventsFile, "events-file", "",
		"JSONL measurement event stream; '-' for stdout, empty to disable")
	flag.DurationVar(&healthTTL, "health-ttl", 3*time.Second,
		"how long an agent health report stays fresh, measured from when the controller accepted it; "+
			"past this an attachment is Unknown and published as not ready")
	flag.StringVar(&healthAddr, "health-bind-address", ":9090",
		"gRPC listener the node agents report to")
	flag.StringVar(&healthAud, "health-audience", "health-controller",
		"token audience the agents must present; a dedicated value stops a plain "+
			"kube-api ServiceAccount token being reused as a health credential")
	flag.StringVar(&agentNS, "agent-namespace", "multus-service-system",
		"namespace the node agents run in")
	flag.StringVar(&agentSA, "agent-service-account", "multus-service-agent",
		"ServiceAccount the node agents run as; a valid token from any other "+
			"identity is refused before the agent registry is consulted")
	flag.StringVar(&agentLabel, "agent-label", "app=multus-service-agent",
		"label key=value identifying agent Pods")
	flag.StringVar(&tlsDir, "tls-dir", "/etc/multus-service/tls",
		"directory holding tls.crt and tls.key for the health transport; "+
			"empty serves plaintext (never in production)")
	flag.BoolVar(&requireAuth, "require-agent-auth", true,
		"require producer authentication/authorization (G2): TokenReview + AgentRegistry + "+
			"node binding. false keeps TLS, the Registry (G1) and instance/sequence/lease (G3) "+
			"but trusts the self-asserted envelope node -- for the RQ3 cost comparison only")

	zapOpts := zap.Options{Development: true}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	setupLog := ctrl.Log.WithName("setup")

	events, err := obs.NewRecorder(eventsFile)
	if err != nil {
		setupLog.Error(err, "opening events file", "path", eventsFile)
		os.Exit(1)
	}
	defer events.Close()

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "secondary-service.boanlab.io",
	})
	if err != nil {
		setupLog.Error(err, "creating manager")
		os.Exit(1)
	}

	health := controller.NewHealthStore(healthTTL)
	registry := controller.NewRegistry()
	agents := controller.NewAgentRegistry()
	sessions := controller.NewSessionManager()

	// A deregistered agent loses its live streams immediately, so a deleted
	// agent Pod (or an attacker holding its retired token) cannot keep reporting.
	agents.OnRemove(func(podUID string) {
		if n := sessions.Revoke(podUID); n > 0 {
			events.Emit("sessions_revoked", "pod_uid", podUID, "streams", n)
		}
	})

	labelKey, labelVal, ok := strings.Cut(agentLabel, "=")
	if !ok {
		setupLog.Error(nil, "--agent-label must be key=value", "value", agentLabel)
		os.Exit(1)
	}

	authClient, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "building TokenReview client")
		os.Exit(1)
	}
	authn := &controller.K8sAuthenticator{
		Client:               authClient,
		Audience:             healthAud,
		Agents:               agents,
		ExpectNamespace:      agentNS,
		ExpectServiceAccount: agentSA,
	}

	if err := (&controller.AgentPodReconciler{
		Client:   mgr.GetClient(),
		Registry: agents,
		Events:   events,
		LabelKey: labelKey,
		LabelVal: labelVal,
		AgentSA:  agentSA,
		AgentsNS: agentNS,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "registering agent-pod controller")
		os.Exit(1)
	}

	r := &controller.ServiceReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("secondary-service"),
		Health:       health,
		Events:       events,
		Registry:     registry,
		HealthEvents: make(chan event.TypedGenericEvent[*corev1.Service], 1024),
	}
	if err := r.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "registering service controller")
		os.Exit(1)
	}

	// The health transport and the expiry sweeper run only on the leader.
	// A standby with an empty registry would reject every report it received.
	tlsCert, tlsKey := "", ""
	if tlsDir != "" {
		tlsCert, tlsKey = tlsDir+"/tls.crt", tlsDir+"/tls.key"
	}
	if err := mgr.Add(&controller.Serve{
		Addr:    healthAddr,
		TLSCert: tlsCert,
		TLSKey:  tlsKey,
		Server: &controller.HealthServer{
			Store:       health,
			Registry:    registry,
			Events:      events,
			Auth:        authn,
			Sessions:    sessions,
			RequireAuth: requireAuth,
			Notify:      r.Enqueue,
		},
	}); err != nil {
		setupLog.Error(err, "registering health transport")
		os.Exit(1)
	}

	if err := mgr.Add(&controller.ExpirySweeper{
		Store:    health,
		Registry: registry,
		Events:   events,
		Notify:   r.Enqueue,
		Interval: healthTTL / 3,
	}); err != nil {
		setupLog.Error(err, "registering expiry sweeper")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		setupLog.Error(err, "adding healthz")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("ping", healthz.Ping); err != nil {
		setupLog.Error(err, "adding readyz")
		os.Exit(1)
	}

	events.Emit("controller_started",
		"health_ttl_ms", healthTTL.Milliseconds(), "health_addr", healthAddr,
		"health_audience", healthAud, "tls", tlsCert != "", "require_agent_auth", requireAuth)
	setupLog.Info("starting controller", "healthTTL", healthTTL, "healthAddr", healthAddr)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}
