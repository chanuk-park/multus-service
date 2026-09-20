// Command agent runs the per-node observer.
//
// It resolves each Pod's sandbox network namespace through the CRI runtime,
// enters it, and reports what the secondary interface actually looks like.
// It never decides readiness: that also depends on Pod state and on report
// freshness, which only the controller can judge.
package main

import (
	"flag"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/boanlab/multus-service/internal/agent"
	"github.com/boanlab/multus-service/internal/agent/local"
	agentnetns "github.com/boanlab/multus-service/internal/agent/netns"
	"github.com/boanlab/multus-service/internal/agent/probe"
	"github.com/boanlab/multus-service/internal/obs"
)

var scheme = runtime.NewScheme()

func init() { _ = clientgoscheme.AddToScheme(scheme) }

func hostOf(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[:i]
	}
	return addr
}

func main() {
	var (
		nodeName   string
		endpoint   string
		eventsFile string
		resync     time.Duration
		probeAddr  string
		metricsAdr string
		ctrlAddr   string
		ctrlName   string
		tokenPath  string
		caPath     string
		refresh    time.Duration
		maxSession time.Duration
		pathMode   string
		probeEvery time.Duration
		probeWait  time.Duration
		failK      int
		successM   int
		evalMode   bool
	)
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"),
		"name of the node this agent observes; usually injected via the downward API")
	flag.StringVar(&endpoint, "runtime-endpoint", "unix:///run/k3s/containerd/containerd.sock",
		"CRI runtime endpoint used to resolve Pod UID to sandbox netns")
	flag.StringVar(&eventsFile, "events-file", "-",
		"JSONL measurement event stream; '-' for stdout, empty to disable")
	flag.DurationVar(&resync, "resync", 2*time.Second,
		"upper bound on how long a missed netlink event goes unnoticed; also the report heartbeat")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "liveness/readiness endpoint")
	flag.StringVar(&metricsAdr, "metrics-bind-address", "0", "metrics endpoint; 0 disables")
	flag.StringVar(&ctrlAddr, "controller-address", os.Getenv("CONTROLLER_ADDRESS"),
		"controller health transport, host:port; empty observes locally without reporting")
	flag.StringVar(&ctrlName, "controller-server-name", os.Getenv("CONTROLLER_SERVER_NAME"),
		"TLS server name to verify the controller certificate against")
	flag.StringVar(&tokenPath, "token-path", "/var/run/secrets/multus-service/health-token",
		"projected Pod-bound token presented to the controller as a bearer credential")
	flag.StringVar(&caPath, "ca-path", "/etc/multus-service/ca/ca.crt",
		"CA certificate for server-authenticated TLS to the controller")
	flag.StringVar(&pathMode, "path-probe", agent.PathNone,
		"path state source: none | icmp | assume-ready. "+
			"assume-ready is a synthetic scaffold that reports every path up without probing")
	flag.DurationVar(&probeEvery, "probe-interval", 500*time.Millisecond,
		"how often each path is sampled")
	flag.DurationVar(&probeWait, "probe-timeout", 400*time.Millisecond,
		"how long one sample waits for a reply. Keep it below --probe-interval: a "+
			"failing path fails by timing out, and a timeout longer than the interval "+
			"stretches the effective sampling period and with it the time the failure "+
			"thresholds take to trip")
	flag.IntVar(&failK, "failure-threshold", 3,
		"consecutive failed samples before a path is declared down")
	flag.IntVar(&successM, "success-threshold", 2,
		"consecutive successful samples before a path is declared up again")
	flag.BoolVar(&evalMode, "evaluation-mode", false,
		"refuse to start with a synthetic path source, so measurement runs cannot "+
			"silently report numbers the system never measured")
	flag.DurationVar(&maxSession, "max-session", 300*time.Second,
		"cap a health stream's lifetime, then reconnect to re-read the projected token; "+
			"keep below the token lifetime so a rotated credential is picked up. 0 disables")
	flag.DurationVar(&refresh, "refresh", 1*time.Second,
		"how often the full state is resent regardless of change; must stay well under the controller's health TTL")

	zapOpts := zap.Options{Development: true}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	setupLog := ctrl.Log.WithName("setup")

	if nodeName == "" {
		setupLog.Error(nil, "--node-name or NODE_NAME is required")
		os.Exit(1)
	}
	// A synthetic path source produces plausible, entirely made-up convergence
	// numbers. Refusing to start is cheaper than discovering later that a graph
	// was drawn from them.
	if evalMode && pathMode != agent.PathICMP {
		setupLog.Error(nil, "--evaluation-mode requires a real path probe",
			"path-probe", pathMode, "want", agent.PathICMP)
		os.Exit(1)
	}
	switch pathMode {
	case agent.PathNone, agent.PathAssumeReady, agent.PathICMP:
	default:
		setupLog.Error(nil, "unknown --path-probe", "value", pathMode)
		os.Exit(1)
	}

	events, err := obs.NewRecorder(eventsFile)
	if err != nil {
		setupLog.Error(err, "opening events file", "path", eventsFile)
		os.Exit(1)
	}
	defer events.Close()

	ctx := ctrl.SetupSignalHandler()

	resolver, err := agentnetns.NewCRIResolver(ctx, endpoint)
	if err != nil {
		setupLog.Error(err, "connecting to the container runtime", "endpoint", endpoint)
		os.Exit(1)
	}
	defer resolver.Close()

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAdr},
		HealthProbeBindAddress: probeAddr,
		// NetworkAttachmentDefinitions are read as unstructured objects; without
		// this they would be fetched from the API server on every sweep.
		Client: client.Options{Cache: &client.CacheOptions{Unstructured: true}},
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				// Only this node's Pods are ever relevant, and on a large
				// cluster caching the rest would dwarf everything else here.
				&corev1.Pod{}: {
					Field: fields.OneTermEqualSelector("spec.nodeName", nodeName),
				},
			},
		},
	})
	if err != nil {
		setupLog.Error(err, "creating manager")
		os.Exit(1)
	}

	// A fresh instance id per process is what lets the controller discard
	// reports still in flight from a previous agent, without either side having
	// to trust the other's clock.
	instanceID := uuid.New().String()

	var sink agent.Sink = agent.NopSink{}
	if ctrlAddr != "" {
		// TLS and the bearer token are only wired when their files exist, so the
		// same binary still runs plaintext in a bare test harness.
		usableCA, usableTok := caPath, tokenPath
		if _, err := os.Stat(caPath); err != nil {
			usableCA = ""
		}
		if _, err := os.Stat(tokenPath); err != nil {
			usableTok = ""
		}
		sn := ctrlName
		if sn == "" {
			sn = hostOf(ctrlAddr)
		}
		gs := &agent.GRPCSink{
			Addr:       ctrlAddr,
			NodeName:   nodeName,
			InstanceID: instanceID,
			Events:     events,
			Refresh:    refresh,
			MaxSession: maxSession,
			TokenPath:  usableTok,
			CAPath:     usableCA,
			ServerName: sn,
		}
		if err := mgr.Add(gs); err != nil {
			setupLog.Error(err, "registering health sink")
			os.Exit(1)
		}
		sink = gs
	} else {
		setupLog.Info("no controller address; observing locally without reporting")
	}

	a := &agent.Agent{
		Client:       mgr.GetClient(),
		NodeName:     nodeName,
		Resolver:     agentnetns.NewCache(resolver),
		Monitor:      local.NewMonitor(256),
		Sink:         sink,
		Events:       events,
		Resync:       resync,
		PathMode:     pathMode,
		ProbeTimeout: probeWait,
	}

	if pathMode == agent.PathICMP {
		pm := probe.NewManager(probe.NewICMPProber(), probeEvery, failK, successM, nodeName, events)
		if err := mgr.Add(pm); err != nil {
			setupLog.Error(err, "registering probe manager")
			os.Exit(1)
		}
		a.Probes = pm
	}
	if err := a.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "registering agent")
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

	setupLog.Info("starting agent", "node", nodeName, "runtime", endpoint,
		"instance", instanceID, "controller", ctrlAddr)
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}
