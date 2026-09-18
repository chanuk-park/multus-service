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
	"time"

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
	"github.com/boanlab/multus-service/internal/obs"
)

var scheme = runtime.NewScheme()

func init() { _ = clientgoscheme.AddToScheme(scheme) }

func main() {
	var (
		nodeName   string
		endpoint   string
		eventsFile string
		resync     time.Duration
		probeAddr  string
		metricsAdr string
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

	zapOpts := zap.Options{Development: true}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	setupLog := ctrl.Log.WithName("setup")

	if nodeName == "" {
		setupLog.Error(nil, "--node-name or NODE_NAME is required")
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

	a := &agent.Agent{
		Client:   mgr.GetClient(),
		NodeName: nodeName,
		Resolver: agentnetns.NewCache(resolver),
		Monitor:  local.NewMonitor(256),
		Sink:     agent.LogSink{Events: events},
		Events:   events,
		Resync:   resync,
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

	setupLog.Info("starting agent", "node", nodeName, "runtime", endpoint)
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}
