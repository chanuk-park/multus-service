// Command controller runs the Discovery/Ready controller.
//
// It owns Kubernetes state only: it watches Services, Pods and its own
// EndpointSlices, and publishes the secondary addresses it finds. Network
// measurement belongs to the node agents, which report in over gRPC.
package main

import (
	"flag"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	if err := mgr.Add(&controller.Serve{
		Addr: healthAddr,
		Server: &controller.HealthServer{
			Store:    health,
			Registry: registry,
			Events:   events,
			Notify:   r.Enqueue,
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
		"health_ttl_ms", healthTTL.Milliseconds(), "health_addr", healthAddr)
	setupLog.Info("starting controller", "healthTTL", healthTTL, "healthAddr", healthAddr)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}
