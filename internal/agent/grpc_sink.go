package agent

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sigs.k8s.io/controller-runtime/pkg/log"

	healthpb "github.com/boanlab/multus-service/api/healthpb"
	"github.com/boanlab/multus-service/internal/model"
	"github.com/boanlab/multus-service/internal/obs"
)

// GRPCSink ships observations to the controller over one long-lived stream.
//
// The connection is treated as the unit of trust. A fresh stream always begins
// with a full snapshot, because neither side can assume the other survived: a
// controller restart empties the health store, and an agent that only sent
// deltas would stay silent for as long as the network stayed healthy, stranding
// every endpoint at Unknown.
type GRPCSink struct {
	Addr       string
	NodeName   string
	InstanceID string
	Events     *obs.Recorder

	// TokenPath is the projected Pod-bound token presented as a bearer
	// credential. Empty disables per-RPC auth (tests only).
	TokenPath string
	// CAPath is the controller CA for server-authenticated TLS. Empty means
	// plaintext (tests only).
	CAPath     string
	ServerName string

	// Refresh bounds how long the controller can go without hearing the full
	// state, independent of whether anything changed. It must stay well under
	// the controller's freshness TTL.
	Refresh time.Duration

	mu           sync.Mutex
	stream       healthpb.HealthReporter_SyncClient
	needSnapshot bool
	lastSnapshot time.Time

	seq   atomic.Uint64
	epoch atomic.Uint64
}

// Start maintains the connection until ctx is cancelled. It satisfies
// manager.Runnable.
func (s *GRPCSink) Start(ctx context.Context) error {
	lg := log.FromContext(ctx).WithName("health-sink")
	backoff := time.Second

	for ctx.Err() == nil {
		started := time.Now()
		err := s.session(ctx, lg)
		if ctx.Err() != nil {
			return nil
		}
		lg.V(1).Info("health stream ended", "err", errString(err))
		s.Events.Emit("health_stream_closed", "node", s.NodeName, "err", errString(err))

		// A stream that lived long enough to be useful resets the backoff.
		// Otherwise one bad patch late in a long run would leave the agent
		// reconnecting at the maximum interval for ever after.
		if time.Since(started) > 10*time.Second {
			backoff = time.Second
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
	return nil
}

func (s *GRPCSink) session(ctx context.Context, lg logger) error {
	var opts []grpc.DialOption
	tc, err := transportCreds(s.CAPath, s.ServerName)
	if err != nil {
		return fmt.Errorf("client tls: %w", err)
	}
	if tc != nil {
		opts = append(opts, grpc.WithTransportCredentials(tc))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if s.TokenPath != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(tokenCreds{path: s.TokenPath, requireTLS: tc != nil}))
	}

	conn, err := grpc.NewClient(s.Addr, opts...)
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.Addr, err)
	}
	defer conn.Close()

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := healthpb.NewHealthReporterClient(conn).Sync(sctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	s.mu.Lock()
	s.stream, s.needSnapshot = stream, true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.stream = nil
		s.mu.Unlock()
	}()

	s.Events.Emit("health_stream_opened",
		"node", s.NodeName, "agent_instance", s.InstanceID, "addr", s.Addr)
	lg.Info("health stream opened", "addr", s.Addr)

	for {
		ack, err := stream.Recv()
		if err != nil {
			return err
		}
		if ack.ResyncRequired {
			s.mu.Lock()
			s.needSnapshot = true
			s.mu.Unlock()
			s.Events.Emit("health_resync_requested", "node", s.NodeName, "reason", ack.Reason)
		}
	}
}

type logger interface {
	Info(msg string, kv ...any)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Publish sends either a full snapshot or just what changed.
func (s *GRPCSink) Publish(_ context.Context, snap Snapshot) error {
	s.mu.Lock()
	stream := s.stream
	full := s.needSnapshot || time.Since(s.lastSnapshot) >= s.Refresh
	s.mu.Unlock()

	if stream == nil {
		return fmt.Errorf("no health stream to %s", s.Addr)
	}
	if full {
		if err := s.sendSnapshot(stream, snap); err != nil {
			return err
		}
		s.mu.Lock()
		s.needSnapshot, s.lastSnapshot = false, time.Now()
		s.mu.Unlock()
		return nil
	}
	return s.sendDeltas(stream, snap)
}

func (s *GRPCSink) envelope(payload any) *healthpb.HealthEnvelope {
	env := &healthpb.HealthEnvelope{
		NodeName:        s.NodeName,
		AgentInstanceId: s.InstanceID,
		Sequence:        s.seq.Add(1),
	}
	switch p := payload.(type) {
	case *healthpb.SnapshotBegin:
		env.Payload = &healthpb.HealthEnvelope_SnapshotBegin{SnapshotBegin: p}
	case *healthpb.SnapshotEnd:
		env.Payload = &healthpb.HealthEnvelope_SnapshotEnd{SnapshotEnd: p}
	case *healthpb.LocalHealth:
		env.Payload = &healthpb.HealthEnvelope_Local{Local: p}
	case *healthpb.PathHealth:
		env.Payload = &healthpb.HealthEnvelope_Path{Path: p}
	case *healthpb.AttachmentRetired:
		env.Payload = &healthpb.HealthEnvelope_Retired{Retired: p}
	}
	return env
}

func (s *GRPCSink) sendSnapshot(stream healthpb.HealthReporter_SyncClient, snap Snapshot) error {
	epoch := s.epoch.Add(1)
	if err := stream.Send(s.envelope(&healthpb.SnapshotBegin{
		Epoch:         epoch,
		ExpectedLocal: uint32(len(snap.Locals)),
		ExpectedPath:  uint32(len(snap.Paths)),
	})); err != nil {
		return err
	}
	for i := range snap.Locals {
		if err := stream.Send(s.envelope(toLocalPB(snap.Locals[i]))); err != nil {
			return err
		}
	}
	for i := range snap.Paths {
		if err := stream.Send(s.envelope(toPathPB(snap.Paths[i]))); err != nil {
			return err
		}
	}
	if err := stream.Send(s.envelope(&healthpb.SnapshotEnd{Epoch: epoch})); err != nil {
		return err
	}
	s.Events.Emit("health_report_sent",
		"node", s.NodeName, "kind", "snapshot", "epoch", epoch,
		"local", len(snap.Locals), "path", len(snap.Paths))

	// A change can land on a sweep that happens to be a refresh, in which case
	// it travels inside the snapshot. The per-attachment anchor is emitted
	// anyway so the evaluation can time a transition without caring which of
	// the two paths carried it.
	for i := range snap.Locals {
		r := snap.Locals[i]
		if snap.Changed[r.AttachmentID] {
			s.Events.Emit("health_report_sent",
				"node", s.NodeName, "kind", "local", "via", "snapshot",
				"attachment_id", r.AttachmentID, "local_ready", r.Ready())
		}
	}
	return nil
}

func (s *GRPCSink) sendDeltas(stream healthpb.HealthReporter_SyncClient, snap Snapshot) error {
	for i := range snap.Locals {
		r := snap.Locals[i]
		if !snap.Changed[r.AttachmentID] {
			continue
		}
		if err := stream.Send(s.envelope(toLocalPB(r))); err != nil {
			return err
		}
		s.Events.Emit("health_report_sent",
			"node", s.NodeName, "kind", "local", "via", "delta",
			"attachment_id", r.AttachmentID, "local_ready", r.Ready())
	}
	for i := range snap.Paths {
		r := snap.Paths[i]
		if !snap.Changed[r.ScopeID] {
			continue
		}
		if err := stream.Send(s.envelope(toPathPB(r))); err != nil {
			return err
		}
		s.Events.Emit("health_report_sent",
			"node", s.NodeName, "kind", "path", "scope_id", r.ScopeID,
			"path_ready", r.PathReady)
	}
	for _, id := range snap.Retired {
		if err := stream.Send(s.envelope(&healthpb.AttachmentRetired{AttachmentId: id})); err != nil {
			return err
		}
		s.Events.Emit("health_report_sent",
			"node", s.NodeName, "kind", "retired", "attachment_id", id)
	}
	return nil
}

func toLocalPB(r model.LocalHealth) *healthpb.LocalHealth {
	return &healthpb.LocalHealth{
		AttachmentId:       r.AttachmentID,
		PodUid:             r.PodUID,
		Namespace:          r.Namespace,
		PodName:            r.PodName,
		Nad:                r.NAD,
		InterfaceName:      r.Interface,
		Ip:                 r.IP,
		LocalReady:         r.Ready(),
		InterfaceExists:    r.InterfaceExists,
		AddressPresent:     r.AddressPresent,
		LinkUsable:         r.LinkUsable,
		ObservedAtUnixNano: r.ObservedAt.UnixNano(),
	}
}

func toPathPB(r model.PathHealth) *healthpb.PathHealth {
	scope := healthpb.ProbeScope_PROBE_SCOPE_ENDPOINT
	if r.Scope == model.ScopeNode {
		scope = healthpb.ProbeScope_PROBE_SCOPE_NODE
	}
	return &healthpb.PathHealth{
		Scope:              scope,
		ScopeId:            r.ScopeID,
		Nad:                r.NAD,
		Target:             r.Target,
		PathReady:          r.PathReady,
		ObservedAtUnixNano: r.ObservedAt.UnixNano(),
	}
}
