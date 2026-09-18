package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	healthpb "github.com/boanlab/multus-service/api/healthpb"
	"github.com/boanlab/multus-service/internal/model"
	"github.com/boanlab/multus-service/internal/obs"
)

// ackEvery bounds how often the controller acknowledges. A per-report ack would
// double the message count for no benefit; rejections and snapshot boundaries
// are acknowledged immediately regardless.
const ackEvery = 32

// HealthServer receives agent reports.
//
// It is deliberately not a discovery channel. Every report is matched against
// the Registry, which the reconciler fills from Service + Pod + network-status,
// and anything unrecognised is rejected rather than stored. An agent can change
// the health of an attachment the controller already knows about; it cannot
// bring one into existence.
type HealthServer struct {
	healthpb.UnimplementedHealthReporterServer

	Store    *HealthStore
	Registry *Registry
	Events   *obs.Recorder

	// Notify enqueues a reconcile for a Service whose health input changed.
	// Without it readiness would only move on the next Service or Pod event.
	Notify func(types.NamespacedName)
}

// Sync consumes one agent's stream.
func (s *HealthServer) Sync(stream healthpb.HealthReporter_SyncServer) error {
	ctx := stream.Context()
	lg := log.FromContext(ctx).WithName("health")

	var (
		node, instance string
		adopted        bool
		inSnapshot     bool
		epoch          uint64
		bufLocal       []model.LocalHealth
		bufPath        []model.PathHealth
		since          int
		lastSeq        uint64
	)

	ack := func(resync bool, reason string) error {
		since = 0
		return stream.Send(&healthpb.HealthAck{
			AcceptedSequence: lastSeq,
			ResyncRequired:   resync,
			Reason:           reason,
		})
	}

	for {
		env, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if !adopted {
			node, instance = env.NodeName, env.AgentInstanceId
			if node == "" || instance == "" {
				return fmt.Errorf("envelope must carry node_name and agent_instance_id")
			}
			prev := s.Store.AdoptInstance(node, instance)
			adopted = true
			s.Events.Emit("agent_connected",
				"node", node, "agent_instance", instance, "superseded", prev)
			lg.Info("agent connected", "node", node, "instance", instance, "superseded", prev)
			// A fresh stream always starts from an unknown state, so ask for a
			// snapshot rather than trusting whatever is already stored.
			if err := ack(true, "new stream"); err != nil {
				return err
			}
		}
		if env.NodeName != node || env.AgentInstanceId != instance {
			s.reject(env, "instance changed mid-stream")
			continue
		}
		lastSeq = env.Sequence
		since++

		origin := model.ReportOrigin{NodeName: node, AgentInstance: instance, Sequence: env.Sequence}

		switch p := env.Payload.(type) {
		case *healthpb.HealthEnvelope_SnapshotBegin:
			inSnapshot, epoch = true, p.SnapshotBegin.Epoch
			bufLocal, bufPath = bufLocal[:0], bufPath[:0]

		case *healthpb.HealthEnvelope_SnapshotEnd:
			if !inSnapshot || p.SnapshotEnd.Epoch != epoch {
				s.reject(env, "snapshot end without a matching begin")
				inSnapshot = false
				if err := ack(true, "snapshot mismatch"); err != nil {
					return err
				}
				continue
			}
			inSnapshot = false
			// Note which entries the snapshot actually moves, before the commit
			// replaces them. A change can arrive inside a refresh rather than as
			// a delta, and the evaluation needs the same per-attachment anchor
			// either way.
			var moved []model.LocalHealth
			for _, r := range bufLocal {
				if cur, fresh := s.Store.Local(r.AttachmentID); !fresh || cur.Ready() != r.Ready() {
					moved = append(moved, r)
				}
			}
			if err := s.Store.ApplySnapshot(origin, bufLocal, bufPath); err != nil {
				s.reject(env, err.Error())
				if errors.Is(err, ErrStaleInstance) {
					// Asking a superseded instance to resync would loop for
					// ever: every snapshot it sends is refused for the same
					// reason. Close the stream so it reconnects and re-adopts.
					_ = ack(true, err.Error())
					return err
				}
				if err := ack(true, err.Error()); err != nil {
					return err
				}
				continue
			}
			s.Events.Emit("health_snapshot_applied",
				"node", node, "agent_instance", instance,
				"epoch", epoch, "local", len(bufLocal), "path", len(bufPath),
				"moved", len(moved))
			for _, r := range moved {
				s.Events.Emit("health_report_applied",
					"attachment_id", r.AttachmentID, "kind", "local",
					"via", "snapshot", "local_ready", r.Ready())
			}
			s.notifyAll(bufLocal, bufPath)
			if err := ack(false, ""); err != nil {
				return err
			}

		case *healthpb.HealthEnvelope_Local:
			r, owners, ok := s.localFrom(node, p.Local)
			if !ok {
				s.reject(env, "unknown attachment")
				if err := ack(false, "unknown attachment"); err != nil {
					return err
				}
				continue
			}
			s.Events.Emit("health_report_received",
				"attachment_id", r.AttachmentID, "node", node,
				"sequence", env.Sequence, "local_ready", r.Ready(),
				"snapshot", inSnapshot)
			if inSnapshot {
				bufLocal = append(bufLocal, r)
				continue
			}
			if err := s.Store.AcceptLocal(origin, r); err != nil {
				s.rejectReport(r.AttachmentID, node, env.Sequence, err)
				if errors.Is(err, ErrStaleInstance) {
					// Nothing this instance says will ever be accepted again.
					// Closing the stream makes it reconnect and re-adopt, which
					// is faster and simpler than asking it to resync in place.
					_ = ack(true, err.Error())
					return err
				}
				continue
			}
			s.Events.Emit("health_report_applied",
				"attachment_id", r.AttachmentID, "kind", "local",
				"via", "delta", "local_ready", r.Ready())
			s.notify(owners)

		case *healthpb.HealthEnvelope_Path:
			r, owners, ok := s.pathFrom(node, p.Path)
			if !ok {
				s.reject(env, "unknown path key")
				if err := ack(false, "unknown path key"); err != nil {
					return err
				}
				continue
			}
			s.Events.Emit("health_report_received",
				"scope_id", r.ScopeID, "node", node, "scope", string(r.Scope),
				"sequence", env.Sequence, "path_ready", r.PathReady,
				"snapshot", inSnapshot)
			if inSnapshot {
				bufPath = append(bufPath, r)
				continue
			}
			if err := s.Store.AcceptPath(origin, r); err != nil {
				s.rejectReport(r.ScopeID, node, env.Sequence, err)
				if errors.Is(err, ErrStaleInstance) {
					_ = ack(true, err.Error())
					return err
				}
				continue
			}
			s.Events.Emit("health_report_applied",
				"scope_id", r.ScopeID, "kind", "path", "path_ready", r.PathReady)
			s.notify(owners)

		case *healthpb.HealthEnvelope_Retired:
			id := p.Retired.AttachmentId
			s.Store.ForgetLocal(id)
			s.Events.Emit("health_report_applied",
				"attachment_id", id, "kind", "retired")
			if _, owners, ok := s.Registry.Attachment(id); ok {
				s.notify(owners)
			}
		}

		if since >= ackEvery {
			if err := ack(false, ""); err != nil {
				return err
			}
		}
	}
}

// localFrom validates a report against the registry and converts it.
func (s *HealthServer) localFrom(node string, m *healthpb.LocalHealth) (model.LocalHealth, []types.NamespacedName, bool) {
	att, owners, ok := s.Registry.Attachment(m.AttachmentId)
	if !ok {
		return model.LocalHealth{}, nil, false
	}
	// The report must describe the attachment the controller knows under that
	// id, not merely quote the id back.
	if att.IP != m.Ip || att.Interface != m.InterfaceName || att.NAD != m.Nad {
		return model.LocalHealth{}, nil, false
	}
	// And it must come from the node the attachment actually runs on. Without
	// this, any node could speak for any endpoint in the cluster.
	if att.NodeName != "" && att.NodeName != node {
		return model.LocalHealth{}, nil, false
	}
	return model.LocalHealth{
		AttachmentID:    m.AttachmentId,
		PodUID:          m.PodUid,
		Namespace:       m.Namespace,
		PodName:         m.PodName,
		NAD:             m.Nad,
		Interface:       m.InterfaceName,
		IP:              m.Ip,
		NodeName:        node,
		InterfaceExists: m.InterfaceExists,
		AddressPresent:  m.AddressPresent,
		LinkUsable:      m.LinkUsable,
		ObservedAt:      time.Unix(0, m.ObservedAtUnixNano),
	}, owners, true
}

func (s *HealthServer) pathFrom(node string, m *healthpb.PathHealth) (model.PathHealth, []types.NamespacedName, bool) {
	owners, ok := s.Registry.PathKey(m.ScopeId)
	if !ok {
		return model.PathHealth{}, nil, false
	}
	scope := model.ScopeEndpoint
	if m.Scope == healthpb.ProbeScope_PROBE_SCOPE_NODE {
		scope = model.ScopeNode
	}
	return model.PathHealth{
		Scope:      scope,
		ScopeID:    m.ScopeId,
		NodeName:   node,
		NAD:        m.Nad,
		Target:     m.Target,
		PathReady:  m.PathReady,
		ObservedAt: time.Unix(0, m.ObservedAtUnixNano),
	}, owners, true
}

func (s *HealthServer) reject(env *healthpb.HealthEnvelope, reason string) {
	s.Events.Emit("health_report_rejected",
		"node", env.NodeName, "agent_instance", env.AgentInstanceId,
		"sequence", env.Sequence, "reason", reason)
}

func (s *HealthServer) rejectReport(id, node string, seq uint64, err error) {
	s.Events.Emit("health_report_rejected",
		"id", id, "node", node, "sequence", seq, "reason", err.Error())
}

func (s *HealthServer) notify(owners []types.NamespacedName) {
	if s.Notify == nil {
		return
	}
	for _, o := range owners {
		s.Notify(o)
	}
}

func (s *HealthServer) notifyAll(locals []model.LocalHealth, paths []model.PathHealth) {
	seen := map[types.NamespacedName]bool{}
	for _, r := range locals {
		if _, owners, ok := s.Registry.Attachment(r.AttachmentID); ok {
			for _, o := range owners {
				seen[o] = true
			}
		}
	}
	for _, r := range paths {
		if owners, ok := s.Registry.PathKey(r.ScopeID); ok {
			for _, o := range owners {
				seen[o] = true
			}
		}
	}
	for o := range seen {
		s.Notify(o)
	}
}

// Serve runs the gRPC listener until ctx is cancelled. It satisfies
// manager.Runnable.
type Serve struct {
	Addr   string
	Server *HealthServer
}

// Start implements manager.Runnable.
func (g *Serve) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", g.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", g.Addr, err)
	}
	srv := grpc.NewServer()
	healthpb.RegisterHealthReporterServer(srv, g.Server)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	log.FromContext(ctx).Info("health transport listening", "addr", g.Addr)
	return srv.Serve(ln)
}
