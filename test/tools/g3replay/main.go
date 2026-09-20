// Command g3replay is a stale-generation / replay adversary.
//
// It authenticates as a *legitimate* node agent (a valid Pod-bound token), so it
// isolates G3 from G2: producer authorization already passed. The question G3
// answers is narrower -- can evidence from a superseded generation (an old
// attachment, an old agent instance, a rolled-back sequence) take effect on the
// current state? The three cases below each push one kind of stale generation
// and report what the controller did with it.
//
//	replay-attachment  a report for a specific attachment id (typically a
//	                   deleted Pod's old id) -> expect "unknown attachment"
//	seq-rollback       within one instance, a report whose sequence goes
//	                   backwards -> expect "sequence did not advance"
//	stale-instance     two streams; the older instance keeps sending after a
//	                   newer one adopted the node -> expect "superseded agent
//	                   instance"
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	healthpb "github.com/boanlab/multus-service/api/healthpb"
)

type bearer struct {
	path string
	tls  bool
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	t, err := os.ReadFile(b.path)
	if err != nil {
		return nil, err
	}
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}
func (b bearer) RequireTransportSecurity() bool { return b.tls }

func main() {
	var (
		addr    = flag.String("addr", "", "controller health transport host:port")
		ca      = flag.String("ca", "", "controller CA")
		srvName = flag.String("server-name", "", "TLS server name")
		token   = flag.String("token", "", "valid node-agent token (isolates G3 from G2)")
		node    = flag.String("node", "", "node to claim (must match the authenticated agent's node)")
		attID   = flag.String("attachment", "", "attachment id to report for")
		nad     = flag.String("nad", "", "canonical ns/name")
		iface   = flag.String("interface", "", "interface")
		ip      = flag.String("ip", "", "address")
		mode    = flag.String("case", "", "replay-attachment | seq-rollback | stale-instance")
		ready   = flag.Bool("ready", true, "local_ready/path_ready value to forge")
	)
	flag.Parse()
	for n, v := range map[string]string{"addr": *addr, "node": *node, "case": *mode} {
		if v == "" {
			fmt.Fprintf(os.Stderr, "--%s required\n", n)
			os.Exit(2)
		}
	}

	dial := func() (healthpb.HealthReporter_SyncClient, func(), error) {
		var opts []grpc.DialOption
		if *ca != "" {
			tc, err := credentials.NewClientTLSFromFile(*ca, *srvName)
			if err != nil {
				return nil, nil, err
			}
			opts = append(opts, grpc.WithTransportCredentials(tc))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
		if *token != "" {
			opts = append(opts, grpc.WithPerRPCCredentials(bearer{path: *token, tls: *ca != ""}))
		}
		conn, err := grpc.NewClient(*addr, opts...)
		if err != nil {
			return nil, nil, err
		}
		s, err := healthpb.NewHealthReporterClient(conn).Sync(context.Background())
		if err != nil {
			conn.Close()
			return nil, nil, err
		}
		return s, func() { conn.Close() }, nil
	}

	local := func() *healthpb.LocalHealth {
		return &healthpb.LocalHealth{
			AttachmentId: *attID, Nad: *nad, InterfaceName: *iface, Ip: *ip,
			LocalReady: *ready, InterfaceExists: *ready, AddressPresent: *ready, LinkUsable: *ready,
			ObservedAtUnixNano: time.Now().UnixNano(),
		}
	}
	env := func(inst string, seq uint64, p *healthpb.LocalHealth) *healthpb.HealthEnvelope {
		return &healthpb.HealthEnvelope{
			NodeName: *node, AgentInstanceId: inst, Sequence: seq,
			Payload: &healthpb.HealthEnvelope_Local{Local: p},
		}
	}
	acks := func(tag string, s healthpb.HealthReporter_SyncClient, n int) {
		for i := 0; i < n; i++ {
			a, err := s.Recv()
			if err != nil {
				return
			}
			fmt.Printf("  %s ack seq=%d resync=%v reason=%q\n", tag, a.AcceptedSequence, a.ResyncRequired, a.Reason)
		}
	}

	switch *mode {
	case "replay-attachment":
		s, done, err := dial()
		if err != nil {
			fail(err)
		}
		defer done()
		_ = s.Send(env("g3-replay", 1, local()))
		_ = s.Send(env("g3-replay", 2, local()))
		acks("REPLAY", s, 3)
		time.Sleep(2 * time.Second)

	case "seq-rollback":
		s, done, err := dial()
		if err != nil {
			fail(err)
		}
		defer done()
		// advance, then send a strictly lower sequence from the same instance.
		_ = s.Send(env("g3-roll", 10, local()))
		time.Sleep(300 * time.Millisecond)
		r := local()
		r.LocalReady = !*ready
		_ = s.Send(env("g3-roll", 5, r)) // rollback
		acks("ROLL", s, 3)
		time.Sleep(2 * time.Second)

	case "stale-instance":
		// Stream 1 adopts instance OLD. Stream 2 adopts instance NEW (supersedes
		// OLD for the node). Stream 1 then keeps sending under OLD.
		s1, d1, err := dial()
		if err != nil {
			fail(err)
		}
		defer d1()
		_ = s1.Send(env("g3-OLD", 1, local()))
		go func() { acks("OLD", s1, 100) }()
		time.Sleep(500 * time.Millisecond)

		s2, d2, err := dial()
		if err != nil {
			fail(err)
		}
		defer d2()
		_ = s2.Send(env("g3-NEW", 1, local()))
		go func() { acks("NEW", s2, 100) }()
		time.Sleep(500 * time.Millisecond)

		// OLD instance keeps reporting after being superseded.
		for i := 0; i < 5; i++ {
			_ = s1.Send(env("g3-OLD", uint64(2+i), local()))
			time.Sleep(200 * time.Millisecond)
		}
		time.Sleep(1 * time.Second)

	default:
		fmt.Fprintf(os.Stderr, "unknown case %q\n", *mode)
		os.Exit(2)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
