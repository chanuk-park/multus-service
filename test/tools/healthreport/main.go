// Command healthreport sends hand-crafted health reports to the controller.
//
// The transport's rejection rules cannot be exercised through the real agent:
// the agent derives its targets from the same Service and Pod objects the
// controller does, so the two converge before a bad report can be produced.
// This tool produces the bad reports on purpose.
//
// It claims a node name and a fresh instance id, which supersedes that node's
// real agent for the duration. The agent's next report is refused, its stream
// closes, and it reconnects with a new instance -- which is itself the
// supersession path under test.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	healthpb "github.com/boanlab/multus-service/api/healthpb"
)

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:19090", "controller health transport")
		node     = flag.String("node", "", "node name to claim")
		instance = flag.String("instance", "harness", "agent instance id to claim")
		mode     = flag.String("case", "", "unknown-attachment | node-scope-path | out-of-order")
		att      = flag.String("attachment", "", "a real attachment id, for the out-of-order case")
		nad      = flag.String("nad", "", "canonical ns/name, for the out-of-order case")
		iface    = flag.String("interface", "", "interface name, for the out-of-order case")
		ip       = flag.String("ip", "", "address, for the out-of-order case")
		wait     = flag.Duration("wait", 3*time.Second, "how long to stay connected after sending")
	)
	flag.Parse()
	if *node == "" || *mode == "" {
		fmt.Fprintln(os.Stderr, "--node and --case are required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail("dial", err)
	}
	defer conn.Close()

	stream, err := healthpb.NewHealthReporterClient(conn).Sync(ctx)
	if err != nil {
		fail("open stream", err)
	}

	var seq uint64
	env := func(p any) *healthpb.HealthEnvelope {
		seq++
		e := &healthpb.HealthEnvelope{NodeName: *node, AgentInstanceId: *instance, Sequence: seq}
		switch v := p.(type) {
		case *healthpb.LocalHealth:
			e.Payload = &healthpb.HealthEnvelope_Local{Local: v}
		case *healthpb.PathHealth:
			e.Payload = &healthpb.HealthEnvelope_Path{Path: v}
		}
		return e
	}

	switch *mode {
	case "unknown-attachment":
		// An id the controller never derived. A report is not a discovery
		// channel, so this must not create anything.
		send(stream, env(&healthpb.LocalHealth{
			AttachmentId: "deadbeefdeadbeef", PodName: "ghost", Nad: "ghost/net",
			InterfaceName: "net1", Ip: "10.255.255.1", LocalReady: true,
			InterfaceExists: true, AddressPresent: true, LinkUsable: true,
			ObservedAtUnixNano: time.Now().UnixNano(),
		}))

	case "node-scope-path":
		// A Node-scope domain key while the controller computes Endpoint scope.
		// Nothing reads path state under that key, so it must be refused.
		send(stream, env(&healthpb.PathHealth{
			Scope:   healthpb.ProbeScope_PROBE_SCOPE_NODE,
			ScopeId: "0123456789abcdef", Nad: *nad, Target: "10.0.0.1",
			PathReady: true, ObservedAtUnixNano: time.Now().UnixNano(),
		}))

	case "out-of-order":
		if *att == "" {
			fail("out-of-order", fmt.Errorf("--attachment is required"))
		}
		mk := func(ready bool) *healthpb.LocalHealth {
			return &healthpb.LocalHealth{
				AttachmentId: *att, Nad: *nad, InterfaceName: *iface, Ip: *ip,
				LocalReady: ready, InterfaceExists: true, AddressPresent: ready,
				LinkUsable: true, ObservedAtUnixNano: time.Now().UnixNano(),
			}
		}
		// Advance normally, then replay with a sequence that already passed.
		send(stream, env(mk(true)))
		seq = 0 // rewind: the next envelope carries sequence 1 again
		send(stream, env(mk(false)))

	default:
		fail("case", fmt.Errorf("unknown case %q", *mode))
	}

	// Stay connected long enough for the controller to process and log.
	deadline := time.After(*wait)
	acks := make(chan *healthpb.HealthAck, 8)
	go func() {
		for {
			a, err := stream.Recv()
			if err != nil {
				close(acks)
				return
			}
			acks <- a
		}
	}()
	for {
		select {
		case <-deadline:
			return
		case a, ok := <-acks:
			if !ok {
				return
			}
			fmt.Printf("ack seq=%d resync=%v reason=%q\n", a.AcceptedSequence, a.ResyncRequired, a.Reason)
		}
	}
}

func send(s healthpb.HealthReporter_SyncClient, e *healthpb.HealthEnvelope) {
	if err := s.Send(e); err != nil {
		fail("send", err)
	}
}

func fail(what string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", what, err)
	os.Exit(1)
}
