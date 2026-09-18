package probe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	vnetns "github.com/vishvananda/netns"
	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
	"golang.org/x/sys/unix"
)

// ICMPProber sends echo requests from an explicitly bound secondary source.
//
// ICMP and not TCP, on purpose. The model already separates three things:
// PodReady covers the application, LocalReady covers the attachment, and
// PathReady covers network reachability. A TCP probe against a remote service
// port would fold remote application availability back into the path term and
// blur the separation the design is built on.
type ICMPProber struct {
	mu    sync.Mutex
	conns map[string]*socket
	seq   atomic.Uint32
	id    int
}

type socket struct {
	pc     net.PacketConn
	v6     bool
	nsPath string
	src    string
	iface  string
}

// NewICMPProber returns a prober that caches one socket per (netns, source,
// interface). The socket is opened inside the Pod netns and stays bound to it,
// so later sends need no namespace switching at all.
func NewICMPProber() *ICMPProber {
	return &ICMPProber{conns: map[string]*socket{}, id: os.Getpid() & 0xffff}
}

func socketKey(s Spec) string {
	return s.NetnsPath + "|" + s.SourceIP + "|" + s.Interface
}

// Probe sends one echo request and waits for its reply.
func (p *ICMPProber) Probe(ctx context.Context, s Spec) Result {
	now := time.Now()
	target := net.ParseIP(s.Target)
	if target == nil {
		return Result{ErrorKind: ErrSocket, Detail: "unparseable target " + s.Target, ObservedAt: now}
	}

	sk, err := p.socketFor(s)
	if err != nil {
		return Result{ErrorKind: ErrSocket, Detail: err.Error(), ObservedAt: now}
	}

	timeout := s.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	if dl, ok := ctx.Deadline(); ok {
		if until := time.Until(dl); until < timeout {
			timeout = until
		}
	}
	if timeout <= 0 {
		return Result{ErrorKind: ErrTimeout, Detail: "no time left", ObservedAt: now}
	}

	seq := int(p.seq.Add(1) & 0xffff)
	typ := icmp.Type(ipv4.ICMPTypeEcho)
	if sk.v6 {
		typ = ipv6.ICMPTypeEchoRequest
	}
	msg := icmp.Message{
		Type: typ,
		Body: &icmp.Echo{ID: p.id, Seq: seq, Data: []byte("multus-service")},
	}
	wire, err := msg.Marshal(nil)
	if err != nil {
		return Result{ErrorKind: ErrSocket, Detail: err.Error(), ObservedAt: now}
	}

	dst := &net.IPAddr{IP: target}
	start := time.Now()
	if err := sk.pc.SetDeadline(start.Add(timeout)); err != nil {
		return Result{ErrorKind: ErrSocket, Detail: err.Error(), ObservedAt: now}
	}
	if _, err := sk.pc.WriteTo(wire, dst); err != nil {
		// A dead route or a downed interface fails here rather than timing out,
		// which is worth distinguishing in the samples.
		return Result{ErrorKind: classify(err), Detail: err.Error(), ObservedAt: now}
	}

	buf := make([]byte, 1500)
	for {
		n, peer, err := sk.pc.ReadFrom(buf)
		if err != nil {
			return Result{ErrorKind: classify(err), Detail: err.Error(), ObservedAt: time.Now()}
		}
		// A raw ICMP socket sees every ICMP packet in the namespace, so replies
		// meant for someone else have to be skipped rather than counted.
		if peer.String() != target.String() {
			continue
		}
		proto := ipv4.ICMPTypeEchoReply.Protocol()
		if sk.v6 {
			proto = ipv6.ICMPTypeEchoReply.Protocol()
		}
		parsed, err := icmp.ParseMessage(proto, buf[:n])
		if err != nil {
			continue
		}
		echo, ok := parsed.Body.(*icmp.Echo)
		if !ok || echo.ID != p.id || echo.Seq != seq {
			continue
		}
		return Result{Success: true, RTT: time.Since(start), ObservedAt: time.Now()}
	}
}

func classify(err error) ErrorKind {
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return ErrTimeout
	}
	if errors.Is(err, unix.EHOSTUNREACH) || errors.Is(err, unix.ENETUNREACH) ||
		errors.Is(err, unix.ENETDOWN) || errors.Is(err, unix.EHOSTDOWN) {
		return ErrUnreachable
	}
	return ErrSend
}

// socketFor opens, or reuses, a socket bound to the spec's source.
func (p *ICMPProber) socketFor(s Spec) (*socket, error) {
	key := socketKey(s)

	p.mu.Lock()
	sk, ok := p.conns[key]
	p.mu.Unlock()
	if ok {
		return sk, nil
	}

	sk, err := openSocket(s)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if prev, raced := p.conns[key]; raced {
		p.mu.Unlock()
		sk.pc.Close()
		return prev, nil
	}
	p.conns[key] = sk
	p.mu.Unlock()
	return sk, nil
}

// openSocket creates the socket inside the target namespace.
//
// Unlike netlink there is no namespace-bound handle for sockets, so the thread
// has to be moved. It is locked for the duration and put back before unlocking:
// leaving a Go worker thread in a Pod's namespace would silently corrupt
// unrelated work later scheduled onto it.
func openSocket(s Spec) (*socket, error) {
	ip := net.ParseIP(s.SourceIP)
	if ip == nil {
		return nil, fmt.Errorf("unparseable source %q", s.SourceIP)
	}
	v6 := ip.To4() == nil
	network := "ip4:icmp"
	if v6 {
		network = "ip6:ipv6-icmp"
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var restore vnetns.NsHandle
	if s.NetnsPath != "" {
		cur, err := vnetns.Get()
		if err != nil {
			return nil, fmt.Errorf("current netns: %w", err)
		}
		restore = cur
		defer restore.Close()

		target, err := vnetns.GetFromPath(s.NetnsPath)
		if err != nil {
			return nil, fmt.Errorf("open netns %s: %w", s.NetnsPath, err)
		}
		defer target.Close()
		if err := vnetns.Set(target); err != nil {
			return nil, fmt.Errorf("enter netns %s: %w", s.NetnsPath, err)
		}
		defer func() { _ = vnetns.Set(restore) }()
	}

	pc, err := net.ListenPacket(network, s.SourceIP)
	if err != nil {
		return nil, fmt.Errorf("icmp socket on %s: %w", s.SourceIP, err)
	}

	// Binding the source address is not quite enough: a policy route or a
	// second attachment could still carry the packet out of another interface.
	// SO_BINDTODEVICE pins the egress device so the sample really does traverse
	// the secondary path it claims to measure.
	if s.Interface != "" {
		if ipc, ok := pc.(*net.IPConn); ok {
			raw, rerr := ipc.SyscallConn()
			if rerr == nil {
				var serr error
				_ = raw.Control(func(fd uintptr) {
					serr = unix.SetsockoptString(int(fd), unix.SOL_SOCKET,
						unix.SO_BINDTODEVICE, s.Interface)
				})
				if serr != nil {
					pc.Close()
					return nil, fmt.Errorf("bind to %s: %w", s.Interface, serr)
				}
			}
		}
	}

	return &socket{pc: pc, v6: v6, nsPath: s.NetnsPath, src: s.SourceIP, iface: s.Interface}, nil
}

// Retain closes sockets whose key is no longer wanted.
func (p *ICMPProber) Retain(keys map[string]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, sk := range p.conns {
		if !keys[k] {
			sk.pc.Close()
			delete(p.conns, k)
		}
	}
}

// Close releases every socket.
func (p *ICMPProber) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, sk := range p.conns {
		sk.pc.Close()
		delete(p.conns, k)
	}
}

// SocketKey exposes the cache key so a manager can express what to retain.
func SocketKey(s Spec) string { return socketKey(s) }
