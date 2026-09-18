package local

import (
	"sync"

	"github.com/vishvananda/netlink"
	vnetns "github.com/vishvananda/netns"
)

// Monitor subscribes to link and address events inside Pod network namespaces.
//
// Both event families are needed. Taking a link down emits only RTM_NEWLINK
// with the DOWN flag -- the IPv4 address stays put, so an address-only
// subscription misses the fault entirely. Flushing the address emits only
// RTM_DELADDR. Watching one of the two leaves a real failure invisible.
//
// Nothing here can observe an underlay blackhole: that breaks reachability
// without changing any local kernel state, so neither subscription fires. That
// is what the active path probe is for.
type Monitor struct {
	mu       sync.Mutex
	watching map[string]*subscription
	out      chan string
	closed   bool
}

type subscription struct {
	path string
	done chan struct{}
}

// NewMonitor returns a monitor whose event channel buffers buf Pod UIDs.
func NewMonitor(buf int) *Monitor {
	return &Monitor{
		watching: make(map[string]*subscription),
		out:      make(chan string, buf),
	}
}

// Events yields the Pod UID whose namespace just changed. It is a wake-up
// signal, not a description: the caller re-inspects rather than trusting the
// event payload, so a coalesced or dropped event costs latency and never
// correctness.
func (m *Monitor) Events() <-chan string { return m.out }

// Ensure subscribes to nsPath for podUID, replacing an existing subscription
// when the namespace path changed -- which is what a sandbox recreate looks
// like from here.
func (m *Monitor) Ensure(podUID, nsPath string) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	if cur, ok := m.watching[podUID]; ok {
		if cur.path == nsPath {
			m.mu.Unlock()
			return nil
		}
		close(cur.done)
		delete(m.watching, podUID)
	}
	m.mu.Unlock()

	ns, err := vnetns.GetFromPath(nsPath)
	if err != nil {
		return err
	}
	defer ns.Close()

	done := make(chan struct{})
	linkCh := make(chan netlink.LinkUpdate, 64)
	if err := netlink.LinkSubscribeWithOptions(linkCh, done, netlink.LinkSubscribeOptions{
		Namespace:     &ns,
		ErrorCallback: func(error) {},
	}); err != nil {
		close(done)
		return err
	}
	addrCh := make(chan netlink.AddrUpdate, 64)
	if err := netlink.AddrSubscribeWithOptions(addrCh, done, netlink.AddrSubscribeOptions{
		Namespace:     &ns,
		ErrorCallback: func(error) {},
	}); err != nil {
		close(done)
		return err
	}

	go m.pump(podUID, done, linkCh, addrCh)

	m.mu.Lock()
	m.watching[podUID] = &subscription{path: nsPath, done: done}
	m.mu.Unlock()
	return nil
}

func (m *Monitor) pump(podUID string, done chan struct{}, linkCh chan netlink.LinkUpdate, addrCh chan netlink.AddrUpdate) {
	for {
		select {
		case <-done:
			return
		case _, ok := <-linkCh:
			if !ok {
				return
			}
			m.wake(podUID)
		case _, ok := <-addrCh:
			if !ok {
				return
			}
			m.wake(podUID)
		}
	}
}

// wake never blocks: a full channel already means a sweep is pending, and the
// sweep re-reads everything anyway.
func (m *Monitor) wake(podUID string) {
	select {
	case m.out <- podUID:
	default:
	}
}

// Forget stops watching a Pod.
func (m *Monitor) Forget(podUID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.watching[podUID]; ok {
		close(s.done)
		delete(m.watching, podUID)
	}
}

// Watching reports the Pod UIDs currently subscribed.
func (m *Monitor) Watching() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.watching))
	for uid := range m.watching {
		out = append(out, uid)
	}
	return out
}

// Close tears down every subscription.
func (m *Monitor) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	for uid, s := range m.watching {
		close(s.done)
		delete(m.watching, uid)
	}
}
