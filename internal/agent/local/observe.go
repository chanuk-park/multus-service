// Package local observes a secondary interface from inside its Pod netns.
//
// Host-netns observation is not an option. macvlan and ipvlan move the child
// device wholly into the Pod namespace, so a host-side netlink monitor sees
// nothing at all when that interface goes down -- measured, with the host
// monitor silent through link-down, address-flush and underlay blackhole alike.
package local

import (
	"fmt"
	"net"

	"github.com/vishvananda/netlink"
	vnetns "github.com/vishvananda/netns"
)

// Link is the state of one interface inside one namespace.
type Link struct {
	Exists bool
	// Usable is the link-layer verdict: administratively up and not operdown.
	// It has to be checked separately from addresses, because taking a link
	// down leaves the IPv4 address in place -- an address-only check reports a
	// dead interface as healthy.
	Usable bool
	Flags  string
	Oper   string
	// Addrs is the set of addresses currently on the interface.
	Addrs map[string]bool
}

// Has reports whether the interface currently carries ip.
func (l Link) Has(ip string) bool { return l.Addrs[ip] }

// Inspect reads one interface inside the namespace at nsPath.
//
// It uses a namespace-bound netlink handle rather than setns on the calling
// thread: switching a Go thread's namespace is only safe while the thread is
// locked, and a missed unlock leaks the wrong namespace into unrelated work.
func Inspect(nsPath, iface string) (Link, error) {
	ns, err := vnetns.GetFromPath(nsPath)
	if err != nil {
		return Link{}, fmt.Errorf("open netns %s: %w", nsPath, err)
	}
	defer ns.Close()

	h, err := netlink.NewHandleAt(ns)
	if err != nil {
		return Link{}, fmt.Errorf("netlink handle in %s: %w", nsPath, err)
	}
	defer h.Close()

	link, err := h.LinkByName(iface)
	if err != nil {
		var missing netlink.LinkNotFoundError
		if ok := asLinkNotFound(err, &missing); ok {
			return Link{Exists: false, Addrs: map[string]bool{}}, nil
		}
		return Link{}, fmt.Errorf("link %s in %s: %w", iface, nsPath, err)
	}

	attrs := link.Attrs()
	out := Link{
		Exists: true,
		Usable: attrs.Flags&net.FlagUp != 0 && attrs.OperState != netlink.OperDown,
		Flags:  attrs.Flags.String(),
		Oper:   attrs.OperState.String(),
		Addrs:  map[string]bool{},
	}

	addrs, err := h.AddrList(link, netlink.FAMILY_ALL)
	if err != nil {
		return Link{}, fmt.Errorf("addrs of %s in %s: %w", iface, nsPath, err)
	}
	for _, a := range addrs {
		if a.IP != nil {
			out.Addrs[a.IP.String()] = true
		}
	}
	return out, nil
}

func asLinkNotFound(err error, target *netlink.LinkNotFoundError) bool {
	if e, ok := err.(netlink.LinkNotFoundError); ok {
		*target = e
		return true
	}
	return false
}
