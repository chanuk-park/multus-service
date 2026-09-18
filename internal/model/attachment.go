package model

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"strings"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Attachment is one publishable secondary address: a single IP on a single
// interface of a single Pod, belonging to a single NetworkAttachmentDefinition.
//
// A dual-stack attachment produces two Attachments. That is deliberate -- the
// v4 and v6 paths can fail independently, so they are tracked independently.
type Attachment struct {
	PodUID       types.UID
	PodName      string
	PodNamespace string
	NodeName     string

	// NAD is canonical "ns/name".
	NAD       string
	Interface string
	IP        string

	// PodReady mirrors the Pod's Ready condition, which rides the primary
	// interface and therefore says nothing about this address.
	PodReady bool
}

// ID identifies this exact attachment across controller and agent.
//
// PodUID alone is not enough: a Pod keeps its UID across sandbox recreation
// within a single object, and a stale health report that arrives after the
// attachment changed must not be applied to the new one.
func (a Attachment) ID() string {
	h := sha256.New()
	for _, part := range []string{string(a.PodUID), a.NAD, a.Interface, a.IP} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// AddressType reports which EndpointSlice this attachment belongs in.
func (a Attachment) AddressType() discoveryv1.AddressType {
	ip := net.ParseIP(a.IP)
	if ip == nil || ip.To4() != nil {
		return discoveryv1.AddressTypeIPv4
	}
	return discoveryv1.AddressTypeIPv6
}

// Hostname returns the per-endpoint DNS label for this attachment, or "" when
// the Pod name cannot be used as one. CoreDNS builds
// <hostname>.<svc>.<ns>.svc.<zone> records and SRV targets from it.
func (a Attachment) Hostname() string {
	n := a.PodName
	if n == "" || len(n) > 63 || strings.Contains(n, ".") {
		return ""
	}
	for i := 0; i < len(n); i++ {
		c := n[i]
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
		if !ok {
			return ""
		}
	}
	if n[0] == '-' || n[len(n)-1] == '-' {
		return ""
	}
	return n
}
