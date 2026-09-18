// Package multus parses the Multus network-status annotation.
//
// The annotation is written once by the multus CNI binary when CNI ADD
// completes -- before the Pod is Running and well before it is Ready. It is a
// snapshot of the attachment, never updated afterwards for the life of the
// sandbox. Everything here treats it as such.
package multus

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
)

const (
	// StatusAnnotation holds the attachment result written by multus.
	StatusAnnotation = "k8s.v1.cni.cncf.io/network-status"
	// NetworksAnnotation holds the attachment request written by the user.
	NetworksAnnotation = "k8s.v1.cni.cncf.io/networks"
)

var (
	// ErrNoAttachment means the Pod has no entry for the requested network.
	ErrNoAttachment = errors.New("no attachment for network")
	// ErrAmbiguous means the Pod attached the same network more than once and
	// there is no principled way to pick one. Callers must skip the Pod.
	ErrAmbiguous = errors.New("network attached more than once")
)

// NetworkStatus is one entry of the network-status array.
//
// Field notes, all measured rather than assumed:
//   - Name is "ns/name" for NAD-backed entries but the raw CNI network name
//     (e.g. "cbr0") for the default entry, so the two are not comparable.
//   - Default is present only when true. It is omitted, never false, so it must
//     be read as presence and never as a tri-state.
//   - Interface defaults to net1/net2/... only when the request did not name
//     it. Production Pods routinely carry names like "n2".
//   - IPs is a flat array that mixes families on a dual-stack attachment.
type NetworkStatus struct {
	Name      string   `json:"name"`
	Interface string   `json:"interface,omitempty"`
	IPs       []string `json:"ips,omitempty"`
	MAC       string   `json:"mac,omitempty"`
	Default   bool     `json:"default,omitempty"`
}

// Parse decodes the network-status annotation value. An empty value yields no
// entries and no error: the annotation simply has not been written yet.
func Parse(value string) ([]NetworkStatus, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	var out []NetworkStatus
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		return nil, fmt.Errorf("parse %s: %w", StatusAnnotation, err)
	}
	return out, nil
}

// CanonicalNAD normalizes a NetworkAttachmentDefinition reference to "ns/name".
//
// A bare "n2-net" resolves against defaultNamespace; "oai/n2-net" is already
// canonical. A trailing "@ifname" selector is dropped -- the interface name is
// carried by network-status itself, so accepting it here would create a second
// source of truth.
func CanonicalNAD(ref, defaultNamespace string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", errors.New("empty network reference")
	}
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	parts := strings.Split(ref, "/")
	switch len(parts) {
	case 1:
		if defaultNamespace == "" {
			return "", fmt.Errorf("bare network reference %q needs a namespace", ref)
		}
		return defaultNamespace + "/" + parts[0], nil
	case 2:
		if parts[0] == "" || parts[1] == "" {
			return "", fmt.Errorf("malformed network reference %q", ref)
		}
		return ref, nil
	default:
		return "", fmt.Errorf("malformed network reference %q", ref)
	}
}

// Select returns the single entry matching the canonical NAD name.
//
// Entries flagged Default are skipped unconditionally. A NAD promoted to the
// cluster default network would carry the Pod's primary address, and publishing
// a primary address is exactly the failure this project exists to avoid.
func Select(list []NetworkStatus, canonicalNAD string) (NetworkStatus, error) {
	var found []NetworkStatus
	for _, e := range list {
		if e.Default {
			continue
		}
		if e.Name == canonicalNAD {
			found = append(found, e)
		}
	}
	switch len(found) {
	case 0:
		return NetworkStatus{}, ErrNoAttachment
	case 1:
		return found[0], nil
	default:
		ifaces := make([]string, 0, len(found))
		for _, e := range found {
			ifaces = append(ifaces, e.Interface)
		}
		return NetworkStatus{}, fmt.Errorf("%w: interfaces %s",
			ErrAmbiguous, strings.Join(ifaces, ","))
	}
}

// Addresses returns the entry's IPs as bare addresses, dropping any prefix
// length and anything unparseable.
func (n NetworkStatus) Addresses() []string {
	out := make([]string, 0, len(n.IPs))
	for _, raw := range n.IPs {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if i := strings.Index(raw, "/"); i >= 0 {
			raw = raw[:i]
		}
		if net.ParseIP(raw) == nil {
			continue
		}
		out = append(out, raw)
	}
	return out
}
