package attach

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/boanlab/multus-service/internal/model"
)

// NADGroupVersionKind identifies a NetworkAttachmentDefinition.
var NADGroupVersionKind = schema.GroupVersionKind{
	Group: "k8s.cni.cncf.io", Version: "v1", Kind: "NetworkAttachmentDefinition",
}

// NewNAD returns an empty object typed as a NAD, for a client Get.
func NewNAD() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(NADGroupVersionKind)
	return u
}

// SplitNAD splits a canonical "ns/name".
func SplitNAD(canonical string) (namespace, name string, err error) {
	parts := strings.SplitN(canonical, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("not a canonical network reference: %q", canonical)
	}
	return parts[0], parts[1], nil
}

// ProbeConfig is how a network says it should be health-checked.
//
// It lives on the NAD rather than on a Service because a Node-scope path domain
// is shared by every attachment of one NAD on one node: two Services naming
// different health targets would otherwise be answered by a single shared path
// state, and one of them would be wrong.
type ProbeConfig struct {
	Scope model.ProbeScope
	// Target is the address probed. Empty means the network has not opted in,
	// and no path state is produced for it.
	Target string
	// SourceInterface overrides the interface taken from network-status. It
	// exists for networks whose attachment name differs from the one that
	// should carry probe traffic; normally it is empty.
	SourceInterface string
}

// Configured reports whether the network opted into path probing.
func (p ProbeConfig) Configured() bool { return p.Target != "" }

// ProbeFromNAD reads the probe contract off a NAD's annotations.
func ProbeFromNAD(u *unstructured.Unstructured) (ProbeConfig, error) {
	ann := u.GetAnnotations()
	scope, err := ParseProbeScope(ann[NADProbeScope])
	if err != nil {
		return ProbeConfig{}, fmt.Errorf("%s/%s: %w", u.GetNamespace(), u.GetName(), err)
	}
	return ProbeConfig{
		Scope:           scope,
		Target:          strings.TrimSpace(ann[NADHealthTarget]),
		SourceInterface: strings.TrimSpace(ann[NADSourceInterface]),
	}, nil
}
