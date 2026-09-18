package attach

import (
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"

	"github.com/boanlab/multus-service/internal/model"
)

// Probe configuration is a property of the network, never of a Service.
//
// This is what makes a Node-scope path domain of (node, NAD) sound. If two
// Services sharing one NAD could name different health targets, a single shared
// path state would silently answer for both, and one of them would be wrong.
// Keeping the configuration on the NAD means every attachment in a domain is
// probing the same thing by construction.
const (
	NADProbeScope      = "secondary-service.boanlab.io/probe-scope"
	NADHealthTarget    = "secondary-service.boanlab.io/health-target"
	NADSourceInterface = "secondary-service.boanlab.io/source-interface"
)

var probeAnnotations = []string{NADProbeScope, NADHealthTarget, NADSourceInterface}

// ProbeConfigOnService returns any probe annotations found on a Service, which
// is the one place they must never appear. Honouring them there would break the
// assumption a shared path domain rests on.
func ProbeConfigOnService(svc *corev1.Service) []string {
	var found []string
	for _, a := range probeAnnotations {
		if _, ok := svc.Annotations[a]; ok {
			found = append(found, a)
		}
	}
	sort.Strings(found)
	return found
}

// ParseProbeScope reads the NAD's probe-scope annotation.
//
// The default is Endpoint. Node scope assumes the host path represents the Pod
// path, which is false for macvlan and ipvlan toward same-node endpoints, so it
// has to be asked for explicitly.
func ParseProbeScope(raw string) (model.ProbeScope, error) {
	switch raw {
	case "", "endpoint", "Endpoint":
		return model.ScopeEndpoint, nil
	case "node", "Node":
		return model.ScopeNode, nil
	default:
		return model.ScopeEndpoint, fmt.Errorf("unknown probe scope %q; want endpoint or node", raw)
	}
}
