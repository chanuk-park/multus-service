package netns

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// podUIDLabel is the label the kubelet puts on every sandbox it creates. It is
// the only join key needed here.
const podUIDLabel = "io.kubernetes.pod.uid"

// CRIResolver resolves through the CRI runtime service. It works against any
// CRI runtime; containerd is what k3s ships.
type CRIResolver struct {
	conn   *grpc.ClientConn
	client runtimeapi.RuntimeServiceClient
}

// NewCRIResolver dials the runtime endpoint, e.g.
// unix:///run/k3s/containerd/containerd.sock.
func NewCRIResolver(ctx context.Context, endpoint string) (*CRIResolver, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", endpoint, err)
	}
	c := &CRIResolver{conn: conn, client: runtimeapi.NewRuntimeServiceClient(conn)}

	vctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := c.client.Version(vctx, &runtimeapi.VersionRequest{}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("cri version check on %s: %w", endpoint, err)
	}
	return c, nil
}

// Close releases the gRPC connection.
func (r *CRIResolver) Close() error { return r.conn.Close() }

// sandboxInfo is the verbose half of PodSandboxStatus. Its shape is
// runtime-specific, which is the reason this file exists.
type sandboxInfo struct {
	PID                int  `json:"pid"`
	NetNamespaceClosed bool `json:"netNamespaceClosed"`
	RuntimeSpec        struct {
		Linux struct {
			Namespaces []struct {
				Type string `json:"type"`
				Path string `json:"path"`
			} `json:"namespaces"`
		} `json:"linux"`
	} `json:"runtimeSpec"`
}

// Resolve returns the network namespace of the Pod's current ready sandbox.
//
// When a Pod has been recreated, the kubelet may briefly leave the old sandbox
// around, so the newest ready one wins. Returning the wrong sandbox would make
// the agent observe a namespace that is about to disappear.
func (r *CRIResolver) Resolve(ctx context.Context, podUID string) (Handle, error) {
	resp, err := r.client.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{
		Filter: &runtimeapi.PodSandboxFilter{
			LabelSelector: map[string]string{podUIDLabel: podUID},
		},
	})
	if err != nil {
		return Handle{}, fmt.Errorf("list sandboxes for pod %s: %w", podUID, err)
	}

	var best *runtimeapi.PodSandbox
	for _, s := range resp.Items {
		if s.State != runtimeapi.PodSandboxState_SANDBOX_READY {
			continue
		}
		if best == nil || s.CreatedAt > best.CreatedAt {
			best = s
		}
	}
	if best == nil {
		return Handle{}, ErrNotFound
	}

	st, err := r.client.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{
		PodSandboxId: best.Id,
		Verbose:      true,
	})
	if err != nil {
		return Handle{}, fmt.Errorf("sandbox status %s: %w", best.Id, err)
	}

	var info sandboxInfo
	if raw, ok := st.Info["info"]; ok {
		if err := json.Unmarshal([]byte(raw), &info); err != nil {
			return Handle{}, fmt.Errorf("parse sandbox info %s: %w", best.Id, err)
		}
	}
	if info.NetNamespaceClosed {
		return Handle{}, ErrNotFound
	}

	h := Handle{PodUID: podUID, SandboxID: best.Id, PID: info.PID}

	// The runtime spec path is authoritative and survives the sandbox process
	// being reaped, so prefer it.
	for _, ns := range info.RuntimeSpec.Linux.Namespaces {
		if ns.Type == "network" && ns.Path != "" {
			h.Path = ns.Path
			break
		}
	}
	if h.Path == "" && info.PID > 0 {
		h.Path = fmt.Sprintf("/proc/%d/ns/net", info.PID)
	}
	if h.Path == "" {
		return Handle{}, fmt.Errorf("sandbox %s exposes no network namespace", best.Id)
	}
	if _, err := os.Stat(h.Path); err != nil {
		return Handle{}, fmt.Errorf("netns %s: %w", h.Path, err)
	}
	return h, nil
}
