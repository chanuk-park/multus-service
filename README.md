# multus-service

Kubernetes Services for the secondary network interfaces of a Pod, published only while those interfaces actually work.

Go 1.26 | Kubernetes | Multus CNI | controller-runtime | gRPC

[Quick start](#quick-start) | [How it works](#how-it-works) | [Configuration](#configuration) | [Security model](#security-model) | [Development](#development)

## What it is

A Pod attached through Multus can carry its real traffic on a secondary interface: a 5G core function talks N2, N3 or N4 over `net1`, not over `eth0`. Kubernetes only knows the primary address, so a Service cannot point at the interface that matters, and when that interface fails nothing in the cluster notices. The secondary link can go down, its address can be flushed and its overlay can be blackholed while the Pod stays `Running` and `Ready`.

multus-service publishes the secondary addresses of selected Pods as an EndpointSlice behind a headless Service, so they resolve through cluster DNS like any other Service, and marks each address `ready` only while a node agent has recently confirmed that the interface is up and its path is reachable.

Four decisions shape it.

**1. Membership comes only from Kubernetes objects.** Which addresses belong to a Service is decided by the controller from the Service, its workload selector, the NetworkAttachmentDefinition and each Pod's `network-status` annotation. A health report can change whether a known address is ready. It can never add an address. When two Pods claim the same address on the same network, both are withheld rather than one being picked.

**2. The agent observes; the controller decides.** A DaemonSet agent resolves each Pod by UID to its network namespace through the CRI, watches the interface there with netlink, and probes a declared target from the secondary interface itself. The controller combines that evidence with Pod readiness and freshness and writes the EndpointSlice. An address nobody has checked is never advertised.

**3. Reporting health is a permission, scoped to one node.** An agent connects over server-authenticated TLS with a projected, Pod-bound ServiceAccount token. The controller verifies the token with TokenReview, checks an RBAC grant with SubjectAccessReview, confirms that the Pod is a live agent, and binds the stream to the node that Pod runs on. A valid token from an ordinary workload is refused, and an agent on one node cannot report for another node's endpoints. Deleting an agent Pod revokes its stream immediately.

**4. Old evidence expires.** Attachment IDs include the Pod UID, so a recreated Pod with the same address is a new subject. Each agent instance carries a monotonic sequence, a newer instance supersedes an older one, and every report is valid only for a lease measured from the time the controller received it. When evidence stops, readiness is withdrawn on its own.

## How it works

```
 Operator                                   Each node
 Service + NAD ─┐                           ┌───────────────────────────┐
                ▼                           │ Node Agent                │
 ┌─────────── Controller ───────────┐       │  Observer   netlink in    │
 │ Registry  ◀── Pod network-status │       │             the Pod netns │
 │ Authenticator ◀────── TLS stream ┼───────┤  ICMP probe via net1      │
 │ Report Admission                 │       │  Sender     snapshot +    │
 │ Health Store  (lease, snapshot)  │       │             sequence      │
 │ Slice Writer ──▶ EndpointSlice ──┼─▶ CoreDNS ─▶ secondary IP in DNS  │
 └──────────────────────────────────┘       └───────────────────────────┘
```

An endpoint is published `ready` when the Pod is Ready, the interface exists, holds its address and has a usable link, the path probe succeeds, and both kinds of evidence are fresh.

## Quick start

Requirements: a Kubernetes cluster with Multus CNI and a containerd CRI, `kubectl`, Docker and `openssl`. It is developed and tested on k3s v1.36 with Multus in thin mode.

```bash
git clone https://github.com/chanuk-park/multus-service.git
cd multus-service

# build both images and import them into every node's containerd
make load NODES="<worker-ip> ..."

# CA and server certificate for the agent-to-controller channel
./hack/gen-certs.sh

# RBAC, controller Deployment, agent DaemonSet
make deploy
```

The agent DaemonSet reads the CRI socket at `/run/k3s/containerd/containerd.sock`. On other distributions, set `--runtime-endpoint` in `deploy/agent-daemonset.yaml`.

### Publishing a secondary interface

Declare the network on a selectorless headless Service, and the probe target on the NetworkAttachmentDefinition:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: amf-n2
  annotations:
    secondary-service.boanlab.io/network: n2-net
    secondary-service.boanlab.io/workload-selector: app=amf
spec:
  clusterIP: None
  ports: [{name: n2, port: 38412, protocol: SCTP}]
---
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata:
  name: n2-net
  annotations:
    secondary-service.boanlab.io/probe-scope: endpoint
    secondary-service.boanlab.io/health-target: "10.215.0.1"
spec:
  config: '{ ... }'
```

`amf-n2.<namespace>.svc.cluster.local` then resolves to the `n2-net` addresses of the healthy `app=amf` Pods. A Service with a `spec.selector` or a cluster IP is rejected, because Kubernetes would publish the primary addresses beside it.

## Configuration

Values below are the flag defaults; `deploy/` sets the ones used in a normal installation.

### Controller

| Flag | Default | Meaning |
| --- | --- | --- |
| `--health-bind-address` | `:9090` | gRPC listener for agent reports |
| `--tls-dir` | `/etc/multus-service/tls` | Directory with `tls.crt` and `tls.key`. Empty serves plaintext and is for development only |
| `--health-audience` | `health-controller` | Token audience agents must present |
| `--health-ttl` | `3s` | Lease of a report, measured from receipt. `deploy/` uses `5s` |
| `--agent-namespace` | `multus-service-system` | Namespace of the agent Pods |
| `--agent-service-account` | `multus-service-agent` | ServiceAccount of the agent Pods |
| `--agent-label` | `app=multus-service-agent` | Label that identifies agent Pods |
| `--auth-mode` | `full` | `full` (TokenReview, RBAC, live agent, node binding). `token` and `token-sa` are weaker modes kept for evaluation |
| `--events-file` | empty | JSON-lines event stream, `-` for stdout |
| `--leader-elect` | `false` | Leader election for replicated controllers |

### Agent

| Flag | Default | Meaning |
| --- | --- | --- |
| `--controller-address` | `$CONTROLLER_ADDRESS` | Controller gRPC address |
| `--controller-server-name` | `$CONTROLLER_SERVER_NAME` | Expected name in the controller certificate |
| `--token-path` | `/var/run/secrets/multus-service/health-token` | Projected ServiceAccount token |
| `--ca-path` | `/etc/multus-service/ca/ca.crt` | CA that issued the controller certificate |
| `--runtime-endpoint` | `unix:///run/k3s/containerd/containerd.sock` | CRI socket used to find Pod network namespaces |
| `--path-probe` | `none` | `icmp` probes the secondary path. With `none` no endpoint becomes ready |
| `--probe-interval`, `--probe-timeout` | `500ms`, `400ms` | Path probe cadence |
| `--failure-threshold`, `--success-threshold` | `3`, `2` | Consecutive probes before a path state changes |
| `--refresh` | `1s` | Full state is re-sent at least this often. `deploy/` uses `2s` |
| `--max-session` | `300s` | Stream lifetime before the agent reconnects and re-reads its token |

The right to report is an ordinary RBAC rule: `create` on the virtual resource `healthreports.secondary-service.boanlab.io`, granted to the agent ServiceAccount in `deploy/rbac.yaml`.

## Security model

The controller holds write access to EndpointSlices, so whatever it believes becomes what clients resolve. The design keeps three kinds of authority separate: membership comes from Kubernetes objects, health evidence comes only from the current agent of the node that hosts the endpoint, and evidence from an earlier Pod or agent instance cannot change current state. Authentication runs once per stream (about 6 ms for TokenReview and SubjectAccessReview together), and the per-report path stays in memory.

The threat model, the attacks reproduced against weaker modes and the measured costs are in [docs/security-model.md](docs/security-model.md).

## Development

```bash
make build      # compile
make test       # unit tests
make run        # controller outside the cluster, events to stdout
make e2e        # end-to-end suites against a live cluster
make undeploy   # remove the installation
```

The end-to-end suites inject faults into Pod network namespaces and need root and `crictl` on the node they run from. Attack and measurement harnesses are in `hack/`, and the raw data behind the documented results is in `docs/data/`.

## Repository layout

```
multus-service/
├── api/              gRPC health protocol (health.proto and generated code)
├── cmd/
│   ├── controller/   controller entry point
│   └── agent/        node agent entry point
├── internal/
│   ├── controller/   Registry, authentication, report admission, health store, EndpointSlice writer
│   ├── agent/        netns resolution, netlink observation, path probe, report sender
│   ├── multus/       network-status parsing
│   ├── attach/       annotation contract shared by controller and agent
│   └── model/        attachment and health types
├── deploy/           RBAC, controller Deployment, agent DaemonSet
├── hack/             certificates, attack reproductions, measurement scripts
├── test/             end-to-end suites, fixtures, test clients
└── docs/             design notes, security model, measurement data
```

## Documentation

| Document | Contents |
| --- | --- |
| [docs/security-model.md](docs/security-model.md) | Threat model, authority split, attack reproductions, cost measurements |
| [docs/design.md](docs/design.md) | Implementation notes: netns resolution, path probe, health transport, events |

## License

Apache-2.0. See [LICENSE](LICENSE).
