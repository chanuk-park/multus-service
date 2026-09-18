# multus-service

Publishes the **secondary** (Multus) addresses of a workload as a Kubernetes
Service, and keeps that publication honest by health-checking the addresses
themselves.

A 5G core NF talks over N2/N3/N4, not over `eth0`. Kubernetes only ever learns
the primary address, so a Service cannot name the interface that actually
carries traffic — and nothing in the cluster notices when that interface dies.
Measurements behind this design are in `docs/` of the companion lab write-up;
the short version is that a Pod can have its secondary link down, its address
flushed, and its overlay blackholed, and the Pod object will not change by a
single field.

## Shape

```
              Kubernetes API
                    │
       Service / Pod / EndpointSlice watch
                    ▼
          ┌──────────────────┐
          │ Discovery/Ready  │   owns Kubernetes state
          │   Controller     │   decides the published ready value
          └────────┬─────────┘
                   │ EndpointSlice reconcile
                   ▼
             EndpointSlice ──▶ CoreDNS ──▶ secondary IP in DNS

  each worker node
  ┌─────────────────────┐
  │ Node Agent          │   measures network state only
  │  enters Pod netns   │
  │   ├ local observation (netlink: link + address)
  │   └ active path probe (sourced from the Pod netns)
  └─────────┬───────────┘
            │ gRPC health report
            ▼
        Controller
```

The split is the point: the **controller** never measures, the **agent** never
decides. Readiness depends on Pod state the agent does not watch, so it can only
be computed centrally.

## Contract

A managed Service looks like this:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: amf-n2
  annotations:
    secondary-service.boanlab.io/network: oai/n2-net       # NAD, bare name resolves in this namespace
    secondary-service.boanlab.io/workload-selector: app=oai-amf
spec:
  clusterIP: None        # required
  # no spec.selector     # required
  ports:
    - {name: n2, port: 38412, protocol: TCP}
```

### Invariants

| Item | Rule |
| --- | --- |
| Service | `clusterIP: None` and no `spec.selector`. Both are enforced; a violation withdraws the slices and raises a Warning event. |
| Network selection | Service annotation names the NAD. |
| Workload selection | Service annotation carries the label selector. |
| Secondary IP | Read from `network-status`, for the named NAD only. |
| Interface | Taken from `network-status.interface`. `net1` is never assumed. |
| New endpoint | Created `ready=false`. |
| Healthy | `PodReady && LocalReady && PathReady && report is fresh`. |
| `ready` | Always written explicitly. Never nil. |
| Local state | Observed only from inside the Pod netns. |
| Node scope | Shares the *path* probe across a `(node, NAD)`. It does **not** remove the netns entry — local observation stays per-Pod. |
| Duplicate address | Two Pods claiming one address publish one endpoint, and the collision is reported. |
| Kernel bypass | DPDK / vfio-pci out of scope. |

Two of those deserve their reasons spelled out, because they look like style
choices and are not:

**No `spec.selector`.** With a selector the built-in EndpointSlice controller
adopts the Service and creates its own slice from primary Pod IPs. CoreDNS
merges every slice carrying the same `kubernetes.io/service-name` label, so the
primary address appears in the answer beside the secondary one. Measured on a
live cluster: the answer set came back as `10.100.60.15` *and* `10.42.0.88`.

**`ready` is never nil.** CoreDNS treats a missing `ready` as healthy — both
`conditions: {}` and an absent `conditions` block resolve. There is no "unknown"
value at the DNS layer, so omitting `ready` while the health state is still
unknown advertises an unchecked address.

### Two things the API does that are easy to trip over

**A selectorless headless Service is stored as dual-stack.** The API server
defaults it to `ipFamilyPolicy: RequireDualStack, ipFamilies: [IPv4, IPv6]`,
while the same Service *with* a selector gets `SingleStack, [IPv4]`. On a
single-stack cluster that means you cannot convert one into the other by
patching — adding a selector is rejected with
`spec.ipFamilies[1]: Invalid value: "IPv6": not configured on this cluster`.
In practice the invariant is therefore violated at creation time, not by
mutation, which is how `test/e2e/phase1.sh` exercises it.

**Per-node IPAM produces duplicate addresses.** `host-local` allocates from its
range independently on every node, so one NAD subnet shared across nodes hands
the same address to Pods on different nodes. DNS is cluster-wide, so publishing
both would put a node-local address into an answer any client can receive. The
controller keeps one endpoint — chosen by `(IP, PodUID)` so the winner never
flaps — and raises a `DuplicateAddress` Warning event naming the losers. Use
per-node ranges or a cluster-wide IPAM to avoid the situation entirely.

## Status

| Phase | Scope | State |
| --- | --- | --- |
| 1 | network-status parser; Service → EndpointSlice; Pod lifecycle; `ready=false` pre-registration | **done**, 23/23 acceptance on a live cluster |
| 2 | Node agent: netns mapping, netlink local health | next |
| 3 | gRPC health protocol, controller health store | protocol drafted in `api/health.proto` |
| 4 | Endpoint-scope active path probe, hysteresis | |
| 5 | Final readiness, all-endpoints-down guard | partly in `readiness.go` |
| 6 | Node-scope shared path probe | last |

Until an agent exists, no report is ever fresh, so every endpoint stays
`ready=false`. That is the correct behaviour rather than a placeholder: an
address nobody has checked must not be advertised.

## Layout

```
cmd/controller        controller entrypoint
cmd/agent             node agent entrypoint (Phase 2)
internal/multus       network-status parsing and NAD canonicalisation
internal/model        Attachment, Report, attachment identity
internal/controller   reconcile, EndpointSlice diff/apply, health store, readiness
internal/obs          JSONL measurement event stream
api/health.proto      agent → controller protocol
deploy/               RBAC and Deployment
test/fixtures         NADs, workload, Service, VXLAN lab helper
test/e2e/phase1.sh    Phase 1 acceptance
```

## Running

```bash
make test                  # unit tests
make run                   # controller out of cluster, events to stdout
make load NODES=10.10.10.171   # build image, import into every k3s node
make deploy
make e2e                   # Phase 1 acceptance against the running controller
```

`make load` imports straight into each node's containerd, so no registry is
needed.

## Measurement

The controller writes one JSON object per line to `--events-file` (`-` for
stdout). This is wired in from the first commit because the expensive mistake is
finishing a failure-injection campaign and then discovering the timestamps were
never emitted.

```json
{"ts":1789...,"event":"attachment_discovered","pod":"amf-0","ip":"10.244.77.3","attachment_id":"a1b2..."}
{"ts":1789...,"event":"slice_patched","slice":"amf-n2-secondary-ipv4","op":"update","endpoints":5,"ready":4}
```

The intervals the evaluation reports are differences between these events and
two externally observed points:

| Interval | From | To |
| --- | --- | --- |
| Detection | fault injected (external `t0`) | `failure_detected` |
| Report | `failure_detected` | `health_report_received` |
| Control plane | `health_report_received` | `slice_patched` |
| DNS convergence | `slice_patched` | address gone from DNS (external `t4`) |
| User-visible outage | `t0` | last failed request (external `t5`) |

Bench numbers so far: control plane ≈ 0.23 s, DNS convergence ≈ 0.97 s at a
1 s TTL (5 s is the CoreDNS default). Detection dominates everything else, which
is why Phase 2–4 matter more than any optimisation on this side.

## Attachment identity

`attachment_id = hash(PodUID | NAD | interface | IP)`.

A Pod UID alone is not enough. A health report that was in flight while the
sandbox was recreated would otherwise be applied to the new attachment, which
may sit on a different interface with a different address. The controller
recomputes the id from the current `network-status` and drops any report that
does not match.

## Events raised on the Service

| Reason | When |
| --- | --- |
| `InvalidService` | `spec.selector` is set, or the Service is not headless. Owned slices are withdrawn. |
| `MissingSelector` / `InvalidSelector` | The workload-selector annotation is absent or unparseable. An empty selector would match every Pod, so it is refused rather than defaulted. |
| `InvalidNetwork` | The network annotation is not a usable NAD reference. |
| `AmbiguousAttachment` | A Pod attaches the named NAD more than once. The Pod is skipped; no interface is guessed. |
| `DuplicateAddress` | Two Pods claim one address. One endpoint is published, the rest named in the message. |
| `SliceLimit` | More than 1000 attachments for one family. |
| `UnresolvedTargetPort` | A named `targetPort` cannot be resolved here; the Service port is published instead. |
| `BadNetworkStatus` | A Pod's network-status annotation does not parse. |
