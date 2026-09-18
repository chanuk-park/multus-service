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
| Duplicate address | Two Pods claiming one address: **neither** is published, and the collision is reported. |
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

**A selectorless headless Service is stored as dual-stack.** *(A lab note, not a
design argument — the invariant below does not lean on it.)* The API server
defaults it to `ipFamilyPolicy: RequireDualStack, ipFamilies: [IPv4, IPv6]`,
while the same Service *with* a selector gets `SingleStack, [IPv4]`. On a
single-stack cluster that means you cannot convert one into the other by
patching — adding a selector is rejected with
`spec.ipFamilies[1]: Invalid value: "IPv6": not configured on this cluster`.
In practice the invariant is therefore violated at creation time, not by
mutation, which is how `test/e2e/phase1.sh` exercises it. The controller still
rejects `spec.selector` outright: a user can create such a Service from scratch
on any cluster, so the correctness argument must not rest on an API quirk.

**Per-node IPAM produces duplicate addresses.** `host-local` allocates from its
range independently on every node, so one NAD subnet shared across nodes hands
the same address to Pods on different nodes. Found in the Phase 1 E2E, not
hypothesised.

Every claimant is withdrawn, not all but one. Picking a winner is deterministic
but it is not correct: DNS carries the address alone, so a client cannot be
steered to the Pod the controller chose, `targetRef` plays no part in
forwarding, and both Pods still own the address in the data plane whatever the
slice says. Publishing neither, with a `DuplicateAddress` Warning event naming
every claimant, is the only honest answer. Use per-node ranges or a
cluster-wide IPAM to avoid the situation entirely.

## Status

| Phase | Scope | State |
| --- | --- | --- |
| 1 | network-status parser; Service → EndpointSlice; Pod lifecycle; `ready=false` pre-registration | **done**, 23/23 acceptance on a live cluster |
| 2 | Node agent: Pod UID → sandbox netns, netlink local health | **done**, 14/14 acceptance |
| 3 | gRPC health transport, stale-report rejection | protocol fixed in `api/health.proto`; store and freshness already in place |
| 4 | Endpoint-scope active path probe, hysteresis | |
| 5 | Final readiness, all-endpoints-down guard | `readiness.go` computes it; the guard is outstanding |
| 6 | Node-scope shared path probe | last |

The agent now supplies local state; until a path probe exists no path report is
ever fresh, so every endpoint stays `ready=false`. That is the correct behaviour
rather than a placeholder: an address nobody has checked must not be advertised.

### Two kinds of state, two streams

Local state and path state are reported separately because they have different
cardinality, and collapsing them would hide that:

```
local state   one per attachment, always

path state    Endpoint scope -> one per attachment
              Node scope     -> one per (node, NAD)

    Pod A ─ local A ┐
    Pod B ─ local B ├── shared Path(node1, NAD-X)
    Pod C ─ local C ┘
```

A single message carrying both would force the agent to copy one Node-scope
probe result into N per-Pod reports — the duplication the scope exists to
remove — and give each copy its own hysteresis, reintroducing the transition
skew that sharing is meant to eliminate. The controller joins the two on
`attachment_id` and on the scope key, and `HealthStore` keeps one map per kind.

Node scope therefore removes the *shared path probe and its hysteresis*, not the
netns entry. Local observation stays per Pod under both scopes.

## Layout

```
cmd/controller           controller entrypoint
cmd/agent                node agent entrypoint
internal/multus          network-status parsing and NAD canonicalisation
internal/attach          the annotation contract, shared by controller and agent
internal/model           Attachment identity, LocalHealth, PathHealth, path domains
internal/controller      reconcile, EndpointSlice diff/apply, health store, readiness
internal/agent           the per-node observation loop
internal/agent/netns     Pod UID → sandbox netns (Resolver interface + CRI impl)
internal/agent/local     netns-scoped netlink inspection and subscription
internal/obs             JSONL measurement event stream
api/health.proto         agent → controller protocol
deploy/                  RBAC, Deployment, agent DaemonSet
test/fixtures            NADs, workload, Service, VXLAN/dummy lab helper
test/e2e/phase1.sh       Phase 1 acceptance (24 checks)
test/e2e/phase2.sh       Phase 2 acceptance (14 checks)
hack/measure-detection.sh  detection-latency measurement
```

## Running

```bash
make test                       # unit tests
make run                        # controller out of cluster, events to stdout
make load NODES=10.10.10.171    # build both images, import into every k3s node
make deploy                     # controller Deployment + agent DaemonSet
make e2e                        # phase 1 and phase 2 acceptance
```

`make load` imports straight into each node's containerd, so no registry is
needed. `test/e2e/phase2.sh` injects faults into Pod network namespaces, so it
must run on a node with root and `crictl`.

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

Bench numbers so far, on the two-node k3s cluster:

| Interval | Measured |
| --- | --- |
| Detection (link down → `failure_detected`) | 121–158 ms, median **129 ms** over 6 runs |
| Control plane (slice patch → API watch event) | ≈ **0.23 s** |
| DNS convergence (slice change → answer change) | ≈ **0.97 s** at a 1 s TTL, 5 s is the default |

Detection is the term the design controls, and at netlink speed it is now the
smallest of the three. What netlink cannot see at all — an underlay blackhole,
which changes no local kernel state — is what Phase 4 adds the active probe for.
Reproduce the first row with `hack/measure-detection.sh`.

## Resolving a Pod to its network namespace

```
Pod UID -> CRI PodSandbox -> sandbox netns -> network-status.interface
```

No step involves an IP address. `PodUID -> IP -> netns` would be the obvious
shortcut and it is unsafe: per-node IPAM means two Pods on two nodes can hold
the same address, so the lookup is ambiguous exactly when it matters.

`internal/agent/netns` keeps this behind a `Resolver` interface so the
runtime-specific part stays one file deep. The CRI implementation lists
sandboxes filtered by the `io.kubernetes.pod.uid` label, takes the newest ready
one, and reads the network namespace path out of the verbose
`PodSandboxStatus`, falling back to `/proc/<pid>/ns/net`. A cache in front of it
drops an entry as soon as its namespace file disappears, which is what a sandbox
recreate looks like from here.

## Why the agent must enter the namespace

macvlan and ipvlan move the child device wholly into the Pod netns, leaving
nothing on the host for netlink to watch. Measured: through link-down,
address-flush and underlay blackhole alike, a host-netns monitor stayed
completely silent while the Pod-netns monitor saw the first two.

Both link and address events are needed. Taking a link down emits only
`RTM_NEWLINK` with the DOWN flag and **leaves the IPv4 address in place**, so an
address-only subscription misses the fault entirely; flushing the address emits
only `RTM_DELADDR`. `test/e2e/phase2.sh` asserts exactly this: during link-down
the agent reports `address_present=true` alongside `link_usable=false`.

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

## Events emitted by the node agent

| Event | Meaning |
| --- | --- |
| `agent_started` | Agent came up on a node. |
| `attachment_observed` | First observation of an attachment, carrying the resolved netns path and sandbox id. |
| `local_health` | One observation per attachment per resync, with all three checks reported individually. Also the report heartbeat that keeps the controller's freshness check satisfied. |
| `failure_detected` / `recovery_detected` | Local readiness changed. These are the anchors detection latency is measured from. |
| `attachment_retired` | The attachment is gone; a late report carrying its id can no longer be matched. |
