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
| Probe config | `probe-scope`, `health-target`, `source-interface` live on the **NAD**, never on a Service. |
| Report authority | A health report can only change an attachment the controller already derived. It can never create one. |
| Freshness | Judged from when the controller accepted a report, never from the agent's clock. |
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
| 3 | gRPC health transport, snapshots, stale-report rejection | **done**, 21/21 acceptance |
| 4 | Endpoint-scope active path probe, hysteresis | next — replaces the `--path-probe=assume-ready` scaffold |
| 5 | Final readiness, all-endpoints-down guard | `readiness.go` computes it; the guard is outstanding |
| 6 | Node-scope shared path probe, `probe-scope` read from the NAD | last |

The agent supplies local state and, with `--path-probe=assume-ready`, a Phase 3
scaffold that asserts path state instead of measuring it. With `--path-probe=none`
no path report is ever fresh, so every endpoint stays `ready=false` — the correct
behaviour rather than a placeholder: an address nobody has checked must not be
advertised. Phase 4 replaces the scaffold with a real probe.

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
internal/controller      reconcile, EndpointSlice diff/apply, registry, health
                         store, readiness, gRPC server, expiry sweeper
internal/agent           the per-node observation loop and the gRPC sink
internal/agent/netns     Pod UID → sandbox netns (Resolver interface + CRI impl)
internal/agent/local     netns-scoped netlink inspection and subscription
internal/obs             JSONL measurement event stream
api/health.proto         agent → controller protocol
api/healthpb             generated Go bindings
deploy/                  RBAC, Deployment, agent DaemonSet
test/fixtures            NADs, workload, Service, VXLAN/dummy lab helper
test/e2e/phase1.sh       Phase 1 acceptance (24 checks)
test/e2e/phase2.sh       Phase 2 acceptance (14 checks)
test/e2e/phase3.sh       Phase 3 acceptance (21 checks)
test/tools/healthreport  sends deliberately bad reports, for the rejection tests
hack/measure-detection.sh    detection latency alone
hack/measure-convergence.sh  full t0 → t6 decomposition
```

## Running

```bash
make test                       # unit tests
make run                        # controller out of cluster, events to stdout
make load NODES=10.10.10.171    # build both images, import into every k3s node
make deploy                     # controller Deployment + agent DaemonSet
make e2e                        # phase 1, 2 and 3 acceptance
```

`make load` imports straight into each node's containerd, so no registry is
needed. `test/e2e/phase2.sh` and `phase3.sh` inject faults into Pod network
namespaces, so they must run on a node with root and `crictl`. `phase1.sh` parks
the node agents while it runs, because it checks the controller's behaviour with
no health reports at all, and restores them on exit.

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

### Measured decomposition

Eight runs on the two-node k3s cluster, CoreDNS at its default 5 s TTL, local
failure injected as `ip link set net1 down` inside the Pod netns
(`hack/measure-convergence.sh`):

| Stage | | Measured |
| --- | --- | --- |
| `t0 → t1` | agent detects (netlink) | 124 – 203 ms, median **129 ms** |
| `t1 → t2` | agent sends the report | 0.1 – 0.3 ms |
| `t2 → t3` | controller receives it | 0.4 – 1.6 ms |
| `t3 → t4` | controller applies it | 0.0 – 0.2 ms |
| `t4 → t5` | EndpointSlice patched | 6.8 – 11.2 ms |
| `t5 → t6` | address gone from DNS | −41 – 3764 ms |
| `t0 → t6` | end to end | **99 – 3905 ms** |

Everything from fault to published slice takes **140–225 ms**. The rest is
CoreDNS caching: `t5 → t6` is effectively uniform over `[0, TTL]`, so it averages
about half the TTL — ~2.5 s at the 5 s default. It can come out slightly
negative because the controller logs `slice_patched` after its API write
returns, while CoreDNS can already have seen that same write.

### Which term dominates depends on the failure class

This is not a single headline number, and stating one would be wrong.

**Local failures** — link down, address lost. netlink sees them in ~130 ms, so
detection is now the *smallest* term and DNS caching dominates by an order of
magnitude.

**Path failures** — underlay, tunnel, or peer loss. netlink is blind to these:
they break reachability while changing no local kernel state, so neither a host
nor a Pod netlink monitor fires at all. Detection then costs probe interval ×
failure threshold, and is likely to dominate again. Phase 4 is where that number
gets measured.

`hack/measure-detection.sh` reproduces the first row alone.

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

## The health transport

Four rules shape it, and each exists because of a way the obvious design breaks.

**Reports are not discovery.** The controller derives the attachment set from
Service + Pod + network-status and computes `attachment_id` itself. An agent
report is matched against that registry; an unknown id is rejected, never used
to create an endpoint. A report must also describe the attachment the controller
knows under that id — same NAD, interface and address — and must come from the
node the attachment actually runs on, so no node can speak for another's
endpoints.

**No clocks are compared.** Ordering uses `agent_instance_id` + `sequence`:
a fresh instance id per agent process, monotonic sequence within it. The newest
stream to open is authoritative for its node, so anything still in flight from a
restarted agent is discarded. Freshness is measured from `accepted_at`, the
controller's own clock at the moment it took the report. `observed_at` travels
on every report and is used for measurement only — a skewed node clock cannot
make a stale report look current, nor a current one look stale.

**Health is a renewable lease, not an event log.** An agent that only spoke on
change would go silent after a controller restart: the network is still healthy,
so nothing changes, and every endpoint would sit at Unknown for ever. Agents
therefore resend the full state on connect and refresh it periodically
(`--refresh`, which must stay well under the controller's `--health-ttl`).
Changes still go out immediately as deltas, so a real failure does not wait for
the next refresh.

**Snapshots commit atomically.** A controller that restarted mid-send would
otherwise reconcile against a partial set and publish the remainder as not
ready. `SnapshotBegin` … `SnapshotEnd` brackets a node's full state and the
controller replaces that node's entries in one step. Only that node's entries —
one agent's snapshot never clears another's.

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

## Health transport events

| Event | Side | Meaning |
| --- | --- | --- |
| `agent_connected` | controller | A stream opened; names the instance it superseded. |
| `health_stream_opened` / `health_stream_closed` | agent | Transport lifecycle, with the error that ended it. |
| `health_report_sent` | agent | One report left the node. `via` is `delta` or `snapshot`. |
| `health_report_received` | controller | One report arrived. |
| `health_report_applied` | controller | The store moved. `via` distinguishes the two paths. |
| `health_report_rejected` | controller | With the reason: unknown attachment, unknown path key, superseded instance, sequence did not advance. |
| `health_snapshot_applied` | controller | A node's full state was replaced; `moved` counts what actually changed. |
| `health_resync_requested` | agent | The controller asked for a full snapshot. |
| `health_expired` | controller | An entry crossed the freshness window — the agent stopped talking, which is a different event from a reported failure. |
