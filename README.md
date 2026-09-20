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

## Security direction

The work is being reframed around **secondary endpoint publication integrity**:
the publication path is a new authority chain outside Kubernetes' native
control plane, and a forged health report can add or remove what a Service
resolves to. See `docs/security-model.md`. Two attacks reproduce against the
system as built (`hack/attack-spoof.sh`), and the Kubernetes-native defence is
confirmed feasible (`hack/tokenreview-spike.sh`). The systems phases below
become the substrate the security mechanisms build on, not the contribution.

## Status

| Phase | Scope | State |
| --- | --- | --- |
| 1 | network-status parser; Service → EndpointSlice; Pod lifecycle; `ready=false` pre-registration | **done**, 23/23 acceptance on a live cluster |
| 2 | Node agent: Pod UID → sandbox netns, netlink local health | **done**, 14/14 acceptance |
| 3 | gRPC health transport, snapshots, stale-report rejection | **done**, 21/21 acceptance |
| 4 | Endpoint-scope active path probe, hysteresis | **done**, 21/21 acceptance |
| 5 | Security: G2 subject-bound authority (TokenReview, AgentRegistry, session revocation, TLS) | **done**, 8/8 acceptance; attack now blocked |
| 6 | Node-scope shared path probe, `probe-scope` read from the NAD | last — the probe manager already keys on scope, so this is target construction, not new machinery |

`--path-probe=icmp` probes the real secondary datapath. `--path-probe=none`
produces no path state, so every endpoint stays `ready=false` — the correct
answer rather than a placeholder: an address nobody has checked must not be
advertised. `assume-ready` asserts path state without measuring it. It is a debugging aid
only — no acceptance test uses it — and `--evaluation-mode` refuses to start
with it, so a measurement run cannot silently report numbers the system never
took.

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
internal/agent/probe     ICMP prober, hysteresis state machine, probe manager
internal/obs             JSONL measurement event stream
api/health.proto         agent → controller protocol
api/healthpb             generated Go bindings
deploy/                  RBAC, Deployment, agent DaemonSet
test/fixtures            NADs, workload, Service, VXLAN/dummy lab helper
test/e2e/phase1.sh       Phase 1 acceptance (24 checks)
test/e2e/phase2.sh       Phase 2 acceptance (14 checks)
test/e2e/phase3.sh       Phase 3 acceptance (21 checks)
test/e2e/phase4.sh       Phase 4 acceptance (21 checks)
test/e2e/phase5.sh       Phase 5 security acceptance (G2, 8 checks)
test/tools/healthreport  sends deliberately bad reports, for the rejection tests
hack/measure-detection.sh    detection latency alone
hack/measure-convergence.sh  full t0 → t6 decomposition
hack/attack-spoof.sh         forged-report attack reproduction (A1/A2/G1)
hack/tokenreview-spike.sh    G2 feasibility: Pod-bound token → authoritative node
hack/gen-certs.sh            issue the controller CA + server cert
test/tools/spoof             the adversary used by attack-spoof.sh
docs/security-model.md       threat model, split-authority design, spike results
```

## Running

```bash
make test                       # unit tests
make run                        # controller out of cluster, events to stdout
make load NODES=10.10.10.171    # build both images, import into every k3s node
make deploy                     # controller Deployment + agent DaemonSet
make e2e                        # phase 1 through 4 acceptance
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

### Method

Anchors are grouped by the clock that produced them, and each interval is
reported within one clock wherever possible:

```
driver      t0  fault injected            t6  address gone from the DNS answer
agent       a1  failure_detected          a2  health_report_sent
controller  c1  health_report_received    c2  health_report_applied
            c3  slice_patch_begin         c4  slice_patched (write returned)
```

Two things this fixes. DNS convergence is measured from **c3**, when the write
is issued, not from c4: CoreDNS watches the API server and can observe the write
before the controller's call returns, so measuring from c4 produces negative
intervals that are an artefact rather than a result. And a run whose anchors are
not monotonic is reported invalid rather than averaged in — an interval that
runs backwards means the measurement is wrong, not that the system is fast.

### Measured decomposition

20 runs, all valid, two-node k3s cluster, CoreDNS at its default 5 s TTL, local
failure injected as `ip link set net1 down` inside the Pod netns
(`hack/measure-convergence.sh`). All values in ms:

| Interval | Clock | min | median | p95 | max |
| --- | --- | --- | --- | --- | --- |
| detection `t0 → a1` | cross | 108.4 | **127.7** | 133.2 | 142.5 |
| agent queue `a1 → a2` | agent | 0.1 | 0.2 | 0.2 | 0.5 |
| transport `a2 → c1` | cross | 0.4 | 0.7 | 2.4 | 2.4 |
| store apply `c1 → c2` | ctrl | 0.0 | 0.1 | 0.1 | 0.2 |
| to patch `c2 → c3` | ctrl | 0.6 | 0.8 | 1.8 | 8.4 |
| patch write `c3 → c4` | ctrl | 5.5 | 8.8 | 9.8 | 10.2 |
| DNS converge `c3 → t6` | cross | 120.9 | **2694.3** | 4151.8 | 4243.3 |
| user visible `t0 → t6` | driver | 245.1 | **2812.9** | 4284.4 | 4387.1 |

The system itself adds little: transport and store apply are sub-millisecond,
the EndpointSlice write is ~9 ms, and everything from fault to issued write is
about 130 ms. The rest is CoreDNS caching, effectively uniform over `[0, TTL]`,
so it averages about half the TTL.

A path failure adds `probe interval × failure threshold` in front of all of
this. Measured at the defaults (500 ms, k=3): **1000 ms** from the first failed
sample to withdrawal, over exactly 3 failed samples.

### Which term dominates depends on the failure class

This is not a single headline number, and stating one would be wrong.

**Local failures are event-driven.** A link going down or an address being lost
emits netlink immediately: ~130 ms to detection, so detection is the *smallest*
term and DNS caching dominates by an order of magnitude.

**Path failures are probe-driven.** netlink is blind to them — they break
reachability while changing no local kernel state, so neither a host nor a Pod
netlink monitor fires at all. Detection then costs `interval × k`, which at the
defaults is 1000 ms: comparable to the DNS term rather than dwarfed by it, and
tunable in a way the DNS term is not.

The two classes therefore have their convergence budgets dominated by different
components, and a single headline number would misrepresent both.

`hack/measure-detection.sh` reproduces the netlink detection row alone.

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

## The path probe

What a path probe means is deliberately narrow:

```
P(e) = reachability from the endpoint's secondary source
       to the health target declared on its NAD
```

It is **not** a claim that every client can reach the endpoint. A partial
partition, or connectivity that differs per client, is outside what one probe can
say. The value comes from where the sample is sourced, not from the target being
special.

**Sourced inside the Pod netns, bound explicitly.** Entering the namespace and
letting its routing table pick the interface would usually work, and "usually"
is not a correctness argument: a default route, a second attachment or a policy
rule could send the probe out of a different interface and the result would still
look like a healthy secondary path. The socket is opened inside the namespace,
bound to the secondary address, and pinned to the interface with
`SO_BINDTODEVICE`. Every sample records `source_ip`, `interface` and `target`.

**ICMP, not TCP.** The model already separates three things — `PodReady` covers
the application, `LocalReady` covers the attachment, `PathReady` covers network
reachability. A TCP probe against a remote service port would fold remote
application availability back into the path term and blur that separation.

**The probe does not decide.** It produces samples; a separate state machine
applies hysteresis; only then does a `PathHealth` exist:

```
Raw probe ── success / failure / RTT ──▶ state machine ── k fails, m successes ──▶ PathHealth
```

Keeping them apart is what lets Node scope share one state machine across a
whole `(node, NAD)` domain instead of running one per attachment:

```
Endpoint scope                     Node scope
  A → probe A → hysteresis A         (node, NAD) → probe → hysteresis ─┬─ A
  B → probe B → hysteresis B                                          ├─ B
  C → probe C → hysteresis C                                          └─ C
```

Every sample is logged as `path_probe`, every transition as
`path_state_changed`. Both are needed: the gap between them is the hysteresis
cost, and it can only be measured if the first failed sample is in the stream
alongside the decision it eventually caused.

**Timeout below interval.** A failing path fails by timing out, so a timeout
longer than the probe interval stretches the effective sampling period — and the
thresholds are expressed in samples, so the stretch shows up as unexplained
delay. A round that overruns is skipped and recorded as `probe_round_skipped`
rather than silently pushing the next one out.

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

## Threat model and scope

The controller, the node agents and the channel between them are assumed to be a
**trusted control plane**. The transport is plain gRPC with no peer
authentication.

That bounds what one of its checks buys. A report must come from the node its
attachment actually runs on, which stops a misconfigured or confused agent from
speaking for another node's endpoints. It does **not** stop a malicious client
that simply puts the target node's name in the envelope. Making it do so would
need mTLS or a node-bound credential, which is outside this work — and is a
reasonable place for follow-on work to start.

The registry check is a different matter and holds regardless: an attachment the
controller never derived cannot be created by a report, whatever the reporter
claims to be.

## Probe events

| Event | Meaning |
| --- | --- |
| `probe_loop_started` | Probe loop came up, with its interval and thresholds. |
| `path_probe` | One sample: success, RTT, error kind, and the source it was bound to. |
| `path_state_changed` | The debounced state moved, with the streak that caused it. |
| `probe_round_skipped` | A round overran its interval, so the effective sampling period is longer than configured. |
| `readiness_changed` | The controller's verdict for an attachment changed, naming which term of the conjunction blocked it. |
