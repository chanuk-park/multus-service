# Secondary Service Endpoint Publication Integrity

## The problem

For a primary (`eth0`) Service, endpoint membership and readiness are decided by
Kubernetes' own control plane: the EndpointSlice controller reads `podIP`, the
Pod's Ready condition, and the Service selector, and the `NodeRestriction`
admission plugin ensures a kubelet can only touch its **own** node's objects. The
ownership boundary is provided by the platform.

This project publishes **secondary** (Multus) addresses, and in doing so it
builds a *new authority chain outside* that platform machinery:

```
Service / NAD / Pod ──network-status──▶ Controller ── membership + address
                                             ▲
                          node agent ──health evidence──┘ ── readiness
                                             │
                                             ▼
                                      EndpointSlice.ready ──▶ CoreDNS ──▶ DNS
```

The security question is therefore not "how do we move a Service to net1" but:

> **which subject, presenting which evidence, may make or unmake a secondary
> address a resolvable endpoint of a Service?**

## Demonstrated attacks (`hack/attack-spoof.sh`)

The agent→controller transport is plain gRPC on a ClusterIP, reachable by any
workload with cluster network access and no credential. Measured on the live
cluster, no code changes:

| # | Attack | Result |
| --- | --- | --- |
| A1 | Forge `local_ready=false` for a **live** endpoint | DNS presence 6/6 → **0/14**; healthy endpoint removed from Service DNS |
| A2 | Kill the path, then forge `ready=true` for the **dead** endpoint | Presence **14/15** while the path is genuinely down; clients blackholed |
| G1 | Report an attachment id the controller never derived | **Rejected** — the Registry already blocks non-creation |

A1 and A2 share one root cause: the controller trusts `envelope.node`, and the
newest stream to adopt a node wins. Node identity is **self-asserted**. The
node-match check (`att.NodeName == envelope.node`) added earlier stops a confused
agent, not an impersonating one.

## Split authority

Neither control-plane intent nor runtime evidence alone may decide endpoint
visibility.

```
                Control-plane intent               Runtime evidence
        Service ∧ Pod ∧ NAD ∧ authorized       interface / address / path
        workload  → Authorized attachment       observed in the Pod netns
                         │                                │
                         ▼                                ▼
        membership: only the Controller     readiness: only the node that
        may create it (from API objects)    hosts the attachment may change it
                         └──────────────┬──────────────┘
                                        ▼
                          VisibleEndpoints(S) ⊆ AuthorizedEndpoints(S)
```

Runtime evidence cannot **create** authority; it can only **restrict or restore**
an authority the control plane already granted.

### Goals

- **G1 Non-creation.** Health evidence cannot introduce a new endpoint or
  address. *Already enforced:* the Registry is filled only by the reconciler
  from Service + Pod + network-status, and a report for an unknown
  `attachment_id` is rejected.
- **G2 Subject-bound authority.** Health evidence for an attachment may be
  submitted only by the agent that actually hosts it — established by
  authenticated provenance, never by a self-asserted string.
- **G3 Generation / freshness safety.** Evidence from a previous attachment or a
  previous agent must not change the current endpoint. *Largely in place:*
  `attachment_id = hash(PodUID | NAD | interface | IP)`, `agent_instance_id +
  sequence`, and receive-time freshness.

## G2 is buildable Kubernetes-native (`hack/tokenreview-spike.sh`, 9/9 confirmed)

The agent presents a **projected, Pod-bound ServiceAccount token** with a
dedicated audience (`health-controller`). The controller authenticates it and
derives the node from the *validated* Pod identity, never from the envelope:

```
projected Pod-bound token (aud: health-controller)
        │
        ▼  TokenReview(token, audiences=[health-controller])
authenticated=true
  user.extra:
    pod-uid    ← validated (Pod exists, UID matches)   ── authoritative
    pod-name   ← validated
    node-name  ← claim only, NOT validated at auth time ── cross-check
        │
        ▼  look the Pod up by uid
Pod.spec.nodeName  ── the authoritative node identity
        │
        ▼
accept the report only if attachment.NodeName == that nodeName
envelope.node is removed from authorization entirely
```

Confirmed on k3s v1.36 (node claims are stable since Kubernetes v1.32):

| Check | Result |
| --- | --- |
| Pod-bound token → `authenticated=true`, audience honored, SA username | ✓ |
| `user.extra` carries `pod-uid`, `pod-name`, `node-name`, `node-uid` | ✓ |
| review `pod-uid` == real Pod UID; resolves to `spec.nodeName` | ✓ |
| node-B credential resolves to node B — cannot gain node-A authority whatever `envelope.node` says | ✓ |
| health-audience token rejected for the kube-api audience, and vice versa (no credential reuse) | ✓ |
| token bound to a wrong Pod UID rejected | ✓ |
| token invalidated the moment its Pod is deleted (bonus generation safety) | ✓ |

The critical design point, per the Kubernetes docs: the `node-name` claim inside
the token is **not** verified by the apiserver at authentication time. Only the
Pod binding (existence + UID) is. So the node must be resolved via the validated
`pod-uid → Pod.spec.nodeName`, and the claim used only as a consistency check.

### Why this is more than "we added authentication"

Kubernetes' native publication path gets its ownership boundary from admission
control (`NodeRestriction`). Secondary-network publication routes around that
boundary and, left as built, permits cross-node health impersonation. The
contribution is to **restore that authority boundary** for secondary endpoints
by combining authenticated workload-to-node provenance with runtime attachment
evidence — not to bolt TLS onto a channel.

Before → after:

```
before:  Registry membership ∩ self-asserted node / runtime evidence
after:   Registry intent ∩ authenticated Pod/node provenance ∩ runtime attachment evidence
```

## Attack taxonomy

| Attack | Vulnerable baseline | This design |
| --- | --- | --- |
| Arbitrary attachment report | endpoint state manipulable | rejected outside the Registry (G1, holds) |
| Cross-node spoof | forge `envelope.node` | authenticated pod-uid → nodeName binding (G2) |
| Previous-Pod replay | can taint the current endpoint | attachment-generation binding (G3) |
| Previous-agent replay | stale state applied | instance + sequence + receive-time lease (G3) |
| Agent asserts an address | publication injection | membership is the Controller's alone (G1) |

## Status

- A1 / A2 / G1 reproduced against the system as built — motivation secured.
- G2 mechanism confirmed feasible and Kubernetes-native via the spike.
- **Next:** implement G2 in the transport (projected token from the agent,
  TokenReview + pod-uid→nodeName in the controller, `envelope.node` demoted to a
  cross-check), then re-run `attack-spoof.sh` to show A1/A2 blocked. Then extend
  the taxonomy (independent replay / stale-evidence reproductions).
