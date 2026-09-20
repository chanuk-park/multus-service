#!/usr/bin/env bash
# RQ3: availability cost of the security session lifecycle.
#
# Deletes the agent Pod hosting a ready endpoint and times the secure handoff:
#
#   t0 agent Pod deleted
#      -> SessionManager revoke, old stream torn down
#      -> new DaemonSet Pod, projected token, TokenReview + AgentRegistry auth
#      -> new stream (t_auth)
#      -> SnapshotBegin .. SnapshotEnd atomic commit (t_snap)
#      -> endpoint ready restored (t1)
#
# Three metrics per iteration:
#   revoke -> authenticated stream        (session re-establishment)
#   authenticated stream -> snapshot apply (state recovery)
#   agent deletion -> endpoint ready       (user-visible recovery)
#
# usage: measure-reconnect-recovery.sh [N]
set -u
N=${1:-20}
NS=${NS:-ms-recov}
SVC=amf-recov
SLICE="$SVC-secondary-ipv4"
CTRL_NS=${CTRL_NS:-multus-service-system}
CTRL=${CTRL:-multus-service-controller}
AGENT_DS=${AGENT_DS:-multus-service-agent}
NODE=${NODE:-$(hostname)}
TARGET_IP=${TARGET_IP:-10.220.0.200}
OUT=${OUT:-/tmp/reconnect-recovery.jsonl}
FIXTURES="$(cd "$(dirname "$0")/../test/fixtures" && pwd)"

retry(){ local d=$(( $(date +%s)+$1 )); shift; while :; do eval "$@" && return 0; [ "$(date +%s)" -ge "$d" ] && return 1; sleep 0.3; done; }
k(){ kubectl -n "$NS" "$@"; }
agent_pod(){ kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" --field-selector spec.nodeName="$NODE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null; }
ready0(){ k get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].conditions.ready}' 2>/dev/null; }

cleanup(){ kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1; "$FIXTURES/lab.sh" down >/dev/null 2>&1; }
trap cleanup EXIT

echo "setup"
kubectl get ns "$NS" >/dev/null 2>&1 && { kubectl delete ns "$NS" --wait=false >/dev/null 2>&1; retry 240 '! kubectl get ns "$NS" >/dev/null 2>&1'; }
kubectl create ns "$NS" >/dev/null
"$FIXTURES/lab.sh" up-local >/dev/null
cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-recov, namespace: $NS, annotations: {secondary-service.boanlab.io/probe-scope: endpoint, secondary-service.boanlab.io/health-target: "$TARGET_IP"}}
spec: {config: '{"cniVersion":"0.3.1","name":"sec-recov","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"host-local","ranges":[[{"subnet":"10.220.0.0/24","rangeStart":"10.220.0.10","rangeEnd":"10.220.0.99"}]]}}]}'}
---
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-recov-target, namespace: $NS}
spec: {config: '{"cniVersion":"0.3.1","name":"sec-recov-target","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"static","addresses":[{"address":"$TARGET_IP/24"}]}}]}'}
---
apiVersion: v1
kind: Pod
metadata: {name: tgt, namespace: $NS, annotations: {k8s.v1.cni.cncf.io/networks: sec-recov-target}}
spec: {nodeSelector: {kubernetes.io/hostname: $NODE}, containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
---
apiVersion: v1
kind: Service
metadata: {name: $SVC, namespace: $NS, annotations: {secondary-service.boanlab.io/network: sec-recov, secondary-service.boanlab.io/workload-selector: app=recov}}
spec: {clusterIP: None, ports: [{name: n2, port: 8080, protocol: TCP}]}
---
apiVersion: v1
kind: Pod
metadata: {name: recov, namespace: $NS, labels: {app: recov}, annotations: {k8s.v1.cni.cncf.io/networks: sec-recov}}
spec: {nodeSelector: {kubernetes.io/hostname: $NODE}, containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
EOP
retry 240 '[ "$(k get pod recov tgt --no-headers 2>/dev/null | grep -c Running)" = "2" ]' || { echo "fixtures never ready"; exit 1; }
retry 180 '[ "$(ready0)" = "true" ]' || { echo "victim never ready"; exit 1; }
echo "victim ready; measuring $N recoveries"
: > "$OUT"

clog_since(){ kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since="$1" 2>/dev/null; }

for i in $(seq 1 "$N"); do
  retry 120 '[ "$(ready0)" = "true" ]' >/dev/null
  APOD=$(agent_pod); APUID=$(kubectl -n "$CTRL_NS" get pod "$APOD" -o jsonpath='{.metadata.uid}')
  # t0: delete the agent Pod
  T0=$(date +%s%3N)
  kubectl -n "$CTRL_NS" delete pod "$APOD" --grace-period=1 >/dev/null 2>&1
  # endpoint goes not-ready (lease expiry / revoke), then a new agent restores it
  retry 60 '[ "$(ready0)" != "true" ]' >/dev/null
  retry 180 '[ "$(ready0)" = "true" ]' >/dev/null
  T1=$(date +%s%3N)

  # Parse controller events since t0 for the new agent's auth + snapshot.
  win=$(( ($(date +%s%3N) - T0)/1000 + 3 ))s
  L=$(clog_since "$win")
  # the revoked session (old APUID) and the new agent_authenticated (different pod)
  REVOKE_TS=$(grep '"event":"sessions_revoked"' <<<"$L" | grep "$APUID" | head -1 | grep -o '"ts":[0-9]*' | cut -d: -f2)
  AUTH_TS=$(grep '"event":"agent_authenticated"' <<<"$L" | grep "\"node\":\"$NODE\"" | grep -v "$APOD" | tail -1 | grep -o '"ts":[0-9]*' | cut -d: -f2)
  SNAP_TS=$(grep '"event":"health_snapshot_applied"' <<<"$L" | grep "\"node\":\"$NODE\"" | tail -1 | grep -o '"ts":[0-9]*' | cut -d: -f2)

  python3 - "$OUT" "$i" "$T0" "$T1" "${REVOKE_TS:-0}" "${AUTH_TS:-0}" "${SNAP_TS:-0}" <<'PY'
import sys
out,i,t0ms,t1ms,revoke,auth,snap = sys.argv[1:8]
t0=int(t0ms)*1_000_000; t1=int(t1ms)*1_000_000
revoke=int(revoke); auth=int(auth); snap=int(snap)
def ms(a,b):
    if not a or not b: return None
    return round((b-a)/1e6,1)
rec={
  "run":int(i),
  "revoke_to_auth_ms": ms(revoke,auth),
  "auth_to_snapshot_ms": ms(auth,snap),
  "deletion_to_ready_ms": round((t1-t0)/1e6,1),
}
import json
open(out,"a").write(json.dumps(rec)+"\n")
print("  run %s: del->ready=%sms revoke->auth=%s auth->snap=%s" % (i, rec["deletion_to_ready_ms"], rec["revoke_to_auth_ms"], rec["auth_to_snapshot_ms"]))
PY
done

echo
python3 - "$OUT" <<'PY'
import sys,json
rows=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]
def stat(key):
    v=sorted(r[key] for r in rows if r.get(key) is not None)
    if not v: return "n/a"
    p=lambda q: v[min(len(v)-1,int(round((len(v)-1)*q)))]
    return "median=%.0f  p95=%.0f  (n=%d)" % (p(0.5),p(0.95),len(v))
print("secure recovery, %d iterations:" % len(rows))
print("  revoke -> authenticated stream    %s ms" % stat("revoke_to_auth_ms"))
print("  authenticated -> snapshot commit  %s ms" % stat("auth_to_snapshot_ms"))
print("  agent deletion -> endpoint ready  %s ms" % stat("deletion_to_ready_ms"))
PY
echo "raw: $OUT"
