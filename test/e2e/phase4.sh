#!/usr/bin/env bash
# Phase 4 acceptance: Endpoint-scope active path probe.
#
# What is under test is not "can we ping". It is that a failure which leaves
# every local kernel state untouched -- so neither host nor Pod netlink fires --
# is still detected, attributed to the right endpoint, and debounced.
#
# The health target is deliberately narrow: reachability from the endpoint's
# secondary source to the address named on its NAD. It is not a claim that every
# client can reach the endpoint.
# No pipefail: `grep -q` exits on first match and SIGPIPEs its upstream stage,
# which under pipefail turns a successful assertion into a failed pipeline --
# intermittently, depending on whether the upstream had finished writing.
set -u

NS=${NS:-ms-e2e4}
SVC=${SVC:-amf-p4}
SLICE="$SVC-secondary-ipv4"
CTRL_NS=${CTRL_NS:-multus-service-system}
NODE=${NODE:-$(hostname)}
TARGET_IP=${TARGET_IP:-10.215.0.200}
FIXTURES="$(cd "$(dirname "$0")/../fixtures" && pwd)"

# Must match the DaemonSet; the hysteresis assertions are expressed in them.
INTERVAL_MS=${INTERVAL_MS:-500}
FAIL_K=${FAIL_K:-3}
SUCCESS_M=${SUCCESS_M:-2}

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$1"; [ $# -gt 1 ] && printf '        %s\n' "$2"; }
note() { printf '  \033[33mNOTE\033[0m %s\n' "$1"; }
head_(){ printf '\n\033[1m%s\033[0m\n' "$1"; }

retry() {
  local deadline=$(( $(date +%s) + $1 )); shift
  while :; do
    if eval "$@"; then return 0; fi
    [ "$(date +%s)" -ge "$deadline" ] && return 1
    sleep 0.5
  done
}
k() { kubectl -n "$NS" "$@"; }
agent_pod() {
  kubectl -n "$CTRL_NS" get pod -l app=multus-service-agent \
    --field-selector spec.nodeName="$NODE" -o jsonpath='{.items[0].metadata.name}'
}
alog() { kubectl -n "$CTRL_NS" logs "$(agent_pod)" --since="${1:-5m}" 2>/dev/null; }
clog() { kubectl -n "$CTRL_NS" logs deploy/multus-service-controller --since="${1:-5m}" 2>/dev/null; }
ready_map() { k get endpointslice "$SLICE" -o jsonpath='{range .endpoints[*]}{.addresses[0]}={.conditions.ready}{" "}{end}' 2>/dev/null; }
dns() { k exec dnsprobe -- nslookup "$SVC.$NS.svc.cluster.local" 2>/dev/null | awk '/^Address/{print $NF}' | grep -v ':53$'; }

netns_of() {
  local uid id
  uid=$(k get pod "$1" -o jsonpath='{.metadata.uid}')
  id=$(sudo -n crictl pods -o json 2>/dev/null | python3 -c "
import json,sys
for p in json.load(sys.stdin).get('items',[]):
    if p.get('labels',{}).get('io.kubernetes.pod.uid')=='$uid' and p.get('state')=='SANDBOX_READY':
        print(p['id']); break")
  [ -z "$id" ] && return 1
  sudo -n crictl inspectp -o json "$id" 2>/dev/null | python3 -c "
import json,sys
for n in json.load(sys.stdin)['info']['runtimeSpec']['linux']['namespaces']:
    if n['type']=='network': print(n['path']); break"
}
aid_of() {
  local uid; uid=$(k get pod "$1" -o jsonpath='{.metadata.uid}')
  clog 10m | grep '"event":"attachment_discovered"' | grep "\"pod_uid\":\"$uid\"" | tail -1 \
    | grep -o '"attachment_id":"[^"]*"' | cut -d'"' -f4
}
ip_of() {
  k get pod "$1" -o jsonpath='{.metadata.annotations.k8s\.v1\.cni\.cncf\.io/network-status}' \
  | python3 -c "
import json,sys
for e in json.load(sys.stdin):
    if not e.get('default'): print(e['ips'][0]); break"
}
# how many state transitions this scope key has seen
probed()      { alog 2m | grep '"event":"path_probe"' | grep -q "\"scope_id\":\"$1\""; }
transitions() { alog 10m | grep '"event":"path_state_changed"' | grep -c "\"scope_id\":\"$1\""; }
last_state()  { alog 10m | grep '"event":"path_state_changed"' | grep "\"scope_id\":\"$1\"" | tail -1 \
                | grep -o '"to":"[^"]*"' | cut -d'"' -f4; }

cleanup() {
  head_ "cleanup"
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  "$FIXTURES/lab.sh" down >/dev/null 2>&1
}
trap cleanup EXIT

head_ "setup"
if [ -z "$(agent_pod)" ]; then
  echo "  waiting for a node agent on $NODE"
  retry 240 '[ -n "$(agent_pod)" ]' || { bad "no agent pod on $NODE -- deploy the DaemonSet first"; exit 1; }
fi
retry 240 '[ "$(kubectl -n "$CTRL_NS" get pod "$(agent_pod)" -o jsonpath="{.status.phase}" 2>/dev/null)" = "Running" ]' \
  || { bad "agent on $NODE never became Running"; exit 1; }
echo "  agent: $(agent_pod) on $NODE"
if kubectl get ns "$NS" >/dev/null 2>&1; then
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  retry 240 '! kubectl get ns "$NS" >/dev/null 2>&1' || { bad "namespace stuck terminating"; exit 1; }
fi
kubectl create ns "$NS" >/dev/null
"$FIXTURES/lab.sh" up-local >/dev/null

cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata:
  name: sec-p4
  namespace: $NS
  annotations:
    # The probe contract lives here, never on a Service: a Node-scope path
    # domain is shared by every attachment of one NAD on one node.
    secondary-service.boanlab.io/probe-scope: endpoint
    secondary-service.boanlab.io/health-target: "$TARGET_IP"
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-p4","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"host-local","ranges":[[{"subnet":"10.215.0.0/24","rangeStart":"10.215.0.10","rangeEnd":"10.215.0.99"}]]}}]}'
---
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-p4-target, namespace: $NS}
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-p4-target","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"static","addresses":[{"address":"$TARGET_IP/24"}]}}]}'
---
apiVersion: v1
kind: Service
metadata:
  name: $SVC
  namespace: $NS
  annotations:
    secondary-service.boanlab.io/network: sec-p4
    secondary-service.boanlab.io/workload-selector: app=p4
spec:
  clusterIP: None
  ports: [{name: n2, port: 8080, protocol: TCP}]
---
apiVersion: v1
kind: Pod
metadata: {name: p4-a, namespace: $NS, labels: {app: p4}, annotations: {k8s.v1.cni.cncf.io/networks: sec-p4}}
spec:
  nodeSelector: {kubernetes.io/hostname: $NODE}
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
---
apiVersion: v1
kind: Pod
metadata: {name: p4-b, namespace: $NS, labels: {app: p4}, annotations: {k8s.v1.cni.cncf.io/networks: sec-p4}}
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

target_yaml() {
cat <<EOP
apiVersion: v1
kind: Pod
metadata: {name: p4-target, namespace: $NS, annotations: {k8s.v1.cni.cncf.io/networks: sec-p4-target}}
spec:
  nodeSelector: {kubernetes.io/hostname: $NODE}
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
EOP
}
target_yaml | kubectl apply -f - >/dev/null

retry 240 '[ "$(k get pod p4-a p4-b p4-target dnsprobe --no-headers 2>/dev/null | grep -c Running)" = "4" ]' \
  || { bad "fixtures never started"; exit 1; }

NS_A=$(netns_of p4-a); NS_B=$(netns_of p4-b)
IP_A=$(ip_of p4-a);    IP_B=$(ip_of p4-b)
retry 120 '[ -n "$(aid_of p4-a)" ] && [ -n "$(aid_of p4-b)" ]' >/dev/null
AID_A=$(aid_of p4-a);  AID_B=$(aid_of p4-b)
echo "  A pod=p4-a ip=$IP_A attachment=${AID_A:0:8}"
echo "  B pod=p4-b ip=$IP_B attachment=${AID_B:0:8}"
echo "  health target=$TARGET_IP  interval=${INTERVAL_MS}ms k=$FAIL_K m=$SUCCESS_M"

# ---------------------------------------------------------------- C1
head_ "C1  정상 Endpoint scope -> Pod netns 에서 target 도달, PathHealth=true"
if retry 90 'probed "$AID_A"'; then
  ok "probe samples recorded for A"
else
  bad "no path_probe samples for A"
fi
line=$(alog 2m | grep '"event":"path_probe"' | grep "\"scope_id\":\"$AID_A\"" | tail -1)
if grep -q '"success":true' <<<"$line"; then
  ok "sample succeeds ($(grep -o '"rtt_ms":[0-9.]*' <<<"$line") )"
else
  bad "probe not succeeding" "$line"
fi
# the probe must name the secondary source it claims to measure from
if grep -q "\"source_ip\":\"$IP_A\"" <<<"$line" && grep -q '"interface":"net1"' <<<"$line"; then
  ok "sample is bound to the secondary source and interface, not left to routing"
else
  bad "probe source not recorded" "$line"
fi
if retry 90 '[ "$(ready_map | grep -o "=true" | wc -l)" = "2" ]'; then
  ok "both endpoints ready ($(ready_map))"
else
  bad "endpoints not ready" "$(ready_map)"
fi
if retry 40 '[ "$(dns | grep -c "^10\.215\.")" = "2" ]'; then
  ok "both secondary addresses in DNS"
else
  bad "DNS incomplete" "$(dns | tr '\n' ' ')"
fi

# ---------------------------------------------------------------- C2
head_ "C2  한 endpoint 의 경로만 차단 -> 그 attachment 만 false"
before_b=$(transitions "$AID_B")
T_INJECT=$(python3 -c 'import time;print(int(time.time()*1e9))')
sudo -n nsenter --net="$NS_A" ip route add blackhole "$TARGET_IP/32"
if retry 60 '[ "$(last_state '"$AID_A"')" = "Unhealthy" ]'; then
  ok "A's path went Unhealthy"
else
  bad "A's path did not go Unhealthy" "$(last_state "$AID_A")"
fi
if [ "$(transitions "$AID_B")" = "$before_b" ]; then
  ok "B did not transition -- Endpoint scope keeps the two independent"
else
  bad "B transitioned on A's failure"
fi
if retry 60 'ready_map | grep -q "'"$IP_A"'=false" && ready_map | grep -q "'"$IP_B"'=true"'; then
  ok "only A withdrawn ($(ready_map))"
else
  bad "readiness wrong" "$(ready_map)"
fi
# hysteresis cost: first failed sample -> state change
export AGENT_POD="$(agent_pod)" CTRL_NS
python3 - "$T_INJECT" "$AID_A" <<'PY' > /tmp/p4_hyst 2>/dev/null || true
import json,subprocess,sys,os
t0, aid = int(sys.argv[1]), sys.argv[2]
log = subprocess.run(["kubectl","-n",os.environ.get("CTRL_NS","multus-service-system"),
                      "logs",os.environ["AGENT_POD"],"--since=5m"],
                     capture_output=True,text=True).stdout
f0=fc=None
for line in log.splitlines():
    if not line.startswith("{"): continue
    try: e=json.loads(line)
    except ValueError: continue
    if e.get("ts",0) < t0: continue
    if e.get("event")=="path_probe" and e.get("scope_id")==aid and e.get("success") is False and f0 is None:
        f0=e["ts"]
    if e.get("event")=="path_state_changed" and e.get("scope_id")==aid and e.get("to")=="Unhealthy" and fc is None:
        fc=e["ts"]
fails=sum(1 for line in log.splitlines() if line.startswith("{") and
          (lambda e: e.get("event")=="path_probe" and e.get("scope_id")==aid
                     and e.get("success") is False and f0 is not None
                     and f0 <= e.get("ts",0) <= (fc or 0))(json.loads(line)))
if f0 and fc: print("%.0f %d" % ((fc-f0)/1e6, fails))
PY
HYST=$(cut -d' ' -f1 /tmp/p4_hyst 2>/dev/null)
NFAIL=$(cut -d' ' -f2 /tmp/p4_hyst 2>/dev/null)
if [ -n "$HYST" ]; then
  note "hysteresis cost: first failed sample -> withdrawal = ${HYST} ms over ${NFAIL} failed samples"
  note "(k=$FAIL_K, nominal interval ${INTERVAL_MS}ms; a path that fails by timing out"
  note " samples no faster than --probe-timeout)"
fi
sudo -n nsenter --net="$NS_A" ip route del blackhole "$TARGET_IP/32" 2>/dev/null
retry 90 '[ "$(last_state '"$AID_A"')" = "Healthy" ]' >/dev/null \
  && ok "A recovered after the block was removed" || bad "A did not recover"

# ---------------------------------------------------------------- C3
head_ "C3  공유 경로 장애 -> 해당 node 의 endpoint 전부 false, LocalHealth 는 true 유지"
retry 90 '[ "$(ready_map | grep -o "=true" | wc -l)" = "2" ]' >/dev/null
kubectl -n "$NS" delete pod p4-target --wait=true >/dev/null 2>&1
if retry 90 '[ "$(last_state '"$AID_A"')" = "Unhealthy" ] && [ "$(last_state '"$AID_B"')" = "Unhealthy" ]'; then
  ok "both endpoints' paths went Unhealthy"
else
  bad "shared failure not seen by both" "A=$(last_state "$AID_A") B=$(last_state "$AID_B")"
fi
la=$(alog 2m | grep '"event":"local_health"' | grep "\"attachment_id\":\"$AID_A\"" | tail -1)
lb=$(alog 2m | grep '"event":"local_health"' | grep "\"attachment_id\":\"$AID_B\"" | tail -1)
if grep -q '"local_ready":true' <<<"$la" && grep -q '"local_ready":true' <<<"$lb"; then
  ok "LocalHealth stayed true -- netlink is blind to this failure, which is why the probe exists"
else
  bad "local state moved on a path-only failure" "A: $la"
fi
if retry 60 '! ready_map | grep -q "=true"'; then
  ok "both endpoints withdrawn ($(ready_map))"
else
  bad "endpoints still ready" "$(ready_map)"
fi
if retry 40 '[ -z "$(dns)" ]'; then
  ok "DNS empty"
else
  bad "DNS still answering" "$(dns | tr '\n' ' ')"
fi

# ---------------------------------------------------------------- C4
head_ "C4  복구 -> success threshold 이후 true"
target_yaml | kubectl apply -f - >/dev/null
retry 180 '[ "$(k get pod p4-target -o jsonpath="{.status.phase}" 2>/dev/null)" = "Running" ]' >/dev/null
if retry 120 '[ "$(ready_map | grep -o "=true" | wc -l)" = "2" ]'; then
  ok "both endpoints restored ($(ready_map))"
else
  bad "endpoints did not recover" "$(ready_map)"
fi
if retry 60 '[ "$(dns | grep -c "^10\.215\.")" = "2" ]'; then
  ok "DNS restored"
else
  bad "DNS not restored" "$(dns | tr '\n' ' ')"
fi

# ---------------------------------------------------------------- C5
head_ "C5  단발 failure 는 threshold 미달이므로 상태 유지"
before_a=$(transitions "$AID_A")
sudo -n nsenter --net="$NS_A" ip route add blackhole "$TARGET_IP/32"
sleep 0.6
sudo -n nsenter --net="$NS_A" ip route del blackhole "$TARGET_IP/32" 2>/dev/null
sleep 4
if [ "$(transitions "$AID_A")" = "$before_a" ]; then
  ok "a sub-threshold loss did not move the state (k=$FAIL_K, one interval blocked)"
else
  bad "state flapped on a single lost sample" "$(last_state "$AID_A")"
fi
if ready_map | grep -q "$IP_A=true"; then
  ok "A stayed published throughout"
else
  bad "A was withdrawn by a sub-threshold loss" "$(ready_map)"
fi

# ---------------------------------------------------------------- C6
head_ "C6  local failure 는 path 로 대체되지 않음"
sudo -n nsenter --net="$NS_A" ip link set net1 down
if retry 60 'alog 2m | grep "\"event\":\"local_health\"" | grep "\"attachment_id\":\"'"$AID_A"'\"" | tail -1 | grep -q "\"local_ready\":false"'; then
  ok "LocalHealth went false"
else
  bad "local failure not reported"
fi
if retry 60 'clog 3m | grep "\"event\":\"readiness_changed\"" | grep "\"attachment_id\":\"'"$AID_A"'\"" | tail -1 | grep -qE "\"to\":\"(LinkDown|InterfaceMissing|AddressMissing)\""'; then
  reason=$(clog 3m | grep '"event":"readiness_changed"' | grep "\"attachment_id\":\"$AID_A\"" | tail -1 | grep -o '"to":"[^"]*"' | cut -d'"' -f4)
  ok "readiness blocked on the local term, not the path term (reason=$reason)"
else
  bad "readiness reason did not name the local term" \
      "$(clog 3m | grep '"event":"readiness_changed"' | grep "\"attachment_id\":\"$AID_A\"" | tail -1)"
fi
if retry 60 'ready_map | grep -q "'"$IP_A"'=false"'; then
  ok "A withdrawn"
else
  bad "A still published with a dead interface" "$(ready_map)"
fi
sudo -n nsenter --net="$NS_A" ip link set net1 up
retry 120 '[ "$(ready_map | grep -o "=true" | wc -l)" = "2" ]' >/dev/null \
  && ok "recovers once the link is back" || bad "did not recover after link up"

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
