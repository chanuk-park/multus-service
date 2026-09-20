#!/usr/bin/env bash
# Phase 3 acceptance: gRPC health transport, snapshots, stale-report rejection.
#
# The fixture declares a real health target, so readiness here is reached the
# same way it is in production: local state from netlink and path state from an
# actual probe. Nothing in the acceptance suite depends on the assume-ready
# scaffold.
#
# No pipefail: `grep -q` exits on first match and SIGPIPEs its upstream stage,
# which under pipefail turns a successful assertion into a failed pipeline --
# intermittently, depending on whether the upstream had finished writing.
set -u

NS=${NS:-ms-e2e3}
SVC=${SVC:-amf-p3}
SLICE="${SVC}-secondary-ipv4"
CTRL_NS=${CTRL_NS:-multus-service-system}
CTRL=${CTRL:-multus-service-controller}
AGENT_DS=${AGENT_DS:-multus-service-agent}
NODE=${NODE:-$(hostname)}
FIXTURES="$(cd "$(dirname "$0")/../fixtures" && pwd)"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

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
  kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" \
    --field-selector spec.nodeName="$NODE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}
clog() { kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --tail="${1:-4000}" 2>/dev/null; }
# The controller emits a report event per attachment per second, so a tail
# window can scroll past a one-off event. Time-bounded reads avoid that.
clog_since() { kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since="$1" 2>/dev/null; }
slice_ready() { k get endpointslice "$SLICE" -o jsonpath='{range .endpoints[*]}{.addresses[0]}={.conditions.ready}{" "}{end}' 2>/dev/null; }
dns() { k exec dnsprobe -- nslookup "$SVC.$NS.svc.cluster.local" 2>/dev/null | awk '/^Address/{print $NF}' | grep -v ':53$'; }
netstat_field() {
  k get pod -l app=p3 -o jsonpath='{.items[0].metadata.annotations.k8s\.v1\.cni\.cncf\.io/network-status}' 2>/dev/null \
  | python3 -c "
import json,sys
for e in json.load(sys.stdin):
    if not e.get('default'):
        print(e['$1'] if '$1' != 'ip' else e['ips'][0]); break"
}

# The controller's gRPC Service is a ClusterIP, which kube-proxy makes reachable
# from the node. Port-forward was used here first and proved unreliable: it
# accepts the local connection before it has a working path to the pod, so a
# forward that is about to die looks identical to one that is up -- and a dead
# forward reads as a rejection that never fired.
CTRL_ADDR=""
SERVER_NAME="$CTRL.$CTRL_NS.svc"
P3_CA=/tmp/p3-ca.crt
P3_TOK=/tmp/p3-agent.tok
resolve_ctrl() {
  CTRL_ADDR="$(kubectl -n "$CTRL_NS" get svc "$CTRL" -o jsonpath='{.spec.clusterIP}' 2>/dev/null):9090"
  kubectl -n "$CTRL_NS" get configmap controller-ca -o jsonpath='{.data.ca\.crt}' > "$P3_CA" 2>/dev/null || true
  retry 60 'bash -c "exec 3<>/dev/tcp/'"${CTRL_ADDR%:*}"'/9090" 2>/dev/null' \
    || { bad "controller health transport unreachable at $CTRL_ADDR"; return 1; }
}
# A valid Pod-bound token for the real agent on $NODE, so the harness
# authenticates and then still exercises the app-layer rejection checks with an
# authenticated-but-misbehaving identity.
mint_agent_token() {
  local pod uid
  pod=$(kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" --field-selector spec.nodeName="$NODE" -o jsonpath='{.items[0].metadata.name}')
  uid=$(kubectl -n "$CTRL_NS" get pod "$pod" -o jsonpath='{.metadata.uid}')
  kubectl -n "$CTRL_NS" create token "$AGENT_DS" --bound-object-kind Pod --bound-object-name "$pod" --bound-object-uid "$uid" --audience health-controller --duration 1h > "$P3_TOK" 2>/dev/null
}

HARNESS_LOG=$(mktemp)
harness() {
  local inst=""
  for a in "$@"; do [ "$prev" = "--instance" ] && inst=$a; prev=$a; done
  mint_agent_token
  ( cd "$ROOT" && go run ./test/tools/healthreport --addr "$CTRL_ADDR" \
      --ca "$P3_CA" --server-name "$SERVER_NAME" --token "$P3_TOK" --node "$NODE" "$@" ) \
    > "$HARNESS_LOG" 2>&1
  # Without confirming the connection, a port-forward that never came up is
  # indistinguishable from a rejection that never fired.
  if [ -n "$inst" ] && ! retry 30 'clog_since 120s | grep "\"event\":\"agent_connected\"" | grep -q "\"agent_instance\":\"'"$inst"'\""'; then
    bad "harness never connected as $inst" "$(tail -3 "$HARNESS_LOG")"
    return 1
  fi
  return 0
}
prev=""

cleanup() {
  head_ "cleanup"
  kubectl -n "$CTRL_NS" patch ds "$AGENT_DS" --type=json \
    -p '[{"op":"remove","path":"/spec/template/spec/nodeSelector"}]' >/dev/null 2>&1
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  "$FIXTURES/lab.sh" down >/dev/null 2>&1
  rm -f /tmp/p3-ca.crt /tmp/p3-agent.tok
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
  retry 180 '! kubectl get ns "$NS" >/dev/null 2>&1' || { bad "namespace stuck terminating"; exit 1; }
fi
kubectl create ns "$NS" >/dev/null
"$FIXTURES/lab.sh" up-local >/dev/null

cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata:
  name: sec-p3
  namespace: $NS
  annotations:
    secondary-service.boanlab.io/probe-scope: endpoint
    secondary-service.boanlab.io/health-target: "10.213.0.200"
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-p3","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"host-local","ranges":[[{"subnet":"10.213.0.0/24","rangeStart":"10.213.0.10","rangeEnd":"10.213.0.99"}]]}}]}'
---
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-p3-target, namespace: $NS}
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-p3-target","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"static","addresses":[{"address":"10.213.0.200/24"}]}}]}'
---
apiVersion: v1
kind: Pod
metadata: {name: p3-target, namespace: $NS, annotations: {k8s.v1.cni.cncf.io/networks: sec-p3-target}}
spec:
  nodeSelector: {kubernetes.io/hostname: $NODE}
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
---
apiVersion: v1
kind: Service
metadata:
  name: $SVC
  namespace: $NS
  annotations:
    secondary-service.boanlab.io/network: sec-p3
    secondary-service.boanlab.io/workload-selector: app=p3
spec:
  clusterIP: None
  ports: [{name: n2, port: 8080, protocol: TCP}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: p3, namespace: $NS}
spec:
  replicas: 1
  selector: {matchLabels: {app: p3}}
  template:
    metadata:
      labels: {app: p3}
      annotations: {k8s.v1.cni.cncf.io/networks: sec-p3}
    spec:
      nodeSelector: {kubernetes.io/hostname: $NODE}
      containers:
        - name: sh
          image: docker.io/library/busybox:1.36
          command: ["sh","-c","sleep infinity"]
---
apiVersion: v1
kind: Pod
metadata: {name: dnsprobe, namespace: $NS}
spec:
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
EOP

retry 240 '[ "$(k get pod -l app=p3 --no-headers 2>/dev/null | grep -c Running)" = "1" ]' \
  || { bad "workload never started"; exit 1; }
retry 240 '[ "$(k get pod p3-target -o jsonpath="{.status.phase}" 2>/dev/null)" = "Running" ]' \
  || { bad "health target never started"; exit 1; }
retry 120 '[ "$(k get pod dnsprobe -o jsonpath="{.status.phase}" 2>/dev/null)" = "Running" ]' >/dev/null
PODUID=$(k get pod -l app=p3 -o jsonpath='{.items[0].metadata.uid}')
retry 90 '[ -n "$(clog | grep "\"event\":\"attachment_discovered\"" | grep "\"pod_uid\":\"'"$PODUID"'\"" | tail -1)" ]' >/dev/null
AID=$(clog | grep '"event":"attachment_discovered"' | grep "\"pod_uid\":\"$PODUID\"" | tail -1 | grep -o '"attachment_id":"[^"]*"' | cut -d'"' -f4)
IFACE=$(netstat_field interface)
IP=$(netstat_field ip)
echo "  attachment=$AID  node=$NODE  iface=$IFACE  ip=$IP"

# ---------------------------------------------------------------- P3-1
head_ "P3-1 Agent report 가 Controller HealthStore 에 반영"
if retry 60 'clog | grep "\"event\":\"health_report_received\"" | grep -q "\"attachment_id\":\"'"$AID"'\""'; then
  ok "controller received a report for the attachment"
else
  bad "no health_report_received for $AID"
fi
if retry 60 'slice_ready | grep -q "=true"'; then
  ok "endpoint reached ready=true once local and path state were both fresh ($(slice_ready))"
else
  bad "endpoint never became ready" "$(slice_ready)"
fi
if retry 40 'dns | grep -q "^10\.213\."'; then
  ok "secondary address published in DNS ($(dns))"
else
  bad "DNS never returned the secondary address" "$(dns)"
fi

# ---------------------------------------------------------------- P3-2
head_ "P3-2 알 수 없는 attachment_id report 거부 (Agent 는 discovery authority 가 아님)"
# The real agent cannot produce this: it derives its targets from the same
# objects the controller does, so the two converge before a bad report exists.
# The harness claims the node with its own instance id and sends one on purpose.
resolve_ctrl
harness --instance harness-unknown --case unknown-attachment
if retry 60 'clog_since 120s | grep "\"event\":\"health_report_rejected\"" | grep -q "unknown attachment"'; then
  ok "report naming an attachment the registry never derived was rejected"
else
  bad "unknown-attachment report was not rejected"
fi
if k get endpointslice "$SLICE" -o jsonpath='{.endpoints[*].addresses[0]}' 2>/dev/null | grep -q '10.255.255.1'; then
  bad "a rejected report created an endpoint -- reports must not be a discovery channel"
else
  ok "no endpoint appeared for the invented attachment"
fi
retry 120 'slice_ready | grep -q "=true"' >/dev/null \
  && ok "the superseded real agent reconnected and readiness returned" \
  || bad "readiness did not return after the harness disconnected" "$(slice_ready)"

# ---------------------------------------------------------------- P3-3
head_ "P3-3 이전 sequence report 무시"
resolve_ctrl
harness --instance harness-order --case out-of-order \
        --attachment "$AID" --nad "$NS/sec-p3" --interface "$IFACE" --ip "$IP"
if retry 60 'clog_since 120s | grep "\"event\":\"health_report_rejected\"" | grep -q "sequence did not advance"'; then
  ok "a replayed sequence was rejected rather than applied"
else
  bad "out-of-order report was not rejected"
fi
retry 120 'slice_ready | grep -q "=true"' >/dev/null \
  && ok "readiness returned after the harness disconnected" \
  || bad "readiness did not return" "$(slice_ready)"

# ---------------------------------------------------------------- P3-4
head_ "P3-4 Agent report 중단 -> TTL 만료 -> Unknown -> not ready"
kubectl -n "$CTRL_NS" patch ds "$AGENT_DS" --type=merge \
  -p '{"spec":{"template":{"spec":{"nodeSelector":{"multus-service.io/absent":"true"}}}}}' >/dev/null
retry 120 '[ "$(kubectl -n '"$CTRL_NS"' get pod -l app='"$AGENT_DS"' --no-headers 2>/dev/null | grep -c .)" = "0" ]' >/dev/null
if retry 60 'clog_since 90s | grep -q "\"event\":\"health_expired\""'; then
  ok "health_expired fired once reports stopped arriving"
else
  bad "no health_expired event after the agents went away"
fi
if retry 60 '! slice_ready | grep -q "=true"'; then
  ok "endpoint went not-ready on expiry ($(slice_ready))"
else
  bad "endpoint stayed ready with no fresh report" "$(slice_ready)"
fi
if retry 40 '[ -z "$(dns)" ]'; then
  ok "address withdrawn from DNS"
else
  bad "DNS still answers for an expired endpoint" "$(dns)"
fi

# ---------------------------------------------------------------- P3-5 / P3-6
head_ "P3-5/6 Agent 재시작 -> 새 instance, 기존 instance 무효, 연결 시 full snapshot"
before_conn=$(clog | grep -c '"event":"agent_connected"')
kubectl -n "$CTRL_NS" patch ds "$AGENT_DS" --type=json \
  -p '[{"op":"remove","path":"/spec/template/spec/nodeSelector"}]' >/dev/null
retry 240 '[ -n "$(kubectl -n '"$CTRL_NS"' get ds '"$AGENT_DS"' -o jsonpath="{.status.numberReady}")" ] && [ "$(kubectl -n '"$CTRL_NS"' get ds '"$AGENT_DS"' -o jsonpath="{.status.numberReady}")" = "$(kubectl -n '"$CTRL_NS"' get ds '"$AGENT_DS"' -o jsonpath="{.status.desiredNumberScheduled}")" ]' >/dev/null
if retry 90 '[ "$(clog | grep -c "\"event\":\"agent_connected\"")" -gt '"$before_conn"' ]'; then
  ok "new agent instances connected"
else
  bad "no new agent_connected after restart"
fi
line=$(clog | grep '"event":"agent_connected"' | tail -1)
inst=$(grep -o '"agent_instance":"[^"]*"' <<<"$line" | cut -d'"' -f4)
if [ -n "$inst" ]; then
  ok "instance id recorded ($inst)"
else
  bad "agent_connected carries no instance id" "$line"
fi
if retry 90 'clog | grep "\"event\":\"health_snapshot_applied\"" | tail -8 | grep -q "\"local\":[1-9]"'; then
  ok "reconnect resent full state as a snapshot, not just deltas"
else
  bad "no non-empty snapshot after reconnect"
fi
if retry 150 'slice_ready | grep -q "=true"'; then
  ok "readiness recovered after the agent came back"
else
  bad "readiness did not recover" "$(slice_ready)"
fi

# ---------------------------------------------------------------- P3-7
head_ "P3-7 Controller 재시작 -> Agent resync 로 상태 복구"
kubectl -n "$CTRL_NS" rollout restart deploy/"$CTRL" >/dev/null
kubectl -n "$CTRL_NS" rollout status deploy/"$CTRL" --timeout=250s >/dev/null 2>&1
if retry 240 'clog | grep -q "\"event\":\"agent_connected\""'; then
  ok "agents reconnected to the restarted controller"
else
  bad "no agent reconnected after controller restart"
fi
if retry 240 'slice_ready | grep -q "=true"'; then
  ok "readiness rebuilt from the agents' resync, starting from an empty store"
else
  bad "readiness never recovered after controller restart" "$(slice_ready)"
fi
if retry 60 'dns | grep -q "^10\.213\."'; then
  ok "DNS answer restored ($(dns))"
else
  bad "DNS did not recover" "$(dns)"
fi

# ---------------------------------------------------------------- P3-8
head_ "P3-8 Node scope PathHealth 공유 / 범위 밖 key 거부"
resolve_ctrl
harness --instance harness-scope --case node-scope-path --nad "$NS/sec-p3"
if retry 60 'clog_since 120s | grep "\"event\":\"health_report_rejected\"" | grep -q "unknown path key"'; then
  ok "a Node-scope domain key nothing reads was rejected"
else
  bad "node-scope path report was not rejected"
fi
note "sharing itself is exercised by unit tests until Phase 6 wires probe-scope"
note "from the NAD; the controller computes Endpoint scope today"
if ( cd "$ROOT" && go test ./internal/controller/ \
     -run 'TestNodeScopeSharesOnePathResult|TestRegistryPathKeyFollowsScope|TestSharedDomainOneHysteresisOneTransition' \
     -count=1 >/dev/null 2>&1 ); then
  ok "shared-domain unit tests pass"
else
  bad "shared-domain unit tests failed"
fi
retry 150 'slice_ready | grep -q "=true"' >/dev/null \
  && ok "agent recovered after the harness disconnected" || bad "agent did not recover"

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
