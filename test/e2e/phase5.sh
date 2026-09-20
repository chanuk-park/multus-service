#!/usr/bin/env bash
# Phase 5 acceptance: G2 subject-bound publication authority.
#
# Establishes the security property the attack reproduction motivates:
#
#   Visible(e) => Authorized(e) AND FreshEvidence(e) AND EvidenceProducerAuthorized(e)
#
# The producer-authorization half (G2) is new here: health evidence for an
# endpoint is accepted only from the authenticated agent that actually hosts it,
# established by a Pod-bound token via TokenReview, never by a self-asserted
# node string.
#
# No pipefail: grep -q SIGPIPEs its upstream stage, which under pipefail turns a
# true assertion into a false one.
set -u

NS=${NS:-ms-e2e5}
SVC=amf-p5
SLICE="$SVC-secondary-ipv4"
CTRL_NS=${CTRL_NS:-multus-service-system}
CTRL=${CTRL:-multus-service-controller}
AGENT_DS=${AGENT_DS:-multus-service-agent}
NODE_A=${NODE_A:-telco-guard-01}
NODE_B=${NODE_B:-telco-guard-02}
TARGET_IP=${TARGET_IP:-10.217.0.200}
AUD=${AUD:-health-controller}
FIXTURES="$(cd "$(dirname "$0")/../fixtures" && pwd)"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$1"; [ $# -gt 1 ] && printf '        %s\n' "$2"; }
head_(){ printf '\n\033[1m%s\033[0m\n' "$1"; }
retry(){ local d=$(( $(date +%s)+$1 )); shift; while :; do eval "$@" && return 0; [ "$(date +%s)" -ge "$d" ] && return 1; sleep 0.5; done; }
k(){ kubectl -n "$NS" "$@"; }
clog(){ kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since="${1:-3m}" 2>/dev/null; }
CTRL_ADDR="$(kubectl -n "$CTRL_NS" get svc "$CTRL" -o jsonpath='{.spec.clusterIP}'):9090"
SERVER_NAME="$CTRL.$CTRL_NS.svc"
CA=/tmp/p5-ca.crt
kubectl -n "$CTRL_NS" get configmap controller-ca -o jsonpath='{.data.ca\.crt}' > "$CA" 2>/dev/null || true

dns(){ k exec dnsprobe -- nslookup "$SVC.$NS.svc.cluster.local" 2>/dev/null | awk '/^Address/{print $NF}' | grep -v ':53$'; }
ready_true(){ k get endpointslice "$SLICE" -o jsonpath='{.endpoints[*].conditions.ready}' 2>/dev/null | grep -o true | wc -l; }
# mint a pod-bound token for a real agent pod on a node, audience health-controller
agent_token(){  # $1=node -> writes token to stdout
  local pod uid
  pod=$(kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" --field-selector spec.nodeName="$1" -o jsonpath='{.items[0].metadata.name}')
  uid=$(kubectl -n "$CTRL_NS" get pod "$pod" -o jsonpath='{.metadata.uid}')
  kubectl -n "$CTRL_NS" create token "$AGENT_DS" --bound-object-kind Pod --bound-object-name "$pod" --bound-object-uid "$uid" --audience "$AUD" --duration 1h 2>/dev/null
}
spoof(){ ( cd "$ROOT" && go run ./test/tools/spoof "$@" ); }

cleanup(){ head_ cleanup; kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1; rm -f /tmp/p5-*.crt /tmp/p5-*.tok; "$FIXTURES/lab.sh" down >/dev/null 2>&1; }
trap cleanup EXIT

head_ "setup"
[ -s "$CA" ] || { bad "controller CA not found -- run hack/gen-certs.sh"; exit 1; }
retry 240 '[ -n "$(kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" --field-selector spec.nodeName="$NODE_A" -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)" ]' || { bad "no agent on $NODE_A"; exit 1; }
if kubectl get ns "$NS" >/dev/null 2>&1; then kubectl delete ns "$NS" --wait=false >/dev/null 2>&1; retry 240 '! kubectl get ns "$NS" >/dev/null 2>&1'; fi
kubectl create ns "$NS" >/dev/null
"$FIXTURES/lab.sh" up-local >/dev/null

cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata:
  name: sec-p5
  namespace: $NS
  annotations: {secondary-service.boanlab.io/probe-scope: endpoint, secondary-service.boanlab.io/health-target: "$TARGET_IP"}
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-p5","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"host-local","ranges":[[{"subnet":"10.217.0.0/24","rangeStart":"10.217.0.10","rangeEnd":"10.217.0.99"}]]}}]}'
---
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-p5-target, namespace: $NS}
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-p5-target","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"static","addresses":[{"address":"$TARGET_IP/24"}]}}]}'
---
apiVersion: v1
kind: Service
metadata:
  name: $SVC
  namespace: $NS
  annotations: {secondary-service.boanlab.io/network: sec-p5, secondary-service.boanlab.io/workload-selector: app=p5}
spec: {clusterIP: None, ports: [{name: n2, port: 8080, protocol: TCP}]}
---
apiVersion: v1
kind: Pod
metadata: {name: p5, namespace: $NS, labels: {app: p5}, annotations: {k8s.v1.cni.cncf.io/networks: sec-p5}}
spec: {nodeSelector: {kubernetes.io/hostname: $NODE_A}, containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
---
apiVersion: v1
kind: Pod
metadata: {name: tgt, namespace: $NS, annotations: {k8s.v1.cni.cncf.io/networks: sec-p5-target}}
spec: {nodeSelector: {kubernetes.io/hostname: $NODE_A}, containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
---
apiVersion: v1
kind: Pod
metadata: {name: dnsprobe, namespace: $NS}
spec: {containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
EOP
retry 240 '[ "$(k get pod p5 tgt dnsprobe --no-headers 2>/dev/null | grep -c Running)" = "3" ]' || { bad "fixtures never started"; exit 1; }
retry 150 '[ "$(ready_true)" -ge 1 ]' || { bad "victim never became ready"; exit 1; }
PUID=$(k get pod p5 -o jsonpath='{.metadata.uid}')
AID=$(clog 15m | grep '"event":"attachment_discovered"' | grep "\"pod_uid\":\"$PUID\"" | tail -1 | grep -o '"attachment_id":"[^"]*"' | cut -d'"' -f4)
VIP=$(k get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].addresses[0]}')
echo "  victim attachment=$AID ip=$VIP on $NODE_A ; transport $CTRL_ADDR"

# ---------------------------------------------------------------- valid agent
head_ "valid agent token -> accepted"
# agent_authenticated fires once per stream, which may predate this run's window;
# the live proof is that the real agent is currently applying snapshots for its
# node, which only an authenticated stream can do.
if clog 40m | grep '"event":"agent_authenticated"' | grep -q "\"node\":\"$NODE_A\""    || clog 40s | grep '"event":"health_snapshot_applied"' | grep -q "\"node\":\"$NODE_A\""; then
  ok "the real agent is authenticated and its node is bound from its token"
else
  bad "no evidence of an authenticated agent on $NODE_A"
fi

# ---------------------------------------------------------------- no token
head_ "no token -> rejected"
spoof --addr "$CTRL_ADDR" --ca "$CA" --server-name "$SERVER_NAME" --node "$NODE_A" \
  --attachment "$AID" --nad "$NS/sec-p5" --interface net1 --ip "$VIP" --attack withdraw --duration 4s >/dev/null 2>&1 || true
sleep 1
if clog 30s | grep '"event":"stream_rejected"' | tail -1 | grep -q 'authentication'; then
  ok "a stream with no bearer token is rejected"
else
  bad "missing-token stream not rejected" "$(clog 30s | grep stream_rejected | tail -1)"
fi

# ---------------------------------------------------------------- wrong audience
head_ "wrong audience -> rejected"
BADAUD=$(kubectl -n "$CTRL_NS" create token "$AGENT_DS" --duration 1h 2>/dev/null); echo "$BADAUD" > /tmp/p5-badaud.tok
spoof --addr "$CTRL_ADDR" --ca "$CA" --server-name "$SERVER_NAME" --token /tmp/p5-badaud.tok --node "$NODE_A" \
  --attachment "$AID" --nad "$NS/sec-p5" --interface net1 --ip "$VIP" --attack withdraw --duration 4s >/dev/null 2>&1 || true
sleep 1
if clog 20s | grep '"event":"stream_rejected"' | tail -1 | grep -qi 'audience\|authentication'; then
  ok "a kube-api-audience token is rejected for the health transport"
else
  bad "wrong-audience token not rejected" "$(clog 20s | grep stream_rejected | tail -1)"
fi

# ---------------------------------------------------------------- valid non-agent
head_ "valid non-agent token -> rejected (G2: valid credential != agent)"
kubectl -n "$NS" create sa ordinary >/dev/null 2>&1
kubectl -n "$NS" run ordinary --image=docker.io/library/busybox:1.36 --overrides="{\"spec\":{\"serviceAccountName\":\"ordinary\",\"nodeName\":\"$NODE_A\"}}" --command -- sh -c 'sleep infinity' >/dev/null 2>&1
retry 120 '[ "$(k get pod ordinary -o jsonpath="{.status.phase}" 2>/dev/null)" = "Running" ]' >/dev/null
OUID=$(k get pod ordinary -o jsonpath='{.metadata.uid}')
kubectl -n "$NS" create token ordinary --bound-object-kind Pod --bound-object-name ordinary --bound-object-uid "$OUID" --audience "$AUD" --duration 1h > /tmp/p5-ordinary.tok 2>/dev/null
spoof --addr "$CTRL_ADDR" --ca "$CA" --server-name "$SERVER_NAME" --token /tmp/p5-ordinary.tok --node "$NODE_A" \
  --attachment "$AID" --nad "$NS/sec-p5" --interface net1 --ip "$VIP" --attack withdraw --duration 4s >/dev/null 2>&1 || true
sleep 1
if clog 20s | grep '"event":"stream_rejected"' | tail -1 | grep -q 'not a registered node agent'; then
  ok "an authenticated but non-agent workload is refused by the AgentRegistry"
else
  bad "non-agent token not rejected" "$(clog 20s | grep stream_rejected | tail -1)"
fi

# ---------------------------------------------------------------- cross-node
head_ "node-B agent credential -> cannot touch a node-A endpoint"
TOK_B=$(agent_token "$NODE_B"); echo "$TOK_B" > /tmp/p5-nodeb.tok
before=$(k get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].conditions.ready}')
spoof --addr "$CTRL_ADDR" --ca "$CA" --server-name "$SERVER_NAME" --token /tmp/p5-nodeb.tok --node "$NODE_A" \
  --attachment "$AID" --nad "$NS/sec-p5" --interface net1 --ip "$VIP" --attack withdraw --duration 8s >/dev/null 2>&1 || true
sleep 2
# node-B agent authenticates fine, but the attachment is on node A, so its
# report is refused as an unknown/foreign subject -- envelope.node=node-A ignored.
if [ "$(k get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].conditions.ready}')" = "true" ]; then
  ok "victim stayed ready; a node-B credential cannot report for a node-A endpoint"
else
  bad "cross-node report changed the endpoint" "$(k get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].conditions.ready}')"
fi
if clog 30s | grep -q '"event":"envelope_node_mismatch"'; then
  ok "envelope.node=$NODE_A was recorded as a mismatch and ignored"
fi

# ---------------------------------------------------------------- session revocation
head_ "agent Pod deleted -> its live stream is revoked"
APOD=$(kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" --field-selector spec.nodeName="$NODE_A" -o jsonpath='{.items[0].metadata.name}')
APUID=$(kubectl -n "$CTRL_NS" get pod "$APOD" -o jsonpath='{.metadata.uid}')
kubectl -n "$CTRL_NS" delete pod "$APOD" --grace-period=1 >/dev/null 2>&1 &
if retry 60 'clog 90s | grep "\"event\":\"sessions_revoked\"" | grep -q "'"$APUID"'"'; then
  ok "the deleted agent's stream was revoked ($(clog 90s | grep sessions_revoked | tail -1 | grep -o '"streams":[0-9]*'))"
else
  bad "no session revocation on agent deletion"
fi
if retry 60 'clog 60s | grep "\"event\":\"agent_deregistered\"" | grep -q "'"$APUID"'"'; then
  ok "the deleted agent was deregistered from the AgentRegistry"
else
  bad "agent not deregistered"
fi
# a new agent Pod replaces it and readiness returns
retry 180 '[ "$(kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" --field-selector spec.nodeName="$NODE_A" --no-headers 2>/dev/null | grep -c Running)" -ge 1 ]' >/dev/null
if retry 150 '[ "$(ready_true)" -ge 1 ]'; then
  ok "a fresh agent Pod re-adopts under a new UID and readiness returns"
else
  bad "readiness did not return after the agent was replaced"
fi

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
