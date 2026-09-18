#!/usr/bin/env bash
# Phase 2 acceptance: PodUID -> sandbox netns -> local link/address observation.
#
# No path probe yet. The agent must report local state accurately, re-resolve
# the namespace when the sandbox is recreated, and stop reporting an attachment
# that no longer exists.
#
# Runs on a node that has crictl and root, because it injects faults directly
# into Pod network namespaces.
# No pipefail: `grep -q` exits on first match and SIGPIPEs its upstream stage,
# which under pipefail turns a successful assertion into a failed pipeline --
# intermittently, depending on whether the upstream had finished writing.
set -u

NS=${NS:-ms-e2e2}
SVC=${SVC:-amf-local}
CTRL_NS=${CTRL_NS:-multus-service-system}
AGENT_DS=${AGENT_DS:-multus-service-agent}
NODE=${NODE:-$(hostname)}
FIXTURES="$(cd "$(dirname "$0")/../fixtures" && pwd)"
USE_MACVLAN=${USE_MACVLAN:-1}

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$1"; [ $# -gt 1 ] && printf '        %s\n' "$2"; }
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
# JSONL keys come out alphabetically sorted (Go marshals a map), so never grep
# for two keys in one pattern -- chain the greps instead.
has_event() {
  kubectl -n "$CTRL_NS" logs "$(agent_pod)" --tail=8000 2>/dev/null \
    | grep "\"event\":\"$1\"" | grep -q "\"attachment_id\":\"$2\""
}
# last local_health line for an attachment id
last_local() {
  kubectl -n "$CTRL_NS" logs "$(agent_pod)" --tail=6000 2>/dev/null \
    | grep '"event":"local_health"' | grep "\"attachment_id\":\"$1\"" | tail -1
}
field() { grep -o "\"$2\":[a-z0-9\"._-]*" <<<"$1" | tail -1 | cut -d: -f2 | tr -d '"'; }

pod_name() { k get pod -l app=local-amf -o jsonpath='{.items[0].metadata.name}' 2>/dev/null; }
pod_uid()  { k get pod "$(pod_name)" -o jsonpath='{.metadata.uid}' 2>/dev/null; }
pod_netstat() { k get pod "$(pod_name)" -o jsonpath='{.metadata.annotations.k8s\.v1\.cni\.cncf\.io/network-status}' 2>/dev/null; }

# attachment_id as the controller computes it, straight from the event stream
att_id() {
  kubectl -n "$CTRL_NS" logs deploy/multus-service-controller --tail=6000 2>/dev/null \
    | grep '"event":"attachment_discovered"' | grep "\"pod_uid\":\"$1\"" | tail -1 \
    | grep -o '"attachment_id":"[^"]*"' | cut -d'"' -f4
}

sandbox_netns() {
  local uid=$1 id
  id=$(sudo crictl pods -o json 2>/dev/null \
       | python3 -c "
import json,sys
d=json.load(sys.stdin)
for p in d.get('items',[]):
    if p.get('labels',{}).get('io.kubernetes.pod.uid')=='$uid' and p.get('state')=='SANDBOX_READY':
        print(p['id']); break
")
  [ -z "$id" ] && return 1
  sudo crictl inspectp -o json "$id" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for n in d['info']['runtimeSpec']['linux']['namespaces']:
    if n['type']=='network': print(n['path']); break
"
}

cleanup() {
  head_ "cleanup"
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  [ "$USE_MACVLAN" = "1" ] && "$FIXTURES/lab.sh" down >/dev/null 2>&1
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

# A namespace left terminating from a previous run would silently swallow every
# object created into it.
if kubectl get ns "$NS" >/dev/null 2>&1; then
  echo "  waiting for the previous $NS namespace to finish deleting"
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  retry 180 '! kubectl get ns "$NS" >/dev/null 2>&1' || { bad "namespace $NS stuck terminating"; exit 1; }
fi
kubectl create ns "$NS" >/dev/null

if [ "$USE_MACVLAN" = "1" ]; then
  "$FIXTURES/lab.sh" up-local >/dev/null
  NETWORK=sec-local
  cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-local, namespace: $NS}
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-local","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"host-local","ranges":[[{"subnet":"10.211.0.0/24","rangeStart":"10.211.0.10","rangeEnd":"10.211.0.99"}]]}}]}'
EOP
  echo "  macvlan over dummy parent mslab0 (child lives only in the Pod netns)"
else
  NETWORK=sec-net
  sed "s/namespace: ms-e2e/namespace: $NS/" "$FIXTURES/nad-bridge.yaml" | kubectl apply -f - >/dev/null
fi

cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Service
metadata:
  name: $SVC
  namespace: $NS
  annotations:
    secondary-service.boanlab.io/network: $NETWORK
    secondary-service.boanlab.io/workload-selector: app=local-amf
spec:
  clusterIP: None
  ports: [{name: n2, port: 8080, protocol: TCP}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: local-amf, namespace: $NS}
spec:
  replicas: 1
  selector: {matchLabels: {app: local-amf}}
  template:
    metadata:
      labels: {app: local-amf}
      annotations: {k8s.v1.cni.cncf.io/networks: $NETWORK}
    spec:
      nodeSelector: {kubernetes.io/hostname: $NODE}
      containers:
        - name: sh
          image: docker.io/library/busybox:1.36
          command: ["sh","-c","sleep infinity"]
EOP

retry 180 '[ "$(k get pod -l app=local-amf --no-headers 2>/dev/null | grep -c Running)" = "1" ]' \
  || { bad "workload never started"; exit 1; }
UID1=$(pod_uid)
retry 60 '[ -n "$(att_id '"$UID1"')" ]' || { bad "controller never discovered the attachment"; exit 1; }
AID1=$(att_id "$UID1")
NS1=$(sandbox_netns "$UID1")
IFACE=$(pod_netstat | python3 -c "
import json,sys
for e in json.load(sys.stdin):
    if not e.get('default'): print(e['interface']); break")
IP=$(pod_netstat | python3 -c "
import json,sys
for e in json.load(sys.stdin):
    if not e.get('default'): print(e['ips'][0]); break")
echo "  pod=$(pod_name) uid=${UID1:0:8} attachment=${AID1:0:8} iface=$IFACE ip=$IP"
echo "  netns=$NS1"

# ---------------------------------------------------------------- C1
head_ "C1  정상 -> local_ready=true"
if retry 30 '[ "$(field "$(last_local '"$AID1"')" local_ready)" = "true" ]'; then
  ok "agent reports local_ready=true through the resolved netns"
else
  bad "no healthy local report" "$(last_local "$AID1")"
fi
line=$(last_local "$AID1")
if [ "$(field "$line" interface_exists)" = "true" ] && \
   [ "$(field "$line" address_present)" = "true" ] && \
   [ "$(field "$line" link_usable)" = "true" ]; then
  ok "all three checks reported individually"
else
  bad "per-check detail missing" "$line"
fi

# ---------------------------------------------------------------- C2
head_ "C2  ip link set $IFACE down -> IPv4 가 남아 있어도 local_ready=false"
sudo nsenter --net="$NS1" ip link set "$IFACE" down
if retry 30 '[ "$(field "$(last_local '"$AID1"')" local_ready)" = "false" ]'; then
  ok "link-down detected"
else
  bad "link-down not reported" "$(last_local "$AID1")"
fi
line=$(last_local "$AID1")
if [ "$(field "$line" address_present)" = "true" ] && [ "$(field "$line" link_usable)" = "false" ]; then
  ok "address still present, link_usable=false -- an address-only check would have missed this"
else
  bad "link-down detail wrong" "$line"
fi
if has_event failure_detected "$AID1"; then
  ok "failure_detected anchor emitted for the evaluation"
else
  bad "no failure_detected event"
fi
sudo nsenter --net="$NS1" ip link set "$IFACE" up
retry 30 '[ "$(field "$(last_local '"$AID1"')" local_ready)" = "true" ]' >/dev/null \
  && ok "recovers when the link comes back" || bad "no recovery after link up"

# ---------------------------------------------------------------- C3
head_ "C3  secondary IP flush -> local_ready=false"
sudo nsenter --net="$NS1" ip addr flush dev "$IFACE"
if retry 30 '[ "$(field "$(last_local '"$AID1"')" local_ready)" = "false" ]'; then
  ok "address flush detected"
else
  bad "address flush not reported" "$(last_local "$AID1")"
fi
line=$(last_local "$AID1")
if [ "$(field "$line" interface_exists)" = "true" ] && [ "$(field "$line" address_present)" = "false" ]; then
  ok "interface still exists, address_present=false"
else
  bad "flush detail wrong" "$line"
fi
sudo nsenter --net="$NS1" ip addr add "$IP/24" dev "$IFACE" 2>/dev/null
retry 30 '[ "$(field "$(last_local '"$AID1"')" local_ready)" = "true" ]' >/dev/null \
  && ok "recovers when the address returns" || bad "no recovery after address restore"

# ---------------------------------------------------------------- C4
head_ "C4  Pod recreate -> 옛 attachment 은퇴, netns 재resolve"
k delete pod "$(pod_name)" --wait=true >/dev/null 2>&1
retry 240 '[ "$(k get pod -l app=local-amf --no-headers 2>/dev/null | grep -c Running)" = "1" ]' >/dev/null
UID2=$(pod_uid)
retry 90 '[ -n "$(att_id '"$UID2"')" ] && [ "$(att_id '"$UID2"')" != "'"$AID1"'" ]' >/dev/null
AID2=$(att_id "$UID2")
NS2=$(sandbox_netns "$UID2")

if [ -n "$AID2" ] && [ "$AID2" != "$AID1" ]; then
  ok "new sandbox produced a different attachment id (${AID1:0:8} -> ${AID2:0:8})"
else
  bad "attachment id did not change across recreate" "$AID1 / $AID2"
fi
if [ "$NS1" != "$NS2" ]; then
  ok "netns re-resolved to a new path (no IP was involved in the lookup)"
else
  bad "netns path unchanged across recreate" "$NS1"
fi
if has_event attachment_retired "$AID1"; then
  ok "old attachment retired -- a late report for it can no longer be matched"
else
  bad "old attachment was never retired"
fi
if retry 60 '[ "$(field "$(last_local '"$AID2"')" local_ready)" = "true" ]'; then
  ok "new attachment reported healthy from the new namespace"
else
  bad "no report for the new attachment" "$(last_local "$AID2")"
fi
# the old id must be quiet from here on
sleep 6
last_old_ts=$(kubectl -n "$CTRL_NS" logs "$(agent_pod)" --since=5s 2>/dev/null | grep -c "\"attachment_id\":\"$AID1\"")
if [ "$last_old_ts" = "0" ]; then
  ok "no further reports carry the retired attachment id"
else
  bad "agent still reporting the retired attachment ($last_old_ts lines in the last 5s)"
fi

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
