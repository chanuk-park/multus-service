#!/usr/bin/env bash
# Detection latency: fault injected in the Pod netns -> agent failure_detected.
#
# This is the interval the evaluation cares about most, because the bench run
# showed it dominates everything downstream: the control plane costs ~0.23s and
# DNS convergence ~0.97s at a 1s TTL, while detection is a design variable.
#
# usage: measure-detection.sh [iterations]
set -uo pipefail
N=${1:-5}
NS=${NS:-ms-measure}
CTRL_NS=${CTRL_NS:-multus-service-system}
NODE=${NODE:-$(hostname)}
FIXTURES="$(cd "$(dirname "$0")/../test/fixtures" && pwd)"

agent_pod() {
  kubectl -n "$CTRL_NS" get pod -l app=multus-service-agent \
    --field-selector spec.nodeName="$NODE" -o jsonpath='{.items[0].metadata.name}'
}
cleanup() {
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  "$FIXTURES/lab.sh" down >/dev/null 2>&1
}
trap cleanup EXIT

if kubectl get ns "$NS" >/dev/null 2>&1; then
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  echo "waiting for the previous $NS namespace to finish deleting"
  for _ in $(seq 1 240); do kubectl get ns "$NS" >/dev/null 2>&1 || break; sleep 1; done
fi
kubectl create ns "$NS" >/dev/null
"$FIXTURES/lab.sh" up-local >/dev/null

cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-local, namespace: $NS}
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-local","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"host-local","ranges":[[{"subnet":"10.212.0.0/24","rangeStart":"10.212.0.10","rangeEnd":"10.212.0.99"}]]}}]}'
---
apiVersion: v1
kind: Service
metadata:
  name: m
  namespace: $NS
  annotations:
    secondary-service.boanlab.io/network: sec-local
    secondary-service.boanlab.io/workload-selector: app=m
spec:
  clusterIP: None
  ports: [{name: p, port: 8080, protocol: TCP}]
---
apiVersion: v1
kind: Pod
metadata: {name: m, namespace: $NS, labels: {app: m}, annotations: {k8s.v1.cni.cncf.io/networks: sec-local}}
spec:
  nodeSelector: {kubernetes.io/hostname: $NODE}
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
EOP

for i in $(seq 1 120); do
  [ "$(kubectl -n "$NS" get pod m -o jsonpath='{.status.phase}' 2>/dev/null)" = "Running" ] && break; sleep 1
done
# NOTE: not UID -- bash treats that as readonly, so the assignment would be ignored.
PODUID=$(kubectl -n "$NS" get pod m -o jsonpath='{.metadata.uid}')
SB=$(sudo -n crictl pods -o json | python3 -c "
import json,sys
for p in json.load(sys.stdin).get('items',[]):
    if p.get('labels',{}).get('io.kubernetes.pod.uid')=='$PODUID' and p.get('state')=='SANDBOX_READY':
        print(p['id']); break")
NSPATH=$(sudo -n crictl inspectp -o json "$SB" | python3 -c "
import json,sys
for n in json.load(sys.stdin)['info']['runtimeSpec']['linux']['namespaces']:
    if n['type']=='network': print(n['path']); break")
AID=""
for i in $(seq 1 60); do
  AID=$(kubectl -n "$CTRL_NS" logs deploy/multus-service-controller --tail=4000 2>/dev/null \
        | grep '"event":"attachment_discovered"' | grep "\"pod_uid\":\"$PODUID\"" | tail -1 \
        | grep -o '"attachment_id":"[^"]*"' | cut -d'"' -f4)
  [ -n "$AID" ] && break; sleep 1
done
echo "pod=m netns=$NSPATH attachment=$AID"
[ -n "$NSPATH" ] || { echo "could not resolve the sandbox netns; aborting" >&2; exit 1; }
[ -n "$AID" ]    || { echo "controller never reported an attachment id; aborting" >&2; exit 1; }
echo

# Stream the agent log to a local file once. Polling kubectl per iteration
# costs ~300ms a call, which is larger than the quantity being measured. The
# event carries its own nanosecond timestamp, so log delivery delay does not
# enter the number.
LOG=$(mktemp)
kubectl -n "$CTRL_NS" logs -f "$(agent_pod)" --tail=0 > "$LOG" 2>/dev/null &
LOGPID=$!
trap 'kill $LOGPID 2>/dev/null; cleanup' EXIT
sleep 2

samples=""
for i in $(seq 1 "$N"); do
  before=$(grep '"event":"failure_detected"' "$LOG" | grep -c "\"attachment_id\":\"$AID\"")
  t0=$(python3 -c 'import time;print(int(time.time()*1e9))')
  sudo nsenter --net="$NSPATH" ip link set net1 down

  ts=""
  for _ in $(seq 1 60); do
    cnt=$(grep '"event":"failure_detected"' "$LOG" | grep -c "\"attachment_id\":\"$AID\"")
    if [ "$cnt" -gt "$before" ]; then
      ts=$(grep '"event":"failure_detected"' "$LOG" | grep "\"attachment_id\":\"$AID\"" | tail -1 \
           | grep -o '"ts":[0-9]*' | cut -d: -f2)
      break
    fi
    sleep 0.1
  done

  if [ -n "$ts" ]; then
    ms=$(python3 -c "print('%.1f' % (($ts - $t0)/1e6))")
    echo "  run $i: ${ms} ms"
    samples="$samples $ms"
  else
    echo "  run $i: no detection within 6s"
  fi
  sudo nsenter --net="$NSPATH" ip link set net1 up
  sleep 3
done
kill $LOGPID 2>/dev/null
rm -f "$LOG"

echo
python3 -c "
import sys
v=[float(x) for x in '$samples'.split()]
if v:
    v.sort()
    print('n=%d  min=%.1f ms  median=%.1f ms  max=%.1f ms' % (len(v), v[0], v[len(v)//2], v[-1]))
"
