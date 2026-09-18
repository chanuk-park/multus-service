#!/usr/bin/env bash
# Decomposes end-to-end convergence for a local (link-down) failure.
#
# Anchors, grouped by the clock that produced them:
#
#   driver      t0   fault injected in the Pod netns
#               t6   address gone from the DNS answer
#   agent       a1   failure_detected
#               a2   health_report_sent
#   controller  c1   health_report_received
#               c2   health_report_applied
#               c3   slice_patch_begin
#               c4   slice_patched (write returned)
#
# Intervals are reported within a single clock wherever possible. The three
# cross-clock ones are labelled as such: on a single-node deployment they share
# a clock, but the method must not depend on that.
#
# DNS convergence is measured from c3, the moment the write is issued, not from
# c4. CoreDNS watches the API server, so it can observe the write before the
# controller's call returns; measuring from c4 yields negative intervals that
# are an artefact rather than a result.
#
# usage: measure-convergence.sh [iterations]
set -uo pipefail
N=${1:-20}
NS=${NS:-ms-conv}
SVC=amf-conv
SLICE="$SVC-secondary-ipv4"
CTRL_NS=${CTRL_NS:-multus-service-system}
NODE=${NODE:-$(hostname)}
FIXTURES="$(cd "$(dirname "$0")/../test/fixtures" && pwd)"

agent_pod() {
  kubectl -n "$CTRL_NS" get pod -l app=multus-service-agent \
    --field-selector spec.nodeName="$NODE" -o jsonpath='{.items[0].metadata.name}'
}
ALOG_PID=""; CLOG_PID=""; DNS_PID=""
cleanup() {
  kill $ALOG_PID $CLOG_PID $DNS_PID 2>/dev/null
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  "$FIXTURES/lab.sh" down >/dev/null 2>&1
}
trap cleanup EXIT

if kubectl get ns "$NS" >/dev/null 2>&1; then
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  for _ in $(seq 1 240); do kubectl get ns "$NS" >/dev/null 2>&1 || break; sleep 1; done
fi
kubectl create ns "$NS" >/dev/null
"$FIXTURES/lab.sh" up-local >/dev/null

cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-conv, namespace: $NS}
spec:
  config: '{"cniVersion":"0.3.1","name":"sec-conv","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"host-local","ranges":[[{"subnet":"10.214.0.0/24","rangeStart":"10.214.0.10","rangeEnd":"10.214.0.99"}]]}}]}'
---
apiVersion: v1
kind: Service
metadata:
  name: $SVC
  namespace: $NS
  annotations:
    secondary-service.boanlab.io/network: sec-conv
    secondary-service.boanlab.io/workload-selector: app=conv
spec:
  clusterIP: None
  ports: [{name: n2, port: 8080, protocol: TCP}]
---
apiVersion: v1
kind: Pod
metadata: {name: conv, namespace: $NS, labels: {app: conv}, annotations: {k8s.v1.cni.cncf.io/networks: sec-conv}}
spec:
  nodeSelector: {kubernetes.io/hostname: $NODE}
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
---
apiVersion: v1
kind: Pod
metadata: {name: dnsprobe, namespace: $NS}
spec:
  containers:
    - name: sh
      image: docker.io/library/alpine:3.20
      command: ["/bin/sh","-c"]
      args: ["apk add --no-cache python3 bind-tools >/dev/null 2>&1; sleep infinity"]
EOP

for _ in $(seq 1 180); do
  [ "$(kubectl -n "$NS" get pod conv -o jsonpath='{.status.phase}' 2>/dev/null)" = "Running" ] && break; sleep 1
done
for _ in $(seq 1 180); do
  kubectl -n "$NS" exec dnsprobe -- sh -c 'command -v dig >/dev/null && command -v python3 >/dev/null' >/dev/null 2>&1 && break
  sleep 2
done

PODUID=$(kubectl -n "$NS" get pod conv -o jsonpath='{.metadata.uid}')
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
for _ in $(seq 1 90); do
  AID=$(kubectl -n "$CTRL_NS" logs deploy/multus-service-controller --since=10m 2>/dev/null \
        | grep '"event":"attachment_discovered"' | grep "\"pod_uid\":\"$PODUID\"" | tail -1 \
        | grep -o '"attachment_id":"[^"]*"' | cut -d'"' -f4)
  [ -n "$AID" ] && break; sleep 1
done
for _ in $(seq 1 120); do
  kubectl -n "$NS" get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].conditions.ready}' 2>/dev/null | grep -q true && break
  sleep 1
done
IP=$(kubectl -n "$NS" get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].addresses[0]}' 2>/dev/null)
TTL=""
for _ in $(seq 1 60); do
  TTL=$(kubectl -n "$NS" exec dnsprobe -- dig +noall +answer "$SVC.$NS.svc.cluster.local" 2>/dev/null | awk '{print $2; exit}')
  [ -n "$TTL" ] && break; sleep 1
done
echo "attachment=$AID ip=$IP coredns_ttl=${TTL:-unknown}s runs=$N"
echo "netns=$NSPATH"

# In-pod DNS poller. It records dig's exit status: an empty answer from a failed
# lookup is not a withdrawal, and counting it as one is how a measurement ends up
# claiming DNS converged before the fault was even detected.
kubectl -n "$NS" exec -i dnsprobe -- sh -c 'cat > /tmp/w.py' <<'PY'
import subprocess, sys, time
fqdn, dur, iv = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
end, prev = time.time() + dur, None
while time.time() < end:
    t = time.time()
    try:
        p = subprocess.run(["dig","+short","+tries=1","+time=1",fqdn],
                           capture_output=True, text=True, timeout=2)
        rc = p.returncode
        ans = ",".join(sorted(x for x in p.stdout.split() if x)) or "<empty>"
    except Exception:
        rc, ans = 99, "<timeout>"
    cur = (rc, ans)
    if cur != prev:
        print("%d %d %s" % (t*1000, rc, ans), flush=True); prev = cur
    d = iv - (time.time() - t)
    if d > 0: time.sleep(d)
PY

ALOG=$(mktemp); CLOG=$(mktemp); DLOG=$(mktemp); RESULTS=$(mktemp)
kubectl -n "$CTRL_NS" logs -f "$(agent_pod)" --tail=0 > "$ALOG" 2>/dev/null & ALOG_PID=$!
kubectl -n "$CTRL_NS" logs -f deploy/multus-service-controller --tail=0 > "$CLOG" 2>/dev/null & CLOG_PID=$!
RUNTIME=$(( N * 22 + 40 ))
kubectl -n "$NS" exec dnsprobe -- python3 -u /tmp/w.py "$SVC.$NS.svc.cluster.local" "$RUNTIME" 0.05 > "$DLOG" 2>/dev/null & DNS_PID=$!
sleep 3

echo
for i in $(seq 1 "$N"); do
  T0=$(python3 -c 'import time;print(int(time.time()*1e9))')
  sudo -n nsenter --net="$NSPATH" ip link set net1 down
  sleep 9
  python3 "$(dirname "$0")/convergence_parse.py" --mode run \
    --t0 "$T0" --attachment "$AID" --ip "$IP" \
    --agent-log "$ALOG" --controller-log "$CLOG" --dns-log "$DLOG" \
    --run "$i" >> "$RESULTS"
  tail -1 "$RESULTS"
  sudo -n nsenter --net="$NSPATH" ip link set net1 up
  sleep 12
done

echo
python3 "$(dirname "$0")/convergence_parse.py" --mode summary --results "$RESULTS"
