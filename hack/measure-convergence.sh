#!/usr/bin/env bash
# Decomposes end-to-end convergence for a local (link-down) failure.
#
#   t0  fault injected in the Pod netns
#   t1  agent detected it                 (failure_detected)
#   t2  agent sent the report             (health_report_sent)
#   t3  controller received it            (health_report_received)
#   t4  controller applied it             (health_report_applied)
#   t5  controller patched the slice      (slice_patched)
#   t6  DNS stopped answering the address (polled from a client Pod)
#
# Every anchor except t0 and t6 comes from the JSONL event stream, so the
# numbers are the system's own timestamps rather than an observer's.
#
# usage: measure-convergence.sh [iterations]
set -uo pipefail
N=${1:-5}
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
cleanup() {
  kill $ALOG_PID $CLOG_PID $DNS_PID 2>/dev/null
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  "$FIXTURES/lab.sh" down >/dev/null 2>&1
}
ALOG_PID=""; CLOG_PID=""; DNS_PID=""
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
IP=$(kubectl -n "$NS" get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].addresses[0]}' 2>/dev/null)
for _ in $(seq 1 120); do
  kubectl -n "$NS" get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].conditions.ready}' 2>/dev/null | grep -q true && break
  sleep 1
done
# Read the TTL only once the name actually resolves, or dig returns nothing to
# read it from.
TTL=""
for _ in $(seq 1 60); do
  TTL=$(kubectl -n "$NS" exec dnsprobe -- dig +noall +answer "$SVC.$NS.svc.cluster.local" 2>/dev/null | awk '{print $2; exit}')
  [ -n "$TTL" ] && break; sleep 1
done
echo "attachment=$AID ip=$IP netns=$NSPATH coredns_ttl=${TTL:-unknown}s"

# in-pod DNS poller: only prints transitions, with millisecond epochs
kubectl -n "$NS" exec -i dnsprobe -- sh -c 'cat > /tmp/w.py' <<'PY'
import subprocess, sys, time
fqdn, dur, iv = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
end, prev = time.time() + dur, None
while time.time() < end:
    t = time.time()
    try:
        out = subprocess.run(["dig","+short","+tries=1","+time=1",fqdn],
                             capture_output=True, text=True, timeout=2).stdout
        ans = ",".join(sorted(x for x in out.split() if x)) or "<empty>"
    except Exception:
        ans = "<timeout>"
    if ans != prev:
        print("%d %s" % (t*1000, ans), flush=True); prev = ans
    d = iv - (time.time() - t)
    if d > 0: time.sleep(d)
PY

ALOG=$(mktemp); CLOG=$(mktemp); DLOG=$(mktemp)
kubectl -n "$CTRL_NS" logs -f "$(agent_pod)" --tail=0 > "$ALOG" 2>/dev/null & ALOG_PID=$!
kubectl -n "$CTRL_NS" logs -f deploy/multus-service-controller --tail=0 > "$CLOG" 2>/dev/null & CLOG_PID=$!
RUNTIME=$(( N * 22 + 30 ))
kubectl -n "$NS" exec dnsprobe -- python3 -u /tmp/w.py "$SVC.$NS.svc.cluster.local" "$RUNTIME" 0.05 > "$DLOG" 2>/dev/null & DNS_PID=$!
sleep 3

echo
for i in $(seq 1 "$N"); do
  : > /tmp/conv_mark
  T0=$(python3 -c 'import time;print(int(time.time()*1e9))')
  sudo -n nsenter --net="$NSPATH" ip link set net1 down
  sleep 9

  python3 - "$T0" "$AID" "$IP" "$ALOG" "$CLOG" "$DLOG" "$i" <<'PY'
import json, sys, re
t0, aid, ip, alog, clog, dlog, run = sys.argv[1:8]
t0 = int(t0)

def events(path):
    out = []
    for line in open(path, errors="ignore"):
        line = line.strip()
        if not line.startswith("{"): continue
        try: out.append(json.loads(line))
        except Exception: pass
    return out

A, C = events(alog), events(clog)
def first(evs, pred):
    for e in evs:
        if e.get("ts", 0) >= t0 and pred(e): return e["ts"]
    return None

t1 = first(A, lambda e: e.get("event")=="failure_detected" and e.get("attachment_id")==aid)
t2 = first(A, lambda e: e.get("event")=="health_report_sent" and e.get("attachment_id")==aid
                        and e.get("local_ready") is False)
t3 = first(C, lambda e: e.get("event")=="health_report_received" and e.get("attachment_id")==aid
                        and e.get("local_ready") is False)
t4 = first(C, lambda e: e.get("event")=="health_report_applied" and e.get("attachment_id")==aid
                        and e.get("local_ready") is False)
t5 = first(C, lambda e: e.get("event")=="slice_patched" and e.get("ready")==0)

t6 = None
for line in open(dlog, errors="ignore"):
    p = line.split()
    if len(p) < 2 or not p[0].isdigit(): continue
    ms, ans = int(p[0]), p[1]
    # A transient resolver timeout is not a withdrawal. Only a real answer set
    # that no longer carries the address counts.
    if ans == "<timeout>": continue
    if ms*1_000_000 >= t0 and ip not in ans:
        t6 = ms*1_000_000; break

def ms(a, b):
    if a is None or b is None: return None
    return (b - a)/1e6

# Each stage is timed against the previous anchor that actually exists, so one
# missing event blanks its own row instead of cascading through the rest.
stages = [("t1 detect      ", t1), ("t2 send        ", t2), ("t3 receive     ", t3),
          ("t4 apply       ", t4), ("t5 slice patch ", t5), ("t6 dns withdraw", t6)]
rows, prev = [], t0
for name, ts in stages:
    rows.append((name, ms(prev, ts)))
    if ts is not None: prev = ts
print("run %s" % run)
for name, v in rows:
    print("   %s %s" % (name, ("%8.1f ms" % v) if v is not None else "      n/a"))
print("   %s %s" % ("total t0->t6   ", ("%8.1f ms" % ms(t0,t6)) if ms(t0,t6) is not None else "      n/a"))
# t5 -> t6 can come out slightly negative: the controller logs slice_patched
# after its API write returns, while CoreDNS can already have observed that
# same write. The ordering is real, the sign is an artefact of where each
# timestamp is taken.
PY

  sudo -n nsenter --net="$NSPATH" ip link set net1 up
  sleep 12
done
