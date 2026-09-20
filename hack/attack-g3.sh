#!/usr/bin/env bash
# G3 stale-generation / replay attacks, run against the deployed system.
#
# Each attacker authenticates as a legitimate node agent (a valid Pod-bound
# token), so producer authorization (G2) has already passed. What is under test
# is whether evidence from a *superseded generation* can take effect:
#
#   R1 attachment replay      a deleted Pod's old attachment_id       -> 0 accepted
#   R2 sequence rollback       a lower sequence within one instance    -> 0 accepted
#   R2' stale instance         a superseded agent instance keeps going -> 0 accepted
#   R3 lease expiry            no fresh evidence -> endpoint ages out to not-ready
#
# The property demonstrated: stale evidence cannot acquire authority over a new
# attachment generation, and cannot resurrect or hold an endpoint.
set -u

NS=${NS:-ms-g3}
SVC=amf-g3
SLICE="$SVC-secondary-ipv4"
CTRL_NS=${CTRL_NS:-multus-service-system}
CTRL=${CTRL:-multus-service-controller}
AGENT_DS=${AGENT_DS:-multus-service-agent}
NODE=${NODE:-$(hostname)}
TARGET_IP=${TARGET_IP:-10.218.0.200}
AUD=${AUD:-health-controller}
FIXTURES="$(cd "$(dirname "$0")/../test/fixtures" && pwd)"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  \033[32mBLOCKED\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  \033[31mACCEPTED\033[0m %s\n' "$1"; [ $# -gt 1 ] && printf '           %s\n' "$2"; }
head_(){ printf '\n\033[1m%s\033[0m\n' "$1"; }
retry(){ local d=$(( $(date +%s)+$1 )); shift; while :; do eval "$@" && return 0; [ "$(date +%s)" -ge "$d" ] && return 1; sleep 0.5; done; }
k(){ kubectl -n "$NS" "$@"; }
clog(){ kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since="${1:-3m}" 2>/dev/null; }
ready0(){ k get endpointslice "$SLICE" -o jsonpath='{.endpoints[0].conditions.ready}' 2>/dev/null; }

CTRL_ADDR="$(kubectl -n "$CTRL_NS" get svc "$CTRL" -o jsonpath='{.spec.clusterIP}'):9090"
SERVER_NAME="$CTRL.$CTRL_NS.svc"
CA=/tmp/g3-ca.crt; TOK=/tmp/g3-agent.tok
kubectl -n "$CTRL_NS" get configmap controller-ca -o jsonpath='{.data.ca\.crt}' > "$CA" 2>/dev/null || true
mint(){ local pod uid; pod=$(kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" --field-selector spec.nodeName="$NODE" -o jsonpath='{.items[0].metadata.name}'); uid=$(kubectl -n "$CTRL_NS" get pod "$pod" -o jsonpath='{.metadata.uid}'); kubectl -n "$CTRL_NS" create token "$AGENT_DS" --bound-object-kind Pod --bound-object-name "$pod" --bound-object-uid "$uid" --audience "$AUD" --duration 1h > "$TOK" 2>/dev/null; }
# Prefer a prebuilt binary; go run recompiles on every call and dominates the
# runtime. Build once with:  go build -o /tmp/g3replay ./test/tools/g3replay
G3BIN=${G3BIN:-/tmp/g3replay}
g3(){
  if [ -x "$G3BIN" ]; then timeout 20 "$G3BIN" --addr "$CTRL_ADDR" --ca "$CA" --server-name "$SERVER_NAME" --token "$TOK" --node "$NODE" "$@" 2>&1
  else ( cd "$ROOT" && timeout 40 go run ./test/tools/g3replay --addr "$CTRL_ADDR" --ca "$CA" --server-name "$SERVER_NAME" --token "$TOK" --node "$NODE" "$@" ) 2>&1; fi
}
# reports applied for a given attachment id since N seconds ago
applied_for(){ clog "${2:-40s}" | grep '"event":"health_report_applied"' | grep -c "\"attachment_id\":\"$1\""; }

# Everything about an attachment comes from the controller's own
# attachment_discovered event, which is exactly what populated the Registry, so
# a report built from it matches by construction.
att_event(){ local uid; uid=$(k get pod "$1" -o jsonpath='{.metadata.uid}'); clog 15m | grep '"event":"attachment_discovered"' | grep "\"pod_uid\":\"$uid\"" | tail -1; }
ev_field(){ echo "$1" | grep -o "\"$2\":\"[^\"]*\"" | tail -1 | cut -d'"' -f4; }
aid_of(){ ev_field "$(att_event "$1")" attachment_id; }
netfield(){ k get pod "$1" -o jsonpath='{.metadata.annotations.k8s\.v1\.cni\.cncf\.io/network-status}' | python3 -c "
import json,sys
for e in json.load(sys.stdin):
    if not e.get('default'): print(e['$2'] if '$2'!='ip' else e['ips'][0]); break"; }

cleanup(){ head_ cleanup; kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1; rm -f /tmp/g3-*.crt /tmp/g3-*.tok; "$FIXTURES/lab.sh" down >/dev/null 2>&1; }
trap cleanup EXIT

head_ "setup"
[ -s "$CA" ] || { bad "controller CA missing -- run hack/gen-certs.sh"; exit 1; }
retry 240 '[ -n "$(kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" --field-selector spec.nodeName="$NODE" -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)" ]' || { bad "no agent on $NODE"; exit 1; }
kubectl get ns "$NS" >/dev/null 2>&1 && { kubectl delete ns "$NS" --wait=false >/dev/null 2>&1; retry 240 '! kubectl get ns "$NS" >/dev/null 2>&1'; }
kubectl create ns "$NS" >/dev/null
"$FIXTURES/lab.sh" up-local >/dev/null
cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-g3, namespace: $NS, annotations: {secondary-service.boanlab.io/probe-scope: endpoint, secondary-service.boanlab.io/health-target: "$TARGET_IP"}}
spec: {config: '{"cniVersion":"0.3.1","name":"sec-g3","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"host-local","ranges":[[{"subnet":"10.218.0.0/24","rangeStart":"10.218.0.10","rangeEnd":"10.218.0.99"}]]}}]}'}
---
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: sec-g3-target, namespace: $NS}
spec: {config: '{"cniVersion":"0.3.1","name":"sec-g3-target","plugins":[{"type":"macvlan","master":"mslab0","mode":"bridge","ipam":{"type":"static","addresses":[{"address":"$TARGET_IP/24"}]}}]}'}
---
apiVersion: v1
kind: Service
metadata: {name: $SVC, namespace: $NS, annotations: {secondary-service.boanlab.io/network: sec-g3, secondary-service.boanlab.io/workload-selector: app=g3}}
spec: {clusterIP: None, ports: [{name: n2, port: 8080, protocol: TCP}]}
---
apiVersion: v1
kind: Pod
metadata: {name: victim, namespace: $NS, labels: {app: g3}, annotations: {k8s.v1.cni.cncf.io/networks: sec-g3}}
spec: {nodeSelector: {kubernetes.io/hostname: $NODE}, containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
---
apiVersion: v1
kind: Pod
metadata: {name: tgt, namespace: $NS, annotations: {k8s.v1.cni.cncf.io/networks: sec-g3-target}}
spec: {nodeSelector: {kubernetes.io/hostname: $NODE}, containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
EOP
retry 240 '[ "$(k get pod victim tgt --no-headers 2>/dev/null | grep -c Running)" = "2" ]' || { bad "fixtures never started"; exit 1; }
retry 150 '[ "$(ready0)" = "true" ]' || { bad "victim never became ready"; exit 1; }
mint
EVX=$(att_event victim); AID_X=$(ev_field "$EVX" attachment_id); IFACE=$(ev_field "$EVX" interface); IP=$(ev_field "$EVX" ip)
echo "  victim pod=victim attachment(X)=$AID_X ip=$IP iface=$IFACE on $NODE"

# ---------------------------------------------------------------- R1
head_ "R1  attachment replay: old attachment_id after Pod recreate"
k delete pod victim --wait=true >/dev/null 2>&1
# recreate under a fresh Pod (new UID -> new attachment id Y)
cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: {name: victim2, namespace: $NS, labels: {app: g3}, annotations: {k8s.v1.cni.cncf.io/networks: sec-g3}}
spec: {nodeSelector: {kubernetes.io/hostname: $NODE}, containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
EOP
retry 240 '[ "$(k get pod victim2 -o jsonpath="{.status.phase}" 2>/dev/null)" = "Running" ]'
retry 150 '[ -n "$(aid_of victim2)" ]'
EVY=$(att_event victim2); AID_Y=$(ev_field "$EVY" attachment_id); IFACE2=$(ev_field "$EVY" interface); IP2=$(ev_field "$EVY" ip)
echo "  after recreate: attachment(Y)=$AID_Y ip=$IP2  (X=$AID_X ip=$IP)"
# positive control: a well-formed report for Y from the authenticated agent is
# applied, so any "unknown attachment" below is about staleness, not a typo.
pc=$(g3 --attachment "$AID_Y" --nad "$NS/sec-g3" --interface "$IFACE2" --ip "$IP2" --case replay-attachment --ready=true)
if echo "$pc" | grep -q '"unknown attachment"'; then
  echo "  [warn] positive control for Y was unknown -- field mismatch:"; echo "$pc" | sed 's/^/    /'
else
  echo "  positive control: a valid report for Y is accepted (not unknown)"
fi
mint
R1_T0=$(date +%s)
# replay the OLD id, both directions (victim/AID_X's Pod is gone)
g3 --attachment "$AID_X" --nad "$NS/sec-g3" --interface "$IFACE" --ip "$IP" --case replay-attachment --ready=true  | sed 's/^/  X.true  /'
g3 --attachment "$AID_X" --nad "$NS/sec-g3" --interface "$IFACE" --ip "$IP" --case replay-attachment --ready=false | sed 's/^/  X.false /'
sleep 2
# tight window since the replay began: the old Pod is deleted, so the only
# source of AID_X reports is the attacker, and none may be applied.
win=$(( $(date +%s) - R1_T0 + 2 ))s
if [ "$(applied_for "$AID_X" "$win")" = "0" ]; then
  ok "R1: 0 reports for the retired attachment id applied"
else
  bad "R1: a replayed old-attachment report was applied" "$(clog "$win" | grep '"health_report_applied"' | grep "$AID_X" | tail -1)"
fi
if clog 40s | grep '"event":"health_report_rejected"' | grep -q "unknown attachment"; then
  ok "R1: old attachment_id refused as unknown -- stale generation has no authority"
fi

# ---------------------------------------------------------------- R2
head_ "R2  sequence rollback within one instance"
# Re-derive Y's fields from the controller's own event and confirm with a
# positive control, so a mismatch below is about staleness, not a stale IP.
EVY2=$(att_event victim2); AID_Y=$(ev_field "$EVY2" attachment_id); IFACE2=$(ev_field "$EVY2" interface); IP2=$(ev_field "$EVY2" ip)
pc2=$(g3 --attachment "$AID_Y" --nad "$NS/sec-g3" --interface "$IFACE2" --ip "$IP2" --case replay-attachment --ready=true)
if echo "$pc2" | grep -q '"unknown attachment"'; then echo "  [warn] R2 positive control unknown: $(echo "$pc2" | tail -1)"; else echo "  R2 fields valid (Y=$AID_Y ip=$IP2 iface=$IFACE2)"; fi
before=$(clog 20s | grep -c '"reason":"report sequence did not advance"')
g3 --attachment "$AID_Y" --nad "$NS/sec-g3" --interface "$IFACE2" --ip "$IP2" --case seq-rollback --ready=true | sed 's/^/  roll  /'
sleep 1
if [ "$(clog 30s | grep -c '"reason":"report sequence did not advance"')" -gt "$before" ]; then
  ok "R2: a rolled-back sequence was rejected (sequence did not advance)"
else
  bad "R2: sequence rollback was not rejected"
fi

# ---------------------------------------------------------------- R2'
head_ "R2' stale agent instance keeps sending after being superseded"
STALE_OUT=$(g3 --attachment "$AID_Y" --nad "$NS/sec-g3" --interface "$IFACE2" --ip "$IP2" --case stale-instance --ready=true)
echo "$STALE_OUT" | sed 's/^/  stale /'
if echo "$STALE_OUT" | grep '^  OLD ' | grep -q 'superseded agent instance'; then
  ok "R2': the OLD (superseded) instance's later reports were rejected"
else
  bad "R2': the OLD instance was not rejected after supersession" "$(echo "$STALE_OUT" | grep '^  OLD ' | tail -1)"
fi
# and the victim must be unaffected by the whole R2 sequence
retry 60 '[ "$(ready0)" = "true" ]' && ok "victim endpoint unaffected by R2/R2'" || bad "R2 changed the endpoint" "$(ready0)"

# ---------------------------------------------------------------- R3
head_ "R3  lease expiry: no fresh evidence -> endpoint ages out"
# Park the real agent so no fresh report arrives; the attacker cannot refresh
# either (its reports are rejected), so the last accepted state must age out.
kubectl -n "$CTRL_NS" patch ds "$AGENT_DS" --type=merge -p '{"spec":{"template":{"spec":{"nodeSelector":{"multus-service.io/absent":"true"}}}}}' >/dev/null
retry 120 '[ "$(kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" --field-selector spec.nodeName="$NODE" --no-headers 2>/dev/null | grep -c .)" = "0" ]' >/dev/null
# attacker tries to hold it alive by replaying stale healthy evidence
( for i in $(seq 1 8); do g3 --attachment "$AID_Y" --nad "$NS/sec-g3" --interface "$IFACE2" --ip "$IP2" --case replay-attachment --ready=true >/dev/null 2>&1; done ) &
HOLD=$!
if retry 60 '[ "$(ready0)" != "true" ]'; then
  ok "R3: endpoint went not-ready on lease expiry despite the attacker's stale keep-alive"
else
  bad "R3: stale evidence kept the endpoint ready past the lease" "$(ready0)"
fi
kill $HOLD 2>/dev/null
# restore agents
kubectl -n "$CTRL_NS" patch ds "$AGENT_DS" --type=json -p '[{"op":"remove","path":"/spec/template/spec/nodeSelector"}]' >/dev/null 2>&1

printf '\n\033[1m%d blocked, %d accepted(=failures)\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
