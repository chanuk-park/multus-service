#!/usr/bin/env bash
# Phase 1 acceptance: Service annotation -> secondary IP EndpointSlice, ready=false.
#
# Assumes a controller is already running (make deploy). Everything it asserts
# is cluster state, so it works the same against an out-of-cluster controller
# except for the ownership test, which needs to stop the controller.
set -uo pipefail

NS=${NS:-ms-e2e}
SVC=${SVC:-amf-n2}
SLICE="${SVC}-secondary-ipv4"
CTRL_NS=${CTRL_NS:-multus-service-system}
CTRL_DEPLOY=${CTRL_DEPLOY:-multus-service-controller}
SEC_PREFIX=${SEC_PREFIX:-10.244.77.}
FIXTURES="$(cd "$(dirname "$0")/../fixtures" && pwd)"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$1"; [ $# -gt 1 ] && printf '        %s\n' "$2"; }
head_() { printf '\n\033[1m%s\033[0m\n' "$1"; }

# retry <seconds> <shell-snippet> -- succeeds as soon as the snippet does
retry() {
  local deadline=$(( $(date +%s) + $1 )); shift
  while :; do
    if eval "$@"; then return 0; fi
    [ "$(date +%s)" -ge "$deadline" ] && return 1
    sleep 0.5
  done
}

k() { kubectl -n "$NS" "$@"; }

slice_addrs()  { k get endpointslice "$SLICE" -o jsonpath='{range .endpoints[*]}{.addresses[0]}{"\n"}{end}' 2>/dev/null; }
slice_ready()  { k get endpointslice "$SLICE" -o jsonpath='{range .endpoints[*]}{.addresses[0]}={.conditions.ready}{"\n"}{end}' 2>/dev/null; }
slice_uids()   { k get endpointslice "$SLICE" -o jsonpath='{range .endpoints[*]}{.targetRef.uid}{"\n"}{end}' 2>/dev/null; }
all_slices()   { k get endpointslice -l "kubernetes.io/service-name=$SVC" -o jsonpath='{range .items[*]}{.metadata.name}|{.metadata.labels.endpointslice\.kubernetes\.io/managed-by}{"\n"}{end}' 2>/dev/null; }
pod_count()    { k get pods -l app=oai-amf --no-headers 2>/dev/null | grep -c Running; }
dns()          { k exec dnsprobe -- nslookup "$SVC.$NS.svc.cluster.local" 2>/dev/null | awk '/^Address/{print $NF}' | grep -v ':53$'; }

cleanup() {
  head_ "cleanup"
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
  kubectl -n "$CTRL_NS" scale deploy "$CTRL_DEPLOY" --replicas=1 >/dev/null 2>&1
}
trap cleanup EXIT

head_ "setup"
kubectl apply -f "$FIXTURES/namespace.yaml" >/dev/null
kubectl apply -f "$FIXTURES/nad-bridge.yaml" -f "$FIXTURES/service.yaml" \
              -f "$FIXTURES/workload.yaml" -f "$FIXTURES/probe-pod.yaml" >/dev/null
retry 120 'k get pod dnsprobe -o jsonpath="{.status.phase}" 2>/dev/null | grep -q Running' \
  && echo "  probe pod ready" || { bad "probe pod never started"; exit 1; }
# host-local IPAM in the fixture NAD allocates per node, so an unpinned
# workload would legitimately receive the same address on two nodes. Pin it, and
# test the duplicate case deliberately in T10 instead.
NODE_A=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')
NODE_B=$(kubectl get nodes -o jsonpath='{.items[1].metadata.name}' 2>/dev/null)
k patch deploy amf --type=merge \
  -p "{\"spec\":{\"template\":{\"spec\":{\"nodeSelector\":{\"kubernetes.io/hostname\":\"$NODE_A\"}}}}}" >/dev/null
echo "  workload pinned to $NODE_A"
retry 180 '[ "$(pod_count)" = "1" ]' && echo "  workload running" || { bad "workload never started"; exit 1; }

# ---------------------------------------------------------------- T1
head_ "T1  Pod 생성 -> secondary IP가 slice에 등장"
if retry 60 '[ -n "$(slice_addrs)" ]'; then
  addrs=$(slice_addrs)
  if [ "$(echo "$addrs" | wc -l)" = "1" ] && [[ "$addrs" == ${SEC_PREFIX}* ]]; then
    ok "slice holds exactly the secondary address ($addrs)"
  else
    bad "unexpected slice contents" "$addrs"
  fi
else
  bad "slice $SLICE never appeared"
fi

# ---------------------------------------------------------------- T2
head_ "T2  Pod Ready 인데도 ready=false 유지 (agent 없음)"
retry 60 'k get pod -l app=oai-amf -o jsonpath="{.items[0].status.conditions[?(@.type==\"Ready\")].status}" | grep -q True' \
  && echo "  pod is Ready" || bad "pod never became Ready"
sleep 3
r=$(slice_ready)
if [[ "$r" == *"=false" && "$r" != *"=true"* ]]; then
  ok "endpoint stays ready=false with no health report ($r)"
else
  bad "endpoint readiness wrong" "$r"
fi
if k get endpointslice "$SLICE" -o json | grep -q '"ready"'; then
  ok "ready is set explicitly (never nil -- CoreDNS reads nil as healthy)"
else
  bad "ready condition is missing from the endpoint"
fi

# ---------------------------------------------------------------- T3
head_ "T3  primary IP 누출 없음"
slices=$(all_slices)
if [ "$(echo "$slices" | wc -l)" = "1" ] && [[ "$slices" == *"secondary-service.boanlab.io"* ]]; then
  ok "only our slice exists, no built-in controller slice ($slices)"
else
  bad "unexpected slices for $SVC" "$slices"
fi
if slice_addrs | grep -q '^10\.42\.'; then
  bad "a primary Pod IP leaked into the slice"
else
  ok "no primary (10.42.x) address anywhere in the slice"
fi

# ---------------------------------------------------------------- T4
head_ "T4  ready endpoint 이 없으면 DNS 응답도 없음"
sleep 6
if [ -z "$(dns)" ]; then
  ok "service name resolves to no address while every endpoint is ready=false"
else
  bad "DNS returned addresses for an all-not-ready service" "$(dns)"
fi

# ---------------------------------------------------------------- T5
head_ "T5  ready=true 로 뒤집으면 secondary IP가 DNS에 등장 / 컨트롤러가 소유권 회수"
if kubectl -n "$CTRL_NS" get deploy "$CTRL_DEPLOY" >/dev/null 2>&1; then
  kubectl -n "$CTRL_NS" scale deploy "$CTRL_DEPLOY" --replicas=0 >/dev/null
  retry 60 '[ "$(kubectl -n '"$CTRL_NS"' get deploy '"$CTRL_DEPLOY"' -o jsonpath="{.status.replicas}")" = "" ]' >/dev/null
  k patch endpointslice "$SLICE" --type=json \
    -p '[{"op":"replace","path":"/endpoints/0/conditions/ready","value":true}]' >/dev/null
  if retry 30 'dns | grep -q "^'"$SEC_PREFIX"'"'; then
    ok "DNS returns the secondary address once ready=true ($(dns))"
  else
    bad "DNS never returned the secondary address" "$(dns)"
  fi
  if dns | grep -q '^10\.42\.'; then bad "primary IP appeared in DNS"; else ok "primary IP never appears in DNS"; fi

  kubectl -n "$CTRL_NS" scale deploy "$CTRL_DEPLOY" --replicas=1 >/dev/null
  if retry 90 'slice_ready | grep -q "=false"'; then
    ok "controller reclaims the field and resets ready=false"
  else
    bad "controller did not reset the manually flipped endpoint" "$(slice_ready)"
  fi
else
  echo "  (skipped: no in-cluster controller Deployment found)"
fi

# ---------------------------------------------------------------- T6
head_ "T6  scale 1 -> 5 -> secondary IP 5개"
k scale deploy amf --replicas=5 >/dev/null
retry 180 '[ "$(pod_count)" = "5" ]' >/dev/null
if retry 90 '[ "$(slice_addrs | grep -c .)" = "5" ]'; then
  ok "5 distinct secondary addresses published"
else
  bad "expected 5 endpoints" "$(slice_addrs | tr '\n' ' ')"
fi
if [ "$(slice_addrs | sort -u | grep -c .)" = "5" ]; then ok "addresses are distinct"; else bad "duplicate addresses"; fi

# ---------------------------------------------------------------- T7
head_ "T7  Pod 삭제 -> endpoint 제거"
victim=$(k get pod -l app=oai-amf -o jsonpath='{.items[0].metadata.name}')
victim_uid=$(k get pod "$victim" -o jsonpath='{.metadata.uid}')
k scale deploy amf --replicas=4 >/dev/null
k delete pod "$victim" --wait=true >/dev/null 2>&1
if retry 90 '! slice_uids | grep -q '"$victim_uid"''; then
  ok "deleted Pod's endpoint disappeared (uid ${victim_uid:0:8})"
else
  bad "endpoint for the deleted Pod is still published"
fi

# ---------------------------------------------------------------- T8
head_ "T8  Pod recreate -> 옛 UID 제거, 새 UID/IP 반영"
before_uids=$(slice_uids | sort | tr '\n' ' ')
k delete pod -l app=oai-amf --wait=true >/dev/null 2>&1
retry 180 '[ "$(pod_count)" = "4" ]' >/dev/null
if retry 120 '[ "$(slice_uids | grep -c .)" = "4" ]'; then
  after_uids=$(slice_uids | sort | tr '\n' ' ')
  if [ "$before_uids" != "$after_uids" ]; then
    ok "endpoint set fully replaced by the new Pod UIDs"
  else
    bad "slice still carries the old Pod UIDs" "$after_uids"
  fi
else
  bad "slice did not converge after recreate" "$(slice_uids | tr '\n' ' ')"
fi

# ---------------------------------------------------------------- T9
head_ "T9  같은 NAD 2회 attach -> endpoint 생성 안 함 + Warning 이벤트"
cat <<'EOP' | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: amf-double
  namespace: ms-e2e
  labels: {app: oai-amf}
  annotations:
    k8s.v1.cni.cncf.io/networks: '[{"name":"sec-net"},{"name":"sec-net"}]'
spec:
  containers:
    - name: sh
      image: docker.io/library/busybox:1.36
      command: ["sh","-c","sleep infinity"]
EOP
retry 120 'k get pod amf-double -o jsonpath="{.status.phase}" 2>/dev/null | grep -q Running' >/dev/null
double_uid=$(k get pod amf-double -o jsonpath='{.metadata.uid}' 2>/dev/null)
sleep 5
if [ -n "$double_uid" ] && slice_uids | grep -q "$double_uid"; then
  bad "an ambiguous attachment was published instead of being skipped"
else
  ok "ambiguous Pod is skipped rather than guessed at"
fi
if retry 30 'k get events --field-selector reason=AmbiguousAttachment 2>/dev/null | grep -q AmbiguousAttachment'; then
  ok "AmbiguousAttachment Warning event recorded on the Service"
else
  bad "no AmbiguousAttachment event"
fi
k delete pod amf-double --wait=false >/dev/null 2>&1

# ---------------------------------------------------------------- T10
head_ "T10 두 Pod 가 같은 주소를 주장하면 둘 다 게시하지 않고 경고"
kubectl apply -f "$FIXTURES/nad-static.yaml" >/dev/null
DUP_B=${NODE_B:-$NODE_A}
cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Service
metadata:
  name: amf-dup
  namespace: $NS
  annotations:
    secondary-service.boanlab.io/network: sec-net-fixed
    secondary-service.boanlab.io/workload-selector: app=dup
spec:
  clusterIP: None
  ports: [{name: n2, port: 8080, protocol: TCP}]
---
apiVersion: v1
kind: Pod
metadata: {name: dup-a, namespace: $NS, labels: {app: dup}, annotations: {k8s.v1.cni.cncf.io/networks: sec-net-fixed}}
spec:
  nodeSelector: {kubernetes.io/hostname: $NODE_A}
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
---
apiVersion: v1
kind: Pod
metadata: {name: dup-b, namespace: $NS, labels: {app: dup}, annotations: {k8s.v1.cni.cncf.io/networks: sec-net-fixed}}
spec:
  nodeSelector: {kubernetes.io/hostname: $DUP_B}
  containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]
EOP
retry 180 '[ "$(k get pod dup-a dup-b --no-headers 2>/dev/null | grep -c Running)" = "2" ]' >/dev/null
sleep 5
dup_addrs=$(k get endpointslice amf-dup-secondary-ipv4 -o jsonpath='{range .endpoints[*]}{.addresses[0]}{"\n"}{end}' 2>/dev/null)
if [ -z "$(echo "$dup_addrs" | grep -c . | grep -v '^0$')" ]; then
  ok "10.244.77.250 claimed by dup-a and dup-b, neither published"
else
  bad "a conflicting address was published" "$(echo "$dup_addrs" | tr '\n' ' ')"
fi
dup_msg=$(k get events --field-selector reason=DuplicateAddress -o jsonpath='{.items[-1].message}' 2>/dev/null)
if [ -n "$dup_msg" ] && grep -q dup-a <<<"$dup_msg" && grep -q dup-b <<<"$dup_msg"; then
  ok "both claimants named in the event"
else
  bad "event does not name both claimants" "$dup_msg"
fi
if retry 30 'k get events --field-selector reason=DuplicateAddress 2>/dev/null | grep -q DuplicateAddress'; then
  ok "DuplicateAddress Warning event recorded"
else
  bad "duplicate address was dropped silently"
fi
kubectl -n "$NS" delete svc amf-dup --wait=false >/dev/null 2>&1
kubectl -n "$NS" delete pod dup-a dup-b --wait=false >/dev/null 2>&1

# ---------------------------------------------------------------- T10b
head_ "T10b selector 를 가진 Service 는 관리 거부"
cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Service
metadata:
  name: amf-n2-bad
  namespace: $NS
  annotations:
    secondary-service.boanlab.io/network: sec-net
    secondary-service.boanlab.io/workload-selector: app=oai-amf
spec:
  clusterIP: None
  selector: {app: oai-amf}
  ports: [{name: n2, port: 8080, protocol: TCP}]
EOP
sleep 5
mine=$(k get endpointslice -l "kubernetes.io/service-name=amf-n2-bad,endpointslice.kubernetes.io/managed-by=secondary-service.boanlab.io" \
       --no-headers 2>/dev/null | grep -c .)
if [ "$mine" = "0" ]; then
  ok "no secondary slice created for a Service that sets spec.selector"
else
  bad "managed a Service that violates the selectorless invariant"
fi
if retry 30 'k get events --field-selector reason=InvalidService 2>/dev/null | grep -q InvalidService'; then
  ok "InvalidService Warning event recorded"
else
  bad "no InvalidService event"
fi
# and the built-in controller does exactly what the invariant exists to prevent
builtin=$(k get endpointslice -l "kubernetes.io/service-name=amf-n2-bad" \
          -o jsonpath='{range .items[*]}{.endpoints[*].addresses[0]}{"\n"}{end}' 2>/dev/null | grep -c '^10\.42\.')
if [ "$builtin" -ge 1 ]; then
  ok "built-in controller published the primary IP there, as expected ($builtin slice)"
else
  echo "  (note: built-in slice not observed yet; timing)"
fi
kubectl -n "$NS" delete svc amf-n2-bad --wait=false >/dev/null 2>&1

# ---------------------------------------------------------------- T11
head_ "T11 Service 삭제 -> owned EndpointSlice GC"
k delete svc "$SVC" --wait=true >/dev/null
if retry 90 '! k get endpointslice '"$SLICE"' >/dev/null 2>&1'; then
  ok "slice garbage-collected via ownerReference"
else
  bad "slice outlived its Service"
fi

# ---------------------------------------------------------------- T12
head_ "T12 측정용 JSONL 이벤트 스트림"
if kubectl -n "$CTRL_NS" get deploy "$CTRL_DEPLOY" >/dev/null 2>&1; then
  logs=$(kubectl -n "$CTRL_NS" logs deploy/"$CTRL_DEPLOY" --tail=4000 2>/dev/null)
  for ev in attachment_discovered slice_patched; do
    if grep -q "\"event\":\"$ev\"" <<<"$logs"; then ok "emits $ev"; else bad "missing $ev in the event stream"; fi
  done
else
  echo "  (skipped: no in-cluster controller Deployment found)"
fi

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
