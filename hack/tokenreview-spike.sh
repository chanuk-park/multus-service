#!/usr/bin/env bash
# TokenReview spike: can the controller replace the self-asserted envelope.node
# with an authenticated, node-bound identity, using nothing but Kubernetes-native
# mechanisms?
#
# The design under test (G2, subject-bound authority):
#
#   agent presents a projected, Pod-bound SA token with audience health-controller
#        -> controller TokenReview(token, audiences=[health-controller])
#        -> authenticated pod-uid  (validated: pod exists, UID matches)
#        -> Pod.spec.nodeName from that uid  == authoritative node
#        -> envelope.node is never consulted for authorization
#
# node-name IS present in the token as a claim, but the apiserver does not
# validate it at auth time, so the design resolves the node from the validated
# pod-uid instead and uses the claim only as a consistency check.
#
# Run with a kubeconfig that can create TokenReviews (cluster-admin here).
set -u
export KUBECONFIG=${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}
NS=${NS:-tr-spike}
AUD=${AUD:-health-controller}
NODE_A=${NODE_A:-telco-guard-01}
NODE_B=${NODE_B:-telco-guard-02}

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  \033[32mCONFIRMED\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  \033[31mFAILED\033[0m    %s\n' "$1"; [ $# -gt 1 ] && printf '            %s\n' "$2"; }
head_(){ printf '\n\033[1m%s\033[0m\n' "$1"; }

# review <token> <audience> -> "authenticated|pod-uid|node-name|error"
review() {
  python3 - "$1" "$2" <<'PY'
import sys,json,subprocess
tok,aud=sys.argv[1],sys.argv[2]
tr={"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","spec":{"token":tok,"audiences":[aud]}}
p=subprocess.run(["kubectl","create","-f","-","-o","json"],input=json.dumps(tr),capture_output=True,text=True)
if p.returncode!=0:
    print("false||| create-rejected"); sys.exit(0)
st=json.loads(p.stdout)["status"]; u=st.get("user",{}).get("extra",{}) or {}
print("%s|%s|%s|%s" % (st.get("authenticated"),
      u.get("authentication.kubernetes.io/pod-uid",["-"])[0],
      u.get("authentication.kubernetes.io/node-name",["-"])[0],
      (st.get("error") or "none")))
PY
}
field() { echo "$1" | cut -d'|' -f"$2"; }
resolve_node() { kubectl get pods -A -o json | python3 -c "
import json,sys
for p in json.load(sys.stdin)['items']:
    if p['metadata']['uid']=='$1': print(p['spec']['nodeName']); break
else: print('NOT-FOUND')"; }
token_for() { kubectl -n "$NS" create token agent-sa --bound-object-kind Pod \
  --bound-object-name "$1" --bound-object-uid "$2" --audience "$AUD" --duration 1h 2>/dev/null; }

cleanup(){ head_ cleanup; kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1; }
trap cleanup EXIT

head_ "setup: one agent Pod per node, sharing an SA"
kubectl get ns "$NS" >/dev/null 2>&1 && { kubectl delete ns "$NS" --wait=false >/dev/null 2>&1; \
  for _ in $(seq 1 120); do kubectl get ns "$NS" >/dev/null 2>&1 || break; sleep 1; done; }
kubectl create ns "$NS" >/dev/null
kubectl -n "$NS" create sa agent-sa >/dev/null
cat <<EOP | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: {name: agent-a, namespace: $NS}
spec: {serviceAccountName: agent-sa, nodeName: $NODE_A, containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
---
apiVersion: v1
kind: Pod
metadata: {name: agent-b, namespace: $NS}
spec: {serviceAccountName: agent-sa, nodeName: $NODE_B, containers: [{name: sh, image: docker.io/library/busybox:1.36, command: ["sh","-c","sleep infinity"]}]}
EOP
for _ in $(seq 1 60); do [ "$(kubectl -n "$NS" get pod agent-a agent-b --no-headers 2>/dev/null | grep -c Running)" = "2" ] && break; sleep 2; done
UID_A=$(kubectl -n "$NS" get pod agent-a -o jsonpath='{.metadata.uid}')
UID_B=$(kubectl -n "$NS" get pod agent-b -o jsonpath='{.metadata.uid}')
echo "  agent-a uid=$UID_A on $NODE_A ; agent-b uid=$UID_B on $NODE_B"

head_ "1/2  valid Pod-bound token authenticates and carries a validated pod-uid"
R=$(review "$(token_for agent-a "$UID_A")" "$AUD")
[ "$(field "$R" 1)" = "True" ] && ok "authenticated=true, audience $AUD" || bad "not authenticated" "$R"
[ "$(field "$R" 2)" = "$UID_A" ] && ok "pod-uid in review == real agent-a Pod UID" || bad "pod-uid mismatch" "$R"

head_ "3  authoritative node = Pod.spec.nodeName resolved from the validated pod-uid"
N=$(resolve_node "$(field "$R" 2)")
[ "$N" = "$NODE_A" ] && ok "pod-uid -> $N (claim says $(field "$R" 3); used only as a cross-check)" \
  || bad "node resolution wrong" "$N"

head_ "4  a node-B credential cannot gain authority over node A"
RB=$(review "$(token_for agent-b "$UID_B")" "$AUD")
NB=$(resolve_node "$(field "$RB" 2)")
if [ "$NB" = "$NODE_B" ]; then
  ok "agent-b token resolves to $NODE_B; envelope.node=$NODE_A would be ignored"
else
  bad "cross-node binding broke" "$NB"
fi

head_ "5  negative cases are rejected"
R=$(review "$(token_for agent-a "$UID_A")" "https://kubernetes.default.svc")
[ "$(field "$R" 1)" != "True" ] && ok "health token rejected for the kube-api audience (no reuse)" || bad "audience not enforced"
APITOK=$(kubectl -n "$NS" create token agent-sa --duration 1h 2>/dev/null)
R=$(review "$APITOK" "$AUD")
[ "$(field "$R" 1)" != "True" ] && ok "plain kube-api token rejected for $AUD (no reuse the other way)" || bad "api token accepted"
R=$(review "$(kubectl -n "$NS" create token agent-sa --bound-object-kind Pod --bound-object-name agent-a --bound-object-uid 00000000-0000-0000-0000-000000000000 --audience "$AUD" 2>/dev/null | tail -1)" "$AUD")
[ "$(field "$R" 1)" != "True" ] && ok "token bound to a wrong Pod UID rejected" || bad "uid binding not enforced"

# object lifecycle: a retired agent's token stops authenticating
kubectl -n "$NS" run doomed --image=docker.io/library/busybox:1.36 \
  --overrides="{\"spec\":{\"serviceAccountName\":\"agent-sa\",\"nodeName\":\"$NODE_B\"}}" \
  --command -- sh -c 'sleep infinity' >/dev/null 2>&1
for _ in $(seq 1 30); do [ "$(kubectl -n "$NS" get pod doomed -o jsonpath='{.status.phase}' 2>/dev/null)" = "Running" ] && break; sleep 2; done
DUID=$(kubectl -n "$NS" get pod doomed -o jsonpath='{.metadata.uid}')
TT=$(token_for doomed "$DUID")
[ "$(field "$(review "$TT" "$AUD")" 1)" = "True" ] && ok "token valid while its Pod exists" || bad "bound token not valid pre-delete"
kubectl -n "$NS" delete pod doomed --wait=true >/dev/null 2>&1; sleep 4
[ "$(field "$(review "$TT" "$AUD")" 1)" != "True" ] && ok "same token invalidated once the Pod is deleted (generation safety)" || bad "deleted-pod token still valid"

printf '\n\033[1m%d confirmed, %d failed\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
