#!/usr/bin/env bash
# RQ3 connection-time cost: what does producer authentication add at stream setup?
#
# Opens N authenticated streams (fresh Pod-bound token each) and reads the
# controller's own per-connect timing (TokenReview, Pod/AgentRegistry lookup,
# total) from agent_authenticated. Measures establishment, not steady state.
#
# usage: measure-auth-establishment.sh [N]
set -u
N=${1:-100}
CTRL_NS=${CTRL_NS:-multus-service-system}
CTRL=${CTRL:-multus-service-controller}
AGENT_DS=${AGENT_DS:-multus-service-agent}
NODE=${NODE:-$(hostname)}
OUT=${OUT:-/tmp/auth-establishment.jsonl}
G3BIN=${G3BIN:-/tmp/g3replay}

CTRL_ADDR="$(kubectl -n "$CTRL_NS" get svc "$CTRL" -o jsonpath='{.spec.clusterIP}'):9090"
SERVER_NAME="$CTRL.$CTRL_NS.svc"
CA=/tmp/authcost-ca.crt
kubectl -n "$CTRL_NS" get configmap controller-ca -o jsonpath='{.data.ca\.crt}' > "$CA"
POD=$(kubectl -n "$CTRL_NS" get pod -l app="$AGENT_DS" --field-selector spec.nodeName="$NODE" -o jsonpath='{.items[0].metadata.name}')
PUID=$(kubectl -n "$CTRL_NS" get pod "$POD" -o jsonpath='{.metadata.uid}')
TOK=/tmp/authcost.tok
kubectl -n "$CTRL_NS" create token "$AGENT_DS" --bound-object-kind Pod --bound-object-name "$POD" --bound-object-uid "$PUID" --audience health-controller --duration 2h > "$TOK"
trap 'rm -f "$CA" "$TOK"' EXIT

echo "measuring $N authenticated establishments as $POD against $CTRL_ADDR"
start=$(date +%s)
for i in $(seq 1 "$N"); do
  timeout 8 "$G3BIN" --addr "$CTRL_ADDR" --ca "$CA" --server-name "$SERVER_NAME" --token "$TOK" \
    --node "$NODE" --attachment probe --nad x/y --interface net1 --ip 1.2.3.4 --case connect >/dev/null 2>&1
done
win=$(( $(date +%s) - start + 5 ))s

# Pull the controller log for the window and keep our probe's authentications.
# The two real agents also (re)authenticate occasionally; filter to this node's
# probe pod only, which is the agent Pod we minted the token for.
kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since="$win" 2>/dev/null \
  | grep '"event":"agent_authenticated"' | grep "\"pod\":\"$POD\"" > "$OUT"

python3 - "$OUT" "$N" <<'PY'
import sys, json
rows=[]
for line in open(sys.argv[1]):
    line=line.strip()
    if not line.startswith("{"): continue
    try: d=json.loads(line)
    except ValueError: continue
    if "tokenreview_us" in d: rows.append(d)
def stats(key):
    v=sorted(r[key]/1000.0 for r in rows if key in r)  # us -> ms
    if not v: return "n/a"
    p=lambda q: v[min(len(v)-1,int(round((len(v)-1)*q)))]
    return "min=%.3f  median=%.3f  p95=%.3f  max=%.3f ms" % (v[0], p(0.5), p(0.95), v[-1])
print("n=%d authenticated establishments captured (of %s requested)" % (len(rows), sys.argv[2]))
print("  TokenReview        %s" % stats("tokenreview_us"))
print("  Pod/registry look  %s" % stats("pod_lookup_us"))
print("  Total auth         %s" % stats("auth_total_us"))
PY
echo "raw: $OUT"
