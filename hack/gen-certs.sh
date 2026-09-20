#!/usr/bin/env bash
# Issues a CA and a server certificate for the controller's health transport,
# and installs them as cluster objects:
#
#   Secret    controller-tls  (tls.crt, tls.key)  -> mounted by the controller
#   ConfigMap controller-ca   (ca.crt)            -> mounted by every agent
#
# Server-authenticated TLS only. Client identity is the Pod-bound bearer token,
# not a client certificate, so there is no per-agent cert to manage. The bearer
# token is confidential precisely because this channel is encrypted.
set -euo pipefail
export KUBECONFIG=${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}
NS=${NS:-multus-service-system}
SVC=${SVC:-multus-service-controller}
DNS="$SVC.$NS.svc"
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT

echo "issuing CA + server cert for $DNS"
openssl genrsa -out "$DIR/ca.key" 4096 >/dev/null 2>&1
openssl req -x509 -new -nodes -key "$DIR/ca.key" -sha256 -days 3650 \
  -subj "/CN=multus-service-health-ca" -out "$DIR/ca.crt" >/dev/null 2>&1

openssl genrsa -out "$DIR/tls.key" 2048 >/dev/null 2>&1
cat > "$DIR/csr.cnf" <<EOF
[req]
distinguished_name = dn
req_extensions = ext
prompt = no
[dn]
CN = $DNS
[ext]
subjectAltName = @san
[san]
DNS.1 = $DNS
DNS.2 = $DNS.cluster.local
DNS.3 = $SVC
EOF
openssl req -new -key "$DIR/tls.key" -out "$DIR/tls.csr" -config "$DIR/csr.cnf" >/dev/null 2>&1
openssl x509 -req -in "$DIR/tls.csr" -CA "$DIR/ca.crt" -CAkey "$DIR/ca.key" -CAcreateserial \
  -out "$DIR/tls.crt" -days 825 -sha256 -extensions ext -extfile "$DIR/csr.cnf" >/dev/null 2>&1

kubectl get ns "$NS" >/dev/null 2>&1 || kubectl create ns "$NS" >/dev/null
kubectl -n "$NS" create secret tls controller-tls \
  --cert="$DIR/tls.crt" --key="$DIR/tls.key" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" create configmap controller-ca \
  --from-file=ca.crt="$DIR/ca.crt" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "installed Secret/controller-tls and ConfigMap/controller-ca in $NS"
echo "server name for agents: $DNS"
