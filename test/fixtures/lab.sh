#!/usr/bin/env bash
# Creates/removes the VXLAN parent used by nad-macvlan.yaml.
#
# macvlan cannot attach to a device already serving as an ipvlan master -- the
# kernel returns EBUSY -- so this uses a dedicated device rather than borrowing
# an existing overlay.
#
# usage: lab.sh up|down [peer-underlay-ip] [local-underlay-ip]
set -euo pipefail
ACTION=${1:?up|up-local|down}
DEV=mslab0
VNI=200
REMOTE=${2:-}
LOCAL=${3:-}

case "$ACTION" in
  up-local)
    # Single-node parent: a dummy device is enough to exercise real macvlan
    # semantics (child lives in the Pod netns, host sees only the parent).
    ip link show "$DEV" >/dev/null 2>&1 && { echo "$DEV already exists"; exit 0; }
    sudo ip link add "$DEV" type dummy
    sudo ip link set "$DEV" up
    ip -br link show "$DEV"
    ;;
  up)
    [[ -n "$REMOTE" && -n "$LOCAL" ]] || { echo "up needs <remote> <local> underlay IPs" >&2; exit 2; }
    ip link show "$DEV" >/dev/null 2>&1 && { echo "$DEV already exists"; exit 0; }
    sudo ip link add "$DEV" type vxlan id "$VNI" remote "$REMOTE" local "$LOCAL" dstport 4789
    sudo ip link set "$DEV" mtu 1370 up
    ip -br link show "$DEV"
    ;;
  down)
    sudo ip link del "$DEV" 2>/dev/null || true
    echo "$DEV removed"
    ;;
  *) echo "usage: lab.sh up <remote> <local> | up-local | down" >&2; exit 2 ;;
esac
