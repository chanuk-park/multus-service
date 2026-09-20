#!/usr/bin/env bash
# RQ3: authentication is amortized over the stream lifetime, not paid per report.
# Counts TokenReviews (agent_authenticated) vs health reports processed over a
# window on the steady-state cluster.
set -u
WIN=${1:-180}
CTRL_NS=${CTRL_NS:-multus-service-system}
CTRL=${CTRL:-multus-service-controller}
echo "observing steady state for ${WIN}s (no injected load)"
sleep "$WIN"
L=$(kubectl -n "$CTRL_NS" logs deploy/"$CTRL" --since="${WIN}s" 2>/dev/null)
auth=$(grep -c '"event":"agent_authenticated"' <<<"$L")
recv=$(grep -c '"event":"health_report_received"' <<<"$L")
appl=$(grep -c '"event":"health_report_applied"' <<<"$L")
snap=$(grep -c '"event":"health_snapshot_applied"' <<<"$L")
printf 'window=%ss\n  TokenReviews (agent_authenticated) = %s\n  health_report_received             = %s\n  health_report_applied              = %s\n  health_snapshot_applied            = %s\n' "$WIN" "$auth" "$recv" "$appl" "$snap"
python3 -c "
a,r=$auth,$recv+$snap
print('  ratio: %d reports per TokenReview' % (r//max(1,a)) if a else '  ratio: 0 TokenReviews in window (streams already established)')
"
