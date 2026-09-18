#!/usr/bin/env python3
"""Extracts and validates one convergence run, then summarises many.

Anchors are grouped by the clock that produced them. A run whose anchors are not
monotonic is reported invalid rather than averaged in: an interval that runs
backwards means the measurement is wrong, not that the system is fast.
"""
import argparse, json, sys


def events(path):
    out = []
    with open(path, errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    return out


def first(evs, t0, pred):
    for e in evs:
        if e.get("ts", 0) >= t0 and pred(e):
            return e
    return None


def run(a):
    t0 = int(a.t0)
    A, C = events(a.agent_log), events(a.controller_log)

    e = first(A, t0, lambda e: e.get("event") == "failure_detected"
              and e.get("attachment_id") == a.attachment)
    a1 = e["ts"] if e else None
    e = first(A, t0, lambda e: e.get("event") == "health_report_sent"
              and e.get("attachment_id") == a.attachment and e.get("local_ready") is False)
    a2 = e["ts"] if e else None

    e = first(C, t0, lambda e: e.get("event") == "health_report_received"
              and e.get("attachment_id") == a.attachment and e.get("local_ready") is False)
    c1 = e["ts"] if e else None
    e = first(C, t0, lambda e: e.get("event") == "health_report_applied"
              and e.get("attachment_id") == a.attachment and e.get("local_ready") is False)
    c2 = e["ts"] if e else None
    e = first(C, t0, lambda e: e.get("event") == "slice_patch_begin" and e.get("ready") == 0)
    c3, c4 = (e["at"], None) if e else (None, None)
    if e:
        e2 = first(C, t0, lambda x: x.get("event") == "slice_patched" and x.get("ready") == 0)
        if e2:
            c4 = e2.get("end")

    # Only an authoritative answer counts. dig exiting non-zero produces an
    # empty stdout that is indistinguishable from a real withdrawal otherwise.
    t6 = None
    with open(a.dns_log, errors="ignore") as f:
        for line in f:
            p = line.split()
            if len(p) < 3 or not p[0].isdigit():
                continue
            ms, rc, ans = int(p[0]), int(p[1]), p[2]
            if rc != 0:
                continue
            if ms * 1_000_000 >= t0 and a.ip not in ans:
                t6 = ms * 1_000_000
                break

    rec = {"run": int(a.run), "t0": t0, "a1": a1, "a2": a2,
           "c1": c1, "c2": c2, "c3": c3, "c4": c4, "t6": t6}

    problems = []
    if a1 is None:
        problems.append("no detection")
    if t6 is None:
        problems.append("no DNS withdrawal")
    order = [("t0", t0), ("a1", a1), ("a2", a2), ("c1", c1), ("c2", c2), ("c3", c3), ("t6", t6)]
    known = [(n, v) for n, v in order if v is not None]
    for (n1, v1), (n2, v2) in zip(known, known[1:]):
        if v2 < v1:
            problems.append("%s precedes %s" % (n2, n1))
    rec["valid"] = not problems
    rec["problems"] = problems
    print(json.dumps(rec))


def ms(a, b):
    if a is None or b is None:
        return None
    return (b - a) / 1e6


METRICS = [
    ("detection        t0 -> a1", "cross", "t0", "a1"),
    ("agent queue      a1 -> a2", "agent", "a1", "a2"),
    ("transport        a2 -> c1", "cross", "a2", "c1"),
    ("store apply      c1 -> c2", "ctrl ", "c1", "c2"),
    ("to patch         c2 -> c3", "ctrl ", "c2", "c3"),
    ("patch write      c3 -> c4", "ctrl ", "c3", "c4"),
    ("dns converge     c3 -> t6", "cross", "c3", "t6"),
    ("user visible     t0 -> t6", "drive", "t0", "t6"),
]


def summary(a):
    recs = [json.loads(l) for l in open(a.results) if l.strip()]
    good = [r for r in recs if r.get("valid")]
    print("runs: %d total, %d valid" % (len(recs), len(good)))
    for r in recs:
        if not r.get("valid"):
            print("  run %d invalid: %s" % (r["run"], "; ".join(r["problems"])))
    if not good:
        return
    print()
    print("%-26s %-6s %9s %9s %9s %9s" % ("interval", "clock", "min", "median", "p95", "max"))
    for name, clock, x, y in METRICS:
        vals = sorted(v for v in (ms(r[x], r[y]) for r in good) if v is not None)
        if not vals:
            print("%-26s %-6s %9s" % (name, clock, "n/a"))
            continue
        def pct(p):
            k = min(len(vals) - 1, int(round((len(vals) - 1) * p)))
            return vals[k]
        print("%-26s %-6s %8.1f%s %8.1f%s %8.1f%s %8.1f%s" %
              (name, clock, vals[0], "", pct(0.5), "", pct(0.95), "", vals[-1], ""))
    print()
    print("all values in ms. 'cross' spans two clocks; on a single-node")
    print("deployment they are the same clock, but the method does not rely on it.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["run", "summary"])
    ap.add_argument("--t0"); ap.add_argument("--attachment"); ap.add_argument("--ip")
    ap.add_argument("--agent-log"); ap.add_argument("--controller-log"); ap.add_argument("--dns-log")
    ap.add_argument("--run"); ap.add_argument("--results")
    args = ap.parse_args()
    (run if args.mode == "run" else summary)(args)
