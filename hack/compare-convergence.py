#!/usr/bin/env python3
"""Compare two convergence raw files (secure vs insecure) into the RQ3 table.

Each line is a run record from measure-convergence.sh with anchors
t0,a1,a2,c1,c2,c3,c4,t6 (ns) and valid flag. Reports per-clock intervals so the
security-relevant ones (report->apply, apply->slice, failure->slice) are visible.
"""
import sys, json

METRICS = [
    ("agent detect   t0->a1", "t0", "a1"),
    ("report->recv   a2->c1", "a2", "c1"),
    ("recv->apply    c1->c2", "c1", "c2"),
    ("apply->patch   c2->c3", "c2", "c3"),
    ("report->slice  c1->c3", "c1", "c3"),
    ("failure->slice t0->c3", "t0", "c3"),
    ("failure->DNS   t0->t6", "t0", "t6"),
]

def load(path):
    out=[]
    for line in open(path):
        line=line.strip()
        if not line.startswith("{"): continue
        try: r=json.loads(line)
        except ValueError: continue
        if r.get("valid"): out.append(r)
    return out

def ms(r,x,y):
    a,b=r.get(x),r.get(y)
    if a is None or b is None: return None
    return (b-a)/1e6

def col(rows,x,y):
    v=sorted(z for z in (ms(r,x,y) for r in rows) if z is not None)
    if not v: return (None,None,None)
    p=lambda q: v[min(len(v)-1,int(round((len(v)-1)*q)))]
    return (p(0.5), p(0.95), len(v))

sec=load(sys.argv[1]); ins=load(sys.argv[2])
print("secure n=%d valid, insecure n=%d valid\n" % (len(sec),len(ins)))
print("%-24s %18s %18s %10s" % ("interval (ms)","insecure med/p95","secure med/p95","Δ median"))
for name,x,y in METRICS:
    im,ip,_=col(ins,x,y); sm,sp,_=col(sec,x,y)
    d = (sm-im) if (im is not None and sm is not None) else None
    fmt=lambda a,b: ("%7.2f /%7.2f"%(a,b)) if a is not None else "     n/a"
    print("%-24s %18s %18s %10s" % (name, fmt(im,ip), fmt(sm,sp), ("%+.2f"%d) if d is not None else "n/a"))
