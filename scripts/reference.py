#!/usr/bin/env python3
"""Slow, literal reference of rules.md for cross-checking. Usage: reference.py DATA_DIR OUT_DIR (writes CSVs)."""
import calendar, csv, json, os, re, sys
from collections import defaultdict
from pathlib import Path

DAY = 86_400_000


def lines(path):
    data = path.read_bytes()
    parts = data.split(b"\n")
    if parts and parts[-1] == b"":
        parts.pop()
    return parts[1:]


def fields(line, n):
    fs = line.split(b",")
    if len(fs) != n:
        return None
    out = []
    for f in fs:
        if f.startswith(b'"'):
            if len(f) < 2 or not f.endswith(b'"') or b'"' in f[1:-1]:
                return None
            f = f[1:-1]
        elif b'"' in f:
            return None
        out.append(f.decode("utf-8", "surrogateescape"))
    return out


def rows(folder, n):
    for p in sorted(Path(folder).glob("*.csv")):
        for line in lines(p):
            r = fields(line, n)
            if r is not None:
                yield r


D = "0123456789"
F12 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|[+-][0-9]{2}:[0-9]{2})$", re.ASCII)


def ts(s):
    if s == "":
        return None
    if all(c in D for c in s):
        return int(s)
    m = F12.match(s)
    if not m:
        return None
    y, mo, d, h, mi, se = map(int, m.groups()[:6])
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59:
        return None
    if not (1 <= d <= calendar.monthrange(y, mo)[1]) if y > 0 else True:
        return None
    ms = (calendar.timegm((y, mo, d, h, mi, se)) * 1000)
    z = m.group(7)
    if z != "Z":
        oh, om = int(z[1:3]), int(z[4:6])
        if oh > 23 or om > 59:
            return None
        sign = 1 if z[0] == "+" else -1
        ms -= sign * (oh * 60 + om) * 60000
    return ms


def main():
    data, out = Path(sys.argv[1]), Path(sys.argv[2])
    man = json.loads((data / "manifest.json").read_text())
    ws, we = ts(man["window_start"]), ts(man["window_end"])
    prod = {r[0]: r[1] for r in rows_file(data / "products.csv", 2)}
    camps = {r[0]: r[1] for r in rows_file(data / "campaigns.csv", 2)}

    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in rows(data / "links", 2):
        if a and b:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb
    name = {}
    for x in list(parent):
        r = find(x)
        if r not in name or x < name[r]:
            name[r] = x

    def person(u):
        return name[find(u)] if u in parent else u

    best = {}
    for r in rows(data / "events", 7):
        if r[0] == "":
            continue
        ing = ts(r[3])
        if r[0] not in best or ing < best[r[0]][0]:
            best[r[0]] = (ing, r)
    evs = defaultdict(list)
    for ing, r in best.values():
        eid, uid, t, _, typ, pid, price = r
        t = ts(t)
        if not uid or t is None or not (ws <= t < we) or typ not in ("view", "cart", "purchase") or not pid:
            continue
        p = 0
        if typ == "purchase":
            if not price or not all(c in D for c in price) or not (1 <= int(price) <= 10_000_000):
                continue
            p = int(price)
        if ing - t > DAY:
            continue
        evs[person(uid)].append((t, typ, p, prod.get(pid, "unknown")))

    clicks = defaultdict(list)
    for cid, uid, c, t in rows(data / "clicks", 4):
        if not cid or not uid:
            continue
        t = ts(t)
        if t is None or not (ws - 7 * DAY <= t < we) or c not in camps:
            continue
        clicks[person(uid)].append((t, cid, c))

    ud = {}
    cd = defaultdict(lambda: [0, 0, 0, 0])
    for per, es in evs.items():
        es.sort(key=lambda e: e[0])
        sessions = []
        for e in es:
            if sessions and e[0] - sessions[-1][-1][0] <= 1_800_000:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        cl = clicks.get(per, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY
            q = [c for c in cl if start - 7 * DAY <= c[0] <= start]
            if q:
                mt = max(c[0] for c in q)
                credit = min(c[1] for c in q if c[0] == mt)
                credit = next(c[2] for c in q if c[0] == mt and c[1] == credit)
            else:
                credit = "organic"
            rev = sum(e[2] for e in s if e[1] == "purchase")
            purch = any(e[1] == "purchase" for e in s)
            a = cd[(credit, day)]
            a[0] += 1; a[1] += purch; a[2] += rev
            for c in {c[2] for c in q}:
                cd[(c, day)][3] += 1
            u = ud.setdefault((per, day), [0, 0, 0, defaultdict(int)])
            u[0] += 1; u[1] += len(s); u[2] += rev
            for e in s:
                if e[1] == "purchase":
                    u[3][e[3]] += e[2]
    out.mkdir(parents=True, exist_ok=True)
    import datetime
    dd = lambda d: (datetime.date(1970, 1, 1) + datetime.timedelta(days=d)).isoformat()
    with open(out / "user_daily.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (per, day), (s, e, r, cats) in ud.items():
            top = min(cats, key=lambda c: (-cats[c], c.encode("utf-8", "surrogateescape"))) if cats else ""
            w.writerow([per, dd(day), s, e, r, top])
    with open(out / "campaign_daily.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"])
        for (c, day), a in cd.items():
            w.writerow([c, camps.get(c, "organic"), dd(day), *a])


def rows_file(p, n):
    for line in lines(p):
        r = fields(line, n)
        if r is not None and r[0]:
            yield r


if __name__ == "__main__":
    main()
