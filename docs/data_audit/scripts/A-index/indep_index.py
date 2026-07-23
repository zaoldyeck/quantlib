"""A-index 稽核:獨立解析 data/index/**/*.csv(不呼叫 Scala parser),輸出 parquet 供與 PG `index` 表逐欄比對。

run: uv run --project . python docs/data_audit/scripts/A-index/indep_index.py
不依賴 cache.duckdb。
"""
import csv, io, re, sys, os
from pathlib import Path
import polars as pl

ROOT = Path("/Users/zaoldyeck/Documents/scala/quantlib")
IDX = ROOT / "data" / "index"
FN = re.compile(r"^(\d{4})_(\d{1,2})_(\d{1,2})\.csv$")

def read_lines(p: Path):
    raw = p.read_bytes()
    try:
        txt = raw.decode("big5hkscs")
    except UnicodeDecodeError:
        txt = raw.decode("big5hkscs", errors="replace")
    return txt.splitlines()

def num(s):
    s = s.replace(",", "").replace(" ", "").strip()
    if s in ("", "-", "--", "null"):
        return None
    try:
        return float(s)
    except ValueError:
        return None

def parse_twse(p: Path):
    """獨立語意:切掉 備註 footer(不論有無前導引號),取 6/7 欄的資料列。"""
    lines = []
    for ln in read_lines(p):
        s = ln.strip()
        if s.startswith("備註:") or s.startswith('"備註:'):
            break
        lines.append(ln)
    rows = list(csv.reader(io.StringIO("\n".join(lines))))
    out = []
    section = None
    for r in rows:
        r = [c.replace(" ", "").replace(",", "") for c in r]
        if len(r) == 1:
            section = r[0]
            continue
        if len(r) not in (6, 7):
            continue
        if r[0] in ("指數", "報酬指數"):
            continue
        if r[0] == "":
            continue
        out.append(dict(name=r[0], close=num(r[1]), sign=r[2], mag=r[3],
                        pct=num(r[4]), note=r[5] if len(r) > 5 else "", section=section))
    return out

def parse_tpex(p: Path):
    rows = [r for r in csv.reader(io.StringIO("\n".join(read_lines(p))))]
    rows = [[c.replace(" ", "").replace(",", "") for c in r] for r in rows if len(r) == 4]
    out = []
    seen_ret = False
    for r in rows:
        if r[0] == "報酬指數":
            seen_ret = True
            continue
        if r[0] == "指數":
            continue
        out.append(dict(name=r[0], close=num(r[1]), sign="", mag=r[2],
                        pct=num(r[3]), note="", section="return" if seen_ret else "price"))
    return out

recs = []
bad = []
for mkt in ("twse", "tpex"):
    for f in sorted((IDX / mkt).rglob("*.csv")):
        m = FN.match(f.name)
        if not m:
            bad.append((mkt, str(f), "filename"))
            continue
        y, mo, d = (int(x) for x in m.groups())
        size = f.stat().st_size
        try:
            rs = parse_twse(f) if mkt == "twse" else parse_tpex(f)
        except Exception as e:
            bad.append((mkt, str(f), f"parse:{e}"))
            continue
        for r in rs:
            if mkt == "twse":
                sign = r["sign"]
                mag = num(r["mag"])
                chg = None
                if sign == "-":
                    chg = -mag if mag is not None else None
                elif sign == "+":
                    chg = mag if mag is not None else None
                elif sign == "":
                    chg = 0.0
                else:
                    chg = None
            else:
                chg = num(r["mag"])
            recs.append(dict(market=mkt, date=f"{y:04d}-{mo:02d}-{d:02d}", fsize=size,
                             name=r["name"], close=r["close"], sign=r["sign"],
                             mag_raw=r["mag"], change=chg, pct=r["pct"],
                             note=r["note"], section=r["section"] or ""))

df = pl.DataFrame(recs)
out = ROOT / "docs/data_audit/scripts/A-index/indep_index.parquet"
df.write_parquet(out)
print("rows", df.height, "-> ", out)
print(df.group_by("market").agg(pl.len(), pl.col("date").n_unique().alias("days")))
print("bad files:", len(bad))
for b in bad[:20]:
    print("  ", b)
