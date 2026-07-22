"""ev28 PIT pricing-state + revenue-momentum snapshot for qualitative labelling.

For a given as-of date (PIT cutoff) and a candidate {code: name} map, prints per
stock: as-of close, 20d/60d return, distance from 60d high (over-heat check),
pre-window high -> recent low -> as-of recovery (washout / 錯殺 depth), and the
last 4 published monthly-revenue YoY prints (自身催化 vs 背離 check).

Everything is strictly PIT: daily_quote filtered to date <= asof; monthly revenue
filtered to prints whose publish date (~10th of following month) <= asof. The
revenue cutoff is DERIVED from asof (do NOT hardcode a month — that leaks future
prints when the tool is reused for an earlier as-of date).

Depends on: var/cache/cache.duckdb being current (has history; we only read <= asof).
Run (defaults to the 2024-08 map):
    uv run --project research python -m research.evergreen.pit_snapshot
Run for another month / tickers:
    uv run --project research python -m research.evergreen.pit_snapshot 2024-02-15 2408 2344 3260 ...
"""
from __future__ import annotations
import sys
import duckdb
from datetime import date
from research import paths

ASOF = date(2024, 8, 12)
DB = f"{paths.CACHE_DB}"

# candidate universe spanning the live 2024-08 theme map (disciplined down later)
CANDS: dict[str, str] = {
    # 先進封裝 / CoWoS 設備 (瓶頸上游)
    "3131": "弘塑", "3583": "辛耘", "2467": "志聖", "5443": "均豪",
    "6187": "萬潤", "3680": "家登", "3413": "京鼎", "6196": "帆宣",
    "6640": "均華", "3131b": "",
    # 測試介面 / probe card
    "6510": "中華精測", "6515": "穎崴", "6223": "旺矽",
    # 散熱
    "3017": "奇鋐", "3324": "雙鴻", "8996": "高力", "6230": "尼得科超眾",
    # 重電 / 電網
    "1519": "華城", "1503": "士電", "1513": "中興電", "1514": "亞力", "2371": "大同",
    # 記憶體
    "8299": "群聯", "2408": "南亞科", "2344": "華邦電", "4967": "十銓", "3260": "威剛",
    # AI PCB / CCL
    "2383": "台光電", "6274": "台燿", "6213": "聯茂", "2368": "金像電", "3037": "欣興",
    # 矽光子 / CPO (early check)
    "3081": "聯亞", "4977": "眾達", "3363": "上詮", "6442": "光聖", "4979": "華星光",
    # 軍工 / 低軌衛星
    "2634": "漢翔", "8033": "雷虎", "6753": "龍德造船", "3491": "昇達科", "2314": "台揚",
    # 參考大廠
    "2330": "台積電", "3711": "日月光",
}


def fmt(x, nd=1, suf="%"):
    return "n/a" if x is None else f"{x*100:.{nd}f}{suf}" if suf == "%" else f"{x:.{nd}f}"


def rev_cutoff(asof: date) -> tuple[int, int]:
    """Latest (year, month) whose monthly-revenue publish date (~(m+1)/10) <= asof."""
    y, m = asof.year, asof.month
    # walk back until the (y,m) print's publish date is on/before asof
    while True:
        py, pm = (y + 1, 1) if m == 12 else (y, m + 1)
        if date(py, pm, 10) <= asof:
            return y, m
        # step one month earlier
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)


def main(asof: date = ASOF, cands: dict[str, str] = CANDS):
    con = duckdb.connect(DB, read_only=True)
    cy, cm = rev_cutoff(asof)
    codes = [c for c in cands if not c.endswith("b")]
    rows = []
    for code in codes:
        name = cands[code]
        px = con.execute(
            """
            select date, closing_price
            from daily_quote
            where company_code = ? and date <= ?
            order by date
            """,
            [code, asof],
        ).fetchall()
        if len(px) < 70:
            rows.append((code, name, "NO/THIN PRICE DATA", "", "", "", "", ""))
            continue
        closes = [p[1] for p in px if p[1] is not None]
        asof_c = closes[-1]
        c20 = closes[-21]
        c60 = closes[-61]
        win60 = closes[-60:]
        hi60 = max(win60)
        # pre-window high over ~120d before asof, and the min after that high (crash low)
        win120 = closes[-120:]
        hi120 = max(win120)
        hi_idx = len(win120) - 1 - win120[::-1].index(hi120)
        post = win120[hi_idx:]
        low_since_hi = min(post)
        r20 = asof_c / c20 - 1
        r60 = asof_c / c60 - 1
        dist_hi60 = asof_c / hi60 - 1
        dd_from_hi120 = low_since_hi / hi120 - 1     # depth of the sell-off
        recov = asof_c / low_since_hi - 1            # bounce off the low so far
        # monthly revenue: prints published <= asof (derived cutoff, not hardcoded)
        rev = con.execute(
            """
            select year, month, monthly_revenue_yoy
            from operating_revenue
            where company_code = ?
              and (year < ? or (year = ? and month <= ?))
            order by year desc, month desc
            limit 4
            """,
            [code, cy, cy, cm],
        ).fetchall()
        rev_s = " ".join(
            f"{y%100:02d}/{m}:{(yoy if yoy is not None else float('nan')):+.0f}%"
            for (y, m, yoy) in rev
        ) if rev else "no rev"
        rows.append((
            code, name,
            f"c={asof_c:.1f}",
            f"20d={fmt(r20)}",
            f"60d={fmt(r60)}",
            f"d.hi60={fmt(dist_hi60)}",
            f"dd={fmt(dd_from_hi120)} rec={fmt(recov)}",
            rev_s,
        ))
    # print grouped
    print(f"PIT as-of {asof}  rev-cutoff<={cy}-{cm:02d}  (60d=~3mo mom; dd=high120->low; rec=low->asof)")
    print("=" * 130)
    for r in rows:
        print(f"{r[0]:>5} {r[1]:<7} {r[2]:>9} {r[3]:>11} {r[4]:>12} {r[5]:>13} {r[6]:>22}  rev[{r[7]}]")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        asof = date.fromisoformat(sys.argv[1])
        if len(sys.argv) > 2:
            cands = {t: "" for t in sys.argv[2:]}
        else:
            cands = CANDS
        main(asof, cands)
    else:
        main()
