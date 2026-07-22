"""池品質對決:Serenity 標記池 vs Evergreen 標記池(純池,零引擎).

預註冊見 trials ledger「池品質對決」段(2026-07-16)。同窗 2022-08~2026-07、
標記月 +1 月生效、月頻等權、canonical 調整價格;主尺 = 月報酬 block bootstrap
CAGR 5% 下界,輔尺 Sortino/MDD/對 0050 月勝率。

Run(TPEx 走 pg-attach,約 1-3 分鐘):
    uv run --project research python -m research.serenity.backfill.pool_quality_duel
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "research"))

from research.db import connect  # noqa: E402
from research.prices import fetch_adjusted_panel  # noqa: E402

REG_DIR = REPO_ROOT / "research" / "serenity" / "registry"
EV_REG = REPO_ROOT / "research" / "evergreen" / "data" / "registry_v3.parquet"
START_M, END_M = "2022-08", "2026-07"
EV_POOL_MONTHS = 3
BOOT_B, BOOT_BLOCK = 2000, 6


def month_range(a: str, b: str) -> list[str]:
    out, (y, m) = [], map(int, a.split("-"))
    while f"{y}-{m:02d}" <= b:
        out.append(f"{y}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


MONTHS = month_range(START_M, END_M)


def serenity_pools(strip_beneficiary_2025: bool = False) -> dict[str, set[str]]:
    back = pd.read_csv(REG_DIR / "backcast_2022_2024.csv", dtype=str).fillna("")
    live = pd.read_csv(REG_DIR / "thesis_registry_2025.csv", dtype=str).fillna("")
    if strip_beneficiary_2025:
        roles = pd.read_csv(REG_DIR / "member_roles.csv", dtype={"company_code": str})
        bene = set(roles.loc[roles.role == "beneficiary", "company_code"])
        live = live[~live.company_code.isin(bene)]
    reg = pd.concat([back, live], ignore_index=True)
    pools: dict[str, set[str]] = {}
    for m in MONTHS:
        active = reg[
            (reg.active_from.str[:7] < m)
            & ((reg.active_until == "") | (reg.active_until.str[:7] >= m))
        ]
        pools[m] = set(active.company_code)
    return pools


def evergreen_pools(pool_months: int = EV_POOL_MONTHS) -> dict[str, set[str]]:
    reg = pl.read_parquet(EV_REG).to_pandas()
    by_month = reg.groupby("month")["code"].apply(set).to_dict()
    marked = sorted(by_month)
    pools: dict[str, set[str]] = {}
    for m in MONTHS:
        recent = [x for x in marked if x < m][-pool_months:]
        pools[m] = set().union(*(by_month[x] for x in recent)) if recent else set()
    return pools


def monthly_returns(all_codes: set[str]) -> pd.DataFrame:
    """月末調整收盤 → 月報酬(index=YYYY-MM, columns=code)。cache 為全市場
    (2026-07-16 查證:TWSE+TPEx 皆本地化),兩市場都走 cache。"""
    codes = sorted(all_codes)
    frames = []
    con = connect()
    try:
        for market in ("twse", "tpex"):
            frames.append(fetch_adjusted_panel(con, "2022-06-01", "2026-07-16", codes=codes,
                                               market=market, include_extra_history_days=0).to_pandas())
    finally:
        con.close()
    px = pd.concat(frames, ignore_index=True)
    px["ym"] = px["date"].astype(str).str[:7]
    eom = px.sort_values("date").groupby(["company_code", "ym"])["close"].last().unstack(0)
    return eom.pct_change()


def pool_series(pools: dict[str, set[str]], rets: pd.DataFrame) -> tuple[pd.Series, dict]:
    rows, sizes, missing, turn = {}, [], 0, []
    prev: set[str] = set()
    for m in MONTHS:
        members = pools[m]
        have = [c for c in members if c in rets.columns and pd.notna(rets.loc[m, c])] if m in rets.index else []
        missing += len(members) - len(have)
        rows[m] = float(np.mean([rets.loc[m, c] for c in have])) if have else 0.0
        sizes.append(len(have))
        if prev or members:
            union = len(prev | members) or 1
            turn.append(len(prev ^ members) / union)
        prev = members
    ser = pd.Series(rows)
    return ser, {"avg_size": np.mean(sizes), "missing_member_months": missing,
                 "avg_turnover": np.mean(turn)}


def metrics(r: pd.Series) -> dict:
    nav = (1 + r).cumprod()
    yrs = len(r) / 12
    cagr = nav.iloc[-1] ** (1 / yrs) - 1
    downside = r[r < 0].std(ddof=0) or np.nan
    sortino = (r.mean() * 12) / (downside * np.sqrt(12)) if downside else np.nan
    mdd = (nav / nav.cummax() - 1).min()
    return {"cagr": cagr, "sortino": sortino, "mdd": mdd}


def boot_cagr_lb(r: pd.Series, rng: np.random.Generator) -> tuple[float, float, float]:
    """Circular block bootstrap → CAGR 的 5%/50%/95% 分位。"""
    x, n = r.to_numpy(), len(r)
    out = []
    for _ in range(BOOT_B):
        idx, pos = [], int(rng.integers(n))
        while len(idx) < n:
            pos = int(rng.integers(n))
            idx.extend(range(pos, pos + BOOT_BLOCK))
        samp = x[np.array(idx[:n]) % n]
        out.append(float(np.prod(1 + samp) ** (12 / n) - 1))
    return tuple(np.percentile(out, [5, 50, 95]))


def report_arm(name: str, r: pd.Series, extra: dict, bench: pd.Series,
               rng: np.random.Generator, seg: slice = slice(None)) -> dict:
    rr, bb = r.iloc[seg], bench.iloc[seg]
    m = metrics(rr)
    p5, p50, p95 = boot_cagr_lb(rr, rng)
    return {"arm": name, "months": len(rr), **{k: round(v, 3) for k, v in m.items()},
            "boot_cagr_p5": round(p5, 3), "boot_p50": round(p50, 3), "boot_p95": round(p95, 3),
            "win_vs_0050": round(float((rr > bb).mean()), 2),
            "avg_size": round(extra["avg_size"], 1), "avg_turnover": round(extra["avg_turnover"], 2)}


def main() -> None:
    rng = np.random.default_rng(20260716)
    ser_pools = serenity_pools()
    ev_pools = evergreen_pools()
    ser_pure = serenity_pools(strip_beneficiary_2025=True)
    ev_p1, ev_p6 = evergreen_pools(1), evergreen_pools(6)

    all_codes = set().union(*ser_pools.values(), *ev_pools.values(), *ev_p6.values(), {"0050"})
    rets = monthly_returns(all_codes).loc[MONTHS]
    bench = rets["0050"]

    arms = {
        "Serenity(PIT 原樣)": ser_pools, "Evergreen(池籍 3 月)": ev_pools,
        "Serenity(2025 段剔 beneficiary)": ser_pure,
        "Evergreen(池籍 1 月)": ev_p1, "Evergreen(池籍 6 月)": ev_p6,
    }
    seg_map = {"全窗 2022-08~2026-07": slice(None),
               "回溯段 2022-08~2024-12": slice(0, MONTHS.index("2024-12") + 1),
               "近段 2025-01~2026-07": slice(MONTHS.index("2025-01"), None)}

    lines = ["# 池品質對決 — Serenity vs Evergreen(純標記池,零引擎)", "",
             f"月頻等權、標記月 +1 月生效、canonical 調整價;bootstrap B={BOOT_B} block={BOOT_BLOCK}。", ""]
    for seg_name, seg in seg_map.items():
        rows = []
        for name, pools in arms.items():
            r, extra = pool_series(pools, rets)
            rows.append(report_arm(name, r, extra, bench, rng, seg))
        b = metrics(bench.iloc[seg])
        lines += [f"## {seg_name}(0050 同窗:CAGR {b['cagr']:.1%} / MDD {b['mdd']:.1%})", "",
                  pd.DataFrame(rows).to_markdown(index=False), ""]
    out = Path(__file__).parent / "pool_quality_duel_report.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"report -> {out}")


if __name__ == "__main__":
    main()
