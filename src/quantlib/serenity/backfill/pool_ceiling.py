"""池天花板測量:Serenity 加引擎有無機會勝過 Evergreen(預註冊見 trials ledger).

兩把尺、兩池同量(月頻,2022-08~2026-07):
- oracle top-K:每月以當月實現報酬事後選 K 檔(先知上界,不可實現,量池內分散度)
- momentum top-K:每月以前月末 60 交易日報酬選 K 檔(PIT、零優化的現成排序代理)
另量兩池等權相關性與 50/50 blend(互補性)。

Run: uv run --project . python -m quantlib.serenity.backfill.pool_ceiling
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = paths.REPO
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src" / "quantlib"))

from quantlib.db import connect  # noqa: E402
from quantlib.prices import fetch_adjusted_panel  # noqa: E402

from quantlib.serenity.backfill.pool_quality_duel import (  # noqa: E402
from quantlib import paths
    MONTHS, boot_cagr_lb, evergreen_pools, metrics, serenity_pools,
)


def panels(all_codes: set[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """月報酬矩陣 + 前月末動能(60 交易日報酬)矩陣,index=YYYY-MM。"""
    codes = sorted(all_codes)
    frames = []
    con = connect()
    try:
        for market in ("twse", "tpex"):
            frames.append(
                fetch_adjusted_panel(con, "2022-01-01", "2026-07-16", codes=codes,
                                     market=market, include_extra_history_days=180).to_pandas())
    finally:
        con.close()
    px = pd.concat(frames, ignore_index=True).sort_values("date")
    wide = px.pivot_table(index="date", columns="company_code", values="close")
    mom_daily = wide / wide.shift(60) - 1
    ym = wide.index.astype(str).str.slice(0, 7)
    eom_close = wide.groupby(ym).last()
    rets = eom_close.pct_change(fill_method=None)
    mom_eom = mom_daily.groupby(ym).last().shift(1)  # 前月末動能(PIT)
    return rets.loc[[m for m in MONTHS if m in rets.index]], mom_eom


def series_topk(pools: dict, rets: pd.DataFrame, k: int,
                mom: pd.DataFrame | None = None) -> pd.Series:
    """mom=None → oracle(當月實現報酬選 K);否則以前月末動能選 K。"""
    out = {}
    for m in MONTHS:
        cands = [c for c in pools[m] if c in rets.columns and pd.notna(rets.loc[m, c])]
        if not cands:
            out[m] = 0.0
            continue
        if mom is None:
            picks = sorted(cands, key=lambda c: rets.loc[m, c], reverse=True)[:k]
        else:
            scored = [c for c in cands if c in mom.columns and pd.notna(mom.loc[m, c])]
            picks = sorted(scored or cands,
                           key=lambda c: mom.loc[m, c] if c in mom.columns and pd.notna(mom.loc[m, c]) else -9,
                           reverse=True)[:k]
        out[m] = float(np.mean([rets.loc[m, c] for c in picks]))
    return pd.Series(out)


def equal_series(pools: dict, rets: pd.DataFrame) -> pd.Series:
    return series_topk(pools, rets, k=10_000)


def row(name: str, r: pd.Series, rng) -> dict:
    m = metrics(r)
    p5, p50, p95 = boot_cagr_lb(r, rng)
    return {"arm": name, **{k: round(v, 3) for k, v in m.items()},
            "boot_p5": round(p5, 3), "boot_p50": round(p50, 3)}


def main() -> None:
    rng = np.random.default_rng(20260716)
    ser, eg = serenity_pools(), evergreen_pools()
    all_codes = set().union(*ser.values(), *eg.values(), {"0050"})
    rets, mom = panels(all_codes)

    ser_eq, eg_eq = equal_series(ser, rets), equal_series(eg, rets)
    rows = []
    for name, pools, eq in (("Serenity", ser, ser_eq), ("Evergreen", eg, eg_eq)):
        rows.append(row(f"{name} 等權(現況)", eq, rng))
        for k in (5, 10):
            rows.append(row(f"{name} momentum top-{k}(現成排序)", series_topk(pools, rets, k, mom), rng))
        for k in (5, 10):
            rows.append(row(f"{name} ORACLE top-{k}(先知上界)", series_topk(pools, rets, k), rng))
    corr = float(ser_eq.corr(eg_eq))
    blend = 0.5 * ser_eq + 0.5 * eg_eq
    rows.append(row("50/50 blend(等權池)", blend, rng))

    df = pd.DataFrame(rows)
    lines = ["# 池天花板測量(月頻,2022-08~2026-07)", "",
             df.to_markdown(index=False), "",
             f"- 兩池等權月報酬相關性:**{corr:.2f}**", ""]
    # 預註冊判準機械裁決
    ser_o10 = df.loc[df.arm.str.startswith("Serenity ORACLE top-10"), "cagr"].iloc[0]
    ser_m10 = df.loc[df.arm.str.startswith("Serenity momentum top-10"), "cagr"].iloc[0]
    eg_eq_c = df.loc[df.arm == "Evergreen 等權(現況)", "cagr"].iloc[0]
    if ser_o10 <= eg_eq_c:
        verdict = "無望:Serenity oracle top-10 ≤ Evergreen 等權——池內好料不足,引擎優化省下"
    elif ser_m10 >= eg_eq_c:
        verdict = "有據:Serenity 現成動能 top-10 ≥ Evergreen 等權——零優化即追平,引擎投資有據"
    else:
        verdict = ("中間帶:上限存在(oracle 超過)但現成排序追不上——引擎投資屬高風險高報酬,"
                   "需要比動能更聰明的排序才兌現")
    lines.append(f"**預註冊判準裁決:{verdict}**")
    out = Path(__file__).parent / "pool_ceiling_report.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"\nreport -> {out}")


if __name__ == "__main__":
    main()
