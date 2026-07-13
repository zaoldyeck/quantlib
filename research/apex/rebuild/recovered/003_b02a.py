"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T00:19:05.788Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/b02_exits_and_scale.py)
涵蓋 trials(10):b02a_core, b02b_core_xr2, b02c_core_xr6, b02d_core_xr8, b02e_core_minhold10, b02f_core_trail35, b02g_hvn_core, b02h_4axis, b02i_core_n30, b02j_core_regime
"""
"""B02 — 出場消融 × 規模 × 合體 × regime(10 trials;預註冊見 ledger/batches.md)。

Run: uv run --project research python -m research.apex.experiments.b02_exits_and_scale
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from research.apex.engine import ExitSpec, PortSpec

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "B02"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)
C = "company_code"

TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
HVN = {"rev_yoy_accel": 1.0, "hvn_dist": 1.0, "close_pos_20": 1.0}
AX4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "frn_60": 1.0}
CFO_GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)

# regime:0050(TR NAV)在 200 日均線之上才准新進場
regime_ok = (
    bench.sort("date")
    .with_columns((pl.col("nav") > pl.col("nav").rolling_mean(200)).alias("ok"))
    .filter(pl.col("ok"))
    .select(["date"])
)
print(f"features ready in {time.time()-t0:.1f}s | regime-on days: {regime_ok.height}/{bench.height}\n")


def W(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def go(name, hypothesis, weights, *, require=None, topn=20, exit_rank_mult=4,
       min_hold=1, trailing=0.25, entry_date_filter: pl.DataFrame | None = None):
    sc = W(blend_score(feat, elig, weights, require=require))
    entries, flags = entries_and_flags(sc, topn, topn * exit_rank_mult)
    if entry_date_filter is not None:
        entries = entries.join(entry_date_filter, on="date", how="semi")
    return run_trial(
        name=name, hypothesis=hypothesis, family="assembly", batch=BATCH,
        panel=panel, entries=entries, exit_flags=flags, bench=bench,
        window=WINDOW, start=START,
        config={"weights": weights, "topn": topn, "exit_rank": topn * exit_rank_mult,
                "trailing": trailing, "min_hold": min_hold, "gate": str(require),
                "regime": entry_date_filter is not None},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=3, min_hold_days=min_hold),
        exit_spec=ExitSpec(trailing_stop=trailing),
        verbose=False,
    )


runs = [
    go("b02a_core", "e+f 合體:tri+cfo 閘+N20", TRI, require=CFO_GATE),
    go("b02b_core_xr2", "出場消融:exit_rank 2N(緊)", TRI, require=CFO_GATE, exit_rank_mult=2),
    go("b02c_core_xr6", "出場消融:exit_rank 6N(鬆)", TRI, require=CFO_GATE, exit_rank_mult=6),
    go("b02d_core_xr8", "出場消融:exit_rank 8N(最鬆)", TRI, require=CFO_GATE, exit_rank_mult=8),
    go("b02e_core_minhold10", "最短持有 10 日抑 churn", TRI, require=CFO_GATE, min_hold=10),
    go("b02f_core_trail35", "trailing 放寬 35%", TRI, require=CFO_GATE, trailing=0.35),
    go("b02g_hvn_core", "高油門 hvn 版 + 閘 + N20", HVN, require=CFO_GATE),
    go("b02h_4axis", "加第四軸外資流", AX4, require=CFO_GATE),
    go("b02i_core_n30", "N=30 分散", TRI, require=CFO_GATE, topn=30),
    go("b02j_core_regime", "0050>MA200 才准新進場", TRI, require=CFO_GATE,
       entry_date_filter=regime_ok),
]

cmp = pl.DataFrame(
    [
        {k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar",
                            "exposure", "n_trades", "turnover_ann", "excess_cagr"]}
        for r in runs
    ]
).sort("calmar", descending=True)
with pl.Config(tbl_rows=12, tbl_width_chars=140, fmt_float="mixed"):
    print(cmp)
print(f"\ntotal {time.time()-t0:.1f}s")

