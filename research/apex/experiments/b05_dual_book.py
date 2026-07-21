"""B05 — 雙書結構(8 trials;預註冊見 ledger/batches.md)。

兩書獨立模擬,日報酬常數比例混合(近似日再平衡的 constant-mix)。
Run: uv run --project research python -m research.apex.experiments.b05_dual_book
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from research.apex import data, ledger, metrics
from research.apex.assemble import blend_score, build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "B05"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)
C = "company_code"

TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
DEF = {"dy": 1.0, "lowvol_60": 1.0, "cfo_ni_ratio_ttm": 1.0}

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)

month_last = (
    panel.select(pl.col("date").unique().sort())
    .group_by_dynamic("date", every="1mo")
    .agg(pl.col("date").last().alias("d"))
    .get_column("d")
)


def W(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def monthly(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("date").is_in(month_last.implode()))


def sim(entries, flags, *, topn, trailing=0.35):
    return simulate(
        panel, entries, exit_flags=flags,
        exec_spec=ExecSpec(), port_spec=PortSpec(n_slots=topn, max_new_per_day=3),
        exit_spec=ExitSpec(trailing_stop=trailing), start=START,
    )


# ── 主書(b02f 凍結)────────────────────────────────────────────────────
sc_main = W(blend_score(feat, elig, TRI, require=GATE))
e_main, f_main = entries_and_flags(sc_main, 20, 80)
res_main = sim(e_main, f_main, topn=20)

# ── 防禦書:tilt 因子、月頻、N=30、trailing 25% ──────────────────────────
sc_def = monthly(W(blend_score(feat, elig, DEF)))
e_def, f_def = entries_and_flags(sc_def, 30, 120)
res_def = sim(e_def, f_def, topn=30, trailing=0.25)

# ── mega 書:ADV top-50 內 12 月動能 top-3、月頻 ─────────────────────────
adv = data.eligibility(panel).select(["date", C, "adv20"])
mom = (
    panel.sort([C, "date"])
    .with_columns(
        (pl.col("close").shift(21) / pl.col("close").shift(252) - 1).over(C).alias("m")
    )
    .select(["date", C, "m"])
)
mega = (
    monthly(W(mom))
    .join(adv, on=["date", C], how="inner")
    .drop_nulls()
    .with_columns(pl.col("adv20").rank("ordinal", descending=True).over("date").alias("liq_rk"))
    .filter(pl.col("liq_rk") <= 50)
    .select(["date", C, pl.col("m").alias("score")])
)
e_mega, f_mega = entries_and_flags(mega, 3, 12)
res_mega = sim(e_mega, f_mega, topn=3, trailing=0.25)


def blend_navs(a: pl.DataFrame, b: pl.DataFrame, wa: float) -> pl.DataFrame:
    """日報酬常數比例混合 → 合成 NAV(date, nav)。"""
    j = (
        a.select(["date", pl.col("nav").alias("na")])
        .join(b.select(["date", pl.col("nav").alias("nb")]), on="date", how="inner")
        .sort("date")
        .with_columns(
            [
                (pl.col("na") / pl.col("na").shift(1) - 1).fill_null(0.0).alias("ra"),
                (pl.col("nb") / pl.col("nb").shift(1) - 1).fill_null(0.0).alias("rb"),
            ]
        )
        .with_columns(
            ((1 + wa * pl.col("ra") + (1 - wa) * pl.col("rb")).cum_prod()).alias("nav")
        )
    )
    return j.select(["date", "nav"])


def log_book(name, hypothesis, res, config, trades=None):
    summ = metrics.summarize(res.nav, res.trades if trades is None else trades, bench)
    tid = ledger.log_trial(family="dual_book", name=name, hypothesis=hypothesis,
                           config=config, window=WINDOW, metrics=summ, batch=BATCH,
                           curve=res.nav)
    yt = metrics.yearly_table(res.nav)
    y22 = float(yt.filter(pl.col("year") == 2022)["ret"][0])
    return {"trial_id": tid, "name": name, **summ, "y2022": y22}


def log_blend(name, hypothesis, nav, config):
    empty = pl.DataFrame(schema={"company_code": pl.Utf8, "entry_date": pl.Date,
                                 "exit_date": pl.Date, "entry_px": pl.Float64,
                                 "exit_px": pl.Float64, "cost": pl.Float64,
                                 "ret_net": pl.Float64, "days_held": pl.Int32,
                                 "exit_reason": pl.Utf8})
    summ = metrics.perf_stats(nav)
    b = bench.sort("date")
    yrs = summ["years"]
    summ["bench_cagr"] = float((b["nav"][-1] / b["nav"][0]) ** (1 / yrs) - 1)
    summ["excess_cagr"] = summ["cagr"] - summ["bench_cagr"]
    tid = ledger.log_trial(family="dual_book", name=name, hypothesis=hypothesis,
                           config=config, window=WINDOW, metrics=summ, batch=BATCH,
                           curve=nav)
    yt = metrics.yearly_table(nav)
    y22 = float(yt.filter(pl.col("year") == 2022)["ret"][0])
    return {"trial_id": tid, "name": name, **summ, "y2022": y22}


runs = [
    log_book("b05a_def_solo", "防禦書 solo", res_def, {"book": "def", "n": 30}),
    log_book("b05e_mega_solo", "mega 書 solo", res_mega, {"book": "mega", "n": 3}),
]
for nm, wa, other, tag in [
    ("b05b_main_def_8020", 0.8, res_def.nav, "def"),
    ("b05c_main_def_7030", 0.7, res_def.nav, "def"),
    ("b05d_main_def_6040", 0.6, res_def.nav, "def"),
    ("b05f_main_mega_8020", 0.8, res_mega.nav, "mega"),
    ("b05g_main_mega_7030", 0.7, res_mega.nav, "mega"),
    ("b05h_main_bench_7030", 0.7, bench, "0050"),
]:
    nav = blend_navs(res_main.nav, other, wa)
    runs.append(log_blend(nm, f"主書 {wa:.0%} ⊕ {tag} {1-wa:.0%}", nav,
                          {"blend": tag, "w_main": wa}))

cmp = pl.DataFrame(
    [{k: r.get(k) for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar", "y2022"]} for r in runs]
).sort("sharpe", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=120):
    print(cmp)
print(f"\nbaseline b02f: cagr 31.2% sharpe 1.58 mdd -29.2% y2022 ?(補查)")
print(f"total {time.time()-t0:.1f}s")
