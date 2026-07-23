"""M01 meta-study:研發窗長 vs 未來 OOS 表現。

問題:要研發「未來最強」的策略,應該用過去多久的資料?
方法:24-config 網格代理研發自由度,各跑一次全期連續模擬(2012-07 → 2026-07);
站在每年年初 t,用過去 W 年的窗內 KPI 選最優 config,量其未來 1 年 OOS
(排名百分位 + 年化報酬)。預註冊見 ledger/batches.md M01。

Run: uv run --project . python -m quantlib.apex.experiments.m01_window_length
"""
from __future__ import annotations

import itertools
import time
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
PREP_START, END = "2012-01-01", "2026-07-09"
SIM_START = "2012-07-02"                     # 半年特徵暖機後起跑
AX4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}
AX6 = AX4 | {"rev_seq": 0.5, "accel_rel": 0.5}
GRID = [
    {"axes": ax, "n": n, "trail": tr, "adv": adv}
    for ax, n, tr, adv in itertools.product(
        ["ax4", "ax6"], [5, 8, 20], [0.25, 0.35], [5e6, 20e6])
]
T_POINTS = [Date(y, 1, 1) for y in range(2015, 2027)]
WINDOWS = [1, 2, 3, 5, 8, None]              # None = 全部可用歷史


def prep(con, prep_start: str = PREP_START, end: str = END):
    panel, feat, _ = build_features(con, prep_start, end)
    rev = (data.load_monthly_revenue(con, end)
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue").rolling_sum(3)
                / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
               .over(C).alias("rev_seq"),
           ])
           .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance="70d")
            .sort([C, "date"]))
    tax = con.sql(
        "SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
        "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
    fx = (feat.select(["date", C, "rev_yoy_accel"]).drop_nulls().sort("date")
          .join_asof(tax.sort("effective_date"), left_on="date",
                     right_on="effective_date", by=C, strategy="backward")
          .drop_nulls(subset=["industry"]))
    ind_med = fx.group_by(["date", "industry"]).agg(
        pl.col("rev_yoy_accel").median().alias("m"))
    rel = (fx.join(ind_med, on=["date", "industry"], how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("m")).alias("accel_rel"))
           .select(["date", C, "accel_rel"]))
    return panel, feat.join(rel, on=["date", C], how="left")


def run_config(panel, feat, elig_map, cfg, sim_start: str = SIM_START) -> pl.DataFrame:
    wts = AX4 if cfg["axes"] == "ax4" else AX6
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(elig_map[cfg["adv"]], on=["date", C], how="semi")
          .drop_nulls(subset=list(wts))
          .filter(pl.col("cfo_ni_ratio_ttm")
                  >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    expr = None
    for c_, wt in wts.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = (df.with_columns(expr.alias("score"))
          .select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(sim_start).str.to_date()))
    entries, _ = entries_and_flags(sc, cfg["n"], 10**9)
    stale = (feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C])
             .filter(pl.col("date") >= pl.lit(sim_start).str.to_date()))
    res = simulate(panel, entries, exit_flags=stale, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=cfg["n"], max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=cfg["trail"], time_stop=30),
                   start=Date.fromisoformat(sim_start))
    return res.nav.select(["date", "nav"]).sort("date")


def seg(nav: pl.DataFrame, a: Date, b: Date) -> dict | None:
    """[a, b) 切片的 CAGR 與 Sharpe(連續模擬近似)。"""
    s = nav.filter((pl.col("date") >= a) & (pl.col("date") < b))
    if len(s) < 60:
        return None
    v = s["nav"].to_numpy()
    r = v[1:] / v[:-1] - 1
    yrs = (s["date"][-1] - s["date"][0]).days / 365.25
    return {"cagr": (v[-1] / v[0]) ** (1 / yrs) - 1,
            "sharpe": r.mean() / r.std() * np.sqrt(252) if r.std() > 0 else 0.0}


def main() -> None:
    t0 = time.time()
    con = data.connect()
    panel, feat = prep(con)
    elig_map = {
        adv: (data.eligibility(panel, min_adv=adv)
              .filter(pl.col("eligible")).select(["date", C]))
        for adv in [5e6, 20e6]
    }
    print(f"prep {time.time()-t0:.0f}s;跑 {len(GRID)} configs 全期模擬…")
    navs: list[pl.DataFrame] = []
    for i, cfg in enumerate(GRID):
        navs.append(run_config(panel, feat, elig_map, cfg))
        print(f"  [{i+1:2d}/{len(GRID)}] {cfg['axes']}-n{cfg['n']}"
              f"-t{int(cfg['trail']*100)}-adv{int(cfg['adv']/1e6)}M"
              f"  全期 {navs[-1]['nav'][-1]/navs[-1]['nav'][0]:,.0f}x"
              f"  ({time.time()-t0:.0f}s)")

    data_end = navs[0]["date"][-1]
    rows = []
    for t in T_POINTS:
        oos_end = min(Date(t.year + 1, 1, 1), data_end)
        oos = [seg(nv, t, oos_end) for nv in navs]
        if any(o is None for o in oos):
            continue
        oos_cagr = np.array([o["cagr"] for o in oos])
        ranks = oos_cagr.argsort().argsort() / (len(GRID) - 1)  # 0..1 百分位
        for W in WINDOWS:
            tr_a = (Date.fromisoformat(SIM_START) if W is None
                    else Date(t.year - W, t.month, t.day))
            if tr_a < Date.fromisoformat(SIM_START):
                continue
            tr = [seg(nv, tr_a, t) for nv in navs]
            if any(x is None for x in tr):
                continue
            for kpi in ["sharpe", "cagr"]:
                pick = int(np.argmax([x[kpi] for x in tr]))
                rows.append({
                    "t": t.year, "W": "all" if W is None else str(W), "kpi": kpi,
                    "pick": f"{GRID[pick]['axes']}-n{GRID[pick]['n']}"
                            f"-t{int(GRID[pick]['trail']*100)}"
                            f"-adv{int(GRID[pick]['adv']/1e6)}",
                    "oos_cagr": float(oos_cagr[pick]),
                    "oos_rank": float(ranks[pick]),
                })
    res = pl.DataFrame(rows)
    res.write_parquet("src/quantlib/apex/ledger/m01_results.parquet")

    for kpi in ["sharpe", "cagr"]:
        sub = res.filter(pl.col("kpi") == kpi)
        agg = (sub.group_by("W")
               .agg([
                   pl.col("oos_rank").mean().round(3).alias("mean_rank"),
                   pl.col("oos_rank").median().round(3).alias("med_rank"),
                   ((1 + pl.col("oos_cagr")).log().mean().exp() - 1)
                   .round(3).alias("geo_oos_cagr"),
                   pl.len().alias("n"),
               ])
               .sort("mean_rank", descending=True))
        print(f"\n=== 選擇 KPI = {kpi}(隨機期望 rank 0.5)===")
        print(agg)
    picks = (res.group_by(["W", "kpi"]).agg(pl.col("pick").mode().first())
             .sort(["kpi", "W"]))
    print("\n各窗長最常選中的 config:")
    print(picks)
    print(f"\ntotal {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
