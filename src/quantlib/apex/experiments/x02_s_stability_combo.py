"""X02 — S × 穩定書組合前緣(2026-07-21;使用者:「不優化 S,組合/從頭研發能否超越?」)。

「從頭研發」已由 F-LINE 制度性回答(3 年窗自由重研發 → 收斂回 S;年度 refit 監測
漂移)。本批補「組合」的新資產缺口:Q02 穩定書(gm_vol×ni_vol×cfo_ta,N20 純日曆;
Sharpe 1.01/MDD −27.7 防禦之王)是 X01 之後才誕生的**全自研成分**(滿足使用者 X01
裁示「組合成分必須全自研」)。量測:日報酬相關 + 靜態權重前緣(constant-mix 日再
平衡)+ 波動平價變體(參數自由,63d 反波動權重月更)。KPI v3(P5 主尺)裁決。

Run: uv run --project . python -m quantlib.apex.experiments.x02_s_stability_combo
依賴 cache:是。
"""
from __future__ import annotations

import time
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.strategy_s import prep as prep_s, run_s
from quantlib.apex.experiments.q02_pure_financial_books import geo_rank, run_book
from quantlib.evergreen.ev36_walkforward import kpis_full

C = "company_code"
W0 = "2015-07-02"   # 共同窗(S 特徵錨 2014-10 + 暖機;含 2020 崩/2022 熊/2025 關稅)


def rets(nav: pl.DataFrame) -> pl.DataFrame:
    return (nav.sort("date")
            .with_columns((pl.col("nav") / pl.col("nav").shift(1) - 1).alias("r"))
            .drop_nulls().select(["date", "r"]))


def nav_from(r: np.ndarray, dates) -> pl.DataFrame:
    return pl.DataFrame({"date": dates, "nav": np.cumprod(1 + r)})


def main() -> None:
    t0 = time.time()
    con = data.connect()
    de = data.latest_date(con).isoformat()

    # ── S(canonical 引擎)──────────────────────────────────────────────
    panel_s, feat_s, elig_s = prep_s(con)
    nav_s = run_s(panel_s, feat_s, elig_s, W0)
    print(f"S NAV 就緒({time.time()-t0:.0f}s)")

    # ── 穩定書(Q02 exact 配方:gm_vol×ni_vol×cfo_ta,N20 純日曆季頻)────
    panel = data.common_stocks(data.load_panel(con, "2006-06-01", de, warmup_days=420))
    elig_daily = (data.eligibility(panel, min_adv=5_000_000.0)
                  .filter(pl.col("eligible")).select(["date", C]))
    td = pl.DataFrame({"td": panel.select(pl.col("date").unique().sort())
                       .get_column("date")}).sort("td")
    pos = lambda c: pl.when(pl.col(c) > 0).then(pl.col(c))
    rq = (pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
          .sort([C, "year", "quarter"])
          .with_columns([
              (-(pl.col("ni_q").rolling_std(8).over(C)
                 / pos("total_assets"))).alias("ni_vol8_neg"),
              (-pl.col("gross_margin_q").rolling_std(8).over(C)).alias("gm_vol8_neg"),
              (pl.col("cfo_ttm") / pos("total_assets")).alias("cfo_ta"),
          ])
          .with_columns(
              pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
              .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
              .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
              .otherwise(pl.date(pl.col("year") + 1, 3, 31)).alias("deadline"))
          .sort("deadline")
          .join_asof(td, left_on="deadline", right_on="td", strategy="forward")
          .rename({"td": "date"}).drop_nulls(subset=["date"]))
    s_sc = geo_rank(rq.select(["date", C, "gm_vol8_neg", "ni_vol8_neg", "cfo_ta"]),
                    ["gm_vol8_neg", "ni_vol8_neg", "cfo_ta"])
    nav_b = run_book(panel, elig_daily, s_sc, 20, None, W0)
    print(f"穩定書 NAV 就緒({time.time()-t0:.0f}s)")

    # ── 對齊 + 相關 ─────────────────────────────────────────────────────
    j = (rets(nav_s).rename({"r": "rs"})
         .join(rets(nav_b).rename({"r": "rb"}), on="date", how="inner")
         .sort("date"))
    dates = j["date"].to_list()
    rs, rb = j["rs"].to_numpy(), j["rb"].to_numpy()
    corr = float(np.corrcoef(rs, rb)[0, 1])
    print(f"共同窗 {dates[0]} → {dates[-1]}({len(dates)} 日);日報酬相關 = {corr:.2f}\n")

    rows = []
    for w in [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.0]:
        k = kpis_full(nav_from(w * rs + (1 - w) * rb, dates))
        rows.append({"配置": f"S {int(w*100)}/穩定 {int((1-w)*100)}", **k})
    # 波動平價(參數自由):63d 反波動權重,月首更新
    vol_s = pl.Series(rs).rolling_std(63).to_numpy()
    vol_b = pl.Series(rb).rolling_std(63).to_numpy()
    w_arr = np.full(len(rs), 0.5)
    cur = 0.5
    for i, d in enumerate(dates):
        if i > 63 and (i == 64 or d.month != dates[i - 1].month):
            iv_s, iv_b = 1 / max(vol_s[i - 1], 1e-9), 1 / max(vol_b[i - 1], 1e-9)
            cur = iv_s / (iv_s + iv_b)
        w_arr[i] = cur
    k = kpis_full(nav_from(w_arr * rs + (1 - w_arr) * rb, dates))
    rows.append({"配置": f"波動平價(均值 w_S={w_arr.mean():.0%})", **k})

    print(f"{'配置':<22s} {'CAGR':>8s} {'P5':>7s} {'MDD':>7s} {'Martin':>7s}")
    for r in rows:
        print(f"{r['配置']:<22s} {r['cagr']:>8.1%} {r['p5']:>7.1%} "
              f"{r['mdd']:>7.1%} {r['martin']:>7.1f}")
    pl.DataFrame([{**r} for r in rows]).write_parquet(
        "src/quantlib/apex/ledger/x02_s_stability_combo.parquet")
    print(f"\ntotal {time.time()-t0:.0f}s → ledger/x02_s_stability_combo.parquet")


if __name__ == "__main__":
    main()
