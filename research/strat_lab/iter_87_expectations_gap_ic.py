"""iter_87:預期差因子 IC 研究(戰役十前置)。

因子構想(使用者提案,2026-07-07):月營收 nowcast 成長率 vs 市場隱含成長率
(固定 (r, N) 慣例下 PE 的單調變換)之差——「營收已爆炸、市場還沒重新定價」。

需要 cache_tables.py 為最新(讀 cache.duckdb:daily_quote / operating_revenue /
stock_per_pbr;經 research.prices 取調整後價)。

設計:
- 截面:每月,訊號日 = 營收月 M 之次月 12 日當日或之後首個交易日(PIT 保守滯後)
- g_hat(nowcast)= 3 月營收 YoY(M−2..M vs 去年同期)
- g_imp(隱含)= 兩段式 DCF 反解(r=9%, N=10, gt=2.5%, payout 100%)之 PE 單調變換
- 因子:gap_rank = rank(g_hat) − rank(g_imp);gap_card = g_hat_1y − g_imp(cardinal)
- 對照:yoy_3m 單獨、−PE 單獨、古典 PEG;邊際檢定 = gap 對 yoy_3m 殘差化後的 IC
- forward 60 交易日報酬(調整後收盤);過濾:ADV20 ≥ 5,000 萬、價 ≥ 20、0 < PE ≤ 250
- 視窗:2019-01 至 2026-03(最後截面需完整 60d forward)
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from research.prices import fetch_adjusted_panel  # noqa: E402

R, N_YEARS, GT = 0.09, 10, 0.025
START, END = "2019-01-01", "2026-07-06"
LAST_SECTION = "2026-03"  # 最後一個營收月(需完整 forward 窗)
FWD_DAYS = 60


def implied_g_from_pe(pe_grid: np.ndarray) -> np.ndarray:
    """PE → 隱含成長率 g(兩段式 DCF,E0=1,P=PE)。向量化二分。"""
    lo = np.full_like(pe_grid, -0.9, dtype=float)
    hi = np.full_like(pe_grid, 3.0, dtype=float)
    for _ in range(60):
        mid = (lo + hi) / 2
        t = np.arange(1, N_YEARS + 1)
        pv = ((1 + mid[:, None]) ** t / (1 + R) ** t).sum(axis=1)
        tv = (1 + mid) ** N_YEARS * (1 + GT) / (R - GT) / (1 + R) ** N_YEARS
        too_low = (pv + tv) < pe_grid
        lo = np.where(too_low, mid, lo)
        hi = np.where(too_low, hi, mid)
    return (lo + hi) / 2


def main() -> None:
    con = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)

    # ── 調整後價格面板(雙市場)──
    panels = [fetch_adjusted_panel(con, START, END, market=m) for m in ("twse", "tpex")]
    px = pl.concat(panels).select("market", "date", "company_code", "close", "raw_close", "trade_value")
    px = px.sort("company_code", "date").with_columns(
        adv20=pl.col("trade_value").rolling_mean(20).over("company_code"),
        fwd=pl.col("close").shift(-FWD_DAYS).over("company_code") / pl.col("close") - 1,
        tidx=pl.int_range(pl.len()).over("company_code"),
    )

    # ── 營收:3 月滾動 YoY(以營收月 M 標記)──
    rev = con.execute("""
        SELECT market, year, month, company_code, monthly_revenue
        FROM operating_revenue
        WHERE type = '公司' OR type IS NOT NULL
    """).pl()
    rev = (rev.group_by("company_code", "year", "month")
              .agg(pl.col("monthly_revenue").max())
              .sort("company_code", "year", "month"))
    rev = rev.with_columns(
        r3=pl.col("monthly_revenue").rolling_sum(3).over("company_code"),
    ).with_columns(
        r3_ly=pl.col("r3").shift(12).over("company_code"),
    ).with_columns(
        yoy3=(100.0 * (pl.col("r3") / pl.col("r3_ly") - 1.0)),
    ).filter(pl.col("yoy3").is_not_null() & pl.col("r3_ly").is_not_null() & (pl.col("r3_ly") > 0))

    # ── PE(日頻,as-of 訊號日)──
    pe = con.execute("""
        SELECT date, company_code, price_to_earning_ratio AS pe
        FROM stock_per_pbr
        WHERE price_to_earning_ratio > 0 AND price_to_earning_ratio <= 250
    """).pl()
    con.close()

    # 訊號日:營收月 M → 次月 12 日起首個交易日
    trading_days = px["date"].unique().sort()
    td = trading_days.to_list()

    def signal_date(y: int, m: int):
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        import datetime
        target = datetime.date(ny, nm, 12)
        for d in td:
            if d >= target:
                return d
        return None

    months = (rev.select("year", "month").unique().sort("year", "month")
                 .filter((pl.col("year") >= 2019))
                 .filter((pl.col("year").cast(pl.Utf8) + "-" + pl.col("month").cast(pl.Utf8).str.zfill(2)) <= LAST_SECTION))
    sig_map = {(r["year"], r["month"]): signal_date(r["year"], r["month"]) for r in months.iter_rows(named=True)}
    sig_map = {k: v for k, v in sig_map.items() if v is not None}

    rev_sec = (rev.filter(pl.struct(["year", "month"]).map_elements(
                    lambda s: (s["year"], s["month"]) in sig_map, return_dtype=pl.Boolean))
                  .with_columns(sig_date=pl.struct(["year", "month"]).map_elements(
                    lambda s: sig_map[(s["year"], s["month"])], return_dtype=pl.Date)))

    # join:價格(訊號日)+ PE(訊號日)
    base = rev_sec.join(
        px.select("company_code", "date", "raw_close", "adv20", "fwd").rename({"date": "sig_date"}),
        on=["company_code", "sig_date"], how="inner",
    ).join(
        pe.rename({"date": "sig_date"}), on=["company_code", "sig_date"], how="inner",
    ).filter(
        (pl.col("adv20") >= 50_000_000) & (pl.col("raw_close") >= 20) & pl.col("fwd").is_not_null()
    )

    # 隱含成長率(查表插值)
    grid_pe = np.arange(1.0, 251.0, 0.5)
    grid_g = implied_g_from_pe(grid_pe)
    base = base.with_columns(
        g_imp=pl.col("pe").map_elements(
            lambda p: float(np.interp(p, grid_pe, grid_g)), return_dtype=pl.Float64),
        g_hat=(pl.col("yoy3").clip(-60.0, 300.0) / 100.0),
    ).with_columns(
        gap_card=pl.col("g_hat") - pl.col("g_imp"),
        peg=pl.col("pe") / pl.col("yoy3").clip(1.0, None),
    )

    # ── 逐月 IC ──
    def monthly_ic(df: pl.DataFrame, col: str, invert: bool = False) -> pl.DataFrame:
        out = []
        for (ym,), g in df.group_by(["sig_date"]):
            if g.height < 30:
                continue
            x = g[col].rank() * (-1.0 if invert else 1.0)
            y = g["fwd"].rank()
            ic = np.corrcoef(x.to_numpy(), y.to_numpy())[0, 1]
            out.append({"sig_date": ym, "ic": ic, "n": g.height})
        return pl.DataFrame(out).sort("sig_date")

    # gap_rank 需在截面內先算 rank 差
    def add_gap_rank(g: pl.DataFrame) -> pl.DataFrame:
        return g.with_columns(
            gap_rank=(pl.col("g_hat").rank() - pl.col("g_imp").rank()))

    base = base.group_by("sig_date").map_groups(add_gap_rank)

    # 殘差化:gap_rank 對 yoy3 rank 的截面殘差
    def add_resid(g: pl.DataFrame) -> pl.DataFrame:
        x = g["yoy3"].rank().to_numpy()
        y = g["gap_rank"].to_numpy()
        if len(x) < 30:
            return g.with_columns(gap_resid=pl.lit(None, dtype=pl.Float64))
        beta = np.polyfit(x, y, 1)
        return g.with_columns(gap_resid=pl.Series(y - np.polyval(beta, x)))

    base = base.group_by("sig_date").map_groups(add_resid)

    factors = [("yoy3", False), ("pe", True), ("peg", True),
               ("gap_card", False), ("gap_rank", False), ("gap_resid", False)]
    print(f"截面數 {base['sig_date'].n_unique()},平均每截面 {base.height / base['sig_date'].n_unique():.0f} 檔,"
          f"總樣本 {base.height}")
    print(f"{'因子':<12}{'mean IC':>9}{'t-stat':>8}{'IC>0 佔比':>10}")
    ic_store = {}
    for col, inv in factors:
        s = monthly_ic(base.filter(pl.col(col).is_not_null()), col, invert=inv)
        ics = s["ic"].to_numpy()
        t = ics.mean() / ics.std(ddof=1) * np.sqrt(len(ics))
        ic_store[col] = s
        print(f"{col:<12}{ics.mean():>9.4f}{t:>8.2f}{(ics > 0).mean():>10.1%}")

    # 五分位年化報酬(gap_rank)
    def quintile(df: pl.DataFrame, col: str) -> None:
        q = (df.filter(pl.col(col).is_not_null())
               .group_by("sig_date")
               .map_groups(lambda g: g.with_columns(
                   qt=(pl.col(col).rank("ordinal") * 5.0 / (g.height + 1)).floor().clip(0, 4))))
        tbl = (q.group_by("qt").agg(
                  (pl.col("fwd").mean() * (252 / FWD_DAYS) * 100).round(1).alias("annualized_%"),
                  pl.len().alias("n"))
                .sort("qt"))
        print(f"\n{col} 五分位(Q0=最低 → Q4=最高)60 日 forward 年化 %:")
        print(tbl)

    quintile(base, "gap_rank")
    quintile(base, "yoy3")

    # 分期穩健性
    for lo, hi, tag in (("2019-01-01", "2022-12-31", "2019-22"), ("2023-01-01", "2026-06-30", "2023-26")):
        sub = base.filter((pl.col("sig_date") >= pl.lit(lo).cast(pl.Date)) & (pl.col("sig_date") <= pl.lit(hi).cast(pl.Date)))
        s = monthly_ic(sub, "gap_rank")
        r = monthly_ic(sub, "gap_resid")
        y = monthly_ic(sub, "yoy3")
        print(f"[{tag}] gap_rank IC {s['ic'].mean():.4f} | gap_resid IC {r['ic'].mean():.4f} | yoy3 IC {y['ic'].mean():.4f}")

    base.write_parquet(f"{paths.OUT_STRAT_LAB}/iter_87_expectations_gap_sections.parquet")
    print(f"\nsections → {paths.OUT_STRAT_LAB}/iter_87_expectations_gap_sections.parquet")


if __name__ == "__main__":
    main()
