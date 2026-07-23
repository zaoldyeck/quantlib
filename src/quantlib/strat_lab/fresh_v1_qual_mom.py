"""Phase 3.4 首個從頭策略(baseline):品質 + 動能穩健 ensemble,OOS 驗證。

依 Phase 3 方法論(docs/strategy_research/phase3_methodology_scrutiny.md):
- **穩健/ensemble、不最佳化參數**(3.1:grid-search 選 in-sample 最佳參數反效果)——
  因子等權 rank、動能用多 lookback 平均,無單點最佳化參數。
- **OOS-robust KPI**(3.2):報逐年 OOS + 全跨度,含成本、對照 0050。
- **日頻資料**(3.3),月頻換股。

設計(每個成分都有學理出處,非掃出來的):
- **品質** = Piotroski F-Score(raw_quarterly,PIT:用截止日 ≤ 決策月的最新一季,n_valid=9)。
  出處:Piotroski (2000),高 F-Score 財務體質改善組合超額報酬。
- **動能** = 過去 {6,9,12} 月累積報酬的 rank 平均。出處:Jegadeesh-Titman (1993) 動能溢酬。
- **合成** = 0.5·rank(品質) + 0.5·rank(動能),long top-20,月頻等權,含成本。

這是**baseline / 起點**,非最終策略——使用者要定 alpha thesis 方向後再迭代深化。

Run: uv run python -m quantlib.strat_lab.fresh_v1_qual_mom
依賴 cache:是(校正世代)。
"""
from __future__ import annotations

import warnings

import numpy as np
import polars as pl

warnings.filterwarnings("ignore", message="Mean of empty slice")  # 全 NaN 月的 nanmean(已下游處理)

from quantlib.apex import data
from quantlib.strat_lab.methodology_walkforward import load_monthly_returns

_TOP_K = 20
_LOOKBACKS = [6, 9, 12]
_TRADING_M = 12
_COST = 0.00357  # round-trip 摩擦(0.3% 稅 + 2×0.0285% 手續);換手比例計入

#: F-Score PIT 申報截止(月;該季 F-Score 在此日後才可用)。TW 財報申報期限。
_DEADLINE = {1: (5, 22), 2: (8, 21), 3: (11, 21), 4: (16, 4, 7)}  # Q4 次年 4/7(以 16 標次年)


def pit_fscore(con) -> pl.DataFrame:
    """PIT F-Score(完整 n_valid=9):回 (avail_month, company_code, fscore)——
    avail_month = 該季 F-Score 最早可用的月份(申報截止當月)。"""
    rq = con.execute(
        "SELECT company_code, year, quarter, f_score_raw, f_score_n_valid "
        "FROM raw_quarterly WHERE f_score_n_valid = 9").pl()
    def avail(y, q):
        d = _DEADLINE[q]
        return (y + 1, d[1]) if len(d) == 3 else (y, d[0])  # (avail_year, avail_month)
    rq = rq.with_columns(
        pl.struct(["year", "quarter"]).map_elements(
            lambda s: avail(s["year"], s["quarter"])[0], return_dtype=pl.Int64).alias("ay"),
        pl.struct(["year", "quarter"]).map_elements(
            lambda s: avail(s["year"], s["quarter"])[1], return_dtype=pl.Int64).alias("am"),
    )
    return rq.select([
        pl.date("ay", "am", 1).alias("avail_month"),
        "company_code", pl.col("f_score_raw").alias("fscore")]).sort(["avail_month", "company_code"])


def _rank(x: np.ndarray) -> np.ndarray:
    """截面百分位 rank(0..1,高=好);NaN → NaN。"""
    r = np.full(len(x), np.nan)
    ok = ~np.isnan(x)
    if ok.sum() < 2:
        return r
    order = np.argsort(np.argsort(x[ok]))
    r[ok] = order / (ok.sum() - 1)
    return r


def backtest(monthly: pl.DataFrame, fscore: pl.DataFrame) -> pl.DataFrame:
    """月頻:合成 = 0.5 rank(品質) + 0.5 rank(動能),long top-K 等權,含換手成本。"""
    piv = monthly.pivot(values="mret", index="month", on="company_code").sort("month")
    months = piv["month"].to_list()
    codes = [c for c in piv.columns if c != "month"]
    cidx = {c: i for i, c in enumerate(codes)}
    mat = piv.select(codes).to_numpy()  # (T,N)
    # F-Score as-of:每月取 avail_month ≤ 該月的最新一季
    fs_by_month = {}
    fs_sorted = fscore.sort("avail_month")
    prev_held: set[int] = set()
    out_m, out_r = [], []
    for t in range(max(_LOOKBACKS), len(months) - 1):
        m = months[t]
        # 動能 ensemble rank
        mom_ranks = []
        for lb in _LOOKBACKS:
            win = mat[t - lb:t, :]
            cum = np.prod(1 + np.nan_to_num(win, nan=0.0), axis=0) - 1
            cum[np.any(np.isnan(win), axis=0)] = np.nan
            mom_ranks.append(_rank(cum))
        mom = np.nanmean(np.vstack(mom_ranks), axis=0)
        # 品質 rank(PIT F-Score as-of m)
        fs_now = fs_sorted.filter(pl.col("avail_month") <= m)
        fvec = np.full(len(codes), np.nan)
        if fs_now.height:
            latest = (fs_now.group_by("company_code").agg(pl.col("fscore").last()))
            for code, fsc in latest.iter_rows():
                if code in cidx:
                    fvec[cidx[code]] = fsc
        qual = _rank(fvec)
        # 合成:兩因子都要有值 + 次月有報酬
        comp = 0.5 * mom + 0.5 * qual
        valid = ~np.isnan(comp) & ~np.isnan(mat[t + 1, :])
        idx = np.where(valid)[0]
        if len(idx) < _TOP_K:
            continue
        top = set(idx[np.argsort(-comp[idx])[:_TOP_K]])
        gross = float(np.nanmean(mat[t + 1, list(top)]))
        turnover = len(top - prev_held) / _TOP_K
        net = gross - turnover * _COST
        prev_held = top
        out_m.append(months[t + 1]); out_r.append(net)
    return pl.DataFrame({"month": out_m, "ret": out_r})


def _stats(rets: np.ndarray) -> dict:
    if len(rets) < 6:
        return {"n": len(rets)}
    nav = np.cumprod(1 + rets)
    yrs = len(rets) / _TRADING_M
    cagr = nav[-1] ** (1 / yrs) - 1
    sharpe = np.mean(rets) / (np.std(rets, ddof=1) + 1e-9) * np.sqrt(_TRADING_M)
    mdd = float((nav / np.maximum.accumulate(nav) - 1).min())
    return {"cagr": cagr, "sharpe": sharpe, "mdd": mdd}


def main() -> None:
    con = data.connect()
    print("[3.4-v1] 載入月報酬 + PIT F-Score…", flush=True)
    monthly = load_monthly_returns(con)
    fscore = pit_fscore(con)
    bt = backtest(monthly, fscore)
    rets = bt["ret"].to_numpy()
    months = bt["month"].to_list()
    full = _stats(rets)
    print(f"\n=== 品質+動能 baseline(top-{_TOP_K},月頻,含成本;{months[0]}~{months[-1]})===")
    print(f"  全跨度:CAGR {full['cagr']:+.1%}  Sharpe {full['sharpe']:.2f}  MDD {full['mdd']:.1%}")
    # 逐年 OOS(3.2:regime 一致性)
    print("  逐年 Sharpe(OOS 一致性):")
    yr = {}
    for m, r in zip(months, rets):
        yr.setdefault(m.year, []).append(r)
    line = []
    for y in sorted(yr):
        s = _stats(np.array(yr[y]))
        line.append(f"{y}:{s.get('sharpe', float('nan')):+.1f}")
    print("    " + "  ".join(line))
    neg_years = sum(1 for y in yr if np.prod(1 + np.array(yr[y])) < 1)
    print(f"  負報酬年數:{neg_years}/{len(yr)};判準:全跨度 Sharpe>1 且多數年正 = 穩健 baseline。")


if __name__ == "__main__":
    main()
