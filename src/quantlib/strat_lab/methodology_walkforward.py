"""Phase 3.1:重新驗證「3 年 train → 1 年 test」walk-forward 前提是否成立(校正資料)。

使用者質疑:「用過去三年的資料最佳化參數以適用於未來一年的交易,這個論點還正確嗎?」
本分析用一個**簡單、可理解**的截面動能因子(依 lookback 期報酬排名、持 top-K、月頻換股),
在校正後 cache 上做 walk-forward:每個 train 窗挑「train 期 Sharpe 最佳的 lookback」,
套到緊接的 1 年 OOS,量測:

  1. **參數穩定性**:train 最佳 lookback 是否在各窗間亂跳(跳=在擬合雜訊)。
  2. **train→OOS 傳遞**:train Sharpe 與 OOS Sharpe 的相關(高 train 是否預示高 OOS)。
  3. **train 長度比較**:2y / 3y / 5y / expanding——哪個給最好的中位 OOS Sharpe。

判準:若 train 最佳 lookback 穩定、且 train↔OOS 正相關,則「train 選參→OOS 套用」的
方法論成立;若 lookback 亂跳且 train↔OOS 無相關/負相關,代表在最佳化雜訊、前提有問題。

Run: uv run python -m quantlib.strat_lab.methodology_walkforward
依賴 cache:是(校正後世代)。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib import prices

_START, _END = "2009-01-01", "2026-07-01"
_LOOKBACKS = [3, 6, 9, 12]          # 動能回看月數(候選參數)
_TOP_K = 20                          # 持股數
_MIN_ADV = 50_000_000                # 流動性門檻(日均成交額,避免雜訊小股)
_TRADING_M = 12


def load_monthly_returns(con) -> pl.DataFrame:
    """common stocks 的月頻還原報酬(wide:index=月, columns=code)。"""
    panel = data.common_stocks(data.load_panel(con, _START, _END))
    codes = panel.select("company_code").unique().to_series().to_list()
    # 還原價面板 → 日報酬 → 月報酬(月底複利)
    adj = prices.fetch_adjusted_panel(con, _START, _END, market="twse", codes=codes)
    r = prices.daily_returns_from_panel(adj).select(["date", "company_code", "ret"])
    # 月底報酬 = ∏(1+日報酬)-1
    r = r.with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
    monthly = (r.group_by(["month", "company_code"])
               .agg((pl.col("ret") + 1).product().alias("mret"))
               .with_columns(pl.col("mret") - 1))
    return monthly.sort(["month", "company_code"])


def _sharpe(rets: np.ndarray) -> float:
    if len(rets) < 6 or np.std(rets, ddof=1) == 0:
        return 0.0
    return float(np.mean(rets) / np.std(rets, ddof=1) * np.sqrt(_TRADING_M))


def momentum_series(monthly: pl.DataFrame, lookback: int, top_k: int = _TOP_K) -> pl.DataFrame:
    """月頻:依過去 lookback 月累積報酬排名選 top_k,持有次月。回 (month, port_ret)。"""
    piv = monthly.pivot(values="mret", index="month", on="company_code").sort("month")
    months = piv["month"].to_list()
    cols = [c for c in piv.columns if c != "month"]
    mat = piv.select(cols).to_numpy()  # (T, N) 月報酬;NaN=該月無資料
    out_m, out_r = [], []
    for t in range(lookback, len(months) - 1):
        # 過去 lookback 月累積報酬(需全期有資料)
        win = mat[t - lookback:t, :]
        if win.shape[0] < lookback:
            continue
        cum = np.prod(1 + np.nan_to_num(win, nan=0.0), axis=0) - 1
        valid = ~np.isnan(mat[t, :]) & ~np.isnan(mat[t + 1, :]) & np.all(~np.isnan(win), axis=0)
        idx = np.where(valid)[0]
        if len(idx) < top_k:
            continue
        top = idx[np.argsort(-cum[idx])[:top_k]]
        nxt = mat[t + 1, top]  # 次月報酬 = 持有期報酬
        out_m.append(months[t + 1]); out_r.append(float(np.nanmean(nxt)))
    return pl.DataFrame({"month": out_m, "port_ret": out_r})


def walkforward(monthly: pl.DataFrame, train_years: int | None) -> list[dict]:
    """每年一個窗:train(train_years 年,None=expanding)挑最佳 lookback → 次 1 年 OOS。"""
    all_months = sorted(monthly["month"].unique().to_list())
    y0, y1 = all_months[0].year + max(_LOOKBACKS) // 12 + 1, all_months[-1].year
    series = {lb: momentum_series(monthly, lb) for lb in _LOOKBACKS}
    rows = []
    for test_year in range(y0 + (train_years or 3), y1):
        tr_start = f"{test_year - (train_years or 100):04d}-01-01"
        tr_end, oos_end = f"{test_year}-01-01", f"{test_year + 1}-01-01"
        best_lb, best_tr = None, -1e9
        oos_of = {}
        for lb, s in series.items():
            tr = s.filter((pl.col("month") >= pl.lit(tr_start).str.to_date())
                          & (pl.col("month") < pl.lit(tr_end).str.to_date()))["port_ret"].to_numpy()
            oos = s.filter((pl.col("month") >= pl.lit(tr_end).str.to_date())
                           & (pl.col("month") < pl.lit(oos_end).str.to_date()))["port_ret"].to_numpy()
            tr_sh = _sharpe(tr)
            oos_of[lb] = _sharpe(oos)
            if tr_sh > best_tr:
                best_tr, best_lb = tr_sh, lb
        rows.append({"test_year": test_year, "best_lb": best_lb, "train_sharpe": round(best_tr, 2),
                     "oos_sharpe": round(oos_of[best_lb], 2),
                     "oos_mean_all": round(float(np.mean(list(oos_of.values()))), 2),  # 全 lookback 平均(不選參基準)
                     "oos_best_possible": round(max(oos_of.values()), 2)})
    return rows


def main() -> None:
    con = data.connect()
    print("[3.1] 載入 common stocks 月頻還原報酬…", flush=True)
    monthly = load_monthly_returns(con)
    print(f"  {monthly['month'].n_unique()} 月 × {monthly['company_code'].n_unique()} 檔\n")

    for ty in (2, 3, 5, None):
        label = f"{ty}y train" if ty else "expanding"
        rows = walkforward(monthly, ty)
        if not rows:
            continue
        lbs = [r["best_lb"] for r in rows]
        tr = np.array([r["train_sharpe"] for r in rows])
        oos = np.array([r["oos_sharpe"] for r in rows])
        best = np.array([r["oos_best_possible"] for r in rows])
        corr = float(np.corrcoef(tr, oos)[0, 1]) if len(tr) > 2 and np.std(tr) > 0 and np.std(oos) > 0 else float("nan")
        # 選中 lookback 的 OOS vs 「事後最佳 lookback」的 OOS——差距=選參的代價
        regret = float(np.mean(best - oos))
        print(f"=== {label}({len(rows)} 窗)===")
        print(f"  train 最佳 lookback 分佈:{ {lb: lbs.count(lb) for lb in _LOOKBACKS} }(越集中越穩定)")
        print(f"  中位 OOS Sharpe:{np.median(oos):+.2f};train↔OOS 相關:{corr:+.2f}")
        print(f"  選參 regret(事後最佳 - 實選 OOS Sharpe 均):{regret:+.2f}(越小=選參越有效)\n")

    print("=== 判準結論 ===")
    print("  train↔OOS 相關 > 0 且 lookback 集中 → 「train 選參→OOS 套用」成立;")
    print("  相關 ≤ 0 或 lookback 亂跳 + regret 大 → 在最佳化雜訊,前提需修正(改 ensemble/robust 選參)。")


if __name__ == "__main__":
    main()
