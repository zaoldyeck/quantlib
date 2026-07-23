"""Shared backtest engine — 集中 dollar-tracking simulator + metrics + persist。

歷史上 iter_8 ~ iter_30 每個 .py 檔都自己重寫 daily loop（rebal、cost、metric），
造成 weight-compound bug 在多個地方獨立發生。這裡集中後：
  - 一個 dollar-tracking simulator（避免 weight-compound bug）
  - 一個 metrics 計算（Sortino / Sharpe / MDD 公式統一）
  - 一個 NAV CSV persist 格式

任何新 strategy 只要產出 `rebal_picks: list[(date, list[(code, weight)])]`，
就能直接呼叫 `simulate_dollar_tracking()` 拿到 result + 自動寫 CSV。
"""
from __future__ import annotations

import math
import os
import time
from dataclasses import asdict
from datetime import date
from typing import Sequence

import numpy as np
import polars as pl

from ..constants import (
    CAPITAL,
    COMMISSION,
    ROUND_TRIP_COST,
    RF,
    SELL_TAX,
    TDPY,
)
from ._types import BacktestResult


# ──────────────────────────────────────────────────────────────
# Metrics
# ──────────────────────────────────────────────────────────────

def compute_metrics(nav_arr: np.ndarray, days: Sequence[date],
                    capital: float = CAPITAL) -> dict[str, float]:
    """從 NAV array 統一計算 CAGR / Sortino / Sharpe / MDD。

    所有 strategy 用同一個公式，避免 metric 計算 drift 造成
    結果不可比。
    """
    if len(nav_arr) < 2 or len(days) < 2:
        return dict(cagr=0.0, sortino=0.0, sharpe=0.0, mdd=0.0,
                     vol=0.0, downvol=0.0, final_nav=capital)

    rets = np.diff(np.concatenate([[capital], nav_arr])) / \
           np.concatenate([[capital], nav_arr[:-1]])

    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (nav_arr[-1] / capital) ** (1 / years) - 1
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY))
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - RF) / vol if vol > 0 else 0.0
    sortino = (cagr - RF) / downvol if downvol > 0 else 0.0

    peak, mdd = capital, 0.0
    for v in nav_arr:
        peak = max(peak, v)
        mdd = min(mdd, (v - peak) / peak)

    return dict(
        cagr=float(cagr), sortino=float(sortino), sharpe=float(sharpe),
        mdd=float(mdd), vol=vol, downvol=downvol,
        final_nav=float(nav_arr[-1]),
    )


# ──────────────────────────────────────────────────────────────
# Dollar-tracking simulator
# ──────────────────────────────────────────────────────────────

def simulate_dollar_tracking(
    rebal_picks: list[tuple[date, list[tuple[str, float]]]],
    ret_dict: dict[tuple[date, str], float],
    days: Sequence[date],
    cash_buffer_code: str = "0050",
    capital: float = CAPITAL,
) -> tuple[np.ndarray, list[date]]:
    """單一規範的 dollar-tracking portfolio simulator。

    PRINCIPLE: 永遠用「絕對 dollar amount × daily return」累進，
    **絕不**用「weight × ret 累乘 unnormalized weights」這種寫法
    （那會造成 weight compound bug → 虛假動態槓桿，CAGR 灌水 +8pp）。

    Args:
        rebal_picks: [(rebal_date, [(code, weight), ...]), ...]
                     weights 必須加總到 1.0（或可空 list → fallback to cash_buffer）
        ret_dict: {(date, code): daily_return} — pre-computed daily returns
        days: trading-day index（通常 0050 trading days）
        cash_buffer_code: 未投入時放這檔（default 0050）
        capital: 起始 NAV

    Returns:
        (nav_array, days) — 每日 NAV
    """
    pos_dollar: dict[str, float] = {}
    cash_dollar: float = capital
    nav_hist: list[tuple[date, float]] = []
    rebal_idx = 0

    for d in days:
        # Step 1: 既有持股按 daily return 更新（dollar-level，無 weight compound 問題）
        for c in list(pos_dollar.keys()):
            r = ret_dict.get((d, c))
            if r is not None:
                pos_dollar[c] *= (1 + r)
        # Cash buffer (放 0050)
        r0 = ret_dict.get((d, cash_buffer_code))
        if r0 is not None and cash_dollar > 0:
            cash_dollar *= (1 + r0)

        nav = sum(pos_dollar.values()) + cash_dollar

        # Step 2: 若是 rebal day，重設 position dollar 分配
        if rebal_idx < len(rebal_picks) and d >= rebal_picks[rebal_idx][0]:
            target = dict(rebal_picks[rebal_idx][1])
            if target:
                target_dollar = {c: nav * w for c, w in target.items()}
                all_codes = set(pos_dollar.keys()) | set(target_dollar.keys())
                # Turnover cost = |Σ(new - old)| / 2 × round-trip
                delta = sum(
                    abs(target_dollar.get(c, 0) - pos_dollar.get(c, 0))
                    for c in all_codes
                ) / 2
                cost = delta * ROUND_TRIP_COST
                pos_dollar = {c: (nav - cost) * w for c, w in target.items()}
                cash_dollar = 0.0
            else:
                # Empty target → exit all to cash buffer
                cur = sum(pos_dollar.values())
                if cur > 0:
                    cost = cur * ROUND_TRIP_COST
                    cash_dollar = (cash_dollar + cur) - cost
                    pos_dollar = {}
            rebal_idx += 1

        nav = sum(pos_dollar.values()) + cash_dollar
        nav_hist.append((d, nav))

    nav_arr = np.array([n for _, n in nav_hist])
    out_days = [d for d, _ in nav_hist]
    return nav_arr, out_days


# ──────────────────────────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────────────────────────

def save_nav_csv(nav_arr: np.ndarray, days: Sequence[date], out_path: str) -> None:
    """寫 NAV → CSV。Schema: date, nav。"""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    pl.DataFrame({"date": list(days), "nav": nav_arr}).write_csv(out_path)


def run_and_save(
    name: str,
    rebal_picks: list[tuple[date, list[tuple[str, float]]]],
    ret_dict: dict[tuple[date, str], float],
    days: Sequence[date],
    out_path: str,
    capital: float = CAPITAL,
    extra: dict | None = None,
) -> BacktestResult:
    """One-shot：simulate + metrics + save CSV → BacktestResult."""
    t0 = time.time()
    nav_arr, sim_days = simulate_dollar_tracking(
        rebal_picks, ret_dict, days, capital=capital
    )
    save_nav_csv(nav_arr, sim_days, out_path)
    m = compute_metrics(nav_arr, sim_days, capital=capital)
    return BacktestResult(
        name=name,
        cagr=m["cagr"], sortino=m["sortino"], sharpe=m["sharpe"],
        mdd=m["mdd"], final_nav=m["final_nav"],
        n_days=len(sim_days), runtime_s=time.time() - t0,
        extra=extra or {},
    )


# ──────────────────────────────────────────────────────────────
# Sanity check helper
# ──────────────────────────────────────────────────────────────

def assert_no_weight_compound(rebal_picks: list[tuple[date, list[tuple[str, float]]]]) -> None:
    """Strategy author 可呼叫此函數 sanity check rebal_picks weights 加總 = 1.0。

    任何 weights sum > 1.05 或 < 0.95 都會 raise → 防止 weight compound bug 重現。
    """
    for rd, picks in rebal_picks:
        if not picks:
            continue  # empty = fallback to cash, OK
        s = sum(w for _, w in picks)
        if not (0.95 <= s <= 1.05):
            raise ValueError(
                f"Rebal {rd}: weights sum to {s:.4f} (expected ~1.0). "
                f"This will cause weight-compound bug → spurious leverage. "
                f"Picks: {picks}"
            )


__all__ = [
    "compute_metrics",
    "simulate_dollar_tracking",
    "save_nav_csv",
    "run_and_save",
    "assert_no_weight_compound",
]
