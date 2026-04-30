"""Strategy-result dataclasses — 取代過去散落各 script 的 raw dict result。

Type-checked structure 讓拼錯欄位直接 error，IDE 也能 autocomplete。
"""
from __future__ import annotations

from dataclasses import dataclass, asdict, field
from datetime import date
from typing import Any


@dataclass
class Position:
    """Per-stock active position state（用於 daily-loop strategy）。"""
    code: str
    entry_date: date
    entry_px: float
    shares: float
    peak_px: float
    last_px: float
    pyramid_lvl: int = 0           # 0 / 1 / 2 (iter_24 pyramid)
    entry_atr: float | None = None # ATR at entry (for ATR-based trailing)
    source: str = "catalyst"       # 'quality' | 'catalyst' (Option E 用)


@dataclass
class BacktestResult:
    """Full backtest output — 取代 dict result，type-safe."""
    name: str
    cagr: float
    sortino: float
    sharpe: float
    mdd: float
    final_nav: float
    n_days: int
    runtime_s: float
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationResult:
    """validate_all.py 用的 OOS validation result — 含 verdict_class metadata."""
    name: str
    verdict: str           # ✅ REAL ALPHA / ⚠️ MARGINAL / ❌ CURVE-FIT
    verdict_class: str     # SHIP / REAL_ALPHA / BENCHMARK / BIAS_* / BUG_* / etc.
    is_sortino: float
    oos_sortino: float
    oos_cagr: float
    is_cagr: float
    is_mdd: float
    is_calmar: float
    retention_sortino: float
    retention_cagr: float
    n_positive_folds: int
    n_folds: int
    mc_p: float
    mc_sharpe: float
    boot_cagr_lb: float
    boot_sortino_lb: float
    dsr: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RebalPick:
    """Single rebal-day target weight."""
    rebal_d: date
    code: str
    weight: float
