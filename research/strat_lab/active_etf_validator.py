"""Active ETF comparison utilities for execution-aware strategy validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

import duckdb
import polars as pl

from research.prices import total_return_series


ACTIVE_ETFS: tuple[str, ...] = (
    "00400A",
    "00401A",
    "00980A",
    "00981A",
    "00982A",
    "00984A",
    "00985A",
    "00986A",
    "00987A",
    "00988A",
    "00990A",
    "00991A",
    "00992A",
    "00993A",
    "00994A",
    "00995A",
    "00996A",
)


@dataclass(frozen=True)
class ActiveEtfSummary:
    active_etf_count: int
    active_etf_wins: int
    active_etf_losses: int
    active_etf_loss_list: str
    active_etf_worst_alpha: float
    active_etf_avg_alpha: float
    active_etf_worst_total_return_alpha: float
    active_etf_avg_total_return_alpha: float
    active_etf_min_days: float
    active_etf_short_window_losses: int
    active_etf_all_win: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "active_etf_count": self.active_etf_count,
            "active_etf_wins": self.active_etf_wins,
            "active_etf_losses": self.active_etf_losses,
            "active_etf_loss_list": self.active_etf_loss_list,
            "active_etf_worst_alpha": self.active_etf_worst_alpha,
            "active_etf_avg_alpha": self.active_etf_avg_alpha,
            "active_etf_worst_total_return_alpha": self.active_etf_worst_total_return_alpha,
            "active_etf_avg_total_return_alpha": self.active_etf_avg_total_return_alpha,
            "active_etf_min_days": self.active_etf_min_days,
            "active_etf_short_window_losses": self.active_etf_short_window_losses,
            "active_etf_all_win": self.active_etf_all_win,
        }


def annualized_return(start_value: float, end_value: float, start: date, end: date) -> float:
    years = max((end - start).days / 365.25, 1e-9)
    if start_value <= 0 or end_value <= 0:
        return -1.0
    return (end_value / start_value) ** (1.0 / years) - 1.0


def load_active_etf_series(
    con: duckdb.DuckDBPyConnection,
    *,
    start: str = "2005-01-01",
    end: str,
    codes: Iterable[str] = ACTIVE_ETFS,
) -> dict[str, pl.DataFrame]:
    out: dict[str, pl.DataFrame] = {}
    for code in codes:
        series = total_return_series(con, code, start, end, market="twse")
        if series.height >= 2:
            out[code] = series.sort("date").select(["date", pl.col("adj_close").alias("etf_nav")])
    return out


def compare_to_active_etfs(
    strategy_id: str,
    daily: pl.DataFrame,
    active_etfs: dict[str, pl.DataFrame],
) -> tuple[ActiveEtfSummary, pl.DataFrame]:
    strategy = daily.sort("date").select(["date", pl.col("nav").alias("strategy_nav")])
    rows: list[dict[str, object]] = []
    for code, etf in active_etfs.items():
        joined = strategy.join(etf, on="date", how="inner").sort("date")
        if joined.height < 2:
            continue
        start = joined["date"][0]
        end = joined["date"][-1]
        strategy_cagr = annualized_return(
            float(joined["strategy_nav"][0]),
            float(joined["strategy_nav"][-1]),
            start,
            end,
        )
        etf_cagr = annualized_return(
            float(joined["etf_nav"][0]),
            float(joined["etf_nav"][-1]),
            start,
            end,
        )
        strategy_total_return = float(joined["strategy_nav"][-1] / joined["strategy_nav"][0] - 1.0)
        etf_total_return = float(joined["etf_nav"][-1] / joined["etf_nav"][0] - 1.0)
        alpha = strategy_cagr - etf_cagr
        rows.append(
            {
                "strategy_id": strategy_id,
                "etf": code,
                "start": start,
                "end": end,
                "days": float((end - start).days),
                "strategy_cagr": strategy_cagr,
                "etf_cagr": etf_cagr,
                "alpha": alpha,
                "strategy_total_return": strategy_total_return,
                "etf_total_return": etf_total_return,
                "total_return_alpha": strategy_total_return - etf_total_return,
                "win": alpha > 0,
            }
        )

    frame = pl.DataFrame(rows) if rows else empty_active_etf_frame()
    if frame.is_empty():
        summary = ActiveEtfSummary(0, 0, 0, "", 0.0, 0.0, 0.0, 0.0, 0.0, 0, False)
    else:
        wins = int(frame["win"].sum())
        losses = frame.height - wins
        loss_list = ",".join(frame.filter(pl.col("win") == False)["etf"].to_list())  # noqa: E712
        short_window_losses = int(frame.filter((pl.col("win") == False) & (pl.col("days") < 90)).height)  # noqa: E712
        summary = ActiveEtfSummary(
            active_etf_count=frame.height,
            active_etf_wins=wins,
            active_etf_losses=losses,
            active_etf_loss_list=loss_list,
            active_etf_worst_alpha=float(frame["alpha"].min()),
            active_etf_avg_alpha=float(frame["alpha"].mean()),
            active_etf_worst_total_return_alpha=float(frame["total_return_alpha"].min()),
            active_etf_avg_total_return_alpha=float(frame["total_return_alpha"].mean()),
            active_etf_min_days=float(frame["days"].min()),
            active_etf_short_window_losses=short_window_losses,
            active_etf_all_win=losses == 0,
        )
    return summary, frame.sort("alpha")


def empty_active_etf_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "strategy_id": pl.Utf8,
            "etf": pl.Utf8,
            "start": pl.Date,
            "end": pl.Date,
            "days": pl.Float64,
            "strategy_cagr": pl.Float64,
            "etf_cagr": pl.Float64,
            "alpha": pl.Float64,
            "strategy_total_return": pl.Float64,
            "etf_total_return": pl.Float64,
            "total_return_alpha": pl.Float64,
            "win": pl.Boolean,
        }
    )
