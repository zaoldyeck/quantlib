"""apex 因子評估 harness — 截面 IC + decile spread,統一規格、毫秒級。

評估規格(全 campaign 統一):
  - Forward return:fwd_k[t] = close[t+1+k] / close[t+1] − 1(調整價,T+1 起算,零 look-ahead)
  - IC:每日截面 Spearman(rank 後 Pearson),樣本 = eligible universe
  - t_adj:普通 t / √k(粗略校正重疊樣本的自相關高估)
  - decile spread:每日十分位,top − bottom 的平均 fwd 報酬(依 horizon 年化)
因子慣例:值越高越看多(反向因子請先取負再進來)。
結果 append 到 ledger/factors.jsonl(與 trials.jsonl 分開)。
"""
from __future__ import annotations

import json
import math
import os
from datetime import datetime

import polars as pl

FACTORS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ledger", "factors.jsonl")

HORIZONS = (5, 21, 63)


def forward_returns(panel: pl.DataFrame, horizons: tuple[int, ...] = HORIZONS) -> pl.DataFrame:
    """(date, company_code, fwd_5, fwd_21, fwd_63)。T+1 close 起算 k 日報酬。"""
    return (
        panel.sort(["company_code", "date"])
        .with_columns(
            [
                (
                    pl.col("close").shift(-(1 + k)) / pl.col("close").shift(-1) - 1
                )
                .over("company_code")
                .alias(f"fwd_{k}")
                for k in horizons
            ]
        )
        .select(["date", "company_code", *[f"fwd_{k}" for k in horizons]])
    )


def evaluate_factor(
    name: str,
    factor: pl.DataFrame,
    fwd: pl.DataFrame,
    elig: pl.DataFrame,
    *,
    family: str,
    batch: str,
    min_stocks: int = 100,
    horizons: tuple[int, ...] = HORIZONS,
    log: bool = True,
) -> dict:
    """factor: (date, company_code, value)。回傳各 horizon 的 IC/decile 統計。"""
    df = (
        factor.drop_nulls(subset=["value"])
        .filter(pl.col("value").is_finite())
        .join(elig.filter(pl.col("eligible")).select(["date", "company_code"]),
              on=["date", "company_code"], how="semi")
        .join(fwd, on=["date", "company_code"], how="inner")
    )
    out: dict = {"name": name, "family": family, "batch": batch}
    for k in horizons:
        fk = f"fwd_{k}"
        d = df.drop_nulls(subset=[fk])
        # 每日截面 rank → Pearson = Spearman
        daily = (
            d.with_columns(
                [
                    pl.col("value").rank().over("date").alias("_rv"),
                    pl.col(fk).rank().over("date").alias("_rf"),
                    pl.len().over("date").alias("_n"),
                ]
            )
            .filter(pl.col("_n") >= min_stocks)
            .group_by("date")
            .agg(pl.corr("_rv", "_rf").alias("ic"))
            .drop_nulls()
        )
        if daily.height < 50:
            out[f"h{k}"] = None
            continue
        ic = daily["ic"]
        mean_ic, std_ic, n = float(ic.mean()), float(ic.std()), daily.height
        t = mean_ic / std_ic * math.sqrt(n) if std_ic > 0 else 0.0
        # decile spread(年化)
        dec = (
            d.filter(pl.col("value").is_not_nan())
            .with_columns(
                (pl.col("value").rank("ordinal").over("date") * 10 // (pl.len().over("date") + 1))
                .cast(pl.Int8)
                .alias("_q")
            )
            .group_by("_q")
            .agg(pl.col(fk).mean())
            .sort("_q")
        )
        qm = dec[fk].to_list()
        ann = 252.0 / k
        spread = (qm[-1] - qm[0]) * ann if len(qm) == 10 else None
        mono = _rank_corr(qm) if len(qm) == 10 else None
        out[f"h{k}"] = {
            "mean_ic": round(mean_ic, 4),
            "t": round(t, 2),
            "t_adj": round(t / math.sqrt(k), 2),
            "ir": round(mean_ic / std_ic, 3) if std_ic > 0 else 0.0,
            "n_days": n,
            "top_ann": round(qm[-1] * ann, 4) if qm else None,
            "bottom_ann": round(qm[0] * ann, 4) if qm else None,
            "spread_ann": round(spread, 4) if spread is not None else None,
            "monotonic": round(mono, 3) if mono is not None else None,
        }
    if log:
        os.makedirs(os.path.dirname(FACTORS_PATH), exist_ok=True)
        with open(FACTORS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(out | {"ts": datetime.now().isoformat(timespec="seconds")},
                               ensure_ascii=False) + "\n")
    return out


def fmt_factor(r: dict) -> str:
    """單行×3 horizon 摘要。"""
    parts = [f"{r['name']:<24s}"]
    for k in HORIZONS:
        h = r.get(f"h{k}")
        parts.append(
            f"h{k}: IC {h['mean_ic']:+.3f} t' {h['t_adj']:+5.1f} spd {h['spread_ann']:+7.1%} m {h['monotonic']:+.2f}"
            if h
            else f"h{k}: (資料不足)"
        )
    return " | ".join(parts)


def _rank_corr(vals: list[float]) -> float:
    """decile 單調性:decile 序 vs 平均報酬的 Spearman。"""
    n = len(vals)
    order = sorted(range(n), key=lambda i: vals[i])
    rank = [0] * n
    for r, i in enumerate(order):
        rank[i] = r
    mean_r = (n - 1) / 2
    num = sum((i - mean_r) * (rank[i] - mean_r) for i in range(n))
    den = sum((i - mean_r) ** 2 for i in range(n))
    return num / den if den else 0.0
