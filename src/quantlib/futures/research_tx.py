"""Professional TAIFEX index-futures strategy research runner.

This is intentionally self-contained: it builds costed futures sleeves, runs a
survival-constrained validation pass, allocates across finalists, and writes
artifacts + an investor-readable research ranking.
"""

from __future__ import annotations

import math
import sys
import time
from dataclasses import replace
from pathlib import Path
from quantlib import paths

ROOT = paths.REPO
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
STRAT_LAB = ROOT / "strat_lab"
if str(STRAT_LAB) not in sys.path:
    sys.path.insert(0, str(STRAT_LAB))

import duckdb
import matplotlib
import numpy as np
import polars as pl

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from futures.simulator import FuturesExecutionConfig, combine_sleeve_returns, simulate_single_product
from futures.rpt_features import build_all_rpt_daily_features
from futures.specs import FuturesCostConfig, FuturesMarginConfig
from futures.strategies import StrategyCandidate, build_signal, default_candidate_grid, load_product_frame
from futures.validation import (
    add_recent_window_returns,
    futures_objective,
    multi_config_pbo,
    validate_futures_daily,
    verdict,
)
from quantlib.prices import total_return_series
from strat_lab.evaluation import CAPITAL_DEFAULT, nav_metrics
from strat_lab.validator import ValidationConfig, recent_one_year_metrics


BASE = paths.REPO
DB_PATH = paths.CACHE_DB
OUT_DIR = paths.OUT_STRAT_LAB / "futures_tx_professional"
DOC_PATH = BASE / "docs" / "strategy_research" / "futures_strategy_ranking.md"


def _oos_daily(daily: pl.DataFrame) -> pl.DataFrame:
    return daily.filter((pl.col("date").dt.year() >= 2010) & (pl.col("date").dt.year() <= 2026))


def _fast_row(name: str, daily: pl.DataFrame, summary: dict[str, object]) -> dict[str, object]:
    row = {"name": name, **nav_metrics(daily.select(["date", "nav"])), **nav_metrics(_oos_daily(daily).select(["date", "nav"]), prefix="oos_")}
    row.update(recent_one_year_metrics(daily.select(["date", "nav"])))
    row = add_recent_window_returns(row, daily)
    row.update(summary)
    rough = (
        float(row.get("oos_log_cagr", 0.0) or 0.0)
        * max(0.05, min(float(row.get("oos_sortino", 0.0) or 0.0) / 2.0, 1.0))
        * max(0.05, 1.0 - abs(float(row.get("oos_mdd", 0.0) or 0.0)))
    )
    if bool(row.get("margin_breach", False)):
        rough -= 999.0
    row["fast_score"] = rough
    return row


def _candidate_name(candidate: StrategyCandidate, target_vol: float, stop_atr: float | None, trail_atr: float | None) -> str:
    stop = "nostop" if stop_atr is None else f"sl{stop_atr:g}"
    trail = "notrail" if trail_atr is None else f"tr{trail_atr:g}"
    return f"{candidate.name}_tv{target_vol:g}_{stop}_{trail}"


def _make_config(target_vol: float, cost_multiplier: float = 1.0, stop_atr: float | None = 2.5, trail_atr: float | None = 4.0) -> FuturesExecutionConfig:
    return FuturesExecutionConfig(
        target_vol=target_vol,
        cost=FuturesCostConfig(cost_multiplier=cost_multiplier),
        margin=FuturesMarginConfig(
            initial_margin_ratio=0.135,
            maintenance_margin_ratio=0.105,
            required_buffer=1.35,
            liquidation_buffer=1.00,
            max_notional_leverage=6.0,
            stress_notional_move=0.12,
        ),
        stop_loss_atr=stop_atr,
        trailing_stop_atr=trail_atr,
        time_stop_days=40,
        time_stop_min_return=-0.005,
    )


def _simulate_candidate(
    frames: dict[str, pl.DataFrame],
    candidate: StrategyCandidate,
    *,
    target_vol: float,
    cost_multiplier: float = 1.0,
    stop_atr: float | None = 2.5,
    trail_atr: float | None = 4.0,
) -> tuple[str, pl.DataFrame, pl.DataFrame, pl.DataFrame, dict[str, object]]:
    frame = frames[candidate.product]
    signal = build_signal(frame, candidate)
    bars = frame.join(signal.select(["date", "atr"]), on="date", how="left").sort("date")
    name = _candidate_name(candidate, target_vol, stop_atr, trail_atr)
    result = simulate_single_product(
        bars,
        signal.select(["date", "signal"]),
        product=candidate.product,
        name=name,
        cfg=_make_config(target_vol, cost_multiplier=cost_multiplier, stop_atr=stop_atr, trail_atr=trail_atr),
    )
    return name, result.daily, result.fills, result.trades, result.summary


def _normalize_benchmark(series: pl.DataFrame, name: str, start: str, end: str, capital: float = CAPITAL_DEFAULT) -> pl.DataFrame:
    if series.is_empty():
        return pl.DataFrame(schema={"date": pl.Date, name: pl.Float64})
    s = series.filter((pl.col("date") >= pl.lit(start).str.to_date()) & (pl.col("date") <= pl.lit(end).str.to_date())).sort("date")
    if s.is_empty():
        return pl.DataFrame(schema={"date": pl.Date, name: pl.Float64})
    first = float(s["adj_close"][0])
    return s.with_columns((pl.col("adj_close") / first * capital).alias(name)).select(["date", name])


def _write_chart(champion_daily: pl.DataFrame, con: duckdb.DuckDBPyConnection, out_path: Path) -> None:
    start = champion_daily["date"][0].isoformat()
    end = champion_daily["date"][-1].isoformat()
    tx = con.sql(
        f"""
        SELECT date, continuous_close AS adj_close
        FROM taifex_futures_continuous
        WHERE product = 'TX'
          AND date BETWEEN DATE '{start}' AND DATE '{end}'
        ORDER BY date
        """
    ).pl()
    b0050 = _normalize_benchmark(total_return_series(con, "0050", start, end), "0050", start, end)
    b2330 = _normalize_benchmark(total_return_series(con, "2330", start, end), "2330", start, end)
    txn = _normalize_benchmark(tx, "TX_buy_hold", start, end)
    plot = champion_daily.select(["date", pl.col("nav").alias("champion")])
    for bench in [txn, b0050, b2330]:
        if not bench.is_empty():
            plot = plot.join(bench, on="date", how="left")
    pdf = plot.to_pandas().set_index("date")
    fig, ax = plt.subplots(figsize=(13, 7))
    for col in pdf.columns:
        ax.plot(pdf.index, pdf[col], label=col, linewidth=1.7 if col == "champion" else 1.1)
    ax.set_yscale("log")
    ax.set_title("Futures champion vs TX / 0050 / 2330 total-return benchmarks")
    ax.set_ylabel("NAV (log scale, start=1,000,000)")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def _format_pct(value: object) -> str:
    try:
        return f"{float(value):+.2%}"
    except Exception:
        return "n/a"


def _format_num(value: object, digits: int = 3) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "n/a"


def _write_doc(summary: pl.DataFrame, champion: dict[str, object] | None, daily_cutoff: str, rpt_cutoff: str, pbo: float, elapsed: float) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    ranked = summary.sort("objective", descending=True)
    passed = ranked.filter(pl.col("verdict") == "pass")
    diagnostic_ranked = (
        summary.with_columns(
            pl.when(pl.col("name").str.starts_with("PM_lagged_risk_budget_"))
            .then(pl.col("name").str.replace(r"_tv[0-9.]+$", ""))
            .otherwise(pl.col("name"))
            .alias("display_family")
        )
        .sort(["oos_cagr", "recent_1y_cagr", "oos_mdd"], descending=[True, True, True])
        .unique(subset=["display_family"], keep="first", maintain_order=True)
        .drop("display_family")
    )
    top = (passed if not passed.is_empty() else diagnostic_ranked).head(12).to_dicts()
    lines = [
        "# 臺指期專業量化交易策略排行",
        "",
        f"日線資料截止：`{daily_cutoff}`；RPT tick-derived intraday features 截止：`{rpt_cutoff}`。本次研究使用 TAIFEX 官方日線、最後結算價、近三年法人期貨部位、RPT 5m/15m/60m 多時間框架特徵與 DuckDB/Parquet cache。執行時間約 `{elapsed:.1f}` 秒。",
        "",
        "## 結論",
        "",
    ]
    if champion:
        lines += [
            f"目前通過驗證的第一名是 **{champion['name']}**。它是成本、滑價、交易稅、保證金、停損、追蹤停損與 time stop 後的期貨策略，不是 raw NAV。",
            "",
            f"- Full CAGR：{_format_pct(champion.get('cagr'))}",
            f"- OOS CAGR：{_format_pct(champion.get('oos_cagr'))}",
            f"- 最近一年 CAGR：{_format_pct(champion.get('recent_1y_cagr'))}，窗口 `{champion.get('recent_1y_start')}` ~ `{champion.get('recent_1y_end')}`",
            f"- OOS MDD：{_format_pct(champion.get('oos_mdd'))}",
            f"- OOS Sortino：{_format_num(champion.get('oos_sortino'))}",
            f"- DSR：{_format_num(champion.get('dsr'))}",
            f"- Multi-config PBO：{_format_num(champion.get('pbo'))}",
            f"- 最低保證金 buffer：{_format_num(champion.get('min_margin_buffer'))}",
            f"- 最大名目槓桿：{_format_num(champion.get('max_leverage'))}",
            "",
        ]
    else:
        lines += [
            "本輪沒有策略同時通過 DSR、PBO、bootstrap、成本壓力與保證金 survival gate。這代表目前不能把任何臺指期候選升級成正式可上線 champion。",
            "下方表格是未通過策略的診斷排行，排序改用 OOS CAGR，目的是看哪一類訊號有研究價值；它不是可上線排行。",
            "",
        ]
    lines += [
        "## 策略排行",
        "",
        "| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | Profit Factor | SQN | 最大槓桿 | 最低 Margin Buffer |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(top, start=1):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(idx),
                    str(row.get("name")),
                    str(row.get("verdict")),
                    _format_pct(row.get("cagr")),
                    _format_pct(row.get("oos_cagr")),
                    _format_pct(row.get("recent_1y_cagr")),
                    _format_pct(row.get("ret_6m")),
                    _format_pct(row.get("ret_3m")),
                    _format_pct(row.get("ret_1m")),
                    _format_pct(row.get("oos_mdd")),
                    _format_num(row.get("oos_sortino")),
                    _format_num(row.get("dsr")),
                    _format_num(row.get("pbo")),
                    _format_num(row.get("profit_factor")),
                    _format_num(row.get("sqn")),
                    _format_num(row.get("max_leverage")),
                    _format_num(row.get("min_margin_buffer")),
                ]
            )
            + " |"
        )
    lines += [
        "",
        "## 驗證方法",
        "",
        "- 訊號在產生後全部 shift 一天，預設下一個交易日開盤成交，避免 look-ahead。",
        "- 每筆交易扣除固定手續費、股價指數期貨交易稅、滑價 tick 與 roll 額外滑價。",
        "- 部位大小由目標波動與保證金 survival constraint 同時限制；歷史或壓力情境爆倉者淘汰。",
        "- 籌碼候選納入現貨三大法人買賣超、融資融券、借券、外資持股比例變化，以及期貨三大法人未平倉與成交淨額。",
        "- 技術指標候選使用 `stockstats` 批次產生 RSI、MACD、Bollinger、KDJ、ADX/DMI、CCI、ATR、WR、MFI、TRIX、TEMA 與多組 SMA/EMA，再與手寫透明特徵交叉驗證。",
        "- H 模型候選不是未授權課程的完整公式，而是依公開資訊實作的可稽核 approximation：價差指標為主、量指標為濾網、槓桿交由 simulator 的 survival constraint 控制。",
        "- 每個候選都跑 2x 與 5x 成本壓力；正式通過至少需要 2x 成本 OOS CAGR 仍為正。",
        "- DSR 使用候選數作多重試驗修正；PBO 使用 multi-config CSCV，避免只看單一策略美化曲線。",
        f"- 本輪 multi-config PBO：`{pbo:.3f}`。",
        "",
        "## 重要限制",
        "",
        "- 本輪已使用長歷史 RPT tick 轉出的 5m/15m/60m 日內特徵，但 simulator 仍是日線開盤成交模型；真正日內進出場策略需要另建 intraday order simulator。",
        "- 保證金使用保守名目比例 proxy；若要進入 live pilot，必須接入券商或 TAIFEX point-in-time margin table。",
        "- Flow sleeve 只可作近三年 overlay，不能當成長期主模型，因為官方免費法人期貨資料只有 rolling 三年。",
        "",
        "## 資料來源",
        "",
        "- TAIFEX 期貨每日交易行情下載：https://www.taifex.com.tw/cht/3/dlFutDailyMarketView",
        "- TAIFEX 三大法人期貨契約：https://www.taifex.com.tw/cht/3/futContractsDateView?menuid1=03",
        "- TAIFEX 指數期貨最後結算價：https://www.taifex.com.tw/cht/5/futIndxFSP",
        "- TAIFEX 交易歷史資料申請：https://www.taifex.com.tw/cht/3/hisAppForm",
        "- TAIFEX E-Data Shop：https://edatashop.taifex.com.tw/zh/product/list/28",
        "- H 模型公開說明：https://axhuang.com/courses/%E7%AC%AC%E4%B8%80%E5%A1%8A%E9%87%91%E7%A3%9A%EF%BC%9Ah%E6%A8%A1%E5%9E%8B%E5%8E%9F%E7%90%86%E8%88%87%E8%A8%AD%E8%A8%88/",
        "- H 模型歷史資料說明：https://axhuang.com/product/%E5%BB%BA%E6%A7%8Bh%E6%A8%A1%E5%9E%8B%E7%9A%84%E6%AD%B7%E5%8F%B2%E8%B3%87%E6%96%99/",
        "- 台指期價差交易公開說明：https://futuresinvest90223.com/%E5%8F%B0%E6%8C%87%E6%9C%9F%E7%8F%BE%E8%B2%A8%E5%83%B9%E5%B7%AE/",
        "- 台指期基差與三大法人研究摘要：https://www.airitilibrary.com/Article/Detail/19937571-202003-202006240007-202006240007-39-62",
        "",
        "## Artifacts",
        "",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_professional/futures_strategy_summary.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_professional/futures_fast_screen.csv`",
    ]
    if champion:
        lines += [
            f"- `{paths.OUT_STRAT_LAB}/futures_tx_professional/champion_daily.csv`",
            f"- `{paths.OUT_STRAT_LAB}/futures_tx_professional/champion_fills.csv`",
            f"- `{paths.OUT_STRAT_LAB}/futures_tx_professional/champion_trades.csv`",
            f"- `{paths.OUT_STRAT_LAB}/futures_tx_professional/champion_vs_benchmarks.png`",
        ]
    else:
        lines.append("- 本輪無通過 gate 的 champion，因此未輸出 champion 交易 artifacts；未通過候選只保留在 summary 與 fast screen 診斷檔。")
    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run() -> None:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        daily_cutoff = con.execute("SELECT max(date) FROM taifex_futures_continuous WHERE product='TX'").fetchone()[0].isoformat()
        feature_paths = build_all_rpt_daily_features(products=("TX", "MTX", "TMF"), timeframes=("5m", "15m", "60m"))
        rpt_cutoff = "n/a"
        if feature_paths:
            rpt_cutoff = (
                pl.concat([pl.read_parquet(path).select("date") for path in feature_paths])
                .select(pl.col("date").max())
                .item()
                .isoformat()
            )
        frames = {product: load_product_frame(DB_PATH, product) for product in ["TX", "MTX", "TMF"]}
        candidates = default_candidate_grid()
        target_vols = [0.12, 0.20, 0.30, 0.45]
        stop_variants = [(1.5, 3.0), (2.5, 4.0), (None, 4.0)]

        base_daily: dict[str, pl.DataFrame] = {}
        base_fills: dict[str, pl.DataFrame] = {}
        base_trades: dict[str, pl.DataFrame] = {}
        base_summaries: dict[str, dict[str, object]] = {}
        fast_rows = []
        total_sims = len(candidates) * len(target_vols) * len(stop_variants)
        sim_count = 0
        print(f"[screen] candidates={len(candidates)} simulations={total_sims}")
        for candidate in candidates:
            if frames[candidate.product].height < 600:
                continue
            for tv in target_vols:
                for stop_atr, trail_atr in stop_variants:
                    sim_count += 1
                    if sim_count == 1 or sim_count % 100 == 0:
                        print(f"[screen] {sim_count}/{total_sims} {candidate.name} tv={tv:g} stop={stop_atr} trail={trail_atr}")
                    name, daily, fills, trades, summary = _simulate_candidate(
                        frames, candidate, target_vol=tv, stop_atr=stop_atr, trail_atr=trail_atr
                    )
                    if daily.height < 500:
                        continue
                    base_daily[name] = daily
                    base_fills[name] = fills
                    base_trades[name] = trades
                    base_summaries[name] = summary
                    fast_rows.append(_fast_row(name, daily, summary))

        fast = pl.DataFrame(fast_rows).sort("fast_score", descending=True)
        fast.write_csv(OUT_DIR / "futures_fast_screen.csv")
        finalist_names = fast.filter(~pl.col("margin_breach")).head(48)["name"].to_list()

        stress2_daily: dict[str, pl.DataFrame] = {}
        stress5_daily: dict[str, pl.DataFrame] = {}
        validated_rows: list[dict[str, object]] = []
        selected_daily: dict[str, pl.DataFrame] = {}
        for name in finalist_names:
            if (len(validated_rows) + 1) == 1 or (len(validated_rows) + 1) % 10 == 0:
                print(f"[validate] {len(validated_rows) + 1}/{len(finalist_names)} {name}")
            parts = name.split("_tv")
            candidate_name = parts[0]
            candidate = next(c for c in candidates if c.name == candidate_name)
            rest = parts[1]
            tv = float(rest.split("_")[0])
            stop_token = [p for p in rest.split("_") if p.startswith("sl") or p == "nostop"][0]
            trail_token = [p for p in rest.split("_") if p.startswith("tr") or p == "notrail"][0]
            stop_atr = None if stop_token == "nostop" else float(stop_token.replace("sl", ""))
            trail_atr = None if trail_token == "notrail" else float(trail_token.replace("tr", ""))
            _, s2, _, _, s2_summary = _simulate_candidate(frames, candidate, target_vol=tv, cost_multiplier=2.0, stop_atr=stop_atr, trail_atr=trail_atr)
            _, s5, _, _, s5_summary = _simulate_candidate(frames, candidate, target_vol=tv, cost_multiplier=5.0, stop_atr=stop_atr, trail_atr=trail_atr)
            stress2_daily[name] = s2
            stress5_daily[name] = s5
            row = validate_futures_daily(
                name,
                base_daily[name],
                trades=base_trades[name],
                simulator_summary=base_summaries[name],
                n_trials=max(66, len(base_daily)),
                config=ValidationConfig(oos_start_year=2010, oos_end_year=2026, min_trials_for_dsr=max(66, len(base_daily))),
            )
            row["stress_2x_oos_cagr"] = nav_metrics(_oos_daily(s2).select(["date", "nav"]), prefix="oos_")["oos_cagr"]
            row["stress_5x_oos_cagr"] = nav_metrics(_oos_daily(s5).select(["date", "nav"]), prefix="oos_")["oos_cagr"]
            row["stress_2x_margin_breach"] = bool(s2_summary.get("margin_breach", False))
            row["stress_5x_margin_breach"] = bool(s5_summary.get("margin_breach", False))
            validated_rows.append(row)
            selected_daily[name] = base_daily[name]

        # PM allocator over top non-margin finalists before final PBO/verdict.
        prelim = pl.DataFrame(validated_rows).sort("oos_cagr", descending=True)
        sleeve_names = prelim.filter(~pl.col("margin_breach")).head(10)["name"].to_list()
        pm_daily_by_name: dict[str, pl.DataFrame] = {}
        pm_weights_by_name: dict[str, pl.DataFrame] = {}
        for lookback in [42, 63, 126]:
            for top_k in [1, 2, 3]:
                for target_vol in [0.35, 0.50, 0.70]:
                    pm_name = f"PM_lagged_risk_budget_lb{lookback}_top{top_k}_tv{target_vol:g}"
                    daily, weights = combine_sleeve_returns(
                        {n: selected_daily[n] for n in sleeve_names},
                        lookback_days=lookback,
                        top_k=top_k,
                        target_vol=target_vol,
                    )
                    pm_daily_by_name[pm_name] = daily
                    pm_weights_by_name[pm_name] = weights

        all_for_pbo = {**selected_daily, **pm_daily_by_name}
        group_pbo = multi_config_pbo(all_for_pbo)

        final_rows = []
        for row in validated_rows:
            row["pbo"] = group_pbo
            row["verdict"] = verdict(row)
            row["objective"] = futures_objective(row)
            final_rows.append(row)

        for pm_name, daily in pm_daily_by_name.items():
            row = validate_futures_daily(
                pm_name,
                daily,
                trades=None,
                simulator_summary={
                    "product": "MULTI",
                    "max_leverage": 0.0,
                    "min_margin_buffer": min(float(r.get("min_margin_buffer", 999.0) or 999.0) for r in validated_rows),
                    "margin_breach": any(bool(r.get("margin_breach", False)) for r in validated_rows if r.get("name") in sleeve_names),
                },
                n_trials=max(66, len(all_for_pbo)),
                group_pbo=group_pbo,
                config=ValidationConfig(oos_start_year=2010, oos_end_year=2026, min_trials_for_dsr=max(66, len(all_for_pbo))),
            )
            # PM uses already costed sleeves; stress its returns by combining stressed sleeve streams.
            try:
                s2_daily, _ = combine_sleeve_returns(
                    {n: stress2_daily[n] for n in sleeve_names},
                    lookback_days=int(pm_name.split("_lb")[1].split("_")[0]),
                    top_k=int(pm_name.split("_top")[1].split("_")[0]),
                    target_vol=float(pm_name.split("_tv")[1]),
                )
                row["stress_2x_oos_cagr"] = nav_metrics(_oos_daily(s2_daily).select(["date", "nav"]), prefix="oos_")["oos_cagr"]
                s5_daily, _ = combine_sleeve_returns(
                    {n: stress5_daily[n] for n in sleeve_names},
                    lookback_days=int(pm_name.split("_lb")[1].split("_")[0]),
                    top_k=int(pm_name.split("_top")[1].split("_")[0]),
                    target_vol=float(pm_name.split("_tv")[1]),
                )
                row["stress_5x_oos_cagr"] = nav_metrics(_oos_daily(s5_daily).select(["date", "nav"]), prefix="oos_")["oos_cagr"]
            except Exception:
                row["stress_2x_oos_cagr"] = -1.0
                row["stress_5x_oos_cagr"] = -1.0
            row["verdict"] = verdict(row)
            row["objective"] = futures_objective(row)
            final_rows.append(row)

        summary = pl.DataFrame(final_rows).sort("objective", descending=True)
        summary.write_csv(OUT_DIR / "futures_strategy_summary.csv")

        passed = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
        champion_row = passed.head(1).to_dicts()[0] if not passed.is_empty() else None
        if champion_row:
            champion_name = str(champion_row["name"])
            if champion_name in pm_daily_by_name:
                champion_daily = pm_daily_by_name[champion_name]
                pm_weights_by_name[champion_name].write_csv(OUT_DIR / "champion_sleeve_weights.csv")
                pl.DataFrame().write_csv(OUT_DIR / "champion_fills.csv")
                pl.DataFrame().write_csv(OUT_DIR / "champion_trades.csv")
            else:
                champion_daily = base_daily[champion_name]
                base_fills[champion_name].write_csv(OUT_DIR / "champion_fills.csv")
                base_trades[champion_name].write_csv(OUT_DIR / "champion_trades.csv")
            champion_daily.write_csv(OUT_DIR / "champion_daily.csv")
            _write_chart(champion_daily, con, OUT_DIR / "champion_vs_benchmarks.png")
        _write_doc(summary, champion_row, daily_cutoff, rpt_cutoff, group_pbo, time.time() - t0)
        print(f"[done] futures research rows={summary.height} pbo={group_pbo:.3f} champion={champion_row['name'] if champion_row else 'NONE'}")
        print(f"[artifacts] {OUT_DIR}")
        print(f"[doc] {DOC_PATH}")
    finally:
        con.close()


if __name__ == "__main__":
    run()
