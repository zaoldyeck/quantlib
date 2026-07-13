"""Evergreen v3 席位引擎收割(凍結參數)——registry 標記 → NAV。

v3 凍結拓撲:membership(標記 → 4 個月池籍 union、conviction max)→
h120>0.7 無接刀濾網 → conv×h52×mom 幾何排位 → conviction 線性加權席位
clip(0.10, 0.30) → simulate(5 席、日新倉上限 2、trail 35%、loser_time_stop
30、池籍輪換出場)。

兩種 membership 語義:
- monthly_membership:月頻 registry(month, code, conviction)——標記月
  月初起池籍 4 個「標記月」(registry_v1 原語義)。
- daily_membership:日頻標記(signal_date, code, conviction)——
  signal_date 次一交易日起池籍 84 個交易日(EV11+ 事件制語義)。

需要 cache 最新。Run(champion 重現驗證):
  uv run --project research python -m research.evergreen.harvest
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
POOL_MONTHS = 4          # 月頻池籍(標記月數)
POOL_TDAYS = 84          # 日頻池籍(交易日;≈4 個月)


def month_firsts(dates: list[Date]) -> list[Date]:
    out, cur = [], None
    for d in dates:
        if (d.year, d.month) != cur:
            out.append(d)
            cur = (d.year, d.month)
    return out


def build_feats(panel: pl.DataFrame) -> pl.DataFrame:
    return (panel.sort([C, "date"])
            .with_columns([
                (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
                (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
                (pl.col("close").shift(5) / pl.col("close").shift(126) - 1)
                .over(C).alias("mom"),
            ]).select(["date", C, "h120", "h52", "mom"]))


def monthly_membership(reg: pl.DataFrame, dates_all: list[Date],
                       start: Date, end: Date | None = None,
                       lead_tdays: int = 0,
                       lead_entry_only: bool = False) -> pl.DataFrame:
    """registry_v1 語義:month(YYYY-MM-01 str)標記 → 該月起 4 個月池籍。

    lead_tdays > 0:池籍窗整體提前 N 個交易日(EV15 日頻化理論上界用;
    標記資訊當時尚不存在,僅作天花板估計,非可實現策略)。
    lead_entry_only=True 時只提前入池日、出池日不動(消融:分解「早入場」
    與「早出場」各自的貢獻)。
    """
    from datetime import timedelta

    end = end or dates_all[-1]
    days = [d for d in dates_all if start <= d <= end]
    months = month_firsts(days)
    idx = {d: i for i, d in enumerate(dates_all)}

    def shift(d: Date) -> Date:
        return dates_all[max(0, idx[d] - lead_tdays)] if lead_tdays else d

    rows = []
    for i, md in enumerate(months):
        nxt = months[i + 1] if i + 1 < len(months) else end + timedelta(days=1)
        # 月份語義比對(YYYY-MM):防 registry 月鍵(日曆月首 vs 首交易日)
        # 與交易日曆錯位——v1 曾因 -01 正規化導致 21 個月標記靜默失效
        window = [m.isoformat()[:7] for m in months[max(0, i - POOL_MONTHS + 1): i + 1]]
        cur = (reg.filter(pl.col("month").str.slice(0, 7).is_in(window))
               .group_by("code").agg(pl.col("conviction").max()))
        m0 = shift(md)
        m1 = nxt if lead_entry_only else (shift(nxt) if nxt in idx else nxt)
        for r in cur.to_dicts():
            rows.append({"m_start": m0, "m_end": m1, C: r["code"],
                         "conv": r["conviction"]})
    memb = pl.DataFrame(rows)
    day_df = pl.DataFrame({"date": days})
    return (day_df.join(memb, how="cross")
            .filter((pl.col("date") >= pl.col("m_start"))
                    & (pl.col("date") < pl.col("m_end")))
            .select(["date", C, "conv"]).unique(subset=["date", C])
            .sort(["date", C]))


def daily_membership(labels: pl.DataFrame, dates_all: list[Date],
                     start: Date, end: Date | None = None) -> pl.DataFrame:
    """事件制語義:signal_date(Date)→ 次一交易日起池籍 POOL_TDAYS 交易日。

    labels 欄位:code, signal_date, conviction。同 (code, date) 取 conv max。
    """
    end = end or dates_all[-1]
    idx = {d: i for i, d in enumerate(dates_all)}
    rows = []
    for r in labels.to_dicts():
        sd = r["signal_date"]
        nxts = [d for d in dates_all if d > sd]
        if not nxts:
            continue
        i0 = idx[nxts[0]]
        for d in dates_all[i0: i0 + POOL_TDAYS]:
            if start <= d <= end:
                rows.append({"date": d, C: r["code"], "conv": r["conviction"]})
    return (pl.DataFrame(rows)
            .group_by(["date", C]).agg(pl.col("conv").max())
            .sort(["date", C]))


def harvest(panel: pl.DataFrame, feats: pl.DataFrame, membership: pl.DataFrame,
            start: Date, end: Date | None = None):
    """v3 凍結引擎:membership → score/weight → simulate。"""
    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (membership.join(feats, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > 0.7)
          .with_columns((rank("conv") * rank("h52") * rank("mom")).alias("score"))
          .with_columns(((pl.col("conv") / pl.col("conv").mean().over("date")) / 5)
                        .clip(0.10, 0.30).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    days = [d for d in dates_all if d >= start and (end is None or d <= end)]
    all_codes = membership[C].unique().to_list()
    flag = (pl.DataFrame({"date": days})
            .join(pl.DataFrame({C: all_codes}), how="cross")
            .join(membership.select(["date", C]), on=["date", C], how="anti")
            .sort(["date", C]))
    pan = panel if end is None else panel.filter(pl.col("date") <= end)
    return simulate(pan, sc, exit_flags=flag, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                    exit_spec=ExitSpec(trailing_stop=0.35, loser_time_stop=30),
                    start=start)


def main() -> None:
    """Champion 重現驗證:registry_v1 全量,須重現 P5 73.0 / OOS +355%。"""
    from research.apex.experiments.g01_ml_ranker import kpi

    reg = pl.read_parquet("research/evergreen/data/registry_v1.parquet")
    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=300))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    feats = build_feats(panel)
    memb = monthly_membership(reg, dates_all, Date(2022, 7, 1))
    res = harvest(panel, feats, memb, Date(2022, 7, 1))
    nav = res.nav.sort("date")
    tr_nav = nav.filter(pl.col("date") <= Date(2025, 6, 30))
    k_tr = kpi(tr_nav)
    oos = nav.filter(pl.col("date") > Date(2025, 6, 30))
    oos_ret = oos["nav"][-1] / oos["nav"][0] - 1
    print(f"train:CAGR {k_tr['cagr']:.1%}  P5 {k_tr['p5']:.1%}  "
          f"MDD {k_tr['mdd']:.1%}")
    print(f"OOS 2025-07→:總報酬 {oos_ret:+.1%}")
    # Serenity 驗證窗(2025-01-02~2026-07-03,registry lag0 = CAGR 253.3% /
    # MDD −18.0%)同窗對比
    sw = nav.filter((pl.col("date") >= Date(2025, 1, 2))
                    & (pl.col("date") <= Date(2026, 7, 3)))
    yrs = (sw["date"][-1] - sw["date"][0]).days / 365.25
    cagr = (sw["nav"][-1] / sw["nav"][0]) ** (1 / yrs) - 1
    mdd = (sw["nav"] / sw["nav"].cum_max() - 1).min()
    print(f"Serenity 同窗(2025-01-02~2026-07-03):CAGR {cagr:.1%}  "
          f"MDD {mdd:.1%}(Serenity 253.3% / −18.0%)")


if __name__ == "__main__":
    main()
