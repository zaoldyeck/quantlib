"""EV30 — registry_v3 × 凍結 v3 引擎 baseline(Serenity 同窗)。

月中站位語義:每月站位日(10 日後首交易日)起入池、4 個站位月 union、
conviction max。引擎凍結參數。NAV 對比窗 2025-01-02 ~ 2026-07-03。

Run: uv run --project research python -m research.evergreen.ev30_baseline
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex import data
from research.evergreen.harvest import C, build_feats, harvest

_OPEN_END = Date(9999, 12, 31)  # 池籍開放上界(活到資料末端,含當天;見下方 last-day 修正)


def midmonth_membership(reg: pl.DataFrame, dates_all: list[Date],
                        pool_months: int = 4) -> pl.DataFrame:
    stance = {}
    for ym in sorted(reg["month"].unique().to_list()):
        y, m = int(ym[:4]), int(ym[5:7])
        cand = [d for d in dates_all if d.year == y and d.month == m and d.day > 10]
        if cand:  # 站位日須落在資料窗內;未來月/窗末月無站位日 → 略過
            stance[ym] = min(cand)  # (PIT 由日期窗處理,不再靠 load_registry 硬切月份)
    yms = sorted(stance)
    ordered = [stance[ym] for ym in yms]
    rows = []
    for i, ym in enumerate(yms):
        start = ordered[i]
        # 最後 pool_months 個 cohort 無「pool_months 之後的站位日」→ 活到資料末端。
        # 修:用開放上界而非 dates_all[-1],否則 `date < m_end` 排除最後一天
        # (2026-07-20:live「今天」永遠是最後一天,舊寫法令當日池空掉、advisor 無推薦)。
        end = ordered[i + pool_months] if i + pool_months < len(ordered) else _OPEN_END
        window = yms[max(0, i - pool_months + 1): i + 1]
        cur = (reg.filter(pl.col("month").is_in(window))
               .group_by("code").agg(pl.col("conviction").max()))
        for r in cur.to_dicts():
            rows.append({"m_start": start, "m_end": end, C: r["code"],
                         "conv": r["conviction"]})
    memb = pl.DataFrame(rows)
    days = [d for d in dates_all if d >= ordered[0]]
    return (pl.DataFrame({"date": days}).join(memb, how="cross")
            .filter((pl.col("date") >= pl.col("m_start"))
                    & (pl.col("date") < pl.col("m_end")))
            .select(["date", C, "conv"]).unique(subset=["date", C])
            .sort(["date", C]))


def main() -> None:
    reg = pl.read_parquet("research/evergreen/data/registry_v3.parquet")
    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2023-06-01", "2026-07-09", warmup_days=300))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    feats = build_feats(panel)
    memb = midmonth_membership(reg, dates_all)
    res = harvest(panel, feats, memb, Date(2024, 10, 14))
    nav = res.nav.sort("date")
    sw = nav.filter((pl.col("date") >= Date(2025, 1, 2))
                    & (pl.col("date") <= Date(2026, 7, 3)))
    yrs = (sw["date"][-1] - sw["date"][0]).days / 365.25
    cagr = (sw["nav"][-1] / sw["nav"][0]) ** (1 / yrs) - 1
    mdd = (sw["nav"] / sw["nav"].cum_max() - 1).min()
    inv = (nav["invested"] / nav["nav"]).mean()
    print(f"registry_v3 × 凍結引擎:Serenity 同窗 CAGR {cagr:.1%}  "
          f"MDD {mdd:.1%}  交易 {res.trades.height}  日均投入 {inv:.0%}")
    print("對照:Serenity 253.3%/−18.0;v1 真基準同窗 219.1%/−43.4")


if __name__ == "__main__":
    main()
