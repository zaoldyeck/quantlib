"""EV14 端到端對決:A 臂(日頻純質化)vs registry_v1 切片(B 月頻現任)。

同一 v3 凍結引擎(harvest.harvest),NAV 窗 2023-01-03 ~ 2023-11-30。
- v1 切片:registry_v1 的 2023-01~06 標記,月頻語義(標記月初起池籍
  4 個標記月)——v1 原生 PIT(月初出勤,用上月底前資訊)。
- A 臂:EV14 標記(+ EV13 A 臂 2023-02),事件制語義(signal_date+1
  入池,池籍 84 交易日)——判斷只用事件日前資訊。
兩臂各帶真實形態上場;起跑日相同、冷啟動相同(標記皆自 2023-01 起)。

判準(LEDGER EV14 預註冊):主 = 同窗 NAV 總報酬,|差|<5pp 平手;
輔 = MDD、交易數、逐月標記行為。

Run: uv run --project research python -m research.evergreen.ev14_duel
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex import data
from research.evergreen.harvest import (C, build_feats, daily_membership,
                                        harvest, monthly_membership)

START = Date(2023, 1, 3)
END = Date(2023, 11, 30)
MONTHS = [f"2023-0{m}-01" for m in range(1, 7)]


def report(name: str, res) -> dict:
    nav = res.nav.sort("date").filter(pl.col("date") <= END)
    total = nav["nav"][-1] / nav["nav"][0] - 1
    peak = nav["nav"].cum_max()
    mdd = (nav["nav"] / peak - 1).min()
    closed = res.trades.filter(pl.col("exit_reason") != "open")
    win = (closed["ret_net"] > 0).mean() if closed.height else float("nan")
    print(f"{name}:總報酬 {total:+.1%}  MDD {mdd:.1%}  "
          f"交易 {res.trades.height}(勝率 {win:.0%})")
    with pl.Config(tbl_rows=25, tbl_width_chars=110):
        print(res.trades.sort("ret_net", descending=True)
              .select(["company_code", "entry_date", "exit_date",
                       "ret_net", "days_held", "exit_reason"]))
    return {"name": name, "total": total, "mdd": mdd,
            "trades": res.trades.height}


def main() -> None:
    duel = pl.read_parquet("research/evergreen/data/ev14_duel_labels.parquet")
    a13 = (pl.read_parquet("research/evergreen/data/ev13_duel_labels.parquet")
           .filter((pl.col("arm") == "A") & (pl.col("month") == "2023-02")))
    a_labels = (pl.concat([duel, a13], how="diagonal")
                .filter(pl.col("arm") == "A")
                .with_columns(pl.col("signal_date").str.to_date())
                .select(["code", "signal_date", "conviction"]))
    print(f"A 臂標記:{a_labels.height} 筆,"
          f"{sorted(set(d.strftime('%Y-%m') for d in a_labels['signal_date']))}")

    reg = pl.read_parquet("research/evergreen/data/registry_v1.parquet")
    v1 = reg.filter(pl.col("month").is_in(MONTHS))
    print(f"v1 切片:{v1.height} 筆(2023-01~06)")

    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2021-06-01", "2023-12-31", warmup_days=0))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    feats = build_feats(panel)

    memb_v1 = monthly_membership(v1, dates_all, START, END)
    memb_a = daily_membership(a_labels, dates_all, START, END)
    # 消融:A 臂標記改月頻入池(signal_date snap 到該月最後交易日,次一
    # 交易日 = 下月初入池)——把「日頻 vs 月頻」與「Agent 輸入」兩因素拆開
    month_last = {(d.year, d.month): d for d in dates_all}
    a_snap = a_labels.with_columns(
        pl.col("signal_date").map_elements(
            lambda d: month_last[(d.year, d.month)], return_dtype=pl.Date))
    memb_am = daily_membership(a_snap, dates_all, START, END)
    print(f"membership 天×檔:v1 {memb_v1.height}  A {memb_a.height}  "
          f"A月頻 {memb_am.height}\n")

    rows = [
        report("v1 切片(B 月頻現任)", harvest(panel, feats, memb_v1, START, END)),
        report("A 臂(日頻純質化)   ", harvest(panel, feats, memb_a, START, END)),
        report("A 臂消融(月頻入池) ", harvest(panel, feats, memb_am, START, END)),
    ]
    diff = rows[1]["total"] - rows[0]["total"]
    verdict = ("A 勝" if diff > 0.05 else "v1 勝(A 路線關閉)" if diff < -0.05
               else "平手(±5pp 內)→ v1 留任")
    print(f"\nA − v1 = {diff:+.1%} → 裁決:{verdict}")


if __name__ == "__main__":
    main()
