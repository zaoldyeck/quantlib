"""EV24 驗屍:v2 端到端反轉的來源——逐年報酬、MDD 時段、逐月標記差異。

Run: uv run --project . python -m quantlib.evergreen.ev24_autopsy
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from quantlib.apex import data
from quantlib.evergreen.harvest import build_feats, harvest, monthly_membership


def main() -> None:
    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=300))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    feats = build_feats(panel)
    navs = {}
    trades = {}
    for name in ["v1", "v2"]:
        reg = pl.read_parquet(f"src/quantlib/evergreen/data/registry_{name}.parquet")
        memb = monthly_membership(reg, dates_all, Date(2022, 7, 1))
        res = harvest(panel, feats, memb, Date(2022, 7, 1))
        navs[name] = res.nav.sort("date")
        trades[name] = res.trades

    print("=== 逐年報酬 ===")
    for y in range(2022, 2027):
        line = f"{y}:"
        for name, nav in navs.items():
            seg = nav.filter(pl.col("date").dt.year() == y)
            if seg.height > 1:
                line += f"  {name} {seg['nav'][-1]/seg['nav'][0]-1:+8.1%}"
        print(line)

    print("\n=== v2 最深回撤時段 ===")
    nav2 = navs["v2"].with_columns(
        (pl.col("nav") / pl.col("nav").cum_max() - 1).alias("dd"))
    worst = nav2.filter(pl.col("dd") == nav2["dd"].min())["date"][0]
    print(f"MDD 谷底日:{worst}  DD {nav2['dd'].min():.1%}")
    print(nav2.filter(pl.col("dd") < -0.40).group_by(
        pl.col("date").dt.strftime("%Y-%m").alias("ym")).len().sort("ym"))

    print("\n=== v2 最痛交易 top10 ===")
    print(trades["v2"].sort("ret_net").head(10)
          .select(["company_code", "entry_date", "exit_date", "ret_net",
                   "days_held", "exit_reason"]))

    print("\n=== 逐月標記重疊(v1∩v2 / v1∪v2)===")
    v1 = pl.read_parquet("src/quantlib/evergreen/data/registry_v1.parquet")
    v2 = pl.read_parquet("src/quantlib/evergreen/data/registry_v2.parquet")
    rows = []
    for m in sorted(v1["month"].unique().to_list()):
        s1 = set(v1.filter(pl.col("month") == m)["code"].to_list())
        s2 = set(v2.filter(pl.col("month") == m)["code"].to_list())
        if s1 | s2:
            rows.append({"m": m[:7], "jac": len(s1 & s2) / len(s1 | s2),
                         "n1": len(s1), "n2": len(s2)})
    j = pl.DataFrame(rows)
    print(f"平均 Jaccard {j['jac'].mean():.2f};最低 5 個月:")
    print(j.sort("jac").head(5))


if __name__ == "__main__":
    main()
