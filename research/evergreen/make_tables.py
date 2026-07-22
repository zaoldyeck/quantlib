"""Evergreen B 臂月末全池量化快照表生成(ev13_tables 格式)。

原始碼出處:EV13 預註冊 heredoc(2026-07-10 10:54,本檔為其正式落檔),
邏輯逐行保留以維持 protocol 一致——EV13 B 臂 agent 看到的就是這套規格。

格式:代碼 名稱|產業|最新YoY 3月YoY 加速|距52週高% 距120日高% 60日漲%
120日漲%|量能x 日均值M
- universe = data.eligibility(min_adv=5M)(內含價格/歷史門檻)
- 營收 PIT:發布日 avail(次月 10 日)asof;yoy3 = rolling_mean(3);
  加速 = rolling_mean(3) − rolling_mean(12);同 (公司, 月) 多筆取營收大者
- 名稱 = 最新申報簡稱;產業 = industry_taxonomy_pit 最新值截 4 字
- 價格欄 = 還原價;量能x = 5 日均量/(60 日均量+1);日均值 = 20 日
  median 成交值(百萬)

需要 cache_tables.py 為最新。
Run: uv run --project research python -m research.evergreen.make_tables 2023-01 2023-03
"""
from __future__ import annotations

import os
import sys
from datetime import date as Date

import duckdb
import polars as pl

from research.apex import data
from research import paths

C = "company_code"
OUT = "research/evergreen/data/ev13_tables"


def build_feats(panel: pl.DataFrame) -> pl.DataFrame:
    return (panel.sort([C, "date"])
            .with_columns([
                (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
                (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
                (pl.col("close") / pl.col("close").shift(60) - 1).over(C).alias("m60"),
                (pl.col("close") / pl.col("close").shift(120) - 1).over(C).alias("m120"),
                (pl.col("volume").cast(pl.Float64).rolling_mean(5)
                 / (pl.col("volume").cast(pl.Float64).rolling_mean(60) + 1))
                .over(C).alias("vs"),
                (pl.col("trade_value").cast(pl.Float64).rolling_median(20) / 1e6)
                .over(C).alias("advM"),
            ]).select(["date", C, "h52", "h120", "m60", "m120", "vs", "advM"]))


def load_rev(raw: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    rev = raw.sql(
        """SELECT company_code, year, month, monthly_revenue_yoy AS yoy
           FROM (SELECT *, row_number() OVER (PARTITION BY company_code, year, month
                 ORDER BY monthly_revenue DESC) rn FROM operating_revenue) WHERE rn=1"""
    ).pl()
    return (rev.sort([C, "year", "month"])
            .with_columns([
                pl.date(pl.col("year") + pl.col("month") // 12,
                        pl.col("month") % 12 + 1, 10).alias("avail"),
                pl.col("yoy").rolling_mean(3).over(C).alias("yoy3"),
                (pl.col("yoy").rolling_mean(3) - pl.col("yoy").rolling_mean(12))
                .over(C).alias("acc"),
            ]).select([C, "avail", "yoy", "yoy3", "acc"]).sort("avail"))


def main() -> None:
    months = sys.argv[1:]
    if not months:
        raise SystemExit("usage: make_tables.py YYYY-MM ...")
    con = data.connect()
    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    panel = data.common_stocks(
        data.load_panel(con, "2021-07-01", "2026-07-09", warmup_days=300))
    E = (data.eligibility(panel, min_adv=5_000_000.0)
         .filter(pl.col("eligible")).select(["date", C]))
    names = raw.sql(
        "SELECT DISTINCT company_code, last(company_name ORDER BY year*100+month) AS name "
        "FROM operating_revenue GROUP BY company_code").pl()
    tax = raw.sql(
        "SELECT company_code, industry FROM industry_taxonomy_pit "
        "WHERE industry IS NOT NULL QUALIFY row_number() OVER "
        "(PARTITION BY company_code ORDER BY effective_date DESC)=1").pl()
    rev = load_rev(raw)
    feats = build_feats(panel)
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    os.makedirs(OUT, exist_ok=True)

    for tag in months:
        y, m = int(tag[:4]), int(tag[5:7])
        m1 = Date(y + m // 12, m % 12 + 1, 1)
        eod = max(d for d in dates_all if d < m1)
        f = (feats.filter(pl.col("date") == eod)
             .join(E.filter(pl.col("date") == eod), on=["date", C], how="semi"))
        r = (rev.filter(pl.col("avail") <= eod).group_by(C).agg(
            [pl.col("yoy").last().alias("yoy_l"),
             pl.col("yoy3").last(), pl.col("acc").last()]))
        f = (f.join(r, on=C, how="left").join(names, on=C, how="left")
             .join(tax, on=C, how="left").sort(C))
        lines = [f"# {tag} 月末({eod})全池量化快照({f.height} 檔)。"
                 "欄位:代碼 名稱|產業|最新YoY 3月YoY 加速|距52週高% 距120日高% "
                 "60日漲% 120日漲%|量能x 日均值M"]
        for x in f.to_dicts():
            def n(v, p=False):
                return "na" if v is None else (f"{v*100:.0f}" if p else f"{v:.0f}")
            lines.append(
                f"{x[C]} {x['name'] or '?'}|{(x['industry'] or '?')[:4]}|"
                f"{n(x['yoy_l'])} {n(x['yoy3'])} {n(x['acc'])}|"
                f"{n(x['h52'], 1)} {n(x['h120'], 1)} {n(x['m60'], 1)} {n(x['m120'], 1)}|"
                f"{x['vs']:.1f} {n(x['advM'])}")
        path = f"{OUT}/{tag}.txt"
        open(path, "w").write("\n".join(lines) + "\n")
        print(f"{path}  {os.path.getsize(path) // 1024}KB  {f.height} 檔")


if __name__ == "__main__":
    main()
