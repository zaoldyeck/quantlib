"""EV25 月中站位表:asof = 每月 10 日後首交易日(月報甫公佈)+ 籌碼欄。

與 ev17 表同規格(9 欄 + 籌碼 5 欄),唯一差異 = asof 時點:agent 看到
剛公佈的上月營收(比月初站位新一個月)。輸出 ev25_tables/。

需要 cache 最新。Run:
  uv run --project research python -m research.evergreen.ev25_make_tables 2023-02 2023-08 2024-03 2025-04
"""
from __future__ import annotations

import os
import sys
from datetime import date as Date

import duckdb
import polars as pl

from research.apex import data
from research.evergreen.make_tables import C, build_feats, load_rev
from research.evergreen.make_tables_chips import chips_asof

OUT = "research/evergreen/data/ev25_tables"


def main() -> None:
    months = sys.argv[1:]
    if not months:
        raise SystemExit("usage: ev25_make_tables.py YYYY-MM ...")
    con = data.connect()
    raw = duckdb.connect("research/cache.duckdb", read_only=True)
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
        eod = min(d for d in dates_all if d > Date(y, m, 10))
        f = (feats.filter(pl.col("date") == eod)
             .join(E.filter(pl.col("date") == eod), on=["date", C], how="semi"))
        r = (rev.filter(pl.col("avail") <= eod).group_by(C).agg(
            [pl.col("yoy").last().alias("yoy_l"),
             pl.col("yoy3").last(), pl.col("acc").last()]))
        f = (f.join(r, on=C, how="left").join(names, on=C, how="left")
             .join(tax, on=C, how="left").sort(C))
        chips = chips_asof(raw, eod.isoformat())
        lines = [f"# {tag} 月中({eod},上月月報甫公佈)全池量化快照({f.height} 檔)。"
                 "欄位:代碼 名稱|產業|最新月YoY 3月YoY均 加速度|距52週高% 距120日高% "
                 "60日漲% 120日漲%|量能比 日均值M|外持% 外20 投20 資20 券20"
                 "(外資持股比;外資/投信20日淨買佔成交值%;融資/借券餘額20日變化%)"]
        for x in f.to_dicts():
            def n(v, p=False):
                return "na" if v is None else (f"{v*100:.0f}" if p else f"{v:.0f}")
            code = x[C]
            lines.append(
                f"{code} {x['name'] or '?'}|{(x['industry'] or '?')[:4]}|"
                f"{n(x['yoy_l'])} {n(x['yoy3'])} {n(x['acc'])}|"
                f"{n(x['h52'], 1)} {n(x['h120'], 1)} {n(x['m60'], 1)} {n(x['m120'], 1)}|"
                f"{x['vs']:.1f} {n(x['advM'])}|{chips.get(code, 'na na na na na')}")
        path = f"{OUT}/{tag}.txt"
        open(path, "w").write("\n".join(lines) + "\n")
        print(f"{path}  {os.path.getsize(path)//1024}KB  {f.height} 檔  asof {eod}")


if __name__ == "__main__":
    main()
