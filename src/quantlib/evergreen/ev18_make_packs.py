"""EV18 全感官歸因 pack 生成:EV1 原樣本 + 6 個新資料維度。

樣本沿用 EV1(168 暴漲,21 批)與 EV1b(56 偽形對照,7 批),原欄位
(rev_yoy_12m_before / price_120d_before / price_60d_after / gain)逐位元
保留,每樣本追加(PIT 至 t0 前一交易日):
  chips      外資/投信 20・60 日淨買佔成交值%、融資 20・60 日Δ%、
             借券 20 日Δ%(2016 前 na)、外資持股%
  valuation  P/E、P/B、殖利率 + 各自 3 年(756 交易日)分位
  financials 近 4 已公佈季累計毛利率%・營益率%(發布 lag:Q1→5/15、
             Q2→8/14、Q3→11/14、Q4→次年 3/31)
  insider    t0 前 180 天內部人申讓筆數
  buyback    t0 前 180 天庫藏股公告數
  cap_reduction t0 前 365 天減資次數
輸出 src/quantlib/evergreen/data/ev18_packs/{surge,control}_NN.json。

需要 cache 最新。Run: uv run --project . python -m quantlib.evergreen.ev18_make_packs
"""
from __future__ import annotations

import json
import os
import re
from datetime import date as Date, timedelta

import duckdb
from quantlib import paths

SCRATCH = ("/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/"
           "3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad")
OUT = "src/quantlib/evergreen/data/ev18_packs"


def load_batches(js_file: str) -> list:
    src = open(f"{SCRATCH}/{js_file}").read()
    m = re.search(r"const BATCHES = (\[.*?\])\n", src, re.S)
    return json.loads(m.group(1))


def fin_asof(t0: Date) -> list[tuple[int, int]]:
    """t0 已公佈的最近 4 個(year, quarter),依台股發布 lag。"""
    pubs = []
    for y in range(t0.year - 3, t0.year + 1):
        for q, pub in [(1, Date(y, 5, 15)), (2, Date(y, 8, 14)),
                       (3, Date(y, 11, 14)), (4, Date(y + 1, 3, 31))]:
            if pub <= t0:
                pubs.append((pub, y, q))
    pubs.sort()
    return [(y, q) for _, y, q in pubs[-4:]]


class Enricher:
    def __init__(self) -> None:
        self.raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)

    def q1(self, sql: str, args: list):
        return self.raw.execute(sql, args).fetchone()

    def chips(self, code: str, t0: Date) -> dict:
        r = self.q1("""
        WITH win AS (SELECT date, row_number() OVER (ORDER BY date DESC) rn
                     FROM (SELECT DISTINCT date FROM daily_quote WHERE date < ?)
                     ORDER BY date DESC LIMIT 60)
        SELECT
          100.0*sum(CASE WHEN w.rn<=20 THEN t.foreign_investors_difference*q.closing_price END)
            / nullif(sum(CASE WHEN w.rn<=20 THEN q.trade_value END),0),
          100.0*sum(t.foreign_investors_difference*q.closing_price)/nullif(sum(q.trade_value),0),
          100.0*sum(CASE WHEN w.rn<=20 THEN t.trust_difference*q.closing_price END)
            / nullif(sum(CASE WHEN w.rn<=20 THEN q.trade_value END),0),
          100.0*sum(t.trust_difference*q.closing_price)/nullif(sum(q.trade_value),0)
        FROM daily_trading_details t
        JOIN daily_quote q USING (market, date, company_code)
        JOIN win w ON t.date = w.date
        WHERE t.company_code = ?""", [t0, code]) or (None,) * 4
        m = self.q1("""
        WITH win AS (SELECT date, row_number() OVER (ORDER BY date DESC) rn
                     FROM (SELECT DISTINCT date FROM margin_transactions WHERE date < ?)
                     ORDER BY date DESC LIMIT 60)
        SELECT
          100.0*(max(CASE WHEN w.rn=1 THEN margin_balance END)
                 - max(CASE WHEN w.rn=20 THEN margin_balance END))
            / nullif(max(CASE WHEN w.rn=20 THEN margin_balance END),0),
          100.0*(max(CASE WHEN w.rn=1 THEN margin_balance END)
                 - max(CASE WHEN w.rn=60 THEN margin_balance END))
            / nullif(max(CASE WHEN w.rn=60 THEN margin_balance END),0)
        FROM margin_transactions mt JOIN win w ON mt.date = w.date
        WHERE mt.company_code = ?""", [t0, code]) or (None, None)
        s = self.q1("""
        WITH win AS (SELECT date, row_number() OVER (ORDER BY date DESC) rn
                     FROM (SELECT DISTINCT date FROM sbl_borrowing WHERE date < ?)
                     ORDER BY date DESC LIMIT 20)
        SELECT 100.0*(max(CASE WHEN w.rn=1 THEN daily_balance END)
                      - max(CASE WHEN w.rn=20 THEN daily_balance END))
                 / nullif(max(CASE WHEN w.rn=20 THEN daily_balance END),0)
        FROM sbl_borrowing sb JOIN win w ON sb.date = w.date
        WHERE sb.company_code = ?""", [t0, code]) or (None,)
        f = self.q1("""SELECT foreign_held_ratio FROM foreign_holding_ratio
                       WHERE company_code = ? AND date < ?
                       ORDER BY date DESC LIMIT 1""", [code, t0]) or (None,)

        def n(v):
            return None if v is None else round(v, 1)

        return {"外資20日淨買佔成交%": n(r[0]), "外資60日": n(r[1]),
                "投信20日": n(r[2]), "投信60日": n(r[3]),
                "融資20日Δ%": n(m[0]), "融資60日Δ%": n(m[1]),
                "借券20日Δ%": n(s[0]), "外資持股%": n(f[0])}

    def valuation(self, code: str, t0: Date) -> dict:
        r = self.q1("""
        WITH h AS (SELECT price_to_earning_ratio pe, price_book_ratio pb,
                          dividend_yield dy, date
                   FROM stock_per_pbr WHERE company_code = ? AND date < ?
                   ORDER BY date DESC LIMIT 756),
        cur AS (SELECT pe, pb, dy FROM h ORDER BY date DESC LIMIT 1)
        SELECT cur.pe, cur.pb, cur.dy,
               100.0*sum(CASE WHEN h.pe <= cur.pe THEN 1 END)/count(h.pe),
               100.0*sum(CASE WHEN h.pb <= cur.pb THEN 1 END)/count(h.pb)
        FROM h, cur GROUP BY cur.pe, cur.pb, cur.dy""",
                    [code, t0]) or (None,) * 5

        def n(v):
            return None if v is None else round(v, 1)

        return {"PE": n(r[0]), "PB": n(r[1]), "殖利率%": n(r[2]),
                "PE三年分位%": n(r[3]), "PB三年分位%": n(r[4])}

    def financials(self, code: str, t0: Date) -> dict:
        qs = fin_asof(t0)
        gm, om = [], []
        for y, q in qs:
            r = self.q1("""
            SELECT
              100.0*max(CASE WHEN title LIKE '營業毛利%' THEN value END)
                / nullif(max(CASE WHEN title IN ('營業收入','營業收入合計') THEN value END),0),
              100.0*max(CASE WHEN title LIKE '營業利益%' THEN value END)
                / nullif(max(CASE WHEN title IN ('營業收入','營業收入合計') THEN value END),0)
            FROM is_progressive_raw
            WHERE company_code = ? AND year = ? AND quarter = ?""",
                        [code, y, q]) or (None, None)
            gm.append(None if r[0] is None else round(r[0], 1))
            om.append(None if r[1] is None else round(r[1], 1))
        return {"近4季累計毛利率%": gm, "近4季累計營益率%": om}

    def events(self, code: str, t0: Date) -> dict:
        ins = self.q1("""SELECT count(*) FROM insider_holding
                         WHERE company_code = ? AND report_date BETWEEN ? AND ?""",
                      [code, t0 - timedelta(days=180), t0])[0]
        bb = self.q1("""SELECT count(*) FROM treasury_stock_buyback
                        WHERE company_code = ? AND announce_date BETWEEN ? AND ?""",
                     [code, t0 - timedelta(days=180), t0])[0]
        cr = self.q1("""SELECT count(*) FROM capital_reduction
                        WHERE company_code = ? AND date BETWEEN ? AND ?""",
                     [code, t0 - timedelta(days=365), t0])[0]
        return {"內部人申讓筆數_180日": ins, "庫藏股公告_180日": bb,
                "減資_365日": cr}

    def industry(self, code: str, t0: Date, fallback: str) -> str:
        """正規化 PIT 產業(industry_taxonomy_pit asof t0),優先於原欄。"""
        r = self.q1("""SELECT industry FROM industry_taxonomy_pit
                       WHERE company_code = ? AND effective_date <= ?
                         AND industry IS NOT NULL
                       ORDER BY effective_date DESC LIMIT 1""", [code, t0])
        return r[0] if r else fallback

    def enrich(self, s: dict) -> dict:
        t0 = Date.fromisoformat(s["t0"])
        return {**s,
                "industry": self.industry(s["code"], t0, s.get("industry", "?")),
                "chips": self.chips(s["code"], t0),
                "valuation": self.valuation(s["code"], t0),
                "financials": self.financials(s["code"], t0),
                "corp_events": self.events(s["code"], t0)}


def main() -> None:
    os.makedirs(OUT, exist_ok=True)
    en = Enricher()
    for js, tag in [("ev1_attribution.js", "surge"),
                    ("ev1b_control.js", "control")]:
        batches = load_batches(js)
        # BATCHES 可能是平面樣本列表或批次列表
        if isinstance(batches[0], list):
            groups = batches
        else:
            groups = [batches[i:i + 8] for i in range(0, len(batches), 8)]
        for i, g in enumerate(groups):
            enriched = [en.enrich(s) for s in g]
            path = f"{OUT}/{tag}_{i:02d}.json"
            json.dump(enriched, open(path, "w"), ensure_ascii=False)
            print(f"{path}  {len(enriched)} 檔  {os.path.getsize(path)//1024}KB")


if __name__ == "__main__":
    main()
