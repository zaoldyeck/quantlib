"""回溯標記先導 — 機械種子訊號(PIT,防倖存者偏誤).

對指定各月的標記日(該月首個交易日)計算「當時可見」的客觀市場訊號:
1. 動能聚類:ret_60d top 60(價 ≥20、ADV20 ≥5000 萬),按 industry_taxonomy_pit
   (asof 標記日)聚類,輸出聚類 ≥3 檔的產業。
2. 營收加速聚類:PIT 營收(report_date = 資料月次月 10 日的保守慣例)yoy_3m ≥ 50%
   且較前 3 月加速,同樣聚類。

種子餵給標記 agent 作強制檢核清單——當時熱、後來崩的主題必然在種子中,
agent 必須對其產出入冊/拒絕記錄。這是倖存者偏誤的機械防禦。

限制(如實):cache 僅 TWSE(TPEx pipeline gap);ret 用未調整收盤價(粗訊號,
除權息噪音對聚類判定影響二階)。

Run: uv run --project research python -m research.serenity.backfill.seed_signals --months 2023-01 2023-02 2023-03 2023-04 2023-05 2023-06
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import duckdb
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
CACHE = paths.CACHE_DB
SEEDS = Path(__file__).parent / "seeds"


def label_day(con, ym: str) -> date:
    y, m = map(int, ym.split("-"))
    return con.execute(
        "SELECT min(date) FROM daily_quote WHERE date >= ?", [date(y, m, 1)]
    ).fetchone()[0]


def momentum_clusters(con, day: date) -> list[dict]:
    rows = con.execute(
        """
        WITH px AS (
            SELECT company_code, date, closing_price,
                   lag(closing_price, 60) OVER (PARTITION BY company_code ORDER BY date) AS c60,
                   avg(closing_price * trade_volume) OVER (
                       PARTITION BY company_code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ) AS adv20
            FROM daily_quote
        ),
        snap AS (
            SELECT company_code, closing_price, closing_price / c60 - 1 AS ret_60d, adv20
            FROM px WHERE date = ? AND c60 > 0 AND closing_price >= 20 AND adv20 >= 50e6
        ),
        tax AS (
            SELECT company_code, industry FROM (
                SELECT company_code, industry,
                       row_number() OVER (PARTITION BY company_code ORDER BY effective_date DESC) rn
                FROM industry_taxonomy_pit WHERE effective_date <= ?
            ) WHERE rn = 1
        )
        SELECT s.company_code, t.industry, round(s.ret_60d * 100, 1) AS ret60_pct
        FROM snap s JOIN tax t USING (company_code)
        ORDER BY s.ret_60d DESC LIMIT 60
        """,
        [day, day],
    ).fetchall()
    by_ind: dict[str, list] = {}
    for code, ind, ret in rows:
        by_ind.setdefault(ind or "(未分類)", []).append({"code": code, "ret60_pct": ret})
    return [
        {"industry": ind, "members": ms}
        for ind, ms in sorted(by_ind.items(), key=lambda kv: -len(kv[1]))
        if len(ms) >= 3
    ]


def revenue_clusters(con, day: date) -> list[dict]:
    rows = con.execute(
        """
        WITH rev AS (
            SELECT company_code, make_date(year, month, 1) AS ym, monthly_revenue_yoy AS yoy,
                   -- PIT:資料月的保守可見日 = 次月 10 日
                   make_date(year, month, 1) + INTERVAL 40 DAY AS visible_from
            FROM operating_revenue WHERE monthly_revenue_yoy IS NOT NULL
        ),
        vis AS (SELECT * FROM rev WHERE visible_from <= ?),
        r AS (
            SELECT company_code, ym, yoy,
                   avg(yoy) OVER (PARTITION BY company_code ORDER BY ym ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS yoy3,
                   avg(yoy) OVER (PARTITION BY company_code ORDER BY ym ROWS BETWEEN 5 PRECEDING AND 3 PRECEDING) AS yoy3_prev,
                   row_number() OVER (PARTITION BY company_code ORDER BY ym DESC) rn
            FROM vis
        ),
        snap AS (
            SELECT company_code, round(yoy3, 0) AS yoy3, round(yoy3_prev, 0) AS yoy3_prev
            FROM r WHERE rn = 1 AND yoy3 >= 50 AND yoy3 > yoy3_prev
        ),
        tax AS (
            SELECT company_code, industry FROM (
                SELECT company_code, industry,
                       row_number() OVER (PARTITION BY company_code ORDER BY effective_date DESC) rn
                FROM industry_taxonomy_pit WHERE effective_date <= ?
            ) WHERE rn = 1
        ),
        liq AS (
            SELECT company_code FROM (
                SELECT company_code, avg(closing_price * trade_volume) AS adv
                FROM daily_quote WHERE date <= ? AND date >= ? - INTERVAL 30 DAY
                GROUP BY company_code
            ) WHERE adv >= 50e6
        )
        SELECT s.company_code, t.industry, s.yoy3, s.yoy3_prev
        FROM snap s JOIN tax t USING (company_code) JOIN liq USING (company_code)
        """,
        [day, day, day, day],
    ).fetchall()
    by_ind: dict[str, list] = {}
    for code, ind, y3, y3p in rows:
        by_ind.setdefault(ind or "(未分類)", []).append(
            {"code": code, "yoy_3m": y3, "yoy_3m_prev": y3p}
        )
    return [
        {"industry": ind, "members": ms}
        for ind, ms in sorted(by_ind.items(), key=lambda kv: -len(kv[1]))
        if len(ms) >= 3
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", required=True, help="e.g. 2023-01 2023-02")
    args = parser.parse_args()
    SEEDS.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(CACHE), read_only=True)
    for ym in args.months:
        day = label_day(con, ym)
        seed = {
            "label_month": ym,
            "label_day": day.isoformat(),
            "momentum_clusters": momentum_clusters(con, day),
            "revenue_accel_clusters": revenue_clusters(con, day),
        }
        out = SEEDS / f"{ym}.json"
        out.write_text(json.dumps(seed, ensure_ascii=False, indent=1), encoding="utf-8")
        print(
            f"{ym} (label_day {day}): momentum clusters={len(seed['momentum_clusters'])}, "
            f"revenue clusters={len(seed['revenue_accel_clusters'])} -> {out.name}"
        )
    con.close()


if __name__ == "__main__":
    main()
