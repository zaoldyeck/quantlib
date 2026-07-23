"""EV17 升級表生成:讀現有 EV3 月初表,每行追加 5 欄籌碼 → ev17_tables/。

前 9 欄與 registry_v1 標記時所見逐位元一致(直接複用 ev3_tables 原文),
唯一差異 = 行尾追加籌碼欄:
  |外持% 外20 投20 資20 券20
  - 外持% = 外資持股比(asof 日)
  - 外20 / 投20 = 外資/投信 20 日淨買超金額佔同期成交值 %(正=吸籌)
  - 資20 = 融資餘額 20 日變化 %
  - 券20 = 借券賣出餘額 20 日變化 %(2016 前 na)
asof 日 = EV3 表檔名(月首交易日),數據截至當日收盤(PIT 同 EV3)。

需要 cache 最新。Run:
  uv run --project . python -m quantlib.evergreen.make_tables_chips 2023-02-01 2023-08-01
"""
from __future__ import annotations

import os
import sys
from datetime import date as Date

import duckdb
import polars as pl
from quantlib import paths

SRC = "src/quantlib/evergreen/data/ev3_tables"
OUT = "src/quantlib/evergreen/data/ev17_tables"


def chips_asof(raw: duckdb.DuckDBPyConnection, asof: str) -> dict[str, str]:
    """每檔一條籌碼欄字串(以 code 為鍵)。20 日窗 = asof 往前 20 交易日。"""
    q = raw.sql(f"""
    WITH win AS (
        SELECT DISTINCT date FROM daily_quote
        WHERE date <= DATE '{asof}' ORDER BY date DESC LIMIT 20
    ),
    flow AS (
        SELECT t.company_code,
               100.0 * sum((t.foreign_investors_difference) * q.closing_price)
                   / nullif(sum(q.trade_value), 0) AS f20,
               100.0 * sum((t.trust_difference) * q.closing_price)
                   / nullif(sum(q.trade_value), 0) AS t20
        FROM daily_trading_details t
        JOIN daily_quote q USING (market, date, company_code)
        WHERE t.date IN (SELECT date FROM win)
        GROUP BY t.company_code
    ),
    mg AS (
        SELECT company_code,
               100.0 * (last(margin_balance ORDER BY date)
                        - first(margin_balance ORDER BY date))
                   / nullif(first(margin_balance ORDER BY date), 0) AS m20
        FROM margin_transactions
        WHERE date IN (SELECT date FROM win)
        GROUP BY company_code
    ),
    sbl AS (
        SELECT company_code,
               100.0 * (last(daily_balance ORDER BY date)
                        - first(daily_balance ORDER BY date))
                   / nullif(first(daily_balance ORDER BY date), 0) AS s20
        FROM sbl_borrowing
        WHERE date IN (SELECT date FROM win)
        GROUP BY company_code
    ),
    fh AS (
        SELECT company_code, foreign_held_ratio AS fr
        FROM foreign_holding_ratio WHERE date = DATE '{asof}'
    )
    SELECT coalesce(flow.company_code, mg.company_code, sbl.company_code,
                    fh.company_code) AS code,
           fh.fr, flow.f20, flow.t20, mg.m20, sbl.s20
    FROM flow
    FULL JOIN mg USING (company_code)
    FULL JOIN sbl USING (company_code)
    FULL JOIN fh USING (company_code)
    """).pl()

    def n(v: float | None) -> str:
        return "na" if v is None else f"{v:.0f}"

    return {r["code"]: f"{n(r['fr'])} {n(r['f20'])} {n(r['t20'])} "
                       f"{n(r['m20'])} {n(r['s20'])}"
            for r in q.to_dicts() if r["code"]}


def main() -> None:
    tags = sys.argv[1:]
    if not tags:
        raise SystemExit("usage: make_tables_chips.py YYYY-MM-DD ...")
    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    os.makedirs(OUT, exist_ok=True)
    for tag in tags:
        src = open(f"{SRC}/{tag}.txt").read().splitlines()
        chips = chips_asof(raw, tag)
        header = (src[0] + "|外持% 外20 投20 資20 券20"
                  "(外資持股比;外資/投信20日淨買佔成交值%;"
                  "融資/借券餘額20日變化%)")
        lines = [header]
        for line in src[1:]:
            code = line.split()[0]
            lines.append(f"{line}|{chips.get(code, 'na na na na na')}")
        path = f"{OUT}/{tag}.txt"
        open(path, "w").write("\n".join(lines) + "\n")
        print(f"{path}  {os.path.getsize(path) // 1024}KB  {len(lines) - 1} 檔")


if __name__ == "__main__":
    main()
