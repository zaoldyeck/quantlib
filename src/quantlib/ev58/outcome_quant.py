"""EV58 階段 B:揭曉後的結果視窗量化 + 機械跳動稽核(station_quant 的事後對稱件)。

為什麼要這支
------------
階段 B 要回答「它為什麼漲」,但在問這句話之前必須先回答一個更前面的問題:
**它真的漲了嗎,還是還原價漏套了一次公司行動、把機械跳動記成了報酬?**

樣本標籤 `fwd_max_ret` 由 `quantlib.evergreen.ev57_station_sample` 用 canonical
還原價面板算出(站位後 120 個交易日的還原收盤最高值 ÷ 站位日還原收盤 − 1)。
還原價只認兩張事件表:`ex_right_dividend`(除權息參考價)與 `capital_reduction`
(減資參考價)。**事件表沒收錄的公司行動,還原價就套不上**——價格路徑上的那一段
機械跳動會原封不動變成「報酬」。

本模組做三件事,全部只用官方封存資料、可重跑:
1. 逐位重現 EV57 的結果視窗口徑(同一個面板、同一個 120 日水平線),另給
   `days_to_peak` 與中途期末報酬,供時序描述使用。
2. 掃描視窗內的**未解釋跳動**:相鄰交易日的原始收盤比值落在漲跌幅上限之外,
   且當日在兩張事件表裡都查無紀錄 → 就是還原價漏掉的一次公司行動。
3. 把已知的機械成分剝掉,給出「扣掉機械跳動後的報酬」,讓歸因不會把
   換股比例當成 alpha。

**`mechanical_factor_before_peak` 是機械成分的上界,不是機械成分本身。**它取的是
觀測到的原始收盤比值,而恢復買賣當天本來就會有真實走勢疊在換股比例上(實測案例:
觀測比值 1.8939 = 換股比例 1.7709 × 當日真實走勢 1.0695)。要精確拆解,得用公告的
縮減比例算出理論換股係數 `1 / (1 − 減幅)`,兩者相除即當日真實走勢。本模組不自動去查
公告減幅(那是逐案的質化材料),故只給上界並在此註明——用上界會**低估**真實報酬。

判準寫死在資料裡,不靠人工設門檻:單日漲跌幅上限由站位日期所屬制度期別決定
(2015-06-01 前 7%、之後 10%),跳動比值須超過上限的 1.5 倍才算「不可能由行情
造成」,這個倍數是為了讓連續兩日的資料錯位不會被誤判成公司行動。

需要 cache.duckdb 為最新(讀 daily_quote / ex_right_dividend / capital_reduction)。

用法
----
    uv run --project . python -m quantlib.ev58.outcome_quant \
        --code 5468 --market tpex --dates 2011-07-11,2011-08-11
"""

from __future__ import annotations

import argparse
import json
from datetime import date as Date

import polars as pl

from quantlib import db, prices

FWD_DAYS = 120  # 與 ev57_station_sample.FWD_DAYS 同口徑
LIMIT_ERA_SPLIT = Date(2015, 6, 1)
#: 跳動比值須超過當期漲跌幅上限的這個倍數才算「行情做不到」。1.5 倍是為了在
#: 上限 7% 的年代仍與「連兩根停板」(1.07^2 = 1.145)保持距離。
UNEXPLAINED_SLACK = 1.5


def _limit(station: Date) -> float:
    return 0.07 if station < LIMIT_ERA_SPLIT else 0.10


def _panel(con, code: str, market: str, lo: str, hi: str) -> pl.DataFrame:
    return (prices.fetch_adjusted_panel(con, lo, hi, codes=[code], market=market)
            .filter(pl.col("close") > 0).sort("date"))


def outcome_stats(code: str, market: str, station: str,
                  fwd_days: int = FWD_DAYS) -> dict:
    """結果視窗(站位後 `fwd_days` 個交易日)的還原報酬統計,逐位對齊 EV57。"""
    con = db.connect()
    st = Date.fromisoformat(station)
    # 多抓兩年日曆天以確保 120 個交易日取得完整;站位前只需 1 筆做基準。
    df = _panel(con, code, market, station, str(st.replace(year=st.year + 2)))
    df = df.filter(pl.col("date") >= pl.lit(station).str.to_date())
    if df.height == 0:
        return {"code": code, "station": station, "error": "no_price"}
    base_row = df.row(0, named=True)
    fwd = df.filter(pl.col("date") > pl.lit(station).str.to_date()).head(fwd_days)
    if fwd.height == 0:
        return {"code": code, "station": station, "error": "no_forward_window"}
    base = float(base_row["close"])
    closes = fwd["close"].to_list()
    dates = fwd["date"].to_list()
    peak_i = max(range(len(closes)), key=lambda i: closes[i])
    return {
        "code": code, "market": market, "station": station,
        "station_asof": str(base_row["date"]),
        "base_adj_close": round(base, 4),
        "base_raw_close": round(float(base_row["raw_close"]), 4),
        "fwd_days_available": fwd.height,
        "fwd_max_ret": round(max(closes) / base - 1, 6),
        "days_to_peak": peak_i + 1,
        "peak_date": str(dates[peak_i]),
        "fwd21_ret": round(closes[20] / base - 1, 6) if fwd.height > 20 else None,
        "fwd63_ret": round(closes[62] / base - 1, 6) if fwd.height > 62 else None,
        "fwd_end_ret": round(closes[-1] / base - 1, 6),
        "fwd_end_date": str(dates[-1]),
        "fwd_min_ret": round(min(closes) / base - 1, 6),
    }


def unexplained_jumps(code: str, market: str, station: str,
                      fwd_days: int = FWD_DAYS) -> dict:
    """視窗內「事件表查無、行情又做不到」的原始價跳動 = 還原價漏套的公司行動。"""
    con = db.connect()
    st = Date.fromisoformat(station)
    df = _panel(con, code, market, station, str(st.replace(year=st.year + 2)))
    df = (df.filter(pl.col("date") >= pl.lit(station).str.to_date())
            .head(fwd_days + 1)
            .select(["date", "raw_close", "close", "adj_factor"]))
    if df.height < 2:
        return {"jumps": [], "events_in_window": {}, "note": "資料不足"}

    lo, hi = str(df["date"][0]), str(df["date"][-1])
    erd = con.sql(f"""
        SELECT date FROM ex_right_dividend
        WHERE market='{market}' AND company_code='{code}'
          AND date BETWEEN DATE '{lo}' AND DATE '{hi}'
    """).pl()["date"].to_list()
    cr = con.sql(f"""
        SELECT date FROM capital_reduction
        WHERE market='{market}' AND company_code='{code}'
          AND date BETWEEN DATE '{lo}' AND DATE '{hi}'
    """).pl()["date"].to_list()

    lim = _limit(st)
    thresh = (1 + lim) ** UNEXPLAINED_SLACK
    seq = df.to_dicts()
    jumps = []
    for prev, cur in zip(seq, seq[1:]):
        ratio = float(cur["raw_close"]) / float(prev["raw_close"])
        if thresh > ratio > 1 / thresh:
            continue
        jumps.append({
            "date": str(cur["date"]),
            "prev_date": str(prev["date"]),
            "calendar_gap_days": (cur["date"] - prev["date"]).days,
            "raw_close_ratio": round(ratio, 4),
            "adj_factor_changed": abs(float(cur["adj_factor"])
                                      - float(prev["adj_factor"])) > 1e-9,
            "in_ex_right_table": cur["date"] in erd,
            "in_capital_reduction_table": cur["date"] in cr,
        })
    return {
        "limit_pct": lim,
        "unexplained_threshold_ratio": round(thresh, 4),
        "jumps": jumps,
        "events_in_window": {"ex_right_dividend": [str(d) for d in erd],
                             "capital_reduction": [str(d) for d in cr]},
    }


def table_coverage(market: str, upto: str) -> dict:
    """兩張事件表在該市場的最早紀錄日——用來判斷『查無』是真沒事還是表沒收錄。"""
    con = db.connect()
    out = {}
    for tbl in ("ex_right_dividend", "capital_reduction"):
        r = con.sql(f"SELECT min(date), count(*) FROM {tbl} "
                    f"WHERE market='{market}'").fetchone()
        n_before = con.sql(f"SELECT count(*) FROM {tbl} WHERE market='{market}' "
                           f"AND date <= DATE '{upto}'").fetchone()[0]
        out[tbl] = {"earliest": str(r[0]), "rows_total": r[1],
                    "rows_upto_station": n_before}
    return out


def audit(code: str, market: str, station: str, fwd_days: int = FWD_DAYS) -> dict:
    """一站位的完整事後量化:結果視窗 + 跳動稽核 + 剝掉機械成分後的報酬。"""
    st = outcome_stats(code, market, station, fwd_days)
    if "error" in st:
        return st
    jz = unexplained_jumps(code, market, station, fwd_days)
    st["mechanical"] = jz
    st["table_coverage"] = table_coverage(market, station)

    # 機械成分:所有「事件表查無」的跳動比值連乘。峰值日之前發生的才會灌進
    # fwd_max_ret,之後才發生的不影響最高值,故以峰值日為界。
    peak = st["peak_date"]
    mech = 1.0
    for j in jz["jumps"]:
        if (not j["in_ex_right_table"] and not j["in_capital_reduction_table"]
                and j["date"] <= peak):
            mech *= j["raw_close_ratio"]
    st["mechanical_factor_before_peak"] = round(mech, 6)
    st["fwd_max_ret_ex_mechanical"] = round((1 + st["fwd_max_ret"]) / mech - 1, 6)
    return st


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="EV58 階段 B 結果視窗量化 + 機械跳動稽核")
    ap.add_argument("--code", required=True)
    ap.add_argument("--market", required=True, choices=["twse", "tpex"])
    ap.add_argument("--dates", required=True, help="逗號分隔的站位日")
    ap.add_argument("--fwd-days", type=int, default=FWD_DAYS)
    a = ap.parse_args(argv)
    out = [audit(a.code, a.market, d.strip(), a.fwd_days)
           for d in a.dates.split(",") if d.strip()]
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
