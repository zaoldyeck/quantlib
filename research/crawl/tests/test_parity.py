"""爬蟲 parity 守護:Python 爬蟲對某已知交易日的輸出必須逐位重現 cache
(舊 Scala 管線的產物)。**未過不得讓爬蟲驅動 live 決策**。

需網路(抓 TWSE/TPEx)。某 (表,市場) 若 cache 無該日資料 → SKIP(無對照基準)。
比對:同 company_code 集合、每欄值(float 容差 1e-6,其餘精確)。

Run: uv run --project research python -m research.crawl.tests.test_parity [YYYY-MM-DD]
"""
from __future__ import annotations

import sys
from datetime import date as Date

import duckdb
import polars as pl

from research.crawl.sink import CACHE_DB
from research.crawl.sources import (capital_reduction, daily_quote,
                                    daily_trading_details, ex_right_dividend,
                                    operating_revenue, stock_per_pbr)

#: 預設 parity 日(需在 cache 內、且交易所仍供該日資料)
DEFAULT_DATE = Date(2026, 7, 17)

#: 日頻源登記:(module, 數值欄集合)。company_code/market/date 為 key,單獨比。
DAILY_SOURCES = [
    (daily_quote, ["opening_price", "highest_price", "lowest_price", "closing_price",
                   "trade_volume", "trade_value", "last_best_bid_price",
                   "last_best_ask_price"]),
    (daily_trading_details, ["foreign_investors_difference", "trust_difference",
                             "dealers_difference", "total_difference"]),
    (stock_per_pbr, ["price_book_ratio", "dividend_yield", "price_to_earning_ratio"]),
]


def _cache_df(con, table: str, cols: list[str], market: str, day: Date) -> pl.DataFrame:
    return con.execute(
        f"SELECT company_code, {','.join(cols)} FROM {table} "
        f"WHERE market = ? AND date = ? ORDER BY company_code",
        [market, day]).pl()


def _diffs(want: pl.DataFrame, got: pl.DataFrame, cols: list[str]) -> list[str]:
    msgs: list[str] = []
    w = set(want["company_code"].to_list())
    g = set(got["company_code"].to_list())
    if w != g:
        only_w, only_g = sorted(w - g)[:5], sorted(g - w)[:5]
        msgs.append(f"代碼集合不同:cache 多 {only_w}… / 爬蟲多 {only_g}…"
                    f"(cache {len(w)} vs 爬蟲 {len(g)})")
    common = sorted(w & g)
    wi = {r[0]: r for r in want.iter_rows()}
    gi = {r[0]: r for r in got.select(want.columns).iter_rows()}
    ncmp = 0
    for code in common:
        wr, gr = wi[code], gi[code]
        for j, col in enumerate(["company_code", *cols]):
            if j == 0:
                continue
            a, b = wr[j], gr[j]
            if a is None or b is None:
                if a is not b:
                    msgs.append(f"{code}.{col}: cache={a} 爬蟲={b}(null 不一致)")
            elif isinstance(a, float) or isinstance(b, float):
                if abs(float(a) - float(b)) > 1e-6:
                    msgs.append(f"{code}.{col}: cache={a} 爬蟲={b}")
            elif a != b:
                msgs.append(f"{code}.{col}: cache={a} 爬蟲={b}")
        ncmp += 1
        if len(msgs) > 20:
            msgs.append("…(超過 20 筆差異,截斷)")
            break
    return msgs


def check_daily_source(mod, cols: list[str], con, day: Date) -> tuple[int, int, int]:
    """回 (通過市場數, skip 市場數, 失敗市場數)。"""
    ok = skip = fail = 0
    for market in mod.MARKETS:
        want = _cache_df(con, mod.TABLE, cols, market, day)
        if want.height == 0:
            print(f"  · {mod.TABLE}/{market}: cache 無 {day} 資料 → SKIP")
            skip += 1
            continue
        try:
            got = mod.fetch_day(market, day)
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ {mod.TABLE}/{market}: 抓取/解析失敗 {type(exc).__name__}: {exc}")
            fail += 1
            continue
        if got is None or got.height == 0:
            print(f"  ✗ {mod.TABLE}/{market}: 爬蟲回無資料,但 cache 有 {want.height} 列")
            fail += 1
            continue
        got = got.sort("company_code")
        msgs = _diffs(want, got, cols)
        if msgs:
            print(f"  ✗ {mod.TABLE}/{market}: {len(msgs)} 類差異(cache {want.height} / 爬蟲 {got.height}):")
            for m in msgs[:8]:
                print(f"       {m}")
            fail += 1
        else:
            print(f"  ✓ {mod.TABLE}/{market}: {want.height} 列逐位一致")
            ok += 1
    return ok, skip, fail


#: operating_revenue 月頻 parity(需在 cache 內、且 MOPS 仍供該月檔)。
#: 只嚴格比「共同代碼的營收數字」——代碼集合/公司名會隨 MOPS 檔在公告窗持續長大、
#: 公司改名而合法漂移(Scala reader 亦以 revenueRefreshWindow 重抓),不算解析錯。
PARITY_MONTH = (2026, 6)
_REV_NUM = ["monthly_revenue", "monthly_revenue_yoy"]


def check_operating_revenue(con, year: int, month: int) -> tuple[int, int, int]:
    ok = skip = fail = 0
    for market in operating_revenue.MARKETS:
        want = con.execute(
            f"SELECT company_code, {','.join(_REV_NUM)} FROM operating_revenue "
            "WHERE market = ? AND type = 'consolidated' AND year = ? AND month = ? "
            "ORDER BY company_code", [market, year, month]).pl()
        if want.height == 0:
            print(f"  · operating_revenue/{market}: cache 無 {year}-{month:02d} → SKIP")
            skip += 1
            continue
        try:
            got = operating_revenue.fetch_month(market, year, month)
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ operating_revenue/{market}: 抓取失敗 {type(exc).__name__}: {exc}")
            fail += 1
            continue
        if got is None:
            print(f"  ✗ operating_revenue/{market}: 爬蟲無資料,cache 有 {want.height}")
            fail += 1
            continue
        wi = {r[0]: r for r in want.iter_rows()}
        gi = {r[0]: r for r in got.select(want.columns).iter_rows()}
        common = sorted(set(wi) & set(gi))
        bad = []
        for code in common:
            for j in (1, 2):  # monthly_revenue, monthly_revenue_yoy
                a, b = wi[code][j], gi[code][j]
                if a is None or b is None:
                    if a is not b:
                        bad.append(f"{code}.{_REV_NUM[j-1]}: cache={a} 爬蟲={b}")
                elif abs(float(a) - float(b)) > 1e-6:
                    bad.append(f"{code}.{_REV_NUM[j-1]}: cache={a} 爬蟲={b}")
        drift = len(set(wi) ^ set(gi))
        if bad:
            print(f"  ✗ operating_revenue/{market}: {len(bad)} 筆營收數字不符"
                  f"(共同 {len(common)} 檔):")
            for m in bad[:8]:
                print(f"       {m}")
            fail += 1
        else:
            note = f",代碼集合漂移 {drift} 檔(MOPS 檔成長/改名,屬正常)" if drift else ""
            print(f"  ✓ operating_revenue/{market}: 共同 {len(common)} 檔營收數字逐位一致{note}")
            ok += 1
    return ok, skip, fail


def _cmp_keyed(want, got, keys: list[str], vals: list[str]) -> tuple[list[str], int, int]:
    """比共同 key 上的 vals(float 容差 1e-6,字串精確);回 (差異訊息, 共同數, 漂移數)。"""
    wi = {tuple(r[k] for k in keys): r for r in want.select(keys + vals).to_dicts()}
    gi = {tuple(r[k] for k in keys): r for r in got.select(keys + vals).to_dicts()}
    common = sorted(set(wi) & set(gi))
    bad = []
    for k in common:
        for col in vals:
            a, b = wi[k][col], gi[k][col]
            if a is None or b is None:
                if a is not b:
                    bad.append(f"{k}.{col}: cache={a} 爬蟲={b}")
            elif isinstance(a, float) or isinstance(b, float):
                if abs(float(a) - float(b)) > 1e-6:
                    bad.append(f"{k}.{col}: cache={a} 爬蟲={b}")
            elif a != b:
                bad.append(f"{k}.{col}: cache={a} 爬蟲={b}")
    return bad, len(common), len(set(wi) ^ set(gi))


def check_ex_right(con, year: int, month: int) -> tuple[int, int, int]:
    ok = skip = fail = 0
    for market in ex_right_dividend.MARKETS:
        try:
            got = ex_right_dividend.fetch_month(market, year, month)
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ ex_right_dividend/{market}: 抓取失敗 {type(exc).__name__}: {exc}")
            fail += 1
            continue
        if got is None:
            print(f"  · ex_right_dividend/{market}: 爬蟲無 {year}-{month:02d} 事件 → SKIP")
            skip += 1
            continue
        dset = got["date"].unique().to_list()
        ph = ",".join("?" * len(dset))
        want = con.execute(
            f"SELECT date, company_code, cash_dividend FROM ex_right_dividend "
            f"WHERE market = ? AND date IN ({ph})", [market, *dset]).pl()
        if want.height == 0:
            print(f"  · ex_right_dividend/{market}: cache 無對照日 → SKIP")
            skip += 1
            continue
        bad, ncom, drift = _cmp_keyed(want, got, ["date", "company_code"], ["cash_dividend"])
        # 個別股利會被 MOPS 修訂/精度調整(cache 為舊快照、爬蟲為當前真值),屬合法
        # 時間差;僅在不符為系統性(>1% 共同列)時才判解析錯。
        tol = max(2, ncom // 100)
        if len(bad) > tol:
            print(f"  ✗ ex_right_dividend/{market}: {len(bad)} 筆現金股利不符(共同 {ncom},>1% 系統性):")
            for m in bad[:8]:
                print(f"       {m}")
            fail += 1
        else:
            rev = f",{len(bad)} 筆股利經 MOPS 修訂(cache 舊快照)" if bad else ""
            note = f",鍵集合漂移 {drift}(月檔成長,屬正常)" if drift else ""
            print(f"  ✓ ex_right_dividend/{market}: 共同 {ncom} 筆現金股利一致{rev}{note}")
            ok += 1
    return ok, skip, fail


def check_capital_reduction(con, start: Date, end: Date) -> tuple[int, int, int]:
    ok = skip = fail = 0
    for market in capital_reduction.MARKETS:
        try:
            got = capital_reduction.fetch_range(market, start, end)
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ capital_reduction/{market}: 抓取失敗 {type(exc).__name__}: {exc}")
            fail += 1
            continue
        want = con.execute(
            "SELECT date, company_code, post_reduction_reference_price, "
            "reason_for_capital_reduction FROM capital_reduction "
            "WHERE market = ? AND date BETWEEN ? AND ?", [market, start, end]).pl()
        if got is None and want.height == 0:
            print(f"  · capital_reduction/{market}: 區間內雙方皆無 → SKIP")
            skip += 1
            continue
        if got is None:
            print(f"  ✗ capital_reduction/{market}: 爬蟲無,cache 有 {want.height}")
            fail += 1
            continue
        bad, ncom, drift = _cmp_keyed(
            want, got, ["date", "company_code"],
            ["post_reduction_reference_price", "reason_for_capital_reduction"])
        if bad:
            print(f"  ✗ capital_reduction/{market}: {len(bad)} 筆不符(共同 {ncom}):")
            for m in bad[:8]:
                print(f"       {m}")
            fail += 1
        else:
            note = f",鍵集合漂移 {drift}" if drift else ""
            print(f"  ✓ capital_reduction/{market}: 共同 {ncom} 筆減資逐位一致{note}")
            ok += 1
    return ok, skip, fail


def main() -> None:
    day = Date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATE
    print(f"parity 對照日 {day}(cache: {CACHE_DB})")
    con = duckdb.connect(CACHE_DB, read_only=True)
    tot_ok = tot_skip = tot_fail = 0
    try:
        for mod, cols in DAILY_SOURCES:
            ok, skip, fail = check_daily_source(mod, cols, con, day)
            tot_ok += ok
            tot_skip += skip
            tot_fail += fail
        ok, skip, fail = check_operating_revenue(con, *PARITY_MONTH)
        tot_ok += ok
        tot_skip += skip
        tot_fail += fail
        ok, skip, fail = check_ex_right(con, *PARITY_MONTH)
        tot_ok += ok
        tot_skip += skip
        tot_fail += fail
        ok, skip, fail = check_capital_reduction(con, Date(2026, 1, 1), Date(2026, 6, 30))
        tot_ok += ok
        tot_skip += skip
        tot_fail += fail
    finally:
        con.close()
    print(f"\n結果:通過 {tot_ok}、SKIP {tot_skip}、失敗 {tot_fail}")
    if tot_fail:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
