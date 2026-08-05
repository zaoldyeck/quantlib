"""月頻源覆蓋稽核 + 補洞——治「只回頭看最近 N 個月」造成的永久資料空洞。

## 為什麼需要這支(2026-08-04 由 EV55 的發現追出來的**通病**)
月頻源的 `refresh()` 一律只重抓最近 `_REFRESH_MONTHS = 3` 個月。含義:

    爬蟲停超過 3 個月(或那幾個月抓取失敗)→ 那些月份**永遠不會再被看一眼**,
    而且**沒有任何機制會發現**。

實證:`ex_right_dividend` 的 TWSE 與 TPEx **2023-09 ~ 2024-06 整整 10 個月零筆**,
raw 封存也沒有檔案(2023 止於 `2023_8_19.csv`、2024 起於 `2024_7.csv`)。實測 MOPS
端點對那幾個月**都回得出資料**——不是端點不給,是爬蟲從沒跑過那幾個月。

影響鏈:ex_right_dividend 是**還原價因子的原料**。缺一個月的除權息 → 那個月的
還原價出現機械跳空 → 下游一切以還原價為輸入的東西(暴漲樣本掃描、策略回測、
NAV 模擬)在該窗全部失真,而且症狀長得跟真的行情一模一樣。EV30 抓到的「幽靈
暴漲」正是同一個機制。該窗還正好落在 live 策略的 refit 窗(2023-07 ~ 2026-07)內。

## 這支做什麼
1. **稽核**:對每個月頻源,列出 cache 裡「該有卻沒有」的月份(以該源自己的
   最早月份為起點,連續到指定截止月)。零推測,只看 cache 事實。
2. **補洞**:`--fix` 把缺的月份逐一 `fetch_month` 回來(該函式本身就會先原子
   落地 raw 才 parse,遵守原始檔封存鐵律),upsert 進 cache。

## 為什麼不是「把 _REFRESH_MONTHS 調大」
調大只是把窗拉寬,停機夠久照樣掉;而且沒有人會知道掉了。**窗是效率手段,
覆蓋稽核才是正確性保證。** 兩者並存:每日 refresh 走窄窗(便宜),覆蓋稽核
週期性跑(找洞)。

Run:
  uv run --project . python -m quantlib.crawl.monthly_coverage            # 只稽核
  uv run --project . python -m quantlib.crawl.monthly_coverage --fix      # 稽核 + 補洞
依賴 cache: 是。補洞會連網,長任務。
"""
from __future__ import annotations

import argparse
import re

from quantlib import paths
from quantlib.apex import data
from quantlib.crawl.sink import Sink
from quantlib.crawl.sources import (ex_right_dividend, operating_revenue,
                                    treasury_stock_buyback)

#: 有 `fetch_month(market, year, month)` 介面的月頻源。
#: capital_reduction 走 `fetch_range` 不是逐月,語義不同,不納入本稽核。
SOURCES = {
    "ex_right_dividend": ex_right_dividend,
    "operating_revenue": operating_revenue,
    "treasury_stock_buyback": treasury_stock_buyback,
}


def _months_in_cache(con, table: str, market: str) -> set[tuple[int, int]]:
    """cache 中該源該市場實際有資料的 (年, 月) 集合。

    月份一律取**資料列自己的日期欄**,不取檔名——ex_right 的 tpex 舊制是「範圍檔」
    (檔名是區間結尾、內容橫跨多年),用檔名判斷會整段誤判。
    """
    cols = con.sql(f"DESCRIBE {table}").pl()["column_name"].to_list()
    # 各源的「事件時間」欄名不同,逐一指名——不用萬用猜測,猜錯會把整段覆蓋誤判。
    for cand in ("date", "announce_date"):
        if cand in cols:
            expr = cand
            break
    else:
        if {"year", "month"} <= set(cols):
            expr = "make_date(year, month, 1)"
        else:
            raise ValueError(f"{table} 找不到可判定月份的欄位(欄:{cols})")
    q = con.sql(f"""
        SELECT DISTINCT year({expr}) AS y, month({expr}) AS m
        FROM {table} WHERE market = '{market}' AND {expr} IS NOT NULL
    """).pl()
    return {(r["y"], r["m"]) for r in q.iter_rows(named=True)}


def _fetched_months(source: str, market: str) -> set[tuple[int, int]]:
    """raw 封存中「這個月曾被抓過」的集合——檔名前綴 `{year}_{month}` 即證據。

    為什麼需要第二個信號:抓取是按**公告月**(MOPS 表單的 year/month),而 cache 裡
    的月份是**事件日期月**——同一次抓取回來的除權息事件,日期常落在公告月的下一、
    兩個月。只看 cache 會把「抓過但當月無事件生效」誤報成空洞,永遠歸不了零。
    兩個信號並用:cache 無資料 **且** raw 無該月檔 → 才是真的沒抓過。
    """
    out: set[tuple[int, int]] = set()
    base = paths.RAW / source / market
    if not base.exists():
        return out
    for f in base.glob("*/*"):
        m = re.match(r"^(\d{4})_(\d{1,2})(?:[_.]|$)", f.name)
        if m:
            out.add((int(m.group(1)), int(m.group(2))))
    return out


def _span(months: set[tuple[int, int]], upto: tuple[int, int]) -> list[tuple[int, int]]:
    """從該源最早的月份連續列到 upto——起點取實測最早值,不寫死任何年份。"""
    if not months:
        return []
    y, m = min(months)
    out = []
    while (y, m) <= upto:
        out.append((y, m))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def audit(con, upto: tuple[int, int]) -> dict[tuple[str, str], list[tuple[int, int]]]:
    gaps: dict[tuple[str, str], list[tuple[int, int]]] = {}
    for name, mod in SOURCES.items():
        for market in getattr(mod, "MARKETS", ("twse", "tpex")):
            have = _months_in_cache(con, getattr(mod, "TABLE", name), market)
            fetched = _fetched_months(name, market)
            # 真空洞 = cache 沒資料 **且** raw 也沒抓過該月
            missing = [ym for ym in _span(have, upto)
                       if ym not in have and ym not in fetched]
            if missing:
                gaps[(name, market)] = missing
    return gaps


def _fmt(ms: list[tuple[int, int]]) -> str:
    """把連續月份縮成區間,長清單才看得懂。"""
    runs: list[list[tuple[int, int]]] = [[ms[0]]]
    for prev, ym in zip(ms, ms[1:]):
        nxt = (prev[0] + 1, 1) if prev[1] == 12 else (prev[0], prev[1] + 1)
        if ym == nxt:
            runs[-1].append(ym)
        else:
            runs.append([ym])          # 開新段,**不可** cur.clear()——那會把已收錄的同一個
                                       # list 物件一起清空(aliasing),整段輸出變成重複月份
    return "、".join(f"{r[0][0]}-{r[0][1]:02d}" if len(r) == 1
                     else f"{r[0][0]}-{r[0][1]:02d}~{r[-1][0]}-{r[-1][1]:02d}({len(r)} 個月)"
                     for r in runs)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fix", action="store_true", help="把缺的月份抓回來並寫入 cache")
    ap.add_argument("--upto", default=None, help="稽核到哪個月 YYYY-MM(預設 cache 最新交易日的上個月)")
    a = ap.parse_args()

    con = data.connect()
    if a.upto:
        y, m = (int(x) for x in a.upto.split("-"))
    else:
        last = con.sql("SELECT max(date) FROM daily_quote").pl().item()
        y, m = (last.year - 1, 12) if last.month == 1 else (last.year, last.month - 1)
    print(f"稽核月頻源覆蓋,截止 {y}-{m:02d}\n")

    gaps = audit(con, (y, m))
    if not gaps:
        print("✓ 所有月頻源逐月連續,無空洞")
        return
    for (name, market), ms in sorted(gaps.items()):
        print(f"✗ {name}/{market}:缺 {len(ms)} 個月 —— {_fmt(ms)}")

    if not a.fix:
        print("\n(只稽核。要補洞加 --fix)")
        return

    # DuckDB 不允許同程序內同時持有「讀」與「寫」兩種 configuration 的連線
    # (Connection Error: Can't open a connection to same database file with a
    #  different configuration than existing connections)。稽核用讀連線,補洞要寫,
    # 故**先把讀連線關掉**才開 Sink——不關的話 Sink() 直接拋例外,補洞一列都不會寫。
    con.close()
    print("\n開始補洞…")
    sink = Sink()
    try:
        for (name, market), ms in sorted(gaps.items()):
            mod = SOURCES[name]
            table, keys = getattr(mod, "TABLE", name), mod.KEY_COLS
            for (yy, mm) in ms:
                try:
                    df = mod.fetch_month(market, yy, mm)
                except Exception as exc:  # noqa: BLE001 - 單月失敗不擋其他月
                    print(f"  {name}/{market} {yy}-{mm:02d} 抓取失敗:{type(exc).__name__}: {exc}")
                    continue
                if df is None or df.is_empty():
                    print(f"  {name}/{market} {yy}-{mm:02d}: 該月無事件")
                    continue
                n = sink.upsert(table, df, keys)
                print(f"  {name}/{market} {yy}-{mm:02d}: {n} 列")
    finally:
        sink.close()
    print("\n補洞完成。請重跑本稽核確認。")


if __name__ == "__main__":
    main()
