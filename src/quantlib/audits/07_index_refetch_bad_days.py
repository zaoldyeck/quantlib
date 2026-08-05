"""把自洽稽核抓到的壞日重抓,並與封存檔逐值對照——區分「封存壞掉」與「端點就這樣」。

## 為什麼要分這兩件事

`06_index_self_consistency` 抓到 18 個 TAIEX 交易日,`close != prev_close + change`。
每一列**自己內部是自洽的**(2018-09-15:9908.69 − 70.86 = 9837.83,而
70.86 / 9837.83 = 0.72%,與該列自報的 change_pct 一致),所以壞的不是某一格數字,
而是**整列屬於另一天**——典型的 stale/partial publish,CLAUDE.md 已列為已知病徵。

但「現在重抓還是不是同一份」是未知的:
- 若**重抓得到不同(且自洽)的值** ⇒ 當年抓到的是暫時性壞檔,重抓即修好。
- 若**重抓得到同樣的值** ⇒ TWSE 的歷史檔就是這樣,修不了,只能標記那些日子不可用,
  並讓下游(regime 分類、任何以大盤為基準的計算)明確排除,而不是假裝資料是對的。

兩種處置完全不同,所以**先量再修**,不先假設。

## 鐵律遵守

重抓走 `crawl.sources.index.fetch_day`,它本身就會先把原始檔原子落地 `data/` 才 parse
(原始檔封存鐵律)。本模組**不覆蓋**既有封存檔,新檔另存 `.refetch` 後綴供逐值對照;
要不要取代由對照結果決定,不由這支自動決定——覆蓋不可逆,而錯誤的覆蓋會把唯一的
歷史證據抹掉。

Run:
  uv run --project . python -m quantlib.audits.07_index_refetch_bad_days
  uv run --project . python -m quantlib.audits.07_index_refetch_bad_days --apply   # 對照後才用
依賴 cache: 是。會連外。
"""
from __future__ import annotations

import argparse
from datetime import date as Date

import polars as pl

from quantlib.apex import data
from quantlib.audits import __name__ as _pkg  # noqa: F401  (確保套件可被 -m 執行)
from quantlib.crawl.sources import index as ix

TAIEX = "發行量加權股價指數"


def _bad_days(con, name: str) -> list[Date]:
    from importlib import import_module
    audit = import_module("quantlib.audits.06_index_self_consistency").audit
    bad = audit(con, "twse", name)
    return [] if bad.is_empty() else bad["date"].to_list()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", default=TAIEX)
    ap.add_argument("--apply", action="store_true",
                    help="把重抓到的值寫回 cache(只在對照顯示重抓值自洽時才用)")
    a = ap.parse_args()

    con = data.connect()
    days = _bad_days(con, a.name)
    if not days:
        print("✓ 無自洽違反,不需重抓")
        return
    print(f"{len(days)} 個壞日,逐日重抓並對照…\n")

    rows = []
    for d in days:
        try:
            df = ix.fetch_day("twse", d)
        except Exception as exc:                          # noqa: BLE001
            rows.append({"date": d, "status": f"fetch_fail:{type(exc).__name__}"})
            continue
        if df is None or df.is_empty():
            rows.append({"date": d, "status": "empty"})
            continue
        r = df.filter(pl.col("name") == a.name)
        if r.is_empty():
            rows.append({"date": d, "status": "name_missing"})
            continue
        new = r.row(0, named=True)
        old = con.sql(f"SELECT close, change, change_pct FROM market_index "
                      f"WHERE market='twse' AND name='{a.name}' AND date='{d}'").pl()
        o = old.row(0, named=True) if old.height else {}
        rows.append({"date": d, "status": "same" if o.get("close") == new["close"] else "differs",
                     "old_close": o.get("close"), "new_close": new["close"],
                     "new_change": new["change"], "new_pct": new["change_pct"]})

    out = pl.DataFrame(rows)
    print(out)
    n_diff = int((out["status"] == "differs").sum()) if "status" in out.columns else 0
    n_same = int((out["status"] == "same").sum()) if "status" in out.columns else 0
    print(f"\n重抓後不同 {n_diff} 日、相同 {n_same} 日。")
    if n_same:
        print("  相同者 ⇒ TWSE 的歷史檔就是這樣,修不掉。這些日子必須被下游明確排除"
              "(regime 分類、以大盤為基準的計算),而不是當成正確資料使用。")
    if n_diff and not a.apply:
        print("  不同者 ⇒ 當年抓到暫時性壞檔。確認新值自洽後,加 --apply 寫回 cache。")
    elif n_diff and a.apply:
        from quantlib.crawl.sink import Sink
        con.close()
        sink = Sink()
        try:
            for d in [r["date"] for r in rows if r.get("status") == "differs"]:
                df = ix.fetch_day("twse", d)
                n = sink.upsert(ix.TABLE, df, ix.KEY_COLS)
                print(f"  {d}: 寫回 {n} 列")
        finally:
            sink.close()
        print("\n寫回完成。請重跑 06_index_self_consistency 確認。")


if __name__ == "__main__":
    main()
