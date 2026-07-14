"""收盤後補存當日 1 分 K(自建歷史的獨立入口)。

執行器每次跑完會自動 dump 它經手的代碼;沒跑執行器的日子用本工具補。
**富邦 REST 只提供當日 K 線——漏掉的日子補不回來**,所以建議收盤後
(14:00 之後)每天跑一次,或掛 launchd 排程。

缺檔的退化行為(daily_context 已內建):昨日價值區 prior 自動改用
「最近一個有檔的交易日」;日線結構位(cache.duckdb)與盤中 TPO 不受影響。

用法:
  # 存目前富邦庫存全部代碼(預設)
  uv run --project research python -m research.trading.execution.archive_candles
  # 追加候選池代碼
  uv run --project research python -m research.trading.execution.archive_candles --codes 6488,3026,8261
"""

from __future__ import annotations

import argparse

from research.brokers.fubon import FubonBroker, load_env_file

from .daily_context import dump_candles


def main() -> None:
    p = argparse.ArgumentParser(description="收盤後補存當日 1 分 K")
    p.add_argument("--codes", default="", help="逗號分隔的額外代碼(預設含富邦庫存全部)")
    p.add_argument("--no-inventory", action="store_true", help="不含庫存代碼")
    args = p.parse_args()

    load_env_file()
    broker = FubonBroker.from_env()
    broker.login()
    broker.sdk.init_realtime()
    rest = broker.sdk.marketdata.rest_client.stock

    codes: list[str] = [c.strip().zfill(4) for c in args.codes.split(",") if c.strip()]
    if not args.no_inventory:
        from research.trading.portfolio import positions_from_fubon_inventories
        codes += list(positions_from_fubon_inventories(broker.get_inventories()))
    codes = sorted(set(codes))
    if not codes:
        raise SystemExit("沒有代碼可存(庫存為空且未給 --codes)")

    saved = skipped = 0
    for code in codes:
        try:
            res = rest.intraday.candles(symbol=code)
            bars = res.get("data") if isinstance(res, dict) else getattr(res, "data", None)
        except Exception as exc:  # noqa: BLE001 - 單檔失敗不擋整批
            print(f"✗ {code}: {str(exc)[:80]}")
            skipped += 1
            continue
        path = dump_candles(code, list(bars or []))
        if path is None:
            print(f"— {code}: 無 K 線(今日未交易?)")
            skipped += 1
        else:
            print(f"✓ {code}: {len(bars)} 根 → {path.name}")
            saved += 1
    print(f"\n存檔 {saved} 檔、略過 {skipped} 檔")


if __name__ == "__main__":
    main()
