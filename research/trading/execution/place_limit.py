"""一次性掛限價單(place-and-exit):掛了就離開,不盯盤、不自動撤單。

與盤中執行器的分工:
- 執行器(buy/sell)= 盯盤動態管理:結構錨隨盤更新、微結構訊號、死線保底,
  程式退出時會撤掉在途單。
- 本工具 = 掛一張**靜態**限價單就結束,單留在交易所直到成交或收盤流單。
  適合「先掛個價等成交、不想開著程式」的情境。

注意:
- 零股(qty < 1000)走盤中零股,委託時段 09:00-13:30(每分鐘集合競價);
  富邦盤外時段是否收預約單依券商規定,被拒就等時段內再掛。
- 集合競價語義:賣單限價 = 願賣下限(成交在撮合價,不會真用你的低價);
  買單限價 = 願買上限。想「保證成交」就把賣單掛低/買單掛高一點。
- 撤單用 cancel_all(或券商 App)。
- 安全三閘與執行器相同:--live + FUBON_DRY_RUN=false + QL_STRATEGY_CAPITAL_TWD。

用法:
  uv run --project research python -m research.trading.execution.place_limit \
      --code 2408 --side Sell --qty 1 --price 448 [--live]
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from research.brokers.fubon import FubonBroker, StockOrderRequest, load_env_file

from .ticks import snap_down, snap_up

TERMINAL = {30, 40, 50, 90}


def main() -> None:
    p = argparse.ArgumentParser(description="一次性掛限價單(不盯盤、不自動撤單)")
    p.add_argument("--code", required=True)
    p.add_argument("--side", required=True, choices=("Buy", "Sell"))
    p.add_argument("--qty", required=True, type=int)
    p.add_argument("--price", required=True, type=float)
    p.add_argument("--live", action="store_true")
    args = p.parse_args()

    load_env_file()
    if args.live:
        if os.environ.get("FUBON_DRY_RUN", "true").lower() != "false":
            sys.exit("--live 需要 FUBON_DRY_RUN=false(由使用者自行武裝)")

    px = snap_up(args.price) if args.side == "Sell" else snap_down(args.price)
    market_type = "IntradayOdd" if args.qty < 1000 else "Common"

    broker = FubonBroker.from_env()
    broker.login()

    # 資訊性防呆(不強制擋):今日同代碼委託現況 + 賣出庫存夾緊
    qty = args.qty
    try:
        res = broker.get_order_results()
        for o in (res.data or []):
            if getattr(o, "stock_no", None) == args.code:
                st = getattr(o, "status", None)
                tag = "在途" if st not in TERMINAL else "終態"
                print(f"⚠ 今日已有 {args.code} 委託({tag}):{getattr(o, 'buy_sell', '?')} "
                      f"{getattr(o, 'quantity', '?')} 股 @ {getattr(o, 'price', '?')} status={st}")
    except Exception:
        pass
    if args.side == "Sell":
        try:
            from research.trading.portfolio import positions_from_fubon_inventories
            held = positions_from_fubon_inventories(broker.get_inventories()).get(args.code, 0)
            if held <= 0:
                sys.exit(f"庫存 0,無可賣 {args.code}")
            if qty > held:
                print(f"庫存夾緊:{qty} → {held} 股")
                qty = held
        except SystemExit:
            raise
        except Exception:
            print("⚠ 庫存查詢失敗,未夾緊——自行確認可賣量")

    mode = "LIVE 真實下單" if args.live else "DRY-RUN(不送單)"
    print(f"\n=== 掛單計劃({mode})===")
    print(f"{args.side} {args.code} × {qty} 股 @ {px:g} 限價 ROD "
          f"[{'盤中零股' if market_type == 'IntradayOdd' else '整股'}]")
    print("掛出後本程式即結束;此單不會被自動撤銷(撤單用 cancel_all 或券商 App)。")
    if not args.live:
        print("dry-run 結束(要真掛請加 --live 並武裝環境變數)。")
        return
    print("5 秒後送出,Ctrl+C 取消…")
    time.sleep(5)

    req = StockOrderRequest(symbol=args.code, side=args.side, quantity=qty,
                            price_type="Limit", market_type=market_type,
                            time_in_force="ROD", order_type="Stock",
                            price=f"{px:g}", user_def="QLMANUAL")
    res = broker.place_stock_order(req)
    if not getattr(res, "is_success", False):
        sys.exit(f"下單失敗:{getattr(res, 'message', res)}")
    seq = getattr(getattr(res, "data", None), "seq_no", None)
    print(f"✓ 已掛單 seq_no={seq};成交與否以券商回報為準。")


if __name__ == "__main__":
    main()
