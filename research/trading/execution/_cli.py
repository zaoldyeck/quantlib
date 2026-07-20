"""buy/sell CLI 的共用骨架(參數、live 閘門、selftest)。"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from research.brokers.fubon import FubonBroker, load_env_file

from .engine import ExecutionEngine, MarketHub, Quote, QuoteFeed
from .policy import PROFILES, LadderProfile

TAIPEI = ZoneInfo("Asia/Taipei")


def build_parser(side: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=f"盤中{ '買入' if side == 'Buy' else '賣出' }執行器(預設 dry-run 模擬)")
    if side == "Trade":
        p.add_argument("--buy", default="", help="買入代碼,逗號多檔,每檔可帶股數:"
                                                 "\"2408:2,3006:5\";省略股數 = 買 1 股(或 --qty)")
        p.add_argument("--sell", default="", help="賣出代碼,逗號多檔,每檔可帶股數:"
                                                  "\"4973,5289:3\";**省略股數 = 賣全部庫存**(或 --qty)")
        p.add_argument("--code", help=argparse.SUPPRESS, default=None)
    else:
        _def = "省略股數 = 賣全部庫存" if side == "Sell" else "省略股數 = 買 1 股"
        p.add_argument("--code", help=f"股票代碼,逗號多檔併發,每檔可帶股數:\"4973:1,5289:3\";"
                                      f"{_def}(或 --qty 覆蓋);與 --plan 二選一")
    # 唯一的股數旗標。給了 → 買賣兩側未逐檔指定者都用它;不給 → 用內建預設
    # (買 1 股、賣全部庫存)。逐檔 `代碼:股數` 永遠優先。單一旗標,零重複。
    p.add_argument("--qty", type=int, default=None,
                   help="預設股數,買賣兩側通用(逐檔 :股數 優先;不給則買 1 股、賣全部)")
    p.add_argument("--plan", help=f"auto_trader plan JSON;執行其中所有 {side} 腿")
    p.add_argument("--cap-pct", type=float, default=None,
                   help="價格護欄(小數;買=上限、賣=下限;預設取 profile)")
    p.add_argument("--deadline", default=None, help="升級死線 HH:MM(預設取 profile)")
    p.add_argument("--round-sec", type=float, default=60.0,
                   help="撮合輪詢週期秒數(盤中零股逐分鐘撮合 → 預設 60)")
    if side in ("Sell", "Trade"):
        p.add_argument("--urgency", choices=("normal", "exit", "stop"),
                       default="exit" if side == "Trade" else "normal",
                       help="賣腿模式。exit = 系統出場(六道門;Trade 模式預設):結構錨整場撈"
                            "相對高點、盤中永不因時間跨價,收盤未竟→盤後掛收盤價收尾(護欄 -3%%);"
                            "stop = 急殺(僅事實級利空 override):首輪即跨價;normal = 一般賣出(吃 --patience)")
    p.add_argument("--no-micro", action="store_true",
                   help="關閉微結構擇時層(OFI/VPIN/TPO/SMC 加速/減速訊號)")
    p.add_argument("--trigger-strict", action="store_true",
                   help="狙擊模式:micro 加速需全部條件 AND(止穩+竭盡/掃蕩+資金流+價值區)")
    p.add_argument("--cap-auto", action="store_true",
                   help="護欄改為波動自適應:8×(1 分 K 平均振幅),夾在 0.4%%~2%%")
    p.add_argument("--avoid-open-min", type=int, default=3,
                   help="開盤前 N 分鐘只被動不跨價(輪動噪音迴避;預設 3)")
    p.add_argument("--slice-qty", type=int, default=None,
                   help="大單切片股數上限(預設:整股 ≥2 張自動 1 張/child;零股不切)")
    p.add_argument("--patience", choices=("balanced", "price"), default="price",
                   help="price(預設)=價格優先:整場掛跨日/盤中結構位撈價,只有狙擊級微結構"
                        "訊號才主動取價,收盤未竟→盤後掛收盤價收尾;"
                        "balanced=階梯完成優先(12:30 死線跨價;每日 loop 派工用,回測語意)")
    p.add_argument("--position-mode", choices=("auto", "own", "add"), default="auto",
                   help="買入語意:own=目標是『持有 ≥ qty』(先查庫存,已持有就跳過/只補差額);"
                        "add=嚴格加碼 qty(auto:--code 預設 own;--plan 讀 plan 的 position_mode,無則 add)")
    p.add_argument("--allow-refill", action="store_true",
                   help="今日同向同代碼已有成交仍繼續執行(預設擋下防重複)")
    p.add_argument("--live", action="store_true",
                   help="真實下單(需 FUBON_DRY_RUN=false;由使用者自行武裝)")
    p.add_argument("--selftest", action="store_true", help="離線自測(不連 SDK、不需憑證)")
    return p


def resolve_profile(side: str, args: argparse.Namespace) -> LadderProfile:
    patient = getattr(args, "patience", "price") == "price"
    if side == "Buy":
        prof = PROFILES["buy_patient" if patient else "buy_normal"]
    elif getattr(args, "urgency", "normal") == "stop":
        prof = PROFILES["sell_stop"]  # 急殺(事實級 override)不吃 patience
    elif getattr(args, "urgency", "normal") == "exit":
        prof = PROFILES["sell_exit"]  # 系統出場:整場撈相對高,收盤未竟→盤後收盤價收尾
    else:
        prof = PROFILES["sell_patient" if patient else "sell_normal"]
    from dataclasses import replace
    if args.cap_pct is not None:
        prof = replace(prof, cap_pct=float(args.cap_pct))
    if args.deadline:
        prof = replace(prof, deadline_hhmm=args.deadline)
    return prof


#: 賣出腿的「賣全部庫存」哨兵值;庫存同步後解析成真實股數
SELL_ALL = "all"
#: 買入未指定股數時的內建預設(使用者定調:買入很明確,一次 1 股)
BUY_DEFAULT_QTY = 1


def _leg_qty(qty_part: str, flag: str, item: str, side: str) -> int | str:
    """把 ':股數' 的字串解析成 int 或 SELL_ALL 哨兵。"""
    q = qty_part.strip().lower()
    if q == SELL_ALL:
        if side == "Buy":
            raise SystemExit(f"{flag} 的 {item!r} 不能買 all(買入要明確股數)")
        return SELL_ALL
    try:
        n = int(q)
    except ValueError:
        raise SystemExit(f"{flag} 的 {item!r} 股數不是整數(或 all)")
    if n <= 0:
        raise SystemExit(f"{flag} 的 {item!r} 股數需 > 0")
    return n


def _parse_legs(blob: str, flag: str, default_qty: "int | str | None",
                side: str) -> list[tuple[str, "int | str"]]:
    """把 "2408:2,3006,5289:all" 解析成 [(code, qty)],qty 是 int 或 SELL_ALL。

    每檔可用 `代碼:股數` 指定;省略者用該側預設(不給 --qty 時:買 1 股、
    賣全部庫存)。買入不接受 all。
    """
    legs: list[tuple[str, int | str]] = []
    for item in str(blob or "").split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            code_part, qty_part = item.split(":", 1)
            code = code_part.strip().zfill(4)
            qty = _leg_qty(qty_part, flag, item, side)
        else:
            code = item.zfill(4)
            if default_qty is None:
                raise SystemExit(
                    f"{flag} 的 {code} 沒指定股數:寫成 {code}:股數"
                    + ("(或 --qty 覆蓋)" if side == "Buy"
                       else "(賣出省略 = 全部,或 --qty 覆蓋)"))
            qty = default_qty
        legs.append((code, qty))
    codes = [c for c, _ in legs]
    dup = {c for c in codes if codes.count(c) > 1}
    if dup:
        raise SystemExit(f"{flag} 重複代碼:{sorted(dup)}")
    return legs


def _side_defaults(args: argparse.Namespace) -> tuple["int", "int | str"]:
    """(買入預設, 賣出預設)。

    `--qty` 給了就兩側都用它(單一旗標、無重複);沒給則用內建預設——
    買 1 股、賣全部庫存(2026-07-17 使用者定調:買明確、賣清倉)。
    """
    if args.qty is not None:
        return args.qty, args.qty
    return BUY_DEFAULT_QTY, SELL_ALL


def collect_legs(side: str, args: argparse.Namespace) -> list[dict]:
    """回傳 legs,每腿自帶 side/qty(Trade 模式買賣混合,一套併發機器同時執行)。"""
    buy_default, sell_default = _side_defaults(args)
    if side == "Trade":
        legs = [{"code": c, "qty": q, "ref": None, "name": "", "side": s}
                for s, flag, blob, dflt in (("Buy", "--buy", args.buy, buy_default),
                                            ("Sell", "--sell", args.sell, sell_default))
                for c, q in _parse_legs(blob, flag, dflt, s)]
        both = {l["code"] for l in legs if sum(1 for x in legs if x["code"] == l["code"]) > 1}
        if both:
            raise SystemExit(f"同一代碼同時出現在 --buy 與 --sell:{sorted(both)}")
        if not legs:
            raise SystemExit("Trade 模式需要 --buy 與/或 --sell")
        return legs
    if args.plan:
        payload = json.loads(Path(args.plan).read_text(encoding="utf-8"))
        args._plan_position_mode = payload.get("position_mode")
        legs = [
            {"code": str(o["symbol"]).zfill(4), "qty": int(o["quantity"]),
             "ref": o.get("reference_price"), "name": o.get("name", ""), "side": side}
            for o in payload.get("orders", []) if o.get("side") == side
        ]
        if not legs:
            raise SystemExit(f"plan 內沒有 {side} 腿")
        return legs
    if not args.code:
        raise SystemExit("需要 --code(或 --plan)")
    dflt = sell_default if side == "Sell" else buy_default
    parsed = _parse_legs(args.code, "--code", dflt, side)
    if not parsed:
        raise SystemExit("--code 解析後沒有任何腿")
    return [{"code": c, "qty": q, "ref": None, "name": "", "side": side} for c, q in parsed]


def arm_live_or_exit(args: argparse.Namespace) -> bool:
    """回傳是否 live。武裝條件不足直接退出——武裝永遠是使用者的動作。

    兩道閘:--live + FUBON_DRY_RUN=false。資本上限 gate 已移除(2026-07-14
    使用者政策:不設管理資金上限;執行器的代碼與股數本來就由使用者逐一
    給定,上限變數在此只是形式)。每日 loop 的計畫 sizing 另有自己的資本
    設定(auto_trader/LiveTradingConfig),不受影響。"""
    if not args.live:
        return False
    load_env_file()
    if os.environ.get("FUBON_DRY_RUN", "true").lower() not in {"0", "false", "no"}:
        raise SystemExit("--live 但 FUBON_DRY_RUN 仍為 true;請自行設 FUBON_DRY_RUN=false 後重跑")
    return True


def run(side: str) -> None:
    """CLI 外殼:統一 Ctrl+C 行為,並保證程序真的退出。

    富邦 websocket 執行緒是非 daemon——正常 return 會卡在直譯器等它收線,
    這正是「Ctrl+C 退不掉」的元兇之一,故一律 os._exit 收尾。
    """
    import os
    import sys

    try:
        _run_inner(side)
        code = 0
    except KeyboardInterrupt:
        print("\n[Ctrl+C] 強制中止。若曾進入 LIVE,請跑 cancel_all 檢查殘留委託。")
        code = 130
    except SystemExit as exc:  # argparse 或本程式的 SystemExit(訊息)
        if isinstance(exc.code, int) or exc.code is None:
            code = exc.code or 0
        else:
            print(exc.code)
            code = 1
    except Exception as exc:  # noqa: BLE001 - 永遠乾淨收尾(ws 非 daemon 執行緒
        # 會卡住直譯器,裸 traceback 退出會連 os._exit 都跑不到)
        import traceback
        traceback.print_exc()
        print(f"\n✋ 執行器異常退出:{type(exc).__name__}: {exc}\n"
              "   若曾進入 LIVE,請跑 cancel_all 確認無殘留委託。")
        code = 1
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


def _run_inner(side: str) -> None:
    args = build_parser(side).parse_args()
    if args.selftest:
        if side == "Trade":
            selftest("Buy")
            selftest("Sell", urgency=getattr(args, "urgency", "normal"))
        else:
            selftest(side, urgency=getattr(args, "urgency", "normal"))
        return
    live = arm_live_or_exit(args)
    legs = collect_legs(side, args)
    profiles = {s: resolve_profile(s, args) for s in {l["side"] for l in legs}}
    # 公司名補齊(核對是給人看的):本地 cache 一筆查詢,fail-open 顯示代碼
    from .daily_context import lookup_names
    names = lookup_names([leg["code"] for leg in legs if not leg.get("name")])
    for leg in legs:
        leg["name"] = leg.get("name") or names.get(leg["code"], "")

    # ── 啟動核對:完整交易計劃 ──
    mode = "LIVE(真實下單)" if live else "DRY-RUN(僅模擬,不送單)"
    print("═" * 62)
    print(f"交易計劃核對|方向 {'Buy+Sell 混合' if side == 'Trade' else side}|模式 {mode}")
    for s, prof in sorted(profiles.items()):
        ddl = prof.deadline_hhmm or "無(整場撈價,收盤未竟→盤後掛收盤價收尾)"
        print(f"{s}: profile {prof.name}|護欄 {'+' if s == 'Buy' else '−'}{prof.cap_pct:.1%}"
              f"|盤中死線 {ddl}|輪距 {args.round_sec:g}s"
              f"|micro {'off' if args.no_micro else 'on'}|防重複 {'off(--allow-refill)' if args.allow_refill else 'on'}")
    total_ref = 0.0
    for i, leg in enumerate(legs, 1):
        if leg["qty"] == SELL_ALL:  # 賣全部:股數待庫存同步後解析
            print(f"  {i:>2}. {leg['side']} {leg['code']} {leg.get('name', '')} × 全部庫存(依同步結果)")
            continue
        lot = "盤中零股" if leg["qty"] < 1000 else "整股"
        ref = f"|參考價 {leg['ref']:,.1f}|參考額 {leg['ref'] * leg['qty']:,.0f}" if leg.get("ref") else ""
        total_ref += (leg["ref"] or 0.0) * leg["qty"]
        print(f"  {i:>2}. {leg['side']} {leg['code']} {leg.get('name', '')} × {leg['qty']} 股({lot}){ref}")
    if total_ref:
        print(f"  合計參考金額 ≈ NT$ {total_ref:,.0f}(實際以盤中成交為準,護欄封頂)")
    print("═" * 62)
    if live:
        import time as _time
        print("⚠️  LIVE 模式:5 秒後開始執行,核對有誤請立刻 Ctrl+C 取消…")
        for s in range(5, 0, -1):
            print(f"   {s}…", flush=True)
            _time.sleep(1)

    broker = FubonBroker.from_env()
    broker.login()  # dry-run 也要登入以取得行情(唯讀)
    print(f"登入成功;模式 = {'LIVE(真實下單)' if live else 'DRY-RUN(僅模擬,不送單)'}")

    # 盤前啟動自動等到 09:00 即動作;盤中啟動立即動作;收盤後啟動拒絕。
    import time as _time
    from datetime import datetime as _dt
    waited = 0.0
    while True:
        now = _dt.now(TAIPEI).strftime("%H:%M:%S")
        if now >= "13:30:00":
            raise SystemExit("已過收盤(13:30),今日不執行")
        if now >= "09:00:20":
            break
        print(f"[開盤等待] 現在 {now},開盤即動作…")
        _time.sleep(30)
        waited += 30
    if waited >= 600:
        # 長等待後 session 可能已閒置過期(2026-07-13 事故:等 3 小時後
        # Not Login 炸死,錯過早盤)——開盤瞬間無條件換新 session
        broker.login()
        print("[開盤等待] 開盤;已重新登入(session 刷新)")

    # 庫存唯讀同步:live 用來夾賣量/對庫存,dry-run 也載入以解析「賣全部」與
    # 給準確預覽。有 SELL_ALL 腿時必須載入(否則不知道賣幾股)。
    inventory: dict[str, int] | None = None
    needs_inventory = live or any(l["qty"] == SELL_ALL for l in legs)
    if needs_inventory:
        from research.trading.portfolio import positions_from_fubon_inventories
        try:
            inventory = positions_from_fubon_inventories(broker.get_inventories())
        except Exception as exc:  # noqa: BLE001
            if any(l["qty"] == SELL_ALL for l in legs):
                raise SystemExit(f"要賣全部庫存但庫存查詢失敗:{exc}")
            print(f"[庫存] 查詢失敗(dry-run 續跑):{exc}")

    mode = args.position_mode
    if mode == "auto":
        mode = (getattr(args, "_plan_position_mode", None) or "add") if args.plan else "own"
    if any(l["side"] == "Buy" for l in legs):
        print(f"[position-mode] {mode}"
              + ("(own:目標=持有 ≥ qty,先對庫存)" if mode == "own" else "(add:嚴格加碼)"))

    # ── 併發執行:一條 websocket、每腿一個引擎執行緒,全部同時掛單 ──
    import threading

    class ProgressBoard:
        """thread-safe 進度板:任何腿有變動就印一次「剩餘計劃」。"""

        def __init__(self, legs_: list[dict]):
            self._lock = threading.Lock()
            self.state = {l["code"]: "待掛單" for l in legs_}
            self.names = {l["code"]: l.get("name", "") for l in legs_}

        def update(self, code: str, status: str) -> None:
            with self._lock:
                self.state[code] = status
                done = [c for c, s in self.state.items() if s.startswith("✅") or s.startswith("⏭")]
                todo = {c: s for c, s in self.state.items() if c not in done}
                print("┄" * 62)
                print(f"[進度] 完成 {len(done)}/{len(self.state)}"
                      + (":" + "、".join(f"{c} {self.names.get(c, '')}".strip() for c in done)
                         if done else ""))
                for c, s in todo.items():
                    print(f"  ◦ {c} {self.names.get(c, '')}:{s}")
                print("┄" * 62, flush=True)

    board = ProgressBoard(legs)
    stop_event = threading.Event()
    import signal as _signal

    def _sigint(_s, _f):
        if stop_event.is_set():
            raise KeyboardInterrupt
        print("\n[Ctrl+C] 收到中止:各腿本輪結束即撤單退出(再按一次 = 立刻強制)")
        stop_event.set()

    _signal.signal(_signal.SIGINT, _sigint)

    hub = MarketHub(broker)
    prepared: list[tuple[dict, ExecutionEngine]] = []

    def _register_fill_push() -> None:
        """成交即時推播 → 喚醒對應腿(事件驅動;每輪輪詢降為備援)。

        回呼只 set() 執行緒安全的 Event,帳務仍以委託回報對帳為準。交給
        broker 保管:斷網重登會換掉整個 sdk,broker 會自動重新註冊。
        """
        def _on_filled(*cb_args):
            data = cb_args[-1] if cb_args else None
            code_f = str(getattr(data, "stock_no", "") or "")
            for _leg, eng_ in prepared:
                if eng_.code == code_f:
                    eng_.wake.set()
        broker.set_on_filled(_on_filled)
        print("[fill-push] 成交即時回報已註冊(輪詢為備援節拍;重登後自動重掛)")
    for leg in legs:
        code, qty, leg_side = leg["code"], leg["qty"], leg["side"]
        prof = profiles[leg_side]
        if leg_side == "Buy" and live and mode == "own" and inventory is not None:
            held = inventory.get(code, 0)
            if held >= qty:
                print(f"[{code}] 庫存已持有 {held} ≥ 目標 {qty},跳過")
                board.update(code, f"⏭ 已持有 {held} 股,跳過")
                continue
            if held > 0:
                print(f"[{code}] 庫存已持有 {held},只補差額 {qty - held}")
                qty = qty - held
        if leg_side == "Sell" and inventory is not None:  # 賣出前庫存夾緊/解析全部
            avail = inventory.get(code, 0)
            if avail <= 0:
                print(f"[{code}] 庫存 0,跳過賣出")
                board.update(code, "⏭ 庫存 0 跳過")
                continue
            if qty == SELL_ALL:
                print(f"[{code}] 賣全部庫存 → {avail} 股")
                qty = avail
            elif avail < qty:
                print(f"[{code}] 庫存僅 {avail} < 目標 {qty},夾緊為 {avail}")
                qty = avail
        elif leg_side == "Sell" and qty == SELL_ALL:
            raise SystemExit(f"[{code}] 賣全部但無庫存資料——請確認可查詢富邦庫存")
        micro = None
        if not args.no_micro and prof.name != "sell_stop":
            from .microstructure import MicrostructureDetector
            micro = MicrostructureDetector(leg_side)
            try:  # 初始化日極值(買=日低、賣=日高)
                rq = broker.sdk.marketdata.rest_client.stock.intraday.quote(symbol=code)
                d = rq if isinstance(rq, dict) else {}
                micro.day_extreme = float(
                    (d.get("lowPrice") if leg_side == "Buy" else d.get("highPrice")) or 0)
            except Exception:
                pass
        view = hub.add(code, detector=micro)
        engine = ExecutionEngine(
            broker, code=code, side=leg_side, qty=qty, profile=prof,
            round_sec=args.round_sec, live=live, feed=view, micro=micro,
            allow_refill=args.allow_refill,
            stop_event=stop_event, manage_sigint=False, board=board,
            avoid_open_min=args.avoid_open_min, cap_auto=args.cap_auto,
            slice_qty=args.slice_qty, trigger_strict=args.trigger_strict,
        )
        prepared.append((leg, engine))

    if not prepared:
        print("沒有可執行的腿,結束。")
        return
    hub.start()
    if live:
        _register_fill_push()

    threads: list[threading.Thread] = []
    results: dict[str, object] = {}

    def _work(leg_: dict, eng: ExecutionEngine) -> None:
        try:
            results[leg_["code"]] = eng.run()
        except Exception as exc:  # noqa: BLE001 - 一條腿的例外不得無聲吃掉整批
            # 網路類已由引擎內部消化;能到這裡的是真錯誤(憑證/程式 bug/守門)。
            print(f"[{leg_['code']}] ✋ {type(exc).__name__}: {exc}")
            board.update(leg_["code"], f"⏭ {type(exc).__name__}: {str(exc)[:60]}")
            eng.result.aborted = True
            results[leg_["code"]] = eng.result

    for leg, eng in prepared:
        t = threading.Thread(target=_work, args=(leg, eng), name=f"leg-{leg['code']}", daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        while t.is_alive():
            t.join(timeout=1.0)

    print("═" * 62)
    print("全部腿已結束(成交/放棄/中止),總結:")
    aborted_any = False
    for leg, _eng in prepared:
        r = results.get(leg["code"])
        if r is None:
            continue
        aborted_any = aborted_any or r.aborted
        print(json.dumps({
            "code": r.code, "side": leg["side"], "filled": r.filled_qty, "target": r.qty,
            "avg_price": round(r.avg_price, 4), "arrival": r.arrival,
            "shortfall_bps": r.shortfall_bps(), "aborted": r.aborted,
        }, ensure_ascii=False))
        # 目標計分板:買在距當日低多少、賣在距當日高多少(越接近 0 越好)
        cap = r.capture_bps()
        if cap is not None:
            tgt = "當日低" if leg["side"] == "Buy" else "當日高"
            print(f"      ↳ 撈價成績:{'買' if leg['side'] == 'Buy' else '賣'}在 {r.avg_price:g}"
                  f",{tgt} {r.day_extreme:g},距{tgt} {cap:g} bps"
                  + ("(貼著極值 ✓)" if cap <= 20 else "(離極值較遠)" if cap > 80 else ""))
    if aborted_any:
        print("[提醒] 有腿被中止;若在 LIVE,可跑 cancel_all 確認無殘留委託。")
    print("程式自行終止。")


# ── 離線自測:合成行情 + 假時鐘,驗證階梯/護欄/成交邏輯 ──
class _FakeFeed:
    def __init__(self, path: list[tuple[float, float]]):
        self.path = path
        self.i = 0

    def start(self) -> None:  # pragma: no cover - 介面對齊
        pass

    def refresh_rest(self) -> None:
        pass

    def snapshot(self) -> Quote:
        bid, ask = self.path[min(self.i, len(self.path) - 1)]
        self.i += 1
        import time as _t
        return Quote(bid=bid, ask=ask, last=(bid + ask) / 2, ts=_t.time())


def selftest(side: str, urgency: str = "normal") -> None:
    from dataclasses import replace

    base = 100.0
    # 劇本:先走遠(超出護欄,考驗「不追」)再回落到護欄內(capped 單此時成交)。
    away = [(base + 0.1 * i, base + 0.1 * i + 0.1) for i in range(10)]      # 買方視角:漲離
    back = [(base + 0.1, base + 0.2)] * 5 + [(base - 0.2, base - 0.1)] * 5  # 回落
    up_path = away + back
    down_path = [(2 * base - a, 2 * base - b) for (b, a) in up_path]        # 賣方鏡像(跌離再彈回)

    fake_clock_state = {"t": datetime(2026, 1, 5, 9, 30, tzinfo=TAIPEI)}

    def fake_clock() -> datetime:
        fake_clock_state["t"] += timedelta(seconds=60)
        return fake_clock_state["t"]

    prof_name = "buy_normal" if side == "Buy" else ("sell_stop" if urgency == "stop" else "sell_normal")
    prof = replace(PROFILES[prof_name], deadline_hhmm="11:00")
    feed = _FakeFeed(up_path if side == "Buy" else down_path)
    engine = ExecutionEngine(
        FubonBroker(dry_run=True), code="0000", side=side, qty=10,
        profile=prof, round_sec=0.0, live=False, feed=feed,
        clock=fake_clock, sleep=lambda _s: None,
        log_path=Path("/tmp/qlexec_selftest.jsonl"),
    )
    # selftest 繞過鎖與時段(直接打樁 guards)
    engine._guards = lambda: None  # type: ignore[method-assign]
    result = engine.run()
    assert result.filled_qty == 10, f"selftest 未成交:{result.filled_qty}(劇本應在回落段成交)"
    collar_ok = (result.avg_price <= result.arrival * (1 + prof.cap_pct) + 1e-6) if side == "Buy" \
        else (result.avg_price >= result.arrival * (1 - prof.cap_pct) - 1e-6)
    assert collar_ok, f"護欄被突破:avg {result.avg_price} arrival {result.arrival}"
    if prof_name == "sell_stop":
        first_place = next(e for e in result.events if e["event"] == "paper_place")
        assert first_place["price"] <= result.arrival, \
            f"stop 模式首輪應即跨價(掛 ≤ arrival),實際 {first_place['price']} vs {result.arrival}"
    print(f"SELFTEST OK [{prof_name}] filled {result.filled_qty}@{result.avg_price:.2f} "
          f"arrival {result.arrival:.2f} shortfall {result.shortfall_bps()} bps rounds {result.rounds}")
