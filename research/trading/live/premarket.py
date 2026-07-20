"""S 策略盤前編排(systemd 07:20 週一至五)。

更新台股資料 → 讀富邦持股/現金 → 建今日計劃(重用 s_advisor)→ 寄計劃信(含
🛑 取消鈕)→ 計劃落盤供 execute 於 08:55 取用。**本步驟永不下單**。

用法:
  uv run --project research python -m research.trading.live.premarket
  # 測試(不跑爬蟲、手動持倉、只印不寄):
  uv run --project research python -m research.trading.live.premarket \
      --no-refresh --positions "2408:1000" --cash 100000 --no-email
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
STATE_DIR = REPO_ROOT / "research" / "trading" / "live" / "state" / "plans"


def _parse_positions(spec: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        code, shares = part.split(":")
        out[code.strip().zfill(4)] = float(shares)
    return out


def plan_path(date_str: str) -> Path:
    return STATE_DIR / f"{date_str}.json"


def _all_codes(plan) -> list[str]:
    codes = set(plan.buys) | set(plan.sells)
    codes |= {c for c, _ in plan.manual_review}
    codes |= {c for c, _ in plan.keeps}
    codes |= {c for c, _ in plan.queued}
    return sorted(codes)


def main() -> None:
    ap = argparse.ArgumentParser(description="S 策略盤前計劃(永不下單)")
    ap.add_argument("--no-refresh", action="store_true", help="跳過資料爬蟲(用既有 cache)")
    ap.add_argument("--positions", default=None, help='手動持倉 "2408:1000,3006:500"')
    ap.add_argument("--cash", type=float, default=None, help="手動現金(TWD)")
    ap.add_argument("--no-email", action="store_true", help="只印不寄(測試)")
    args = ap.parse_args()

    from research.apex import data
    from research.trading.execution.daily_context import lookup_names
    from research.trading.live import account, notify
    from research.trading.live.s_plan import build_day_plan

    today = notify.today_taipei()
    print(f"[premarket] 交易日 {today}")

    # 1) 資料更新(齊備自檢在爬蟲內);--no-refresh 用既有 cache
    if not args.no_refresh:
        from research.crawl.update import ensure_fresh
        ensure_fresh()

    # 2) 帳戶
    con = data.connect()
    try:
        if args.positions is not None:
            holdings = _parse_positions(args.positions)
            cash = float(args.cash or 0.0)
        else:
            holdings, cash = account.get_holdings_cash()
            if args.cash is not None:
                cash = float(args.cash)
        nav = account.estimate_nav(con, holdings, cash)
        print(f"[premarket] 持股 {len(holdings)} 檔、現金 {cash:,.0f}、NAV≈{nav:,.0f}")

        # 3) 決策(重用 s_advisor)
        plan = build_day_plan(con, holdings, today, nav)
        names = lookup_names(_all_codes(plan))
    finally:
        con.close()

    print(f"[premarket] 買 {plan.buys or '無'}｜賣 {plan.sells or '無'}"
          + (f"｜待人工 {[c for c, _ in plan.manual_review]}" if plan.manual_review else ""))

    # 4) 計劃落盤(execute 取用)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    plan_path(today.isoformat()).write_text(
        json.dumps(plan.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[premarket] 計劃 → {plan_path(today.isoformat())}")

    # 5) 寄計劃信(含取消鈕)
    if args.no_email:
        print("[premarket] --no-email:略過寄信")
        return
    try:
        notify.GmailNotifier.from_env().send_plan_email(plan, names)
        print("[premarket] 計劃信已寄出")
    except Exception as exc:  # noqa: BLE001 - 寄信失敗要響亮(否則你不知道今天要交易)
        print(f"✗ [premarket] 計劃信寄送失敗:{type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
