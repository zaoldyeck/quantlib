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
import os
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
    codes = set(plan.buys) | set(plan.sells) | set(plan.protected_sells)
    codes |= {c for c, _ in plan.manual_review}
    codes |= {c for c, _ in plan.keeps}
    codes |= {c for c, _ in plan.queued}
    return sorted(codes)


def _build_settlement(con, plan, holdings: dict[str, float], cash: float):
    """組出計劃信的金額試算;任何一步失敗都不得擋住交易計劃(交易 > 資訊)。"""
    try:
        from research.apex import data
        from research.trading.cost_basis import BASIS_LABEL, cost_of
        from research.trading.live.money import build_settlement

        codes = sorted(set(plan.buys) | set(plan.sells))
        prices: dict[str, float] = {}
        if codes:
            ph = ",".join("?" * len(codes))
            d0 = data.latest_date(con)
            prices = {str(k): float(v) for k, v in con.execute(
                f"SELECT company_code, closing_price FROM daily_quote "
                f"WHERE date = ? AND company_code IN ({ph})", [d0, *codes]).fetchall()
                if v is not None}
        costs = {}
        for c in plan.sells:
            try:
                px, basis = cost_of(c)
                if px:
                    # 標註走 cost_basis 的唯一真源對照表,不自行造字串
                    lbl = BASIS_LABEL.get(basis, "")
                    costs[c] = (float(px), f"({lbl})" if lbl and lbl != "成交價" else "")
            except Exception:  # noqa: BLE001 - 單檔成本取不到就不顯示 ROI,不編造
                continue
        return build_settlement(cash, plan.buys, plan.sells, _shares_per_buy(),
                                holdings, prices, costs)
    except Exception as exc:  # noqa: BLE001 - 資訊層失敗不得拖垮交易計劃
        print(f"⚠ [premarket] 資金試算失敗(信件將省略金額):{type(exc).__name__}: {exc}",
              file=sys.stderr)
        return None


def _shares_per_buy() -> int:
    """與 execute 同一環境變數(唯一真源),確保信中金額 = 實際下單股數。"""
    try:
        return max(1, int(os.environ.get("QL_S_SHARES_PER_BUY", "1")))
    except ValueError:
        return 1


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
    from research.trading.live.s_plan import build_day_plan, protected_from_env

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

        # 3) 決策(重用 s_advisor)+ 套用保留股(使用者要自己控的持股不自動賣)
        plan = build_day_plan(con, holdings, today, nav, protected_from_env())
        names = lookup_names(_all_codes(plan))
        # 金額/損益試算(信件用):價=cache 最新收盤(盤前無即時報價,已於信中註明);
        # 成本走 cost_basis 唯一真源(收養者標「收養價」,不假裝是真實成本)。
        settle = _build_settlement(con, plan, holdings, cash)
    finally:
        con.close()

    print(f"[premarket] 買 {plan.buys or '無'}｜賣 {plan.sells or '無'}"
          + (f"｜待人工 {[c for c, _ in plan.manual_review]}" if plan.manual_review else ""))

    # 4) 計劃落盤(execute 取用)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    plan_path(today.isoformat()).write_text(
        json.dumps(plan.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[premarket] 計劃 → {plan_path(today.isoformat())}")

    # 4b) 券商端災難停損同步(VM 掛掉時的唯一保護層;定位與證據見 safety_net.py)
    #     水位比策略自身 trail 35% 更寬(50%)→ 正常運作時日頻路徑一定先出場,不干擾。
    #     今日要動的標的(買/賣/保留待確認/人工複核)一律排除,避免「賣掉後條件單裸奔」。
    #     置於計劃落盤之後、寄信之前:計劃已安全落盤(execute 有料),且不受 --no-email 影響。
    try:
        from research.trading.live import safety_net
        if args.positions is not None:
            # **券商端狀態只能依據券商端事實同步**。用 --positions 餵假持倉時去 sync,
            # 會拿測試資料撤掉真實部位的保護單(2026-07-22 實測:撤 7 掛 0,真倉
            # 一度裸奔)。測試就該是測試,不得有真實副作用。
            print("[premarket] 安全網:跳過(--positions 為測試持倉,不得改動券商端)")
        elif safety_net.enabled():
            skip = set(plan.buys) | set(plan.sells) | set(plan.protected_sells)
            skip |= {c for c, _ in plan.manual_review}
            res = safety_net.sync(account.get_broker(), holdings, plan.peaks, skip)
            if res.get("skipped"):
                print(f"[premarket] 安全網:{res['skipped']}")
            else:
                print(f"[premarket] 安全網:目標 {res.get('target')} 檔、"
                      f"撤 {res.get('cancelled')}、新掛 {len(res.get('placed') or [])}"
                      + (f";⚠ 錯誤 {res['errors']}" if res.get("errors") else ""))
    except Exception as exc:  # noqa: BLE001 - 安全網失敗不得拖累今日交易
        print(f"⚠ [premarket] 安全網同步失敗(不影響今日交易):{type(exc).__name__}: {exc}",
              file=sys.stderr)

    # 5) 寄計劃信(含取消鈕)
    if args.no_email:
        print("[premarket] --no-email:略過寄信")
        return
    email_ok = True
    try:
        notify.GmailNotifier.from_env().send_plan_email(plan, names, settle)
        print("[premarket] 計劃信已寄出")
    except Exception as exc:  # noqa: BLE001 - 寄信失敗要響亮(否則你不知道今天要交易)
        print(f"✗ [premarket] 計劃信寄送失敗:{type(exc).__name__}: {exc}", file=sys.stderr)
        email_ok = False

    # 6) 年度 refit(每年 12 月首個盤前自動跑一次;併入盤前=省一個 timer、爬完蟲即用最新
    #    資料)。獨立 try:refit 失敗絕不影響今日交易計劃(交易計劃早已落盤供 execute)。
    try:
        from research.apex import refit
        if refit.maybe_run_annual(today):
            print("[premarket] 年度 refit 已跑並寄出")
    except Exception as exc:  # noqa: BLE001 - refit 失敗不得拖累今日交易
        print(f"⚠ [premarket] 年度 refit 失敗(不影響今日交易):{type(exc).__name__}: {exc}",
              file=sys.stderr)

    if not email_ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
