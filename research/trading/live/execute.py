"""S 策略開盤執行編排(systemd 08:55 週一至五)。

載入今日計劃 → 檢查 Gmail 取消信 → 無取消且有腿 → 派工現成 `execution.trade`
(盤前啟動自動等 09:00 才動作)→ 成交後寄執行結果。

**money-path fail-safe**:取消檢查若因 IMAP/憑證故障讀不到,**一律不交易**並告警——
絕不在無法確認使用者是否按了取消時,擅自送出真錢單。

下單語義:買各 1 股(`QL_S_SHARES_PER_BUY` 可覆蓋)、賣全部庫存。是否真下單由
`FUBON_DRY_RUN` 決定(false → 加 `--live` 真下單;true → dry-run 模擬,不加 `--live`)。

用法:
  uv run --project research python -m research.trading.live.execute
  # 測試(不查取消、強制 dry):
  uv run --project research python -m research.trading.live.execute --no-cancel-check --dry
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
LOG_DIR = paths.OUT / "trading" / "live_executions"
#: execution.trade 走完整場(等 09:00 → 階梯撈價 → 收盤未竟盤後掛收盤價)可能數小時;
#: 上限給到遠過盤後撮合(14:30)以避免無限卡住
_EXEC_TIMEOUT_SEC = 6 * 3600


def _shares_per_buy() -> int:
    try:
        return max(1, int(os.environ.get("QL_S_SHARES_PER_BUY", "1")))
    except ValueError:
        return 1


def _tail(path: Path, n: int = 40) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return "(無 log)"
    return "\n".join(lines[-n:])


def _summary_html(title: str, body: str) -> str:
    import html as _html
    return (f"<div style=\"font-family:-apple-system,'PingFang TC',sans-serif\">"
            f"<h2>{_html.escape(title)}</h2>"
            f"<pre style=\"background:#0b1020;color:#d6e2ff;padding:12px;border-radius:8px;"
            f"overflow-x:auto;font-size:12px\">{_html.escape(body)}</pre></div>")


# ── 過量下單硬防護(使用者鐵律:雲端絕不可下超過信中計劃的股數)──────────────
#: 每檔買入股數上限;1 股營運,任何 >此值 一律視為誤設 → 拒絕執行(不是夾住,是拒絕)
_MAX_SHARES_PER_BUY = 5
#: 單日買入腿數上限;S 每日進場 ≤2,遠超此值視為計劃檔損毀 → 拒絕執行
_MAX_BUY_LEGS = 5


def order_safety_error(n_shares: int, buys: list) -> str | None:
    """過量下單守門(純函式,可測):回違規原因字串,或 None(安全)。

    使用者鐵律:雲端絕不可下超過信中計劃的股數。違規一律「拒絕整批」,不夾住、不降級。
    """
    if n_shares > _MAX_SHARES_PER_BUY:
        return f"每檔股數 {n_shares} 超過安全上限 {_MAX_SHARES_PER_BUY}(QL_S_SHARES_PER_BUY 誤設?)"
    if len(buys) > _MAX_BUY_LEGS:
        return f"買入腿數 {len(buys)} 異常(>{_MAX_BUY_LEGS};S 每日進場應 ≤2)——計劃檔恐損毀"
    return None


def _abort(notifier, date_str: str, reason: str) -> None:
    """安全防護:一律不交易 + 告警 + 非零退出。過量寧可不下,不可下錯。"""
    body = f"🛑 安全防護觸發,今日一律不交易(未送任何單):{reason}"
    print(f"✗ [execute] {body}", file=sys.stderr)
    try:
        notifier.send_fill_summary(date_str, _summary_html("⚠️ 安全中止", body), body)
    except Exception:  # noqa: BLE001 - 連告警都寄不出也要中止
        pass
    raise SystemExit(1)


def main() -> None:
    ap = argparse.ArgumentParser(description="S 策略開盤執行(派工 execution.trade)")
    ap.add_argument("--date", default=None, help="計劃日(預設台北今日)")
    ap.add_argument("--grace-sec", type=int, default=60,
                    help="查取消前的緩衝秒數(讓臨界前寄出的取消信傳播;預設 60)")
    ap.add_argument("--no-cancel-check", action="store_true", help="跳過取消檢查(測試)")
    ap.add_argument("--dry", action="store_true", help="強制 dry-run(不加 --live,不論 env)")
    ap.add_argument("--check", action="store_true",
                    help="只驗證:載入計劃→過守門→印出將派工的確切指令,不真派工(上線前核對)")
    args = ap.parse_args()

    from research.brokers.fubon import load_env_file
    from research.trading.live import notify
    from research.trading.live.premarket import plan_path

    date_str = args.date or notify.today_taipei().isoformat()

    # 0) 通知器(同時是取消檢查器)——構不出來就無法確認取消 → 不交易
    try:
        notifier = notify.GmailNotifier.from_env()
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"✗ [execute] 無法建立 Gmail 通知器({exc});無法確認取消狀態,"
                 "安全起見今日不交易")

    # 1) 載入計劃
    pp = plan_path(date_str)
    if not pp.exists():
        sys.exit(f"✗ [execute] 找不到今日計劃 {pp}(premarket 是否已於 07:20 執行?)")
    from research.trading.live.s_plan import protected_from_env

    load_env_file()
    plan = json.loads(pp.read_text(encoding="utf-8"))
    buys: list[str] = list(plan.get("buys") or [])
    # 所有被建議賣的股(計劃可能已拆保留、也可能沒拆)——execute 為權威閘,一律以
    # env 保留清單重新判定,即使計劃檔沒拆保留股也擋得住(defense in depth)。
    suggested_sells = list(plan.get("sells") or []) + list(plan.get("protected_sells") or [])
    protected = protected_from_env()
    auto_sells = [c for c in suggested_sells if c not in protected]
    protected_suggested = [c for c in suggested_sells if c in protected]
    print(f"[execute] {date_str} 計劃:買 {buys or '無'}｜自動賣 {auto_sells or '無'}"
          + (f"｜保留股待確認 {protected_suggested}" if protected_suggested else ""))

    if not buys and not suggested_sells:
        print("[execute] 今日無下單腿,結束。")
        return

    # 2) 取消檢查(fail-safe:讀不到 → 不交易 + 告警)
    if not args.no_cancel_check:
        if args.grace_sec > 0:
            print(f"[execute] 緩衝 {args.grace_sec}s 讓臨界取消信傳播…")
            time.sleep(args.grace_sec)
        try:
            cancelled = notifier.is_cancelled(date_str)
        except Exception as exc:  # noqa: BLE001 - 讀不到取消 = 不可交易
            body = f"無法確認取消狀態({type(exc).__name__}: {exc});安全起見今日不交易。"
            print(f"✗ [execute] {body}", file=sys.stderr)
            try:
                notifier.send_fill_summary(date_str, _summary_html("⚠️ 今日未交易", body), body)
            except Exception:  # noqa: BLE001 - 連告警都寄不出就只留 log
                pass
            raise SystemExit(1)
        if cancelled:
            body = "偵測到你的取消信,今日已依要求中止執行(未送任何單)。"
            print(f"[execute] {body}")
            notifier.send_fill_summary(date_str, _summary_html("已取消今日執行", body), body)
            return

    # 2b) 保留股確認檢查(fail-safe:讀不到確認 → 保守不賣;預設保留股一律續抱)
    confirmed_sells: list[str] = []
    for c in protected_suggested:
        try:
            if notifier.is_sell_confirmed(c, date_str):
                confirmed_sells.append(c)
        except Exception as exc:  # noqa: BLE001 - 讀不到確認 = 不賣(保守)
            print(f"[execute] 保留股 {c} 確認狀態讀取失敗({exc});保守不賣", file=sys.stderr)
    sells = auto_sells + confirmed_sells
    if protected_suggested:
        kept = [c for c in protected_suggested if c not in confirmed_sells]
        print(f"[execute] 保留股:確認賣出 {confirmed_sells or '無'}、續抱保留 {kept or '無'}")

    if not buys and not sells:
        print("[execute] 過濾保留股後今日無下單腿(全數續抱),結束。")
        return

    # 3) 組下單指令(買各 N 股、賣全部)
    live = (not args.dry) and os.environ.get("FUBON_DRY_RUN", "true").lower() in {"0", "false", "no"}
    n = _shares_per_buy()

    # ── 過量下單硬防護(下真單前的最後一道閘;寧可整批拒絕,不可下超量)──
    err = order_safety_error(n, buys)
    if err:
        _abort(notifier, date_str, err)

    cmd = ["uv", "run", "--project", "research", "python", "-m",
           "research.trading.execution.trade"]
    if buys:
        cmd += ["--buy", ",".join(f"{c}:{n}" for c in buys)]
    if sells:
        cmd += ["--sell", ",".join(f"{c}:all" for c in sells)]
    if live:
        cmd += ["--live"]
    mode = "LIVE(真下單)" if live else "DRY-RUN(模擬)"
    # 明確印出最大曝險:買入嚴格上限 = 每檔 n 股 × 腿數(執行器 own 模式只會更少)
    print(f"[execute] 下單上限:買 {len(buys)} 檔 × {n} 股 = 最多 {n * len(buys)} 股"
          f"(own 模式已持有則跳過);賣 {len(sells)} 檔全部庫存。模式 {mode}")
    print(f"[execute] 派工 execution.trade:{' '.join(cmd[6:])}")

    if args.check:
        print("[execute] --check:僅核對,不派工。上方即今日將送出的確切指令。")
        return

    # 3b) 賣出前必撤該標的的券商端安全網(money-path 命門,雙保險)
    #     條件單掛在券商端會自己活著:我們把部位賣掉後若它仍武裝,日後觸發就會
    #     賣掉不存在的部位(可能變成融券)。盤前 sync 已排除今日賣單,此處再撤一次
    #     以防「盤前後才變更」與歷史殘留。撤不掉要響亮,但不擋交易(部位還在,
    #     多一張停損單不會超賣;真正危險的是賣完還留著,而那由下一次 sync 收斂)。
    if sells and live:
        try:
            from research.trading.live import safety_net
            from research.brokers.fubon import FubonBroker
            _b = FubonBroker.from_env()
            _b.login()
            gone = safety_net.cancel_for(_b, set(sells))
            print(f"[execute] 賣出前撤安全網 {len(gone)} 張({sells})")
        except Exception as exc:  # noqa: BLE001 - 撤不掉要響亮,不擋今日交易
            print(f"⚠ [execute] 安全網撤單失敗({type(exc).__name__}: {exc});"
                  f"賣出後請確認殘留條件單", file=sys.stderr)

    # 4) 阻塞執行(execution.trade 內部自等 09:00);全程 log 落檔,尾段寄回
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"execute_{date_str}.log"
    rc = 1
    try:
        with log_path.open("w", encoding="utf-8") as lf:
            rc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT,
                                timeout=_EXEC_TIMEOUT_SEC, check=False).returncode
    except subprocess.TimeoutExpired:
        body = f"execution.trade 逾 {_EXEC_TIMEOUT_SEC // 3600}h 未結束,已中止;請人工檢查殘留委託。"
        print(f"✗ [execute] {body}", file=sys.stderr)
        notifier.send_fill_summary(date_str, _summary_html("⚠️ 執行逾時", body), body)
        raise SystemExit(1)

    tail = _tail(log_path, 40)
    title = f"{mode} 執行完成" if rc == 0 else f"⚠️ 執行結束但 rc={rc}(請檢查)"
    print(f"[execute] {title}(log:{log_path})")
    notifier.send_fill_summary(date_str, _summary_html(title, tail), tail)
    if rc != 0:
        raise SystemExit(rc)


if __name__ == "__main__":
    main()
