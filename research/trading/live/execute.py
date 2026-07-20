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

REPO_ROOT = Path(__file__).resolve().parents[3]
LOG_DIR = REPO_ROOT / "research" / "out" / "trading" / "live_executions"
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


def main() -> None:
    ap = argparse.ArgumentParser(description="S 策略開盤執行(派工 execution.trade)")
    ap.add_argument("--date", default=None, help="計劃日(預設台北今日)")
    ap.add_argument("--grace-sec", type=int, default=60,
                    help="查取消前的緩衝秒數(讓臨界前寄出的取消信傳播;預設 60)")
    ap.add_argument("--no-cancel-check", action="store_true", help="跳過取消檢查(測試)")
    ap.add_argument("--dry", action="store_true", help="強制 dry-run(不加 --live,不論 env)")
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
    plan = json.loads(pp.read_text(encoding="utf-8"))
    buys: list[str] = list(plan.get("buys") or [])
    sells: list[str] = list(plan.get("sells") or [])
    print(f"[execute] {date_str} 計劃:買 {buys or '無'}｜賣 {sells or '無'}")

    if not buys and not sells:
        print("[execute] 今日無自動下單腿,結束。")
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

    # 3) 組下單指令(買各 N 股、賣全部)
    load_env_file()
    live = (not args.dry) and os.environ.get("FUBON_DRY_RUN", "true").lower() in {"0", "false", "no"}
    n = _shares_per_buy()
    cmd = ["uv", "run", "--project", "research", "python", "-m",
           "research.trading.execution.trade"]
    if buys:
        cmd += ["--buy", ",".join(f"{c}:{n}" for c in buys)]
    if sells:
        cmd += ["--sell", ",".join(f"{c}:all" for c in sells)]
    if live:
        cmd += ["--live"]
    mode = "LIVE(真下單)" if live else "DRY-RUN(模擬)"
    print(f"[execute] 派工 execution.trade [{mode}]:{' '.join(cmd[6:])}")

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
