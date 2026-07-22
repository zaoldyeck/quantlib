"""三策略每日檢查(純程式,零 LLM)——S / Serenity / Evergreen v3.3。

三個策略各自把整個富邦帳戶視為自己的,獨立給出:持股評判(KEEP/SELL+
機械原因)與買賣建議(含目標股數)。**本指令是純決策支援:永不下單、
永不觸發任何執行流水線**。Serenity 段讀 live ledger 的 lot 錨,用與執行
系統同一份六道門規則源(serenity/exit_rules.py)逐 lot 評判——與
`serenity.daily run` 的出場判決恆一致;ledger 更新由執行系統負責。

用法:
  uv run --project research python -m research.tri.daily                 # 富邦 API(庫存+現金)
  uv run --project research python -m research.tri.daily --positions "2330:1000" --cash 500000

報告:終端輸出 + var/reports/tri/YYYY-MM-DD.md 存檔。
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[2]
from datetime import date as Date

import polars as pl

from research.tri.advisors import (Advice, evergreen_advisor, s_advisor,
                                   serenity_advisor)  # noqa: F401

REPORTS = f"{paths.REPORTS / "tri"}"


def order_example() -> str:
    """下單指令範例——單一真相來源(報告檔與終端共用,避免兩份漂移)。

    語法與執行器一致:買入 `代碼:股數`、賣出省略股數 = 全部庫存;`--qty`
    給了就兩側通用、不給則買 1 股/賣全部;逐檔 `:股數` 永遠優先。
    """
    return (
        "── 下單指令範例(全部腿併發,買撈低點、賣撈高點,收盤未竟自動盤後掛收盤價)──\n"
        "  FUBON_DRY_RUN=false \\\n"
        "  uv run --project research python -m research.trading.execution.trade \\\n"
        "      --buy \"2408:2,3006:5\" --sell \"4973,5289\" --live\n"
        "  說明:\n"
        "    • 買入寫「代碼:股數」(2408 買 2 股、3006 買 5 股);省略股數預設買 1 股\n"
        "    • 賣出省略股數 = 賣全部庫存(4973、5289 各清倉);要賣指定量寫 4973:1\n"
        "    • 想全部買賣同一股數:加 --qty N(兩側通用);逐檔 :股數 永遠優先\n"
        "    • 只買或只賣就省略另一個參數;何時啟動都行——盤前自動等開盤、盤中立即執行")


def get_account(args) -> tuple[dict[str, float], float]:
    """回傳 (持倉, 現金)。"""
    if args.positions or args.positions_file:
        if args.positions:
            holdings = {}
            for part in args.positions.split(","):
                code, shares = part.strip().split(":")
                holdings[code.strip()] = float(shares)
        else:
            df = pl.read_csv(args.positions_file,
                             schema_overrides={"code": pl.Utf8})
            holdings = {r["code"]: float(r["shares"]) for r in df.to_dicts()}
        if args.cash is None:
            print("⚠ 手動持倉模式未給 --cash,NAV 以持股市值計(買入股數會偏低)")
        return holdings, float(args.cash or 0)
    try:
        from research.brokers.fubon import FubonBroker
        from research.trading.portfolio import (
            available_balance_from_fubon_bank_remain,
            positions_from_fubon_inventories,
        )
        broker = FubonBroker(dry_run=True)
        pos = positions_from_fubon_inventories(broker.get_inventories())
        # 2026-07-14 修正:「餘額 0」是合法狀態(交割扣款後歸零),不是讀取
        # 失敗——只有查詢真的失敗才警告,且必附原因(先前把兩者混為一談)。
        bank = broker.get_bank_remain()
        if getattr(bank, "is_success", False):
            try:
                cash = available_balance_from_fubon_bank_remain(bank)
                if cash == 0:
                    print("ℹ 富邦可用餘額 0(交割扣款後屬正常;買入股數估算需現金時用 --cash 覆蓋)")
            except Exception as e:  # noqa: BLE001
                cash = 0.0
                print(f"⚠ 富邦餘額欄位解析失敗:{e}(現金以 0 計,可用 --cash 覆蓋)")
        else:
            cash = 0.0
            print(f"⚠ 富邦銀行餘額查詢失敗:{getattr(bank, 'message', bank)}"
                  "(現金以 0 計,可用 --cash 覆蓋)")
        if args.cash is not None:
            cash = float(args.cash)
        return {str(k): float(v) for k, v in pos.items()}, float(cash)
    except Exception as e:  # noqa: BLE001
        sys.exit(f"✗ 富邦讀取失敗:{e}\n"
                 "  改用 --positions \"2330:1000,...\" --cash 500000 手動提供")


def market_maps(codes: list[str]) -> tuple[dict, dict, str]:
    """(收盤價, 公司名, 資料日)——一次查 cache。"""
    import duckdb
    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    d0 = raw.execute("SELECT max(date) FROM daily_quote").fetchone()[0]
    px, names = {}, {}
    if codes:
        ph = ",".join("?" * len(codes))
        px = dict(raw.execute(
            f"SELECT company_code, closing_price FROM daily_quote "
            f"WHERE date = ? AND company_code IN ({ph})", [d0] + codes).fetchall())
        names = dict(raw.execute(
            f"SELECT company_code, last(company_name ORDER BY year*100+month) "
            f"FROM operating_revenue WHERE company_code IN ({ph}) "
            f"GROUP BY company_code", codes).fetchall())
    return px, names, str(d0)


def ensure_fresh_cache(no_refresh: bool) -> None:
    """資料齊備自檢:不齊備就跑**一次**完整更新,齊備就直接用。

    「齊備」的定義見 `research/data_calendar.py` 與 `docs/data_ops/twse_publish_times.md`:
    **D 的資料自 D+1 00:30 起才算齊備**(融資融券官方只保證次一營業日開市前公告、借券
    22:30 才是最終值)。所以 D 日盤中跑本指令,期望日就是上一個交易日——不會為了「今天」
    去跑注定抓不全的更新(2026-07-15 教訓:抓一半 → 表間日期錯位 → 策略閘門靜靜零候選)。
    """
    import subprocess

    from research.data_calendar import latest_complete_trading_day, stale_tables

    want = latest_complete_trading_day()
    stale = stale_tables(want)
    if not stale:
        return
    detail = "、".join(f"{t}={got or '無'}" for t, got in stale.items())
    if no_refresh:
        print(f"⚠ 資料未齊備(齊備日 {want};落後表:{detail}),--no-refresh 指定跳過")
        return
    print(f"⟳ 資料未齊備(齊備日 {want})——更新中(約 10-15 分鐘,詳細過程寫入 log)…", flush=True)
    # 爬蟲細節(進度條、每個資料源)寫進 log 檔,終端只留一行狀態——使用者
    # 要的是「有沒有正常跑」,不是 6,500 行下載明細(QL_VERBOSE=true 可全開)。
    import os as _os

    log_dir = paths.OUT / "trading" / "update_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"update_{Date.today()}.log"
    with open(log_path, "w", encoding="utf-8") as lf:
        rc = subprocess.run(["sbt", "-batch", "runMain Main update"],
                            stdout=lf, stderr=subprocess.STDOUT,
                            env={**_os.environ}).returncode
        if rc != 0:
            print(f"✗ 更新失敗(rc={rc})——詳見 {log_path}")
            raise SystemExit(1)
        subprocess.run(["uv", "run", "--project", "research", "python", "research/cache_tables.py"],
                      stdout=lf, stderr=subprocess.STDOUT, check=True)
    left = stale_tables(want)
    if left:
        print("ℹ 更新後仍缺(該表可能延遲發布,或當日休市):"
              + "、".join(f"{t}={got or '無'}" for t, got in left.items()) + " —— 以現有資料執行")
    else:
        print(f"✓ 全表齊備至 {want}")


def freshness_line(d0: str) -> str:
    gap = (Date.today() - Date.fromisoformat(d0)).days
    if gap > 4:
        return (f"⚠ cache 最新資料 {d0}(落後 {gap} 天)——建議先刷新:\n"
                "  sbt \"runMain Main update\" && uv run python research/cache_tables.py")
    return f"cache 最新資料日:{d0}"


def fmt_advice(a: Advice, holdings: dict[str, float], names: dict) -> str:
    def nm(code):
        return f"{code} {names.get(code, '')}".strip()

    lines = [f"\n── {a.strategy} ──"]
    for n in a.notes:
        lines.append(f"  {n}")
    if a.ideal:
        lines.append(f"  【{a.ideal_title or '理想持倉完全體'}】")
        lines.append("    " + "、".join(f"{nm(c)}({tag})" for c, tag in a.ideal))
    if a.keeps:
        lines.append("  【續抱】")
        for code, why in a.keeps:
            lines.append(f"    KEEP {nm(code)}(現持 {int(holdings.get(code, 0)):,} 股)— {why}")
    if a.sells:
        lines.append("  【賣出】")
        for code, why in a.sells:
            lines.append(f"    SELL {nm(code)}(全部 {int(holdings.get(code, 0)):,} 股)— {why}")
    if a.buys:
        lines.append("  【買入建議】")
        for code, w, why in a.buys:
            lines.append(f"    BUY  {nm(code)}(目標 {w:.1%} 資金)— {why}")
    if not (a.keeps or a.sells or a.buys):
        lines.append("  (今日無動作)")
    return "\n".join(lines)


def ensure_serenity_current(no_curation: bool) -> list[str]:
    """Serenity 全自動狀態機(2026-07-17,使用者定調:一條指令 = 最新交易建議)。

    節奏與回測嚴格同構:月度策展(改冊)= 每月第一次執行時對上月批次;每日輕掃
    (watch log,不改冊)= 每交易日一次;引擎 brief = 當日未產則跑。headless agent
    走 claude CLI(Opus 4.8 / effort max,吃訂閱額度);CLI 缺席則 fail-open 警示。
    回傳要放進報告頂部的策展摘要行。"""
    import json as _json
    import shutil
    import subprocess

    lines: list[str] = []
    if no_curation:
        return ["- ⚠ --no-curation 指定:跳過策展狀態機(建議僅基於既有冊)"]
    state_p = paths.STATE / "serenity" / "curation_state.json"
    state = _json.loads(state_p.read_text()) if state_p.exists() else {}
    today = Date.today()
    prev_month = f"{today.year - (today.month == 1)}-{(today.month - 2) % 12 + 1:02d}"
    cur_dir = REPO_ROOT / "research" / "serenity" / "curation"

    if shutil.which("claude") is None:
        return ["- ⚠ claude CLI 不在 PATH:策展 agent 無法自動執行(建議僅基於既有冊)"]

    def run_agent(prompt_file: str, extra: str, timeout: int, tag: str) -> bool:
        prompt = (cur_dir / prompt_file).read_text(encoding="utf-8") + extra
        print(f"⟳ {tag}(claude-opus-4-8 / effort max,headless)…", flush=True)
        r = subprocess.run(
            ["claude", "-p", prompt, "--model", "claude-opus-4-8", "--effort", "max",
             "--permission-mode", "bypassPermissions"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout,
        )
        ok = r.returncode == 0
        print((r.stdout or r.stderr)[-800:])
        return ok

    # 1) 月度策展(改冊;與回測月批次同構)——每月第一次執行時對上月 M 補跑
    if state.get("last_monthly_for") != prev_month:
        ok = run_agent("monthly_curation_prompt.md",
                       f"\n\n## 本次參數\nM(策展月)= {prev_month};今日 = {today}",
                       timeout=2400, tag=f"月度策展批次(M={prev_month})")
        if ok:
            state["last_monthly_for"] = prev_month
            lines.append(f"- ✅ 月度策展批次已執行(M={prev_month};冊變更見下方策展段)")
        else:
            lines.append(f"- ⚠ 月度策展執行失敗(M={prev_month})——建議基於既有冊;請查看輸出")
    # 2) 每日輕掃(不改冊;watch log 積累)
    # ── 2026-07-20 使用者要求暫停:每日輕掃每交易日都跑 claude CLI(Opus 4.8 /
    #    effort max)很吃訂閱額度。先停用以省 token;要恢復,解除下面四行註解即可。
    #    月度策展(step 1,每月一次改冊)與引擎 brief(step 3)不受影響,照常執行。
    # if state.get("last_daily_sweep") != today.isoformat():
    #     ok = run_agent("daily_watch_prompt.md", f"\n\n## 本次參數\n今日 = {today}",
    #                    timeout=1200, tag="每日輕掃")
    #     if ok:
    #         state["last_daily_sweep"] = today.isoformat()
    lines.append("- ⏸ 每日輕掃已暫停(省 token;月度策展與引擎 brief 照常)")
    state_p.write_text(_json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")

    # 3) 引擎 brief:今日未產 → 跑 serenity daily(引擎 rerun + live book 對帳 + brief)
    brief_p = paths.OUT / "trading" / "briefs" / f"{today}.md"
    if not brief_p.exists():
        print("⟳ serenity daily run(引擎 + live book + brief)…", flush=True)
        r = subprocess.run(
            ["uv", "run", "--project", "research", "python", "-m",
             "research.serenity.daily", "run", "--skip-refresh"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=1800,
        )
        if r.returncode != 0:
            lines.append(f"- ⚠ serenity daily 失敗:{(r.stderr or r.stdout)[-300:]}")

    # 4) 策展摘要進報告
    for f, title in (("curation_monthly_latest.json", "月度策展"),
                     ("curation_sweep_latest.json", "每日輕掃")):
        p = paths.STATE / "serenity" / f
        if p.exists():
            try:
                d = _json.loads(p.read_text())
                urg = d.get("urgent") or [w for w in d.get("warnings", [])
                                          if isinstance(w, dict) and w.get("severity") == "urgent"]
                if urg:
                    lines.insert(0, f"- 🚨 **urgent 警示({title})**:{str(urg)[:300]}")
                if d.get("summary"):
                    lines.append(f"- {title}({d.get('curation_month') or d.get('sweep_date')}):{d['summary']}")
            except Exception:
                pass
    return lines


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--positions", help='手動持倉 "code:shares,code:shares"')
    ap.add_argument("--positions-file", help="csv(code,shares)")
    ap.add_argument("--cash", type=float, help="現金(手動模式或覆蓋 API 值)")
    ap.add_argument("--no-refresh", action="store_true",
                    help="跳過資料新鮮度自動刷新(預設:過期就地更新後再評判)")
    ap.add_argument("--no-curation", action="store_true",
                    help="跳過策展狀態機(每日輕掃/月度策展/引擎 brief 自動化)")
    ap.add_argument("--no-dashboard", action="store_true",
                    help="跳過 PnL 儀表板重生(reports/pnl_dashboard.html)")
    args = ap.parse_args()

    ensure_fresh_cache(args.no_refresh)
    curation_lines = ensure_serenity_current(args.no_curation)
    today = Date.today()
    holdings, cash = get_account(args)
    px, names, d0 = market_maps(list(holdings))
    mv = sum(px.get(c, 0) * s for c, s in holdings.items())
    nav = mv + cash

    parts = [f"# 三策略每日報告 {today}",
             "",
             f"- 資料到 **{d0}**——出場規則已用這段期間的**每日收盤價逐日檢查過**,"
             "你沒跑報告的那幾天也算數(規則觸發過就是觸發過)",
             f"- 帳戶:持股 {len(holdings)} 檔市值 {mv:,.0f} + 現金 {cash:,.0f} = **NAV {nav:,.0f}**",
             "- " + "、".join(f"{c} {names.get(c, '')}×{int(s):,}"
                             for c, s in sorted(holdings.items())),
             "- 三個策略各自把整個帳戶當成自己的來評判,判決會互相矛盾"
             "——**最後由你決定**;本報告由程式產生,理由直接取自當初做決定時存下來的資料"]
    parts += curation_lines

    from research.apex import data
    con = data.connect()
    all_codes = set(holdings)
    advices: dict[str, Advice] = {}
    strategy_parts: list[str] = []
    for key, fn in (("S", s_advisor), ("Evergreen", evergreen_advisor),
                    ("Serenity", serenity_advisor)):
        try:
            adv = fn(con, holdings, today, nav=nav)
            advices[key] = adv
            all_codes |= {c for c, *_ in adv.buys}
            _, more_names, _ = market_maps(list(all_codes))
            names.update(more_names)
            strategy_parts.append(fmt_advice(adv, holdings, names))
        except Exception as e:  # noqa: BLE001
            strategy_parts.append(f"\n── {key} 失敗 ──\n  {e}")

    # 置頂:今天非做不可的事(逾期出場優先——規則已觸發,延遲不代表沒發生)
    from research.tri.report import action_block, stock_appendix, stock_card
    parts.insert(6, "\n" + action_block(advices, names))  # 帳戶總覽之後、深度之前
    # 逐檔深度:為什麼買(策展理由全文+當時材料)、為什麼賣(哪道門、線在哪)
    parts.append("\n---\n\n## 📖 我的每一檔(為什麼持有、什麼會讓我賣、離出場多遠)")
    for code in sorted(holdings):
        parts.append("\n" + stock_card(code, names.get(code, ""), holdings[code], advices))
    buy_codes: dict[str, str] = {}
    for key, adv in advices.items():
        for code, _w, why in adv.buys:
            buy_codes.setdefault(code, f"{key}:{why.split('|')[0]}")
    if buy_codes:
        parts.append("\n---\n\n## 🛒 今天可以買的(為什麼它值得買)")
        for code in sorted(buy_codes):
            parts.append("\n" + stock_card(code, names.get(code, ""), 0, advices))
    parts.append("\n---\n\n## 📋 三策略各自的完整視角(誰想持有什麼)")
    parts += strategy_parts
    # 附錄:原始存證。要查證才看,不擋路
    parts.append("\n---\n\n## 📎 附錄:原始存證(策展當時的材料,供查證)")
    for code in sorted(set(holdings) | set(buy_codes)):
        parts.append("\n" + stock_appendix(code, names.get(code, ""), advices))
    parts.append("\n(三份獨立建議——各策略把整個帳戶視為自己的;"
                 "資金分配與送單由你決定;本指令永不下單)")
    parts.append("\n" + order_example())

    report = "\n".join(parts)
    os.makedirs(REPORTS, exist_ok=True)
    report_path = os.path.abspath(f"{REPORTS}/{today}.md")
    open(report_path, "w").write(report + "\n")

    # PnL 永續追蹤儀表板(2026-07-17):每次執行自動重生,瀏覽器書籤即最新。
    if not args.no_dashboard:
        import subprocess as _sp
        print("⟳ 更新 PnL 儀表板(含持倉與交易;首次或資料更新後約 1-2 分,"
              "同資料世代重跑秒回)…", flush=True)
        rd = _sp.run([sys.executable, "-m", "research.tri.pnl_dashboard"],
                     cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=1200)
        print(rd.stdout.strip()[-200:] if rd.returncode == 0
              else f"⚠ 儀表板更新失敗:{(rd.stderr or rd.stdout)[-300:]}")

    # 終端只印總結,不倒整份報告(2026-07-17:整份 700+ 行倒進 terminal =
    # 雜訊)。深度逐檔、策展理由、附錄一律進檔案,終端引導去看。
    action = action_block(advices, names)
    term = [
        f"═══ 三策略每日報告 {today}(資料到 {d0})═══",
        f"帳戶:持股 {len(holdings)} 檔市值 {mv:,.0f} + 現金 {cash:,.0f} = NAV {nav:,.0f}",
        "",
        action,
        "",
        "── 三策略各自的完整視角 ──",
        *strategy_parts,
        "",
        order_example(),
        "",
        "─" * 60,
        f"📄 完整報告(逐檔為什麼買/賣、離出場多遠、策展理由、原始存證):",
        f"   {report_path}",
        f"   開啟:open '{report_path}'  或  code '{report_path}'",
    ]
    print("\n".join(term))


if __name__ == "__main__":
    main()
