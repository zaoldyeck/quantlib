"""Serenity daily operations orchestrator — live-book architecture.

Design (2026-07-07 rev2, per user direction):

- The ENGINE is the selection brain (candidates, guards, backtest validation).
- The LIVE BOOK is the position manager: every real position — engine-filled
  or ADOPTED from pre-existing holdings — carries its own plan (anchor, peak,
  the five exit rules) and is managed to ITS exit. Nothing is sold merely for
  "not being on today's list".
- Broker inventory is the single source of truth for what we hold; the live
  ledger reconciles to it every morning. New unknown holdings are auto-adopted
  (anchor = adoption-day close, clocks restart) and flagged for a judgment-layer
  Serenity vet (registry/thesis/plan) the same day.

Daily flow (pre-open):
  1. data refresh (through yesterday) + stale fuse
  2. engine rerun (candidates + guard states + research book)
  3. broker inventory sync -> live-ledger reconcile (adopt / drop / qty)
  4. live book update: peaks, five exit rules, overrides -> exits
     slots -> refills from engine's latest scored list (guards & throttle)
  5. write registered target-weights CSV from the LIVE book (planner input)
  6. build Fubon order plan (offline, dry-run) + fuses
  7. daily brief with per-position plans (paper trail)

Submission stays human-gated:
  uv run --project research python -m research.trading.auto_trader submit-plan <plan.json>
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "research"))

RESULTS = REPO_ROOT / "research" / "strat_lab" / "results"
OUT_DIR = REPO_ROOT / "research" / "out" / "trading"
BRIEFS = OUT_DIR / "briefs"
PLANS = OUT_DIR / "plans"
OVERRIDES = Path(__file__).parent / "state" / "overrides.json"
LIVE_BOOK = Path(__file__).parent / "state" / "live_positions.json"
STRATEGY_ID = "serenity_ev_v2_thesis_inst"
VARIANT = "ev_v2_thesis_inst"
ENGINE = REPO_ROOT / "research" / "serenity" / "engine.py"
STATE = RESULTS / f"serenity_event_engine_v1_{VARIANT}_state.json"
PICKS = RESULTS / "serenity_event_engine_v1_picks.csv"
TARGET_WEIGHTS = RESULTS / f"serenity_event_engine_v1_{VARIANT}_target_weights.csv"

MAX_POSITIONS = 10
MAX_NEW_PER_DAY = 3
COOLDOWN_DAYS = 20  # 出場後冷卻(交易日),與 engine.py 回測常數同值鏡像
# 六道門規則與參數集中於 exit_rules(tri 決策支援共用同一真相來源)
from research.serenity.exit_rules import (  # noqa: E402
    ABS_STOP, TAKE_PROFIT, TIME_DAYS, TIME_RET, TRAIL, evaluate_exit,
)


def sh(cmd: list[str], timeout: int = 3600) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout)


def load_json(path: Path, default):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default


def save_json(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def cmd_override(args: argparse.Namespace) -> None:
    data = load_json(OVERRIDES, {"force_exit": {}, "log": []})
    ts = datetime.now().isoformat(timespec="seconds")
    if args.force_exit:
        code = str(args.force_exit).zfill(4)
        data.setdefault("force_exit", {})[code] = {"reason": args.reason, "date": date.today().isoformat()}
        data.setdefault("log", []).append({"ts": ts, "action": "force_exit", "code": code, "reason": args.reason})
        print(f"override recorded: force_exit {code} — {args.reason}")
    elif args.freeze_refill:
        data["refill_freeze"] = {"reason": args.reason, "date": date.today().isoformat()}
        data.setdefault("log", []).append({"ts": ts, "action": "freeze_refill", "reason": args.reason})
        print(f"override recorded: refill freeze ON — {args.reason}")
    else:
        data.pop("refill_freeze", None)
        data.setdefault("log", []).append({"ts": ts, "action": "unfreeze_refill", "reason": args.reason})
        print(f"override recorded: refill freeze OFF — {args.reason}")
    save_json(OVERRIDES, data)
    print("run `serenity_daily run --skip-refresh` to regenerate the plan with this override.")


def fetch_broker_positions() -> tuple[dict[str, int] | None, float | None, str]:
    try:
        from research.brokers.fubon import FubonBroker
        from research.trading.portfolio import (
            available_balance_from_fubon_bank_remain,
            positions_from_fubon_inventories,
        )

        broker = FubonBroker.from_env()
        positions = positions_from_fubon_inventories(broker.get_inventories())
        bank = broker.get_bank_remain()
        balance = (
            available_balance_from_fubon_bank_remain(bank) if getattr(bank, "is_success", False) else None
        )
        return positions, balance, "synced"
    except Exception as exc:
        return None, None, f"券商庫存同步失敗:{exc}"


def market_data(
    con, codes: set[str], cutoff: date
) -> tuple[dict[str, float], dict[str, float], dict[str, float], list[date]]:
    """Latest close, yoy_3m (PIT), 20d institutional net flow, trading calendar."""
    cal = [r[0] for r in con.sql("SELECT DISTINCT date FROM daily_quote ORDER BY date").fetchall()]
    closes: dict[str, float] = {}
    yoy3: dict[str, float] = {}
    inst20: dict[str, float] = {}
    if codes:
        cl = ",".join(f"'{c}'" for c in sorted(codes))
        for code, px in con.sql(
            f"SELECT company_code, closing_price FROM daily_quote WHERE date = '{cutoff}' AND company_code IN ({cl})"
        ).fetchall():
            closes[str(code).zfill(4)] = float(px)
        rev = con.sql(
            f"""
            SELECT company_code, year, month, monthly_revenue_yoy FROM operating_revenue
            WHERE company_code IN ({cl}) ORDER BY company_code, year, month
            """
        ).fetchall()
        by_code: dict[str, list[tuple[int, int, float]]] = {}
        for code, y, m, yoy in rev:
            if yoy is not None:
                by_code.setdefault(str(code).zfill(4), []).append((int(y), int(m), float(yoy)))
        for code, rows in by_code.items():
            # LIVE semantics: a row's presence in the table means it has been
            # published (the crawler re-fetches the in-window month daily), so
            # every completed month on record is usable immediately —
            # event-driven revenue, no waiting for the 10th.
            usable = [yoy for y, m, yoy in rows if date(y, m, 1) < date(cutoff.year, cutoff.month, 1)]
            if len(usable) >= 2:
                yoy3[code] = sum(usable[-3:]) / len(usable[-3:])
        for code, s in con.sql(
            f"""
            SELECT company_code, sum(total_difference) FROM (
                SELECT company_code, date, total_difference,
                       row_number() OVER (PARTITION BY company_code ORDER BY date DESC) rn
                FROM daily_trading_details
                WHERE company_code IN ({cl}) AND date <= '{cutoff}'
            ) WHERE rn <= 20 GROUP BY company_code
            """
        ).fetchall():
            if s is not None:
                inst20[str(code).zfill(4)] = float(s)
    return closes, yoy3, inst20, cal


def trading_days_between(cal: list[date], start: date, end: date) -> int:
    return sum(1 for d in cal if start < d <= end)


def fetch_confcall_schedule(today: date):
    """MOPS 法人說明會一覽(t100sb02_1,sii+otc,當月+次月)。Fail-open:錯誤回空表。"""
    import re
    import urllib.parse
    import urllib.request

    import pandas as pd

    rows_out: list[dict] = []
    months = [(today.year, today.month)]
    nxt = date(today.year + (today.month == 12), today.month % 12 + 1, 1)
    months.append((nxt.year, nxt.month))
    for typek in ("sii", "otc"):
        for y, m in months:
            data = urllib.parse.urlencode(
                {
                    "encodeURIComponent": 1, "step": 1, "firstin": 1, "off": 1,
                    "TYPEK": typek, "year": y - 1911, "month": f"{m:02d}",
                }
            ).encode()
            req = urllib.request.Request(
                "https://mopsov.twse.com.tw/mops/web/ajax_t100sb02_1",
                data=data,
                headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
            )
            html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore")
            for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S):
                cells = [
                    re.sub(r"<[^>]+>", "", c).strip()
                    for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
                ]
                if len(cells) >= 7 and re.fullmatch(r"\d{4,6}", cells[0] or ""):
                    roc = re.fullmatch(r"(\d{3})/(\d{2})/(\d{2})", cells[2] or "")
                    if not roc:
                        continue
                    rows_out.append(
                        {
                            "company_code": cells[0].zfill(4),
                            "company_name": cells[1],
                            "date": date(int(roc[1]) + 1911, int(roc[2]), int(roc[3])).isoformat(),
                            "time": cells[3],
                            "summary": cells[5][:60],
                            "pdf": f"https://mopsov.twse.com.tw/nas/STR/{cells[6]}" if cells[6].endswith(".pdf") else "",
                            "market": typek,
                        }
                    )
    return pd.DataFrame(rows_out).drop_duplicates(["company_code", "date"])


def latest_candidates() -> list[str]:
    import pandas as pd

    if not PICKS.exists():
        return []
    p = pd.read_csv(PICKS, dtype={"company_code": str})
    last = p[p.signal_date == p.signal_date.max()].sort_values("score", ascending=False)
    return [str(c).zfill(4) for c in last["company_code"]]


def cmd_run(args: argparse.Namespace) -> None:
    import duckdb

    BRIEFS.mkdir(parents=True, exist_ok=True)
    today = date.today()
    brief: list[str] = [f"# Serenity daily brief — {today}", ""]

    # 1) data refresh
    if not args.skip_refresh:
        r = sh(["sbt", "runMain Main update"], timeout=3000)
        if r.returncode != 0:
            print(r.stdout[-1500:], r.stderr[-1500:])
            raise SystemExit("crawl failed — no plan today (fail-closed).")
        r = sh(["uv", "run", "--project", "research", "python", "research/cache_tables.py"], timeout=1200)
        if r.returncode != 0:
            raise SystemExit("cache rebuild failed — no plan today (fail-closed).")

    con = duckdb.connect(str(REPO_ROOT / "research" / "cache.duckdb"), read_only=True)
    cutoff = con.sql("select max(date) from daily_quote").fetchone()[0]
    staleness = (today - cutoff).days
    brief.append(f"- data cutoff: **{cutoff}**(距今 {staleness} 天)")

    # Revenue publication first-seen capture: the in-window month's summary is
    # re-crawled daily, so newly appearing codes today == published today.
    # This builds the timestamp dataset that will make the event-driven
    # revenue upgrade backtestable in the future.
    prev_m = date(today.year - (today.month == 1), (today.month - 2) % 12 + 1, 1)
    seen_path = REPO_ROOT / "research" / "data" / "revenue_first_seen.parquet"
    try:
        import pandas as pd

        rows = con.sql(
            f"SELECT DISTINCT company_code FROM operating_revenue WHERE year={prev_m.year} AND month={prev_m.month}"
        ).fetchall()
        published = {str(r[0]).zfill(4) for r in rows}
        seen = (
            pd.read_parquet(seen_path)
            if seen_path.exists()
            else pd.DataFrame(columns=["company_code", "year", "month", "first_seen"])
        )
        known = set(
            seen.loc[(seen["year"] == prev_m.year) & (seen["month"] == prev_m.month), "company_code"]
        )
        fresh = sorted(published - known)
        if fresh:
            seen_path.parent.mkdir(parents=True, exist_ok=True)
            add = pd.DataFrame(
                {
                    "company_code": fresh,
                    "year": prev_m.year,
                    "month": prev_m.month,
                    "first_seen": today.isoformat(),
                }
            )
            pd.concat([seen, add], ignore_index=True).to_parquet(seen_path, index=False)
            brief.append(
                f"- 📬 {prev_m.year}-{prev_m.month:02d} 月營收今日新公布 **{len(fresh)}** 家(已記首見日;累計 {len(published)} 家)"
            )
    except Exception as exc:
        brief.append(f"- ⚠️ 營收首見日收集失敗:{exc}")
    if staleness > 4 and not args.allow_stale:
        con.close()
        raise SystemExit(f"data cutoff {cutoff} stale ({staleness}d) — refusing to plan (--allow-stale to override).")

    # 2) engine rerun (candidates + guards + research book)
    r = sh(
        [
            "uv", "run", "--project", "research", "python", str(ENGINE),
            "--start", "2025-01-01", "--emit-book", VARIANT, "--live-revenue",
        ],
        timeout=1800,
    )
    if r.returncode != 0:
        print(r.stdout[-1500:], r.stderr[-1500:])
        con.close()
        raise SystemExit("engine run failed — no plan today.")
    state = load_json(STATE, {})
    if str(state.get("as_of")) != str(cutoff):
        con.close()
        raise SystemExit(f"engine book as_of {state.get('as_of')} != cutoff {cutoff} — refusing to plan.")
    guards_theme = bool(state.get("theme_risk_off"))
    guards_market = bool(state.get("market_risk_off_today"))
    brief.append(f"- regime guards: theme_risk_off={guards_theme}, market_risk_off={guards_market}")

    # 3) broker inventory -> live-ledger reconcile
    live = load_json(LIVE_BOOK, {"positions": {}, "log": []})
    positions: dict[str, dict] = live.get("positions", {})
    yesterday_plan_buys: set[str] = set()
    plans_sorted = sorted(PLANS.glob("serenity_daily_*.json"))
    if plans_sorted:
        prev = load_json(plans_sorted[-1], {})
        yesterday_plan_buys = {
            str(o.get("symbol")).zfill(4)
            for o in prev.get("orders", [])
            if str(o.get("side", "")).lower() == "buy"
        }

    broker_positions, broker_balance, sync_note = (None, None, "skipped (--no-sync)")
    if not args.no_sync:
        broker_positions, broker_balance, sync_note = fetch_broker_positions()
    if broker_positions is None:
        brief.append(f"- ⚠️ {sync_note} —— 使用上次 live ledger,**送單前務必人工核對庫存**")
        broker_positions = {c: p["qty"] for c, p in positions.items()}
    else:
        bal_txt = f",可用餘額 {broker_balance:,.0f}" if broker_balance is not None else ""
        brief.append(f"- 券商庫存已同步:{len(broker_positions)} 檔{bal_txt}")

    all_codes = set(broker_positions) | set(positions) | set(latest_candidates())
    closes, yoy3, inst20, cal = market_data(con, all_codes, cutoff)
    con.close()

    adopted_today: list[str] = []
    for code, qty in broker_positions.items():
        if code not in positions:
            src = "engine" if code in yesterday_plan_buys else "adopted"
            anchor = closes.get(code)
            positions[code] = {
                "qty": qty,
                "anchor": anchor,
                "peak": anchor,
                "entry_date": cutoff.isoformat(),
                "source": src,
                "vetted": src == "engine",
            }
            live.setdefault("log", []).append(
                {"ts": datetime.now().isoformat(timespec="seconds"), "action": "adopt", "code": code, "source": src, "anchor": anchor}
            )
            if src == "adopted":
                adopted_today.append(code)
        else:
            positions[code]["qty"] = qty
    for code in [c for c in positions if c not in broker_positions]:
        live["log"].append(
            {"ts": datetime.now().isoformat(timespec="seconds"), "action": "closed", "code": code}
        )
        positions.pop(code)
    if adopted_today:
        brief.append(
            f"- 🆕 收養持股(非強賣;錨=收養日收盤,時鐘重啟):{adopted_today} —— **今日判斷層必須完成 Serenity 檢核並補論點註記**"
        )

    # 4) live book update: peaks + five exit rules + overrides
    overrides = load_json(OVERRIDES, {"force_exit": {}})
    force_exit = set(overrides.get("force_exit", {}))
    exits: list[tuple[str, str]] = []
    for code, pos in positions.items():
        px = closes.get(code)
        if px is None:
            continue
        if pos.get("anchor") is None:
            pos["anchor"] = px
        pos["peak"] = max(float(pos.get("peak") or px), px)
        days_held = trading_days_between(cal, date.fromisoformat(pos["entry_date"]), cutoff)
        anchor, peak = float(pos["anchor"]), float(pos["peak"])
        if code in force_exit:
            reason = "override:" + overrides["force_exit"][code]["reason"]
        else:
            reason = evaluate_exit(px=px, anchor=anchor, peak=peak, days_held=days_held,
                                   inst20=inst20.get(code), yoy3=yoy3.get(code))
        if reason:
            exits.append((code, reason))

    exit_codes = {c for c, _ in exits}
    kept = {c: p for c, p in positions.items() if c not in exit_codes}

    # refills from engine candidates (guards + throttle + cooldown)
    # 冷卻期(引擎回測 COOLDOWN_DAYS=20 交易日的 live 鏡像;2026-07-14 修正:
    # live 曾缺此檢查,昨天出場的名字今天就被建議買回——churn 與回測不符)
    cooldown_left: dict[str, int] = {}
    for entry in live.get("log", []):
        if entry.get("action") == "closed" and entry.get("code"):
            closed_on = date.fromisoformat(str(entry["ts"])[:10])
            elapsed = trading_days_between(cal, closed_on, cutoff)
            if elapsed < COOLDOWN_DAYS:
                left = COOLDOWN_DAYS - elapsed
                cooldown_left[entry["code"]] = max(cooldown_left.get(entry["code"], 0), left)
    entries: list[str] = []
    cooled: list[str] = []
    slots = MAX_POSITIONS - len(kept)
    refill_freeze = overrides.get("refill_freeze")
    if refill_freeze:
        slots = 0
        brief.append(
            f"- ⛔ 遞補凍結中(自 {refill_freeze.get('date')}:{refill_freeze.get('reason')})"
            "——出場照常,不補新進場;解除:`daily override --unfreeze-refill --reason ...`"
        )
    elif guards_market:
        slots = 0
    elif guards_theme:
        slots = min(slots, MAX_POSITIONS // 2 - len(kept))
    for code in latest_candidates():
        if slots <= 0 or len(entries) >= MAX_NEW_PER_DAY:
            break
        if code in kept or code in exit_codes or closes.get(code) is None:
            continue
        if code in cooldown_left:
            cooled.append(f"{code}(剩 {cooldown_left[code]} 交易日)")
            continue
        entries.append(code)
        slots -= 1
    if cooled:
        brief.append(f"- ❄ 冷卻中跳過(出場後 {COOLDOWN_DAYS} 交易日不回鍋,與回測一致):{'、'.join(cooled)}")

    # 5) write live target weights (planner-invertible for kept, equal slice for new)
    from research.trading.live_config import LiveTradingConfig

    config = LiveTradingConfig.from_env(require_capital=False)
    capital = config.strategy_capital_twd if config.strategy_capital_twd > 0 else 1_000_000.0
    deployable = capital * (1 - config.cash_buffer_pct)
    mult = 1.0 + config.buy_price_buffer_pct
    rows = [("date", "company_code", "target_weight")]
    weights: dict[str, float] = {}
    for code, pos in kept.items():
        px = closes.get(code)
        if px is None:
            continue
        # Aim at the qty+0.5 midpoint so the planner's floor() lands exactly on
        # qty regardless of float representation (a bare qty weight can floor
        # to qty-1 and emit a phantom 1-share sell).
        weights[code] = ((pos["qty"] + 0.5) * px * mult) / deployable
    for code in entries:
        weights[code] = 1.0 / MAX_POSITIONS
    for code, w in sorted(weights.items()):
        rows.append((cutoff.isoformat(), code, f"{w:.8f}"))
    TARGET_WEIGHTS.write_text("\n".join(",".join(map(str, r)) for r in rows) + "\n", encoding="utf-8")

    save_json(LIVE_BOOK, {"as_of": cutoff.isoformat(), "positions": positions, "log": live.get("log", [])})

    # brief: per-position plans
    brief.append(f"- live book:持有 {len(kept)} 檔;出場 {len(exits)};新進場 {len(entries)}(候選來自引擎最新計分)")
    brief.append("")
    brief.append("| code | src | qty | 錨 | 現價 | 峰 | 持有日 | 止損 | 止盈 | yoy_3m | 狀態 |")
    brief.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for code, pos in sorted(positions.items()):
        px = closes.get(code, float("nan"))
        anchor = float(pos.get("anchor") or float("nan"))
        peak = float(pos.get("peak") or float("nan"))
        days_held = trading_days_between(cal, date.fromisoformat(pos["entry_date"]), cutoff)
        stop = max(anchor * (1 - ABS_STOP), peak * (1 - TRAIL))
        status = dict(exits).get(code, "hold" if pos.get("vetted") else "**待檢核**")
        y3 = yoy3.get(code)
        brief.append(
            f"| {code} | {pos['source']} | {pos['qty']} | {anchor:.1f} | {px:.1f} | {peak:.1f} | {days_held} "
            f"| {stop:.1f} | {anchor * (1 + TAKE_PROFIT):.1f} | {'' if y3 is None else f'{y3:.0f}%'} | {status} |"
        )
    if exits:
        brief.append("")
        brief.append(f"- 今日出場:{exits}")
    if entries:
        # 戰役十四(2026-07-09):進場否決被回測否決(veto 三窗 CAGR -34~-63pp,
        # 「法人邊出、價格邊漲」是本池贏家型態);inst_20d<0 只做透明標註不擋單。
        tagged = [
            f"{code}⚠半觸發(inst_20d<0,隔日可能觸發法人門)" if (inst20.get(code) or 0) < 0 else code
            for code in entries
        ]
        brief.append(f"- 今日新進場(T+1):{tagged}")

    # 5.5) 法說會行事曆:判斷層 read-through 素材 + 事件級資料收集(未來回測)
    try:
        import pandas as pd

        cc = fetch_confcall_schedule(today)
        if not cc.empty:
            watch = set(positions) | set(latest_candidates()[:15])
            cc["d"] = pd.to_datetime(cc["date"]).dt.date
            upcoming = cc[(cc["company_code"].isin(watch)) & (cc["d"] >= today) & (cc["d"] <= today + timedelta(days=14))]
            recent = cc[(cc["company_code"].isin(watch)) & (cc["d"] < today) & (cc["d"] >= today - timedelta(days=7))]
            if len(upcoming) or len(recent):
                brief.append("")
                brief.append("## 📅 法說行事曆(持倉+候選前15)")
                for r in upcoming.itertuples(index=False):
                    brief.append(f"- **即將**:{r.company_code} {r.company_name} {r.date} {r.time} —— 判斷層當日聽讀,更新論點註記")
                for r in recent.itertuples(index=False):
                    link = f"[簡報]({r.pdf})" if r.pdf else "(無簡報連結)"
                    brief.append(f"- 已開:{r.company_code} {r.company_name} {r.date} —— {link} → read-through(客戶/供應商指引回讀註冊表)")
            evp = REPO_ROOT / "research" / "data" / "confcall_events.parquet"
            old = (
                pd.read_parquet(evp)
                if evp.exists()
                else pd.DataFrame(columns=["company_code", "date", "first_seen"])
            )
            known = set(zip(old["company_code"], old["date"]))
            fresh = cc[[(c, d) not in known for c, d in zip(cc["company_code"], cc["date"])]]
            if len(fresh):
                evp.parent.mkdir(parents=True, exist_ok=True)
                add = fresh[["company_code", "date"]].assign(first_seen=today.isoformat())
                pd.concat([old, add], ignore_index=True).to_parquet(evp, index=False)
                brief.append(f"- 🗓 法說事件庫新增 {len(fresh)} 筆(累計供未來 event study)")
    except Exception as exc:
        brief.append(f"- ⚠️ 法說行事曆抓取失敗:{exc}")

    # 6) order plan
    plan_out = PLANS / f"serenity_daily_{today:%Y%m%d}.json"
    r = sh(
        ["uv", "run", "--project", "research", "python", "-m", "research.trading.auto_trader", "plan", "--out", str(plan_out)],
        timeout=600,
    )
    brief.append("")
    if r.returncode != 0:
        brief.append(f"- ⚠️ plan 產生失敗:{(r.stderr or r.stdout)[-400:]}")
        print(r.stdout[-1200:], r.stderr[-600:])
    else:
        plan = load_json(plan_out, {})
        orders = plan.get("orders", [])
        buys = sum(o.get("estimated_notional", 0) for o in orders if str(o.get("side", "")).lower() == "buy")
        sells = sum(o.get("estimated_notional", 0) for o in orders if str(o.get("side", "")).lower() == "sell")
        turnover = (buys + sells) / (plan.get("capital_ceiling_twd") or 1.0)
        brief.append(
            f"- plan: `{plan_out.name}`,orders={len(orders)}(buy {buys:,.0f} / sell {sells:,.0f}),單日換手 {turnover:.1%}"
        )
        if turnover > args.max_turnover and not (11 <= today.day <= 14):
            brief.append(f"- 🛑 換手異常(>{args.max_turnover:.0%} 且非換股窗)——**需人工複核,勿直接送單**")
        brief.append(
            f"- 送單指令(人工步驟):`uv run --project research python -m research.trading.auto_trader submit-plan {plan_out}`"
        )
        if getattr(args, "execute", False) or getattr(args, "execute_live", False):
            import subprocess as _sp

            live_flag = ["--live"] if getattr(args, "execute_live", False) else []
            exec_log_dir = OUT_DIR / "executions"
            exec_log_dir.mkdir(parents=True, exist_ok=True)
            exit_reason = dict(exits)
            spawned: list[str] = []

            def _spawn(cmd: list[str], tag: str) -> None:
                logf = exec_log_dir / f"daily_{today:%Y%m%d}_{tag}.log"
                with logf.open("w", encoding="utf-8") as fh:
                    _sp.Popen(cmd, stdout=fh, stderr=_sp.STDOUT, start_new_session=True)
                spawned.append(f"{tag} → {logf.name}")

            base = ["uv", "run", "--project", "research", "python", "-m"]
            if any(o.get("side") == "Buy" for o in orders):
                # loop 買腿顯式 balanced:引擎回測的完成語意(進場擇時事件研究證明
                # 「等回檔」錯過贏家);手動 CLI 預設才是 price-first
                _spawn(base + ["research.trading.execution.buy", "--plan", str(plan_out),
                               "--patience", "balanced"] + live_flag, "buy_plan")
            for o in orders:
                if o.get("side") != "Sell":
                    continue
                code = str(o["symbol"]).zfill(4)
                # 出場一律撈當日相對高點(urgency=exit:結構錨 + 護欄 -3% + 12:00 死線
                # 保證當日完成——回測出場語義=訊號日收盤前出場,盤中擇價是純增益)。
                # 唯一例外:事實級利空 override(查證協定明文「不等價格」)走急殺 stop。
                urgency = "stop" if exit_reason.get(code, "").startswith("override") else "exit"
                _spawn(base + ["research.trading.execution.sell", "--code", code, "--qty", str(int(o["quantity"])),
                               "--urgency", urgency] + live_flag, f"sell_{code}")
            mode = "LIVE" if live_flag else "DRY-RUN"
            brief.append(f"- 🤖 已派工盤中執行器({mode},等開盤後階梯執行):{'; '.join(spawned) if spawned else '無腿可派'}")
        

    text = "\n".join(brief) + "\n"
    (BRIEFS / f"{today}.md").write_text(text, encoding="utf-8")
    print(text)
    print(f"brief -> {BRIEFS / f'{today}.md'}")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(required=True)
    run_cmd = sub.add_parser("run")
    run_cmd.add_argument("--skip-refresh", action="store_true")
    run_cmd.add_argument("--allow-stale", action="store_true")
    run_cmd.add_argument("--no-sync", action="store_true", help="skip broker inventory sync (offline)")
    run_cmd.add_argument("--max-turnover", type=float, default=0.60)
    run_cmd.add_argument("--execute", action="store_true",
                         help="產完 plan 後自動派工盤中執行器(dry-run 模擬;盤前派工自動等開盤)")
    run_cmd.add_argument("--execute-live", action="store_true",
                         help="派工並帶 --live(仍需 FUBON_DRY_RUN=false + QL_STRATEGY_CAPITAL_TWD;武裝是使用者的動作)")
    run_cmd.set_defaults(func=cmd_run)
    ov = sub.add_parser("override")
    act = ov.add_mutually_exclusive_group(required=True)
    act.add_argument("--force-exit")
    act.add_argument("--freeze-refill", action="store_true",
                     help="凍結席位遞補(出場照常),用於策展檢討期間暫停新進場")
    act.add_argument("--unfreeze-refill", action="store_true")
    ov.add_argument("--reason", required=True)
    ov.set_defaults(func=cmd_override)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
