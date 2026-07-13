"""三策略每日檢查(純程式,零 LLM)——S / Serenity / Evergreen v3.3。

三個策略各自把整個富邦帳戶視為自己的,獨立給出:持股評判(KEEP/SELL+
機械原因)與買賣建議(含目標股數)。**本指令是純決策支援:永不下單、
永不觸發任何執行流水線**。Serenity 段讀 live ledger 的 lot 錨,用與執行
系統同一份六道門規則源(serenity/exit_rules.py)逐 lot 評判——與
`serenity.daily run` 的出場判決恆一致;ledger 更新由執行系統負責。

用法:
  uv run --project research python -m research.tri.daily                 # 富邦 API(庫存+現金)
  uv run --project research python -m research.tri.daily --positions "2330:1000" --cash 500000

報告:終端輸出 + research/tri/reports/YYYY-MM-DD.md 存檔。
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date as Date

import polars as pl

from research.tri.advisors import (Advice, evergreen_advisor, s_advisor,
                                   serenity_advisor)

REPORTS = "research/tri/reports"


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
        bank = broker.get_bank_remain()
        cash = (available_balance_from_fubon_bank_remain(bank)
                if getattr(bank, "is_success", False) else 0.0)
        if not cash:
            print("⚠ 富邦銀行餘額讀取失敗,現金以 0 計(可用 --cash 覆蓋)")
        if args.cash is not None:
            cash = float(args.cash)
        return {str(k): float(v) for k, v in pos.items()}, float(cash)
    except Exception as e:  # noqa: BLE001
        sys.exit(f"✗ 富邦讀取失敗:{e}\n"
                 "  改用 --positions \"2330:1000,...\" --cash 500000 手動提供")


def market_maps(codes: list[str]) -> tuple[dict, dict, str]:
    """(收盤價, 公司名, 資料日)——一次查 cache。"""
    import duckdb
    raw = duckdb.connect("research/cache.duckdb", read_only=True)
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--positions", help='手動持倉 "code:shares,code:shares"')
    ap.add_argument("--positions-file", help="csv(code,shares)")
    ap.add_argument("--cash", type=float, help="現金(手動模式或覆蓋 API 值)")
    args = ap.parse_args()

    today = Date.today()
    holdings, cash = get_account(args)
    px, names, d0 = market_maps(list(holdings))
    mv = sum(px.get(c, 0) * s for c, s in holdings.items())
    nav = mv + cash

    parts = [f"═══ 三策略每日檢查 {today} ═══",
             freshness_line(d0),
             f"帳戶:持股 {len(holdings)} 檔市值 {mv:,.0f} + 現金 {cash:,.0f} "
             f"= NAV {nav:,.0f}",
             "  " + ", ".join(
                 f"{c} {names.get(c, '')}×{int(s):,}"
                 for c, s in sorted(holdings.items()))]

    from research.apex import data
    con = data.connect()
    all_codes = set(holdings)
    for fn in (s_advisor, evergreen_advisor, serenity_advisor):
        try:
            adv = fn(con, holdings, today, nav=nav)
            all_codes |= {c for c, *_ in adv.buys}
            _, more_names, _ = market_maps(list(all_codes))
            names.update(more_names)
            parts.append(fmt_advice(adv, holdings, names))
        except Exception as e:  # noqa: BLE001
            parts.append(f"\n── {fn.__name__} 失敗 ──\n  {e}")
    parts.append("\n(三份獨立建議——各策略把整個帳戶視為自己的;"
                 "資金分配與送單由你決定;本指令永不下單)")

    report = "\n".join(parts)
    print(report)
    os.makedirs(REPORTS, exist_ok=True)
    open(f"{REPORTS}/{today}.md", "w").write(report + "\n")
    print(f"\n報告已存:{REPORTS}/{today}.md")


if __name__ == "__main__":
    main()
