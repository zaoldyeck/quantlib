"""before/after money-path 驗證:exit_replay 總報酬正規化修法對 S 現役決策的影響。

2026-07-23 修 D-serenity-live bug:exit_replay 原本用**原始收盤**評 trailing/輸家/
絕對停損,一遇除權息就被機械跳空假觸發;修法改用「以進場錨日原始收盤為基準的總報酬
序列」(與回測 engine.py 的還原價空間逐位等價)。

本腳本對**當前持股**跑兩次 s_advisor:
  after  = 現行(修後,tr 還原價門檻)
  before = 把 load_paths 回傳的 adj_close 欄拿掉 → replay 退回原始價(等同修法前)
逐檔 diff 出場判決(賣/抱、觸發理由),量化這個 money-path 變更救回/改變了哪幾檔。

使用者鐵律:改 advisor money-path 必附 before/after 輸出 diff。

依賴 cache: 是(讀 daily_quote/ex_right_dividend/operating_revenue/industry_taxonomy_pit)。
Run: uv run --project research python -m research.trading.live.verify_exit_tr_fix
"""
from __future__ import annotations

import duckdb
import polars as pl

from research import paths
from research.trading import exit_replay

# 當前 S 持股(qty 不影響出場決策,佔位即可);今日 = cache 最新交易日。
HOLDINGS = {"2059": 1.0, "2221": 1.0, "2466": 1.0,
            "3374": 1.0, "3704": 1.0, "6446": 1.0}


def _decision(adv, code: str) -> tuple[str, str]:
    """從 Advice 萃取某檔的 (賣/抱, 理由)。"""
    for c, reason in adv.sells:
        if c == code:
            return "賣", reason
    if code in adv.detail:
        d = adv.detail[code]
        return "抱", f"trail 35% 停損線 {d.get('stop'):.2f}、峰 {d.get('peak'):.2f}、持有 {d.get('days_held')} 日" \
            if d.get("stop") else "抱"
    return "?", "(不在 sells 也不在 detail)"


def main() -> None:
    from datetime import date as Date

    from research.tri.advisors import s_advisor

    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    today = con.execute("SELECT max(date) FROM daily_quote").fetchone()[0]
    if isinstance(today, str):
        today = Date.fromisoformat(today)
    print(f"═══ exit_replay 總報酬修法 before/after ═══  持股 {sorted(HOLDINGS)}  今日 {today}\n")

    # ---- after:現行(tr 還原價)----
    adv_after = s_advisor(con, dict(HOLDINGS), today)

    # ---- before:把 adj_close 拿掉,replay 退回原始價(= 修法前行為)----
    orig_load = exit_replay.load_paths

    def load_paths_rawonly(codes, start, end):
        out = orig_load(codes, start, end)
        return {c: (df.drop("adj_close") if "adj_close" in df.columns else df)
                for c, df in out.items()}

    exit_replay.load_paths = load_paths_rawonly
    try:
        adv_before = s_advisor(con, dict(HOLDINGS), today)
    finally:
        exit_replay.load_paths = orig_load
    con.close()

    changed = []
    print(f"{'代碼':<6}{'before(原始價)':<34}{'after(還原價/修後)':<34}{'變化'}")
    print("─" * 96)
    for code in sorted(HOLDINGS):
        ba, br = _decision(adv_before, code)
        aa, ar = _decision(adv_after, code)
        flip = "★ 判決反轉" if ba != aa else ("理由變" if br != ar else "")
        if ba != aa:
            changed.append(code)
        print(f"{code:<6}{ba+' '+br[:30]:<34}{aa+' '+ar[:30]:<34}{flip}")
    print("─" * 96)
    if changed:
        print(f"\n★ {len(changed)} 檔出場判決被修法改變:{changed}")
        print("  → 這些是原始價版本因除權息跳空『假觸發止損』、修後正確續抱(或反之)的部位。")
    else:
        print("\n✓ 6 檔出場判決在 before/after 一致(當前無因除息假觸發者;修法為預防性正確化)。")


if __name__ == "__main__":
    main()
