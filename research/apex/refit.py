"""S 策略年度自主 refit —— 漂移監控 + 網格重選(systemd 每年 12/1,不依賴本機)。

VM 用自己的 cache 跑「近三年 24-config 選最優(P5 主尺)」,email 結果:
- 最優仍是 S(六年穩定,幾乎每年)→ 確認、零動作。
- 最優偏離 S → ⚠ 告警,需**人工**跑完整 F-LINE 認證(DSR/PBO/置換/擾動)通過才改上線。

**定位(誠實界定)**:這**不是完整 F-LINE**——只從 24 個已知 config 挑最優,不重掃
新因子、不跑認證閘。它是「漂移偵測器 + 網格 refit」;真正換策略一律走人工認證
(對照 CLAUDE.md 引擎唯一真源鐵律:money-path 變更不自動上線)。

config 選擇邏輯單一真源 = M01/M02/M03 實驗模組(本檔 import,不重寫)。

依賴 cache:是(近 3 年窗 + 1 年暖機,約 4-5 年;VM slim cache 足夠)。
Run(本機測):uv run --project research python -m research.apex.refit --date 2026-07-20
"""
from __future__ import annotations

import argparse
from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data
from research.apex.experiments.m01_window_length import GRID, prep, run_config
from research.apex.experiments.m02_refit_frequency import (BOOT_SEED, _metrics,
                                                           _rets, _sub_years)
from research.apex.experiments.m03_refit_timing import cfg_name

C = "company_code"
DEPLOYED = "ax6-n5-t35-adv5"   # 現役 S(strategy_s.py:WREL 六軸 × n5 × trail35 × adv5M)
PRIMARY = "p5"                 # apex 主尺:block-bootstrap P5 CAGR


def refit_report(con, refit_date: Date, primary: str = PRIMARY) -> dict:
    """近三年窗選最優 config。回 {top, drift, s_rank, ranking, window}。"""
    latest = data.latest_date(con)
    prep_start = _sub_years(refit_date, 5).isoformat()   # 特徵暖機給足
    panel, feat = prep(con, prep_start=prep_start, end=latest.isoformat())
    elig_map = {adv: (data.eligibility(panel, min_adv=adv)
                      .filter(pl.col("eligible")).select(["date", C]))
                for adv in [5e6, 20e6]}
    sim_start = _sub_years(refit_date, 4).isoformat()    # NAV 起點 = 窗前 1 年
    a, b = _sub_years(refit_date, 3), refit_date         # 評估窗 = 近 3 年
    ranking = []
    for i, cfg in enumerate(GRID):
        nav = run_config(panel, feat, elig_map, cfg, sim_start=sim_start)
        m = _metrics(_rets(nav, a, b), np.random.default_rng(BOOT_SEED))
        if m:
            ranking.append({"config": cfg_name(i), "is_s": cfg_name(i) == DEPLOYED,
                            "cagr": m["cagr"], "sharpe": m["sharpe"], "p5": m["p5"]})
    ranking.sort(key=lambda r: r[primary], reverse=True)
    top = ranking[0]
    s_rank = next((i + 1 for i, r in enumerate(ranking) if r["is_s"]), None)
    return {"top": top, "drift": not top["is_s"], "s_rank": s_rank,
            "ranking": ranking, "window": (a.isoformat(), b.isoformat()),
            "data_latest": latest.isoformat(), "primary": primary}


def render(rep: dict) -> str:
    a, b = rep["window"]
    lines = [f"S 年度 refit — 近三年窗 {a} → {b}(資料到 {rep['data_latest']};主尺 {rep['primary'].upper()})", ""]
    if rep["drift"]:
        lines.append(f"⚠⚠ 漂移:最優已非 S,而是 {rep['top']['config']}"
                     f"(P5 {rep['top']['p5']:.3f})。現役 S 排名第 {rep['s_rank']}。")
        lines.append("→ 不自動上線。請人工跑完整 F-LINE 認證(DSR/PBO/置換/擾動)通過才改。")
    else:
        lines.append(f"✅ 確認 S:最優仍是 S(P5 {rep['top']['p5']:.3f}),零動作。")
    lines.append("")
    lines.append("前 6 名(config | P5 | CAGR | Sharpe):")
    for r in rep["ranking"][:6]:
        star = " ←S" if r["is_s"] else ""
        lines.append(f"  {r['config']:18s} | {r['p5']:.3f} | {r['cagr']:.3f} | {r['sharpe']:.2f}{star}")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="S 年度自主 refit(漂移監控 + 網格重選)")
    ap.add_argument("--date", default=None, help="refit 基準日 YYYY-MM-DD(預設=今天)")
    ap.add_argument("--email", action="store_true", help="寄結果(預設只印)")
    args = ap.parse_args()
    from research.trading.live.notify import today_taipei
    refit_date = Date.fromisoformat(args.date) if args.date else today_taipei()
    con = data.connect()
    try:
        rep = refit_report(con, refit_date)
    finally:
        con.close()
    body = render(rep)
    print(body)
    if args.email:
        from research.trading.live.notify import GmailNotifier
        subj = ("⚠ S refit 漂移需複核 " if rep["drift"] else "✅ S refit 確認 ") + refit_date.isoformat()
        GmailNotifier.from_env().send_text(subj, body)
        print("[refit] email 已寄出")


if __name__ == "__main__":
    main()
