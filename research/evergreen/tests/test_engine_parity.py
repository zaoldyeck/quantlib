"""防復發守護:Evergreen 官方引擎必須逐位重現戰役記錄的 live_config KPI。

2026-07-20 事故:tri.pnl_dashboard 手寫一份 Evergreen 回測(池籍偏離官方
midmonth_membership + 漏 gate),把 live-refit 線畫成與 live_config 差 96pp
的降級版。根治 = 生產路徑一律呼叫 research.evergreen.engine;本測試鎖死
「engine.replay_nav 重現 live_config.json 記錄的 train CAGR/MDD」——任何未來
對引擎資料層/計分/simulate 接線的漂移,都會讓本測試變紅。

Run: uv run --project research python -m research.evergreen.tests.test_engine_parity
     或 uv run --project research pytest research/evergreen/tests/test_engine_parity.py
依賴 cache: 是(需最新;train 窗為歷史區間,cache 世代不影響該窗結果)
"""
from __future__ import annotations

import json
from datetime import date as Date
from pathlib import Path

from research.apex import data
from research.evergreen.engine import LIVE_CONFIG, EvergreenData, replay_nav
from research.evergreen.ev36_walkforward import seg_kpi
from research import testkit

#: 引擎 parity 要重放 live_config 記錄的全窗 KPI —— 瘦身 cache(雲端 VM)沒有那段歷史
pytestmark = testkit.requires_history("2012-01-02", "2025-12-31")

_TOL = 0.005  # 0.5pp:重現 = 逐位(實測 0.00%),留守 rounding


def _recorded_vs_engine() -> tuple[dict, dict]:
    doc = json.loads(Path(LIVE_CONFIG).read_text())
    con = data.connect()
    d = EvergreenData(con, doc["train_window"][1])
    k = seg_kpi(replay_nav(d, doc["config"],
                           Date.fromisoformat(doc["train_window"][0]),
                           Date.fromisoformat(doc["train_window"][1])))
    return doc["train_kpi"], k


def test_engine_reproduces_live_config_cagr() -> None:
    rec, eng = _recorded_vs_engine()
    assert abs(eng["cagr"] - rec["cagr"]) < _TOL, (
        f"引擎 CAGR {eng['cagr']:.4f} 漂離 live_config 記錄 {rec['cagr']:.4f}"
        "——engine 的資料層/計分/simulate 接線已偏離官方 LabL")


def test_engine_reproduces_live_config_mdd() -> None:
    rec, eng = _recorded_vs_engine()
    assert abs(eng["mdd"] - rec["mdd"]) < _TOL, (
        f"引擎 MDD {eng['mdd']:.4f} 漂離 live_config 記錄 {rec['mdd']:.4f}")


def main() -> None:
    rec, eng = _recorded_vs_engine()
    for key in ("cagr", "mdd"):
        ok = abs(eng[key] - rec[key]) < _TOL
        print(f"{key:5s} 記錄 {rec[key]:+.4f} / 引擎 {eng[key]:+.4f} "
              f"/ 差 {abs(eng[key] - rec[key]):.2%}  {'✓' if ok else '✗ 漂移!'}")
    assert abs(eng["cagr"] - rec["cagr"]) < _TOL and abs(eng["mdd"] - rec["mdd"]) < _TOL
    print("✓ PARITY 全過——引擎與官方 live_config 逐位一致")


if __name__ == "__main__":
    main()
