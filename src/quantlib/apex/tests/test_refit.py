"""S 年度自主 refit 的 render / 漂移訊息守護。

money-path 核心:漂移(最優非 S)時,報告**一定要**說「不自動上線、需人工 F-LINE
認證」——這是「VM 不自動改 live 參數」承諾的最後一道文字防線,不可被改壞。

Run: uv run --project . pytest src/quantlib/apex/tests/test_refit.py
"""
from __future__ import annotations

from quantlib.apex.refit import DEPLOYED, render


def _rep(drift: bool, top_cfg: str, s_rank: int) -> dict:
    ranking = [{"config": top_cfg, "is_s": top_cfg == DEPLOYED,
                "p5": 0.5, "cagr": 0.9, "sharpe": 2.0}]
    if top_cfg != DEPLOYED:
        ranking.append({"config": DEPLOYED, "is_s": True,
                        "p5": 0.4, "cagr": 0.8, "sharpe": 1.8})
    return {"top": ranking[0], "drift": drift, "s_rank": s_rank, "ranking": ranking,
            "window": ("2023-12-01", "2026-12-01"), "data_latest": "2026-12-01",
            "primary": "p5"}


def test_render_confirm_s() -> None:
    out = render(_rep(False, DEPLOYED, 1))
    assert "✅ 確認 S" in out and "零動作" in out
    assert "漂移" not in out


def test_render_drift_demands_human_certification() -> None:
    out = render(_rep(True, "ax4-n8-t35-adv20", 3))
    assert "⚠" in out and "漂移" in out
    assert "不自動上線" in out                      # 承諾:不自動改 live 參數
    assert "F-LINE" in out or "認證" in out          # 要走人工完整認證
    assert "ax4-n8-t35-adv20" in out                # 點名漂移到哪個 config
    assert "排名第 3" in out                         # 講現役 S 掉到第幾


def test_deployed_is_s() -> None:
    assert DEPLOYED == "ax6-n5-t35-adv5"


def test_should_refit_annual_gate() -> None:
    """併入盤前的年度閘:只在 12 月、且今年還沒跑過時觸發(重跑當日不重複)。"""
    from datetime import date

    from quantlib.apex.refit import should_refit
    assert should_refit(date(2026, 12, 1), 2025) is True     # 12 月、今年未跑
    assert should_refit(date(2026, 12, 20), 2026) is False    # 今年已跑過 → 不重複
    assert should_refit(date(2026, 7, 20), 2025) is False     # 非 12 月
    assert should_refit(date(2026, 11, 30), 2025) is False    # 11 月不跑
    assert should_refit(date(2027, 12, 1), 2026) is True      # 隔年 12 月再跑


def main() -> None:
    for fn in (test_render_confirm_s, test_render_drift_demands_human_certification,
               test_deployed_is_s, test_should_refit_annual_gate):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ refit 全過")


if __name__ == "__main__":
    main()
