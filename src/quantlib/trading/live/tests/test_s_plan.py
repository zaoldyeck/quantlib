"""s_plan money-path 守護:Advice → DayPlan 的今日動作萃取必須精確。

Run: uv run --project . python -m quantlib.trading.live.tests.test_s_plan
     或 uv run --project . pytest src/quantlib/trading/live/tests/test_s_plan.py
"""
from __future__ import annotations

from datetime import date as Date

from quantlib.tri.advisors import Advice
from quantlib.trading.live.s_plan import plan_from_advice


def _adv() -> Advice:
    a = Advice("S(test)")
    a.buys = [
        ("2408", 0.20, "今日進場 #1(每日上限 2)|約 10 股|fresh=3"),
        ("3006", 0.20, "今日進場 #2(每日上限 2)|約 8 股|fresh=1"),
        ("5483", 0.20, "⏸ 排隊 #3(有席位;每日上限 2,明日起依序進場)|…"),
        ("6488", 0.20, "🕒 遞補(席位已滿,等出缺才輪到)|…"),
    ]
    a.sells = [
        ("1234", "🔴 逾期未出場:… trail 35% 觸發"),
        ("2222", "超額席位(S 上限 5 檔;…)"),
        ("4763", "非本策略標的(不在進場池且未曾通過池檢…)"),
        ("9999", "無法取價(下市/停牌?)人工確認"),
    ]
    a.keeps = [("2330", "今日進場池 geo 排名 #1|席位 1/5|…")]
    a.notes = ["今日 fresh cohort 5 檔;決策日 2026-07-18"]
    return a


def test_buys_only_enter_today() -> None:
    """只有『今日進場』進 buys;排隊/遞補歸 queued(今日不執行)。"""
    p = plan_from_advice(_adv(), Date(2026, 7, 21))
    assert p.buys == ["2408", "3006"]
    assert [c for c, _ in p.queued] == ["5483", "6488"]


def test_sells_exclude_manual() -> None:
    """『人工確認』(無法取價)歸 manual_review、不自動賣;其餘(出場/超額/非本策略)全賣。"""
    p = plan_from_advice(_adv(), Date(2026, 7, 21))
    assert p.sells == ["1234", "2222", "4763"]
    assert [c for c, _ in p.manual_review] == ["9999"]


def test_protected_sells_split() -> None:
    """保留股(使用者要自己控)即使建議賣也不進 sells,歸 protected_sells。"""
    p = plan_from_advice(_adv(), Date(2026, 7, 21), protected={"2222", "4763"})
    assert p.sells == ["1234"]                       # 非保留 → 自動賣
    assert sorted(p.protected_sells) == ["2222", "4763"]  # 保留 → 待確認
    assert [c for c, _ in p.manual_review] == ["9999"]    # 人工確認不受影響


def test_protected_empty_default() -> None:
    """未指定保留 → 賣單照舊全自動、protected_sells 為空。"""
    p = plan_from_advice(_adv(), Date(2026, 7, 21))
    assert p.sells == ["1234", "2222", "4763"]
    assert p.protected_sells == []


def test_has_actions_and_passthrough() -> None:
    p = plan_from_advice(_adv(), Date(2026, 7, 21))
    assert p.has_actions is True
    assert p.date == "2026-07-21"
    assert p.keeps[0][0] == "2330"
    assert p.notes


def test_empty_advice_no_actions() -> None:
    """空建議 → 無腿,has_actions=False(揭露季外零訊號屬正常)。"""
    p = plan_from_advice(Advice("S(empty)"), Date(2026, 7, 21))
    assert p.buys == [] and p.sells == []
    assert p.has_actions is False


def test_peaks_extraction_never_breaks_the_plan() -> None:
    """峰值只服務保險層(安全網);髒值不得讓今日交易計劃產不出來(交易 > 保險)。"""
    a = _adv()
    a.detail = {"2408": {"peak": 100.0}, "3006": {"peak": None},
                "5483": {"peak": "壞掉的值"}, "6488": "不是 dict", "1234": {"peak": -5}}
    p = plan_from_advice(a, Date(2026, 7, 21))
    assert p.peaks == {"2408": 100.0}          # 只留合法正值,其餘靜默略過
    assert p.buys == ["2408", "3006"]          # 計劃本身完全不受影響


def test_to_dict_shape() -> None:
    d = plan_from_advice(_adv(), Date(2026, 7, 21)).to_dict()
    assert d["buys"] == ["2408", "3006"]
    assert d["sells"] == ["1234", "2222", "4763"]
    assert d["manual_review"][0][0] == "9999"


def main() -> None:
    for fn in (test_buys_only_enter_today, test_sells_exclude_manual,
               test_protected_sells_split, test_protected_empty_default,
               test_has_actions_and_passthrough, test_empty_advice_no_actions,
               test_peaks_extraction_never_breaks_the_plan,
               test_to_dict_shape):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ s_plan 全過")


if __name__ == "__main__":
    main()
