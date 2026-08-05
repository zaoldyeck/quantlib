"""盲判完整性守護:階段 A 的卡片不得洩漏答案——不論是直接欄位還是間接訊號。

為什麼要這組測試:重新蒸餾的**全部價值**押在盲判上——它是唯一能回答「這套質化方法
到底有沒有判別力」的機制。而盲判有兩種死法,兩種都無聲:

1. **直接洩漏**:卡片裡出現結果欄位(`fwd_max_ret`)或臂別線索(假名首字母 S/C/Q)。
2. **間接洩漏**:卡片上的量化特徵本身就能猜出答案。這種最毒——盲判 AUC 會很漂亮,
   但量到的是價量動能,不是質化判讀,而報告上看不出差別。

沒有這組測試,日後任何人往 `CARD_FIELDS` 加一欄都可能無聲地毀掉整個實驗。

Run: uv run --project . python -m pytest src/quantlib/evergreen/tests/test_ev58_card_integrity.py
"""
from __future__ import annotations

import json

import numpy as np
import pytest

from quantlib import paths
from quantlib.evergreen.ev58_build_cards import CARD_FIELDS

CARDS = paths.OUT / "ev58_cards"
#: 判別力以**顯著性**判定,不用固定門檻。固定門檻(如 0.60)在小樣本上必然誤報:
#: 實測 `ret20` 在 36 張試跑上 AUC 0.648、在全樣本 432 張上只有 0.524——同一個欄位,
#: 差別純粹是樣本數。改用 AUC 在虛無假設下的標準誤做 z 檢定,門檻自動隨 n 收緊,
#: 且不留任何拍板的數字(3 個標準差是常規顯著性界線,非調校值)。
_Z_MAX = 3.0


def _batches() -> list[tuple[list[dict], list[dict]]]:
    out = []
    for d in sorted(CARDS.glob("batch_*")):
        out.append((json.loads((d / "cards.json").read_text()),
                    json.loads((d / "truth.json").read_text())))
    return out


@pytest.fixture(scope="module")
def batches():
    b = _batches()
    if not b:
        pytest.skip("尚未產生卡片(uv run -m quantlib.evergreen.ev58_build_cards --pilot)")
    return b


def _auc(x: list[float], y: list[int]) -> float:
    xa, ya = np.asarray(x, float), np.asarray(y)
    m = ~np.isnan(xa)
    xa, ya = xa[m], ya[m]
    if len(set(ya.tolist())) < 2:
        return 0.5
    order = np.argsort(xa)
    rank = np.empty(len(xa))
    rank[order] = np.arange(1, len(xa) + 1)
    n1, n0 = ya.sum(), (1 - ya).sum()
    return float((rank[ya == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def test_cards_contain_no_field_outside_whitelist(batches) -> None:
    """白名單之外的欄位一律視為洩漏——用白名單而非黑名單,漏一個的後果不對稱。"""
    for cards, _ in batches:
        for c in cards:
            extra = set(c) - set(CARD_FIELDS)
            assert not extra, f"卡片出現白名單外欄位:{extra}"


def test_cards_never_carry_outcome_or_alias(batches) -> None:
    """結果與蒸餾假名(首字母 S/C/Q 即答案)絕不可出現在階段 A。"""
    banned = {"fwd_max_ret", "arm", "alias", "neg_kind", "matched_to", "regime"}
    for cards, _ in batches:
        for c in cards:
            assert not (set(c) & banned), f"卡片洩漏:{set(c) & banned}"


def test_card_features_cannot_predict_the_answer(batches) -> None:
    """**間接洩漏守護**:任一數值欄位單獨的 AUC 都不得具實質判別力。

    若某欄位能預測答案,盲判 AUC 會被它撐起來,而我們會誤以為質化判讀有效。
    """
    rows = []
    for cards, truth in batches:
        by_id = {c["card_id"]: c for c in cards}
        for t in truth:
            rows.append((by_id[t["card_id"]], 1 if t["arm"] == "positive" else 0))
    ys = [y for _, y in rows]
    numeric = [f for f in CARD_FIELDS
               if all(isinstance(c.get(f), (int, float)) and not isinstance(c.get(f), bool)
                      for c, _ in rows)]
    assert numeric, "找不到數值欄位可檢定"
    n1, n0 = sum(ys), len(ys) - sum(ys)
    se = float(np.sqrt((n1 + n0 + 1) / (12.0 * n1 * n0)))   # AUC 在虛無假設下的標準誤
    for f in numeric:
        a = _auc([c[f] for c, _ in rows], ys)
        z = abs(a - 0.5) / se
        assert z < _Z_MAX, f"{f} 單獨即可預測答案(AUC {a:.3f}, z={z:.2f}, n={len(ys)})"


def test_batches_are_era_homogeneous(batches) -> None:
    """同批同年代——提示詞要求年代語彙表批內共用只採一次,跨代批次會讓該機制失效。"""
    for cards, _ in batches:
        assert len({c["era_code"] for c in cards}) == 1, "批次跨越多個期別"
        assert len({c["limit_era"] for c in cards}) == 1, "批次跨越漲跌幅世代"


def test_positive_ratio_varies_across_batches(batches) -> None:
    """正例數必須在批間浮動——若每批都 3:3,agent 可由結構反推出配對關係。"""
    ratios = {sum(1 for t in truth if t["arm"] == "positive") for _, truth in batches}
    assert len(ratios) > 1, f"每批正例數都相同({ratios}),結構本身即洩漏"


def test_era_code_is_not_chronological(batches) -> None:
    """密封期別碼不得依時序遞增——否則看到 E1<E2 就能推回年代,期別保留組隔離失效。"""
    m = json.loads((CARDS / "_era_map.json").read_text())
    ordered_by_label = [m[k] for k in sorted(m)]
    assert ordered_by_label != sorted(ordered_by_label), "E1..En 與年代單調對應,密封無效"


def test_name_lookup_is_deterministic() -> None:
    """卡片必須可重現——不可重現就無法事後證明「當時給 agent 看的是什麼」。

    實際踩過的坑:`any_value(company_name)` 在 DuckDB 不保證取同一列,兩次執行
    分別給出「德宏工業」與「德宏」。守衛直接鎖死查名的決定性。
    """
    from quantlib.apex import data
    from quantlib.evergreen.ev58_build_cards import _names

    con = data.connect()
    assert _names(con) == _names(con), "查名不具決定性,卡片無法重現"
