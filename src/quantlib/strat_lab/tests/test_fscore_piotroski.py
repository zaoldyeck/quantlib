"""F-Score 學理保真度守護:必須逐條符合 Piotroski (2000)。

2026-07-23 稽核發現 raw_quarterly.py 的 F-Score 有一整批學理偏差:
- ROA/資產週轉率分母用**期末**總資產,Piotroski 用**年初**(槓桿用平均)——38.2%
  的格子分數不同、系統性寬鬆 0.29 分;
- 九項一律 .otherwise(0):缺料當 0 分不是 NULL → 金融業(毛利恆 NULL)f8/f9 恆 0
  形成隱形濾網、2011 前現金流缺料讓「F-Score 逐年上升」變成資料補齊軌跡;
- rolling_sum(4)/shift(4) 按實體列而非日曆季 → 缺一季就把兩季當一季、去年同季錯位;
- 科目樞紐 MAX(value) 挑數字大的候選 → Δ毛利/淨利被灌雜訊。

修法對照 Piotroski (2000) "Value Investing" 原始九項,並以 2330 FY2024 = 8/9
(稽核逐項手算基準)當金鑰。走 pg-attach(不依賴 cache 是否已重建)。

Run: uv run --project . python -m pytest src/quantlib/strat_lab/tests/test_fscore_piotroski.py
"""
from __future__ import annotations

from datetime import date

import pytest

from quantlib.db import connect
from quantlib.strat_lab.raw_quarterly import build_raw_quarterly


@pytest.fixture(scope="module")
def panel():
    try:
        con = connect()  # cache(PG 已退役 2026-07-23;raw IS/BS/CF 基表在 cache 內)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"cache 不可用:{exc}")
    return build_raw_quarterly(con, date(2022, 1, 1), date(2025, 1, 1))


def _row(panel, code, year, q):
    r = panel.filter((panel["company_code"] == code) & (panel["year"] == year)
                     & (panel["quarter"] == q))
    return r.to_dicts()[0] if not r.is_empty() else None


def test_tsmc_fy2024_matches_piotroski_hand_computation(panel) -> None:
    """2330 2024Q4:稽核逐項手算 = 8/9(f7 未發新股 = 0,因股本有增)。"""
    r = _row(panel, "2330", 2024, 4)
    assert r is not None
    assert r["f_score_raw"] == 8, f"F-Score 應為 8,得 {r['f_score_raw']}"
    assert r["f_score_n_valid"] == 9, "9 項全部可算(電子業有毛利)"
    assert r["f7_no_new_eq"] == 0, "TSMC 股本有增 → f7=0(Piotroski 正解)"


def test_roa_uses_beginning_of_year_assets_not_period_end(panel) -> None:
    """ROA 分母必須是年初總資產(4 季前期末),不是期末——這是最大的系統性偏差。"""
    r = _row(panel, "2330", 2024, 4)
    roa_begin = r["roa_ttm"]
    roa_eop = r["roa_ttm_eop"]
    assert roa_begin is not None and roa_eop is not None
    # 年初分母通常小於期末(資產成長),故 roa_begin > roa_eop,且兩者不同
    assert abs(roa_begin - roa_eop) > 1e-6, "roa_ttm 仍用期末分母(未修)"
    assert roa_begin > roa_eop, "資產成長時年初分母的 ROA 應高於期末分母"


def test_missing_inputs_yield_null_not_zero(panel) -> None:
    """缺料項必須是 NULL,不是 0——否則造成隱形濾網 + 系統性低估。
    金融業(如 2882 國泰金)無營業毛利 → f8/f9 應為 NULL → n_valid < 9。"""
    fin = None
    for code in ("2882", "2881", "2891", "2886"):
        r = _row(panel, code, 2024, 4)
        if r is not None:
            fin = r
            break
    if fin is None:
        pytest.skip("找不到金融股 2024Q4 樣本")
    assert fin["f8_d_gm_pos"] is None, "金融業無毛利,f8 應為 NULL 不是 0"
    assert fin["f_score_n_valid"] < 9, "金融業應因缺項而 n_valid<9(消費端據此排除)"


def test_n_valid_never_exceeds_nine_and_score_le_valid(panel) -> None:
    """結構不變式:n_valid ∈ [0,9];f_score_raw ≤ n_valid(不可能加超過有效項)。"""
    bad = panel.filter((panel["f_score_n_valid"] > 9)
                       | (panel["f_score_n_valid"] < 0)
                       | (panel["f_score_raw"] > panel["f_score_n_valid"]))
    assert bad.is_empty(), f"{bad.height} 列違反 n_valid/score 不變式"


def test_no_row_explosion_from_densify(panel) -> None:
    """densify 的佔位列不得進輸出:每 (market,code,year,quarter) 至多一列。"""
    dup = (panel.group_by(["market", "company_code", "year", "quarter"])
           .len().filter(pl_len_gt_one()))
    assert dup.is_empty(), "densify 佔位列洩漏或有重複鍵"


def pl_len_gt_one():
    import polars as pl
    return pl.col("len") > 1

def test_missing_quarter_yields_null_not_two_quarter_sum(panel) -> None:
    """FC5 結案:財報是年度累計數,單季 = 本季 YTD − 上季 YTD。某公司缺一季時,
    缺季的**下一季**不得被算成「兩季合計」——densify 到日曆格線後,缺季是 null
    佔位,單季差分自然傳成 null(而非跨過缺季相減)。

    1256 於 2023 有 Q1/Q3/Q4、缺 Q2:2023Q3 的單季值必須是 null(舊碼會算成
    Q3_YTD − Q1_YTD = Q2+Q3 合計),Q4 正常(Q4−Q3 都在)。"""
    q3 = _row(panel, "1256", 2023, 3)
    q4 = _row(panel, "1256", 2023, 4)
    if q3 is None or q4 is None:
        pytest.skip("1256 2023 樣本不在視窗")
    assert q3["rev_q"] is None, "缺季的下一季被算成兩季合計(FC5 未修)"
    assert q3["ni_q"] is None
    assert q3["rev_ttm"] is None, "TTM 跨缺季必須 null(效度閘)"
    assert q4["rev_q"] is not None, "缺季的隔兩季(Q4−Q3 都在)應正常"

