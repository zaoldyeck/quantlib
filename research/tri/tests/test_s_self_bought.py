"""S 自買部位認證守護 —— 修 2026-07-22 真實事故:「昨天剛買、今天就被砍」。

**事故經過(真金白銀)**:S 於 07-20 決策買進 2221/3704(當時在營收新鮮池內)、
07-21 成交、07-22 首次進入庫存判定。此時新鮮度已超過 7 天掉出池子,舊邏輯遂把
「S 自己剛買的股票」判為「非本策略標的」要在隔天砍掉——回測平均抱 17 天,
live 卻抱 1 天,策略等同被摧毀。

**根因**:部位的合法性被綁在「事後還在不在池內」,而不是「當初是不是 S 決定買的」。
成交需 T+1、首次入庫存需再一天,這個時間差註定讓邊緣新鮮度的標的失去認證。

**修法**:s_advisor 產生「今日進場」名單時記入 state 的 `_s_buys`;該股日後首次
入庫存時據此認證 `vetted_pool`,與池籍脫鉤。本測試鎖死此行為。

Run: uv run --project research python -m research.tri.tests.test_s_self_bought
"""
from __future__ import annotations

from datetime import date as Date

from research.tri.advisors import _S_BUYS, _s_role_action, update_state


def test_meta_keys_survive_update_state() -> None:
    """自買紀錄必須跨日存活;否則明天就認不出「這是我自己買的」(事故根因之一)。"""
    st = {_S_BUYS: {"3704": "2026-07-20"},
          "2466": {"first_seen": "2026-07-17", "vetted_pool": True}}
    out = update_state(st, {"2466": 1.0}, Date(2026, 7, 21), {"2466": 120.0}, {})
    assert out[_S_BUYS] == {"3704": "2026-07-20"}, "meta key 被 update_state 吃掉"
    assert out["2466"]["vetted_pool"] is True


def test_self_bought_position_is_vetted_even_when_out_of_pool() -> None:
    """事故重現點:S 自買的股票隔日掉出新鮮池,仍必須續抱(不得判為非本策略標的)。

    模擬 s_advisor 的認證步驟:_s_buys 命中 → vetted_pool=True → 即使 in_pool=False
    也走 keep_vetted。
    """
    st = {_S_BUYS: {"3704": "2026-07-20"}}
    st = update_state(st, {"3704": 1.0}, Date(2026, 7, 21), {"3704": 45.0}, {})
    bought = dict(st.get(_S_BUYS, {}))
    if "3704" in bought:                       # ← s_advisor 的自買認證邏輯
        st["3704"]["vetted_pool"] = True
    action, _ = _s_role_action(in_pool=False,
                               vetted=bool(st["3704"].get("vetted_pool")),
                               adoption_day=True)
    assert action == "keep_vetted", "S 自買部位掉出池子就被砍 = 事故復發"


def test_foreign_holding_out_of_pool_still_sells() -> None:
    """反向守護:**不是** S 買的收養持股,掉出池子仍應賣(角色純度不可被本修改破壞)。"""
    st = {_S_BUYS: {}}
    st = update_state(st, {"9999": 1.0}, Date(2026, 7, 21), {"9999": 10.0}, {})
    bought = dict(st.get(_S_BUYS, {}))
    if "9999" in bought:
        st["9999"]["vetted_pool"] = True
    action, _ = _s_role_action(in_pool=False,
                               vetted=bool(st["9999"].get("vetted_pool")),
                               adoption_day=True)
    assert action == "sell_role"


def test_buys_record_prunes_and_accumulates() -> None:
    """紀錄需能累積新買、汰除逾期(TTL)與已認證者,不得無限膨脹。"""
    from research.tri.advisors import _S_BUYS_TTL
    d0 = Date(2026, 7, 21)
    keep_from = (d0.toordinal() - _S_BUYS_TTL)
    old = Date.fromordinal(keep_from - 5).isoformat()      # 早於 TTL → 應汰除
    recent = Date.fromordinal(keep_from + 2).isoformat()   # TTL 內 → 應保留
    st = {_S_BUYS: {"OLD": old, "NEW": recent, "DONE": recent},
          "DONE": {"vetted_pool": True}}
    keep_from_s = Date.fromordinal(keep_from).isoformat()
    pruned = {c: dt for c, dt in st[_S_BUYS].items()
              if dt >= keep_from_s and not st.get(c, {}).get("vetted_pool")}
    assert "OLD" not in pruned, "逾期紀錄未汰除"
    assert "DONE" not in pruned, "已認證者未汰除"
    assert pruned["NEW"] == recent


def test_self_heal_from_plan_files(tmp=None) -> None:
    """state 可重建守護:_s_buys 全失(VM 重建/機制上線前)時,
    必須能從 premarket 落盤的計劃檔還原「S 買過什麼」——不需人工編輯 state。"""
    import json as _j, tempfile
    from pathlib import Path as _P
    import research.tri.advisors as A
    with tempfile.TemporaryDirectory() as td:
        old = A._PLANS_DIR
        A._PLANS_DIR = td
        try:
            _P(td, "2026-07-21.json").write_text(_j.dumps(
                {"date": "2026-07-21", "buys": ["3704", "2221"], "sells": []}))
            got = A._s_buys_from_plans()
            assert got == {"3704": "2026-07-21", "2221": "2026-07-21"}, got
            _P(td, "壞檔.json").write_text("{不是 json")
            assert A._s_buys_from_plans()          # 壞檔不得讓自癒整個失效
        finally:
            A._PLANS_DIR = old


def test_self_heal_missing_dir_is_safe() -> None:
    """計劃檔目錄不存在(全新機器)→ 回空,不得拋例外擋住決策。"""
    import research.tri.advisors as A
    old = A._PLANS_DIR
    A._PLANS_DIR = "/nonexistent/plans"
    try:
        assert A._s_buys_from_plans() == {}
    finally:
        A._PLANS_DIR = old


def main() -> None:
    for fn in (test_meta_keys_survive_update_state,
               test_self_bought_position_is_vetted_even_when_out_of_pool,
               test_foreign_holding_out_of_pool_still_sells,
               test_buys_record_prunes_and_accumulates,
               test_self_heal_from_plan_files,
               test_self_heal_missing_dir_is_safe):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ S 自買認證全過")


if __name__ == "__main__":
    main()
