"""S 持股判決守護 —— 身分(誰的部位)與訊號(在不在池)必須分離。

**事故背景(2026-07-22,真金白銀)**:S 於 07-20 決策買進 2221/3704、07-21 成交、
07-22 首次入庫存判定;此時營收新鮮度已過 7 天掉出進場池,舊邏輯遂把「S 自己剛買
的股票」判為「非本策略標的」要隔天砍。回測平均抱 17 天、live 抱 1 天 = 策略被摧毀。

**根因**:用「這檔現在還在不在進場池」(訊號問題)去回答「這個部位是不是我的」
(身分問題)。決策→成交(T+1)→首次入庫存的時間差,註定讓邊緣新鮮度標的失去身分。

**架構(根因解)**
- state 只存**事實**:`origin`(部位來源)於首次出現時依**執行器真實成交紀錄**
  認定一次,此後恆定;不存任何「判斷」。
- 判斷每次執行由純函式 `s_hold_action(origin, in_pool)` 現算 → 無日期、無 state,
  故「哪天跑」「跑幾次」都不影響結果(杜絕 2026-07-21 的狀態污染類 bug)。
- 事實來源是**成交**不是**計劃**:計劃只是意圖,盤前重跑多次/未成交/漲停鎖死
  都會讓意圖與事實分家。

Run: uv run --project research python -m research.tri.tests.test_s_hold_action
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research.tri.advisors import (ORIGIN_ADOPTED, ORIGIN_STRATEGY,
                                   s_filled_buys, s_hold_action)


# ── 判決純函式 ──────────────────────────────────────────────────────────
def test_own_position_kept_even_when_out_of_pool() -> None:
    """**事故重現點**:S 自買部位掉出進場池仍須續抱——出場只由四條出場規則決定
    (與回測一致:回測根本沒有池檢)。"""
    assert s_hold_action(ORIGIN_STRATEGY, in_pool=False) == "keep_own"
    assert s_hold_action(ORIGIN_STRATEGY, in_pool=True) == "keep_own"


def test_adopted_position_follows_role_purity() -> None:
    """收養持股(S 沒買過)→ 角色純度:是今日標的才留,否則賣。"""
    assert s_hold_action(ORIGIN_ADOPTED, in_pool=True) == "keep_pool"
    assert s_hold_action(ORIGIN_ADOPTED, in_pool=False) == "sell_role"


def test_decision_is_pure_and_dateless() -> None:
    """判決不得依賴日期或 state:同輸入永遠同輸出(杜絕『哪天跑結果就不同』)。"""
    for origin in (ORIGIN_STRATEGY, ORIGIN_ADOPTED):
        for in_pool in (True, False):
            first = s_hold_action(origin, in_pool)
            assert all(s_hold_action(origin, in_pool) == first for _ in range(5))


def test_unknown_origin_defaults_to_role_purity() -> None:
    """未知/損壞的 origin 一律走保守路徑(當成收養,受角色純度管束),不得誤放行。"""
    assert s_hold_action("", in_pool=False) == "sell_role"
    assert s_hold_action("garbage", in_pool=False) == "sell_role"


# ── 事實來源:真實成交紀錄 ────────────────────────────────────────────────
def _leg(d: Path, name: str, code: str, side: str, filled: int) -> None:
    (d / name).write_text(json.dumps(
        {"ts": "2026-07-21T09:15:46+08:00", "event": "summary", "code": code,
         "side": side, "filled": filled, "target": 1, "avg_price": 45.0}) + "\n")


def test_only_actually_filled_buys_count() -> None:
    """**使用者指正的命門**:只有真的成交才算數;未成交(filled=0)不得認養,
    否則盤前重跑幾次就會認養一堆從沒買到的股票。"""
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _leg(d, "20260721_090034_buy_3704.jsonl", "3704", "Buy", 1)
        _leg(d, "20260721_090034_buy_9999.jsonl", "9999", "Buy", 0)   # 未成交
        _leg(d, "20260721_090034_sell_2408.jsonl", "2408", "Sell", 1)  # 賣出腿
        got = s_filled_buys(str(d))
        assert got == {"3704": "2026-07-21"}, got


def test_missing_or_corrupt_records_are_safe() -> None:
    """目錄不存在 → 空;壞行不得讓整批重建失效(降級要安全,不得拋)。"""
    assert s_filled_buys("/nonexistent/executions") == {}
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        (d / "20260721_090034_buy_bad.jsonl").write_text("{壞掉的 json\n")
        _leg(d, "20260721_090035_buy_2221.jsonl", "2221", "Buy", 1)
        assert s_filled_buys(str(d)) == {"2221": "2026-07-21"}


def test_records_are_fully_reconstructible() -> None:
    """紀錄可完全重建 = state 不是珍貴資料(對齊 stateless/可復原原則):
    同一份成交紀錄重跑必得同一結果。"""
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _leg(d, "20260721_090034_buy_3704.jsonl", "3704", "Buy", 1)
        assert s_filled_buys(str(d)) == s_filled_buys(str(d))


# ── 路徑錨定(災難級靜默失效守護)──────────────────────────────────────
def test_paths_are_repo_anchored_not_cwd_dependent() -> None:
    """**災難級守護**:成交紀錄/狀態目錄若用相對路徑,換個 cwd 就靜默讀不到 →
    所有部位被判「收養」→ 角色純度把 S 自己的部位全數賣光。必須以 repo 根為錨。"""
    import os
    import research.tri.advisors as A
    assert os.path.isabs(A._EXEC_DIR), "成交紀錄目錄必須絕對路徑"
    assert os.path.isabs(A.STATE_DIR), "狀態目錄必須絕對路徑"
    cwd = os.getcwd()
    try:
        os.chdir("/")                    # 換到完全不同的 cwd
        import importlib
        importlib.reload(A)
        assert os.path.isabs(A._EXEC_DIR) and "research" in A._EXEC_DIR
    finally:
        os.chdir(cwd)
        import importlib
        importlib.reload(A)


def main() -> None:
    for fn in (test_own_position_kept_even_when_out_of_pool,
               test_adopted_position_follows_role_purity,
               test_decision_is_pure_and_dateless,
               test_unknown_origin_defaults_to_role_purity,
               test_only_actually_filled_buys_count,
               test_missing_or_corrupt_records_are_safe,
               test_records_are_fully_reconstructible,
               test_paths_are_repo_anchored_not_cwd_dependent):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ S 持股判決全過")


if __name__ == "__main__":
    main()
