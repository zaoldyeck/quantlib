"""N03 overlay 對決的地基守護:baseline 臂必須逐位重現引擎內建計分。

n03_meanrev_overlay_s 為了走 `run_s_full(_score_fn=...)` 這個 hook,不得不在研究層
複製一份 S 的計分式(hook 契約是「收過濾後的 df、回傳含 score 的 df」,引擎不再代算)。
複製品一旦與 `strategy_s.run_s_full` 內建的計分漂移,**七臂對決的每一個 ΔCAGR 都是髒的**
——因為對照組本身就不是 S。本檔把「複製品 == 內建」釘死。

失效情境重現:把 `_canonical_score` 的權重或 rank 口徑改一個字,本測試即紅。
"""
from __future__ import annotations

import polars as pl
from quantlib import paths, testkit

START = "2023-01-03"          # 短窗:守護的是計分等價,不是績效,窗短跑得快


@testkit.requires_history("2022-01-03", "2023-12-29")
def test_overlay_baseline_arm_matches_builtin_scoring():
    from quantlib.apex import data
    from quantlib.apex.experiments.n03_meanrev_overlay_s import make_score_fn
    from quantlib.apex.strategy_s import prep_cached, run_s_full

    con = data.connect()
    panel, feat, elig = prep_cached(con)

    builtin, _ = run_s_full(panel, feat, elig, START)
    # baseline 臂不讀訊號欄,給空訊號表即可(join 後全 null,filter 分支不觸發)
    empty_sig = pl.DataFrame(
        schema={"date": pl.Date, "company_code": pl.Utf8,
                "dn_run": pl.Int64, "up_run": pl.Int64, "ret5": pl.Float64}
    )
    arm, _ = run_s_full(panel, feat, elig, START,
                        _score_fn=make_score_fn(empty_sig, "baseline"))

    assert arm.height == builtin.height, "NAV 長度不同 = 進出場時點已漂移"
    assert arm["nav"].to_list() == builtin["nav"].to_list(), (
        "baseline 臂與引擎內建計分不逐位一致——n03 的對照組不是 S,所有 ΔCAGR 作廢"
    )


def test_paths_artifact_dir_is_under_var():
    """研究產物一律落 var/(可重生產物根),不得寫進 repo 或系統暫存。"""
    from quantlib.apex.experiments.n02_meanrev_runlength import OUT

    assert OUT.is_relative_to(paths.OUT)
