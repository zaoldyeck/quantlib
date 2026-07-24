"""S 結構參數高原驗證:每個硬編參數 ±擾動(one-at-a-time),證明 S 坐在高原而非幸運尖峰。

**目的不是最佳化**(D1/3.1 已證:挑 in-sample 最佳參數 OOS 更差),是**高原驗證**——出廠閘門
robustness 項的補完:權重已驗(±20% spread 7.8%),本檔補「結構參數」:
- 池/閘:rev_fresh ≤{5,7,10}、stale ≥{21,26,31}、cfo 閘分位 {0.4,0.5,0.6}、ADV {3M,5M,8M}
- 出場:trail {30,35,40}%、time {25,30,35}、loser_time {10,15,20}
- 因子 lookback:high_52w 窗 {200,252,300}、mom {100,126,150}(skip5 固定)、close_pos 窗 {15,20,25}
判準:全域 CAGR spread < 15pp 且無單點跳升 = 高原成立(S 參數非雕出來的);若某方向跳升,
屬可疑(需配對檢定 + 不採 in-sample 挑點)。

lookback 變體由 panel 重算(公式對齊 assemble.build_features 60/61/78-83 行),canonical 窗版本
先與 feat 現值抽樣核對(重現不了=公式抄錯,fail-loud)。

Run: uv run --project . python -m quantlib.strat_lab.s_param_plateau
依賴 cache:是。
"""
from __future__ import annotations

import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExitSpec, PortSpec
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import C, DS, prep_cached, run_s_full


def _factor_col(panel: pl.DataFrame, kind: str, w: int) -> pl.DataFrame:
    """從 panel 重算指定 lookback 的因子欄(公式對齊 assemble)。回 (date, C, value)。"""
    p = panel.sort([C, "date"])
    if kind == "high":
        e = (pl.col("close") / pl.col("close").rolling_max(w)).over(C)
    elif kind == "mom":
        e = (pl.col("close").shift(5) / pl.col("close").shift(w) - 1).over(C)
    elif kind == "cpos":
        e = (pl.when(pl.col("high") > pl.col("low"))
             .then((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low")))
             .otherwise(None)
             .rolling_mean(w, min_samples=w // 2).over(C))
    else:
        raise ValueError(kind)
    return p.select(["date", C, e.alias("value")])


def _swap(feat: pl.DataFrame, panel: pl.DataFrame, col: str, kind: str, w: int) -> pl.DataFrame:
    v = _factor_col(panel, kind, w).rename({"value": col})
    return feat.drop(col).join(v, on=["date", C], how="left")


def _selfcheck(feat: pl.DataFrame, panel: pl.DataFrame) -> None:
    """canonical 窗重算必須重現 feat 現值(抽樣對比;公式抄錯即 fail-loud)。"""
    for col, kind, w in (("high_52w", "high", 252), ("mom_126_5", "mom", 126),
                         ("close_pos_20", "cpos", 20)):
        mine = _factor_col(panel, kind, w).rename({"value": "_v"})
        j = (feat.select(["date", C, col]).join(mine, on=["date", C], how="inner")
             .drop_nulls().with_columns((pl.col(col) - pl.col("_v")).abs().alias("_d")))
        mx = j["_d"].max()
        assert mx is not None and mx < 1e-9, f"{col} 重算不重現 feat(max diff {mx})——公式抄錯"
    print("  [selfcheck] 三個 lookback 因子 canonical 窗重算逐位重現 ✓", flush=True)


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    _selfcheck(feat, panel)

    runs: list[tuple[str, dict]] = [("canonical", {})]
    runs += [(f"fresh≤{v}", {"_fresh_days": v}) for v in (5, 10)]
    runs += [(f"stale≥{v}", {"_stale_days": v}) for v in (21, 31)]
    runs += [(f"cfo_q={v}", {"_cfo_q": v}) for v in (0.4, 0.6)]
    runs += [(f"trail {int(v*100)}%", {"_exit_spec": ExitSpec(trailing_stop=v, time_stop=30, loser_time_stop=15)})
             for v in (0.30, 0.40)]
    runs += [(f"time {v}", {"_exit_spec": ExitSpec(trailing_stop=0.35, time_stop=v, loser_time_stop=15)})
             for v in (25, 35)]
    runs += [(f"loser_t {v}", {"_exit_spec": ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=v)})
             for v in (10, 20)]

    print("=== S 結構參數高原驗證(one-at-a-time;全跨度)===")
    print(f"  {'變體':<16}{'CAGR':>8}{'Sortino':>9}{'MDD':>8}")
    cagrs: dict[str, float] = {}

    def _one(name: str, feat_=None, elig_=None, **kw):
        nav, _ = run_s_full(panel, feat_ if feat_ is not None else feat,
                            elig_ if elig_ is not None else elig, DS, **kw)
        st = perf_stats(nav.sort("date"))
        cagrs[name] = st["cagr"]
        print(f"  {name:<16}{st['cagr']:>+7.1%}{st['sortino']:>9.2f}{st['mdd']:>+7.1%}", flush=True)

    for name, kw in runs:
        _one(name, **kw)
    for v in (3_000_000.0, 8_000_000.0):
        _one(f"ADV {int(v/1e6)}M", elig_=data.eligibility(panel, min_adv=v))
    for w in (200, 300):
        _one(f"high52w w={w}", feat_=_swap(feat, panel, "high_52w", "high", w))
    for w in (100, 150):
        _one(f"mom w={w}", feat_=_swap(feat, panel, "mom_126_5", "mom", w))
    for w in (15, 25):
        _one(f"cpos w={w}", feat_=_swap(feat, panel, "close_pos_20", "cpos", w))

    base = cagrs.pop("canonical")
    diffs = {k: v - base for k, v in cagrs.items()}
    spread = max(cagrs.values()) - min(cagrs.values())
    best = max(diffs, key=diffs.get)
    print(f"\n  canonical {base:+.1%};20 擾動 spread {spread:.1%};最大正偏 {best} {diffs[best]:+.1%}")
    print("  判準:spread <15pp 且無 >+5pp 單點跳升 = 高原成立(參數非雕出;跳升者屬可疑需配對檢定)。")


if __name__ == "__main__":
    main()
