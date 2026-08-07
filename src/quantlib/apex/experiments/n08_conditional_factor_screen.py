"""N08 — 條件因子篩選:在 S 的池內、用右尾機率當量尺,重篩一次。

**兩個維度都是新的**(對照 F01/F02/F03 的既有篩選):

1. **條件在 S 的池內**。F 系列篩選跑在全市場 eligible universe(ADV ≥ 2,000 萬)上。
   但 S 的選股發生在一個小很多的**條件池**:營收公布 7 日內 + cfo_ni ≥ 中位數 +
   ADV ≥ 500 萬。一個因子在全市場平淡、在「剛公布加速營收的股票」之中卻可能很鋒利,
   反之亦然——**沒在正確的條件下篩過的因子,等於沒篩**。
2. **用右尾機率當量尺**。F 系列判準是平均 IC + decile 單調 + spread。但 N04【8】
   量到 S 的績效由火箭驅動(進場 σ 五分位的 P(60 日內最大漲幅 ≥ +50%)由 2.1% 到
   23.3%),而 N05 五臂證明「按平均風險效率配資金」會砍掉右尾。**平均 IC 這把尺
   本身就選錯了目標**;本檔主判準改成火箭機率提升(top vs bottom 五分位)。

**受測因子**:S 現役六因子以外、build_features 已產出的全部欄位(hvn_dist、
range_pos_60、updays_20、fvg_20、donchian_60、rev_yoy、rev_fresh_days、frn_60、
dy、lowvol_60),外加 **sig20(進場時 20 日日波動)**——它在 N04【8】是最強的火箭
預測子,但 S 從未把它放進計分(N05 的 W4 動的是**權重**不是**選股**,且有席位填不滿
的干擾,不算測過)。

**判準**:火箭機率提升需 IS/OOS 同號 + 兩窗皆 ≥ 1.3 倍;附報平均 IC 供對照(但不當
主判準)。過關者才進 N09 端到端對決——條件期望顯著 ≠ 組合可用(F01 老教訓)。

**PIT**:因子值取決策日當列(feat 本身已 PIT);前瞻報酬只往未來取。進場日限制在
資料尾端 60 個交易日之前,避免視窗被截斷造成右尾低估。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n08_conditional_factor_screen
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.strategy_s import WREL, prep_cached, score_pool

C = "company_code"
START = "2015-01-01"
H = 60                      # 右尾觀察窗(交易日)
FWD = 21                    # 平均報酬對照窗(≈ S 的中位持有 16 日的上緣)
ROCKET = np.log(1.5)        # 火箭定義:期間內最大漲幅 ≥ +50%
CRASH = np.log(0.7)         # 崩跌定義:期間內最大跌幅 ≥ 30%
CANDIDATES = ["hvn_dist", "range_pos_60", "updays_20", "fvg_20", "donchian_60",
              "rev_yoy", "rev_fresh_days", "frn_60", "dy", "lowvol_60", "sig20"]
OUT = paths.OUT / "apex" / "n08_cond_screen"


def forward_outcomes(panel: pl.DataFrame) -> pl.DataFrame:
    """(date, code, fwd21, maxgain60, maxdraw60, sig20) — 全部只看未來,不回看。

    未來視窗的極值用 reverse→rolling→reverse:polars 的 over(code) 會把整條運算鏈
    限制在該股票的群組內,故 reverse 不會跨股票污染。下方有斷言驗證這件事。
    """
    lg = pl.col("close").log()
    p = (panel.select(["date", C, "close"]).sort([C, "date"])
         .with_columns([
             lg.alias("lg"),
             (lg - lg.shift(1)).over(C).alias("r"),
         ])
         .with_columns([
             pl.col("r").rolling_std(20).over(C).alias("sig20"),
             lg.shift(-1).over(C).alias("lg1"),
         ])
         .with_columns([
             (lg.shift(-FWD) - lg).over(C).alias("fwd21"),
             (pl.col("lg1").reverse().rolling_max(H, min_periods=1).reverse()
              .over(C) - lg).alias("maxgain60"),
             (pl.col("lg1").reverse().rolling_min(H, min_periods=1).reverse()
              .over(C) - lg).alias("maxdraw60"),
         ]))
    return p.select(["date", C, "sig20", "fwd21", "maxgain60", "maxdraw60"])


def _check_forward_window(p: pl.DataFrame, panel: pl.DataFrame) -> None:
    """對單一股票逐筆重算 maxgain60,確認 reverse-rolling 沒有跨群組污染。"""
    code = panel[C][0]
    a = (panel.filter(pl.col(C) == code).sort("date")["close"].log().to_numpy())
    b = (p.filter(pl.col(C) == code).sort("date")["maxgain60"].to_numpy())
    for i in (0, len(a) // 3, len(a) // 2):
        if i + 1 >= len(a):
            continue
        want = float(a[i + 1: i + 1 + H].max() - a[i])
        assert abs(want - float(b[i])) < 1e-9, f"maxgain60 不符 @{code}[{i}]"


def _tstat(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    return float(x.mean() / (x.std(ddof=1) / np.sqrt(x.size))) if x.size >= 3 else float("nan")


def screen(pool: pl.DataFrame, tag: str) -> pl.DataFrame:
    """逐因子:池內五分位的火箭機率、崩跌機率、平均 IC(逐日算再對日序列做 t)。"""
    rows = []
    for f in CANDIDATES:
        d = pool.drop_nulls([f, "maxgain60", "fwd21"]).filter(pl.col(f).is_finite())
        # 每日至少 10 檔才分五分位,否則分位無意義
        d = d.filter(pl.len().over("date") >= 10)
        if d.height < 2000:
            rows.append({"factor": f, "n": d.height, "備註": "樣本不足"})
            continue
        d = d.with_columns(
            ((pl.col(f).rank("ordinal") / pl.len()).over("date") * 5)
            .ceil().clip(1, 5).cast(pl.Int32).alias("q"))
        hi = d.filter(pl.col("q") == 5)
        lo = d.filter(pl.col("q") == 1)
        p_hi = float((hi["maxgain60"] >= ROCKET).mean())
        p_lo = float((lo["maxgain60"] >= ROCKET).mean())
        c_hi = float((hi["maxdraw60"] <= CRASH).mean())
        c_lo = float((lo["maxdraw60"] <= CRASH).mean())
        ic = (d.group_by("date").agg(
            pl.corr(pl.col(f).rank(), pl.col("fwd21").rank()).alias("ic"))["ic"].to_numpy())
        rows.append({
            "factor": f, "n": d.height,
            "P火箭_高": round(p_hi, 4), "P火箭_低": round(p_lo, 4),
            "火箭提升": round(p_hi / p_lo, 2) if p_lo > 0 else None,
            "P崩跌_高": round(c_hi, 4), "P崩跌_低": round(c_lo, 4),
            "崩跌提升": round(c_hi / c_lo, 2) if c_lo > 0 else None,
            "IC": round(float(np.nanmean(ic)), 4), "IC_t": round(_tstat(ic), 2),
        })
    t = pl.DataFrame(rows)
    print(f"\n--- {tag} ---")
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=190, float_precision=4):
        print(t.sort("火箭提升", descending=True, nulls_last=True))
    return t


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    fo = forward_outcomes(panel)
    _check_forward_window(fo, panel)
    print("前瞻視窗自檢通過(reverse-rolling 未跨股票污染)")

    last = fo.select(pl.col("date").max()).item()
    days = fo.select("date").unique().sort("date")["date"].to_list()
    horizon_cut = days[-(H + 1)]
    pool = (score_pool(feat, elig)
            .filter(pl.col("date") >= pl.lit(START).str.to_date())
            .filter(pl.col("date") <= horizon_cut)     # 視窗完整,不低估右尾
            .join(fo, on=["date", C], how="inner"))
    print(f"池內觀測 {pool.height:,} 列 / {pool['date'].n_unique():,} 交易日 / "
          f"每日中位 {pool.group_by('date').len()['len'].median():.0f} 檔 "
          f"[{pool['date'].min()} ~ {pool['date'].max()};資料尾端 {last}]")
    print(f"池內無條件火箭率 P(60日內最大漲幅≥+50%) = "
          f"{float((pool['maxgain60'] >= ROCKET).mean()):.2%}")
    print(f"S 現役六因子 = {list(WREL)}(不在受測清單,受測的是它們之外的增量)")

    cut = pool.select("date").unique().sort("date")["date"].to_list()
    mid = cut[int(len(cut) * 0.75)]
    print(f"\nIS ≤ {mid} | OOS > {mid}")
    print("\n" + "=" * 82)
    print("【條件因子篩選】主判準 = 火箭提升(高五分位 ÷ 低五分位),IC 僅供對照")
    print("=" * 82)
    a = screen(pool.filter(pl.col("date") <= mid), "IS")
    b = screen(pool.filter(pl.col("date") > mid), "OOS")
    a.write_parquet(OUT / "screen_IS.parquet")
    b.write_parquet(OUT / "screen_OOS.parquet")

    print("\n" + "=" * 82)
    print("【過關名單】火箭提升 IS/OOS 兩窗皆 ≥ 1.3")
    print("=" * 82)
    m = (a.select(["factor", "火箭提升", "崩跌提升", "IC_t"])
         .rename({"火箭提升": "IS火箭", "崩跌提升": "IS崩跌", "IC_t": "IS_IC_t"})
         .join(b.select(["factor", "火箭提升", "崩跌提升", "IC_t"])
               .rename({"火箭提升": "OOS火箭", "崩跌提升": "OOS崩跌", "IC_t": "OOS_IC_t"}),
               on="factor", how="inner"))
    ok = m.filter((pl.col("IS火箭") >= 1.3) & (pl.col("OOS火箭") >= 1.3))
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=190):
        print(ok if ok.height else "(無因子過關)")
        print("\n全部對照:")
        print(m.sort("OOS火箭", descending=True, nulls_last=True))
    m.write_parquet(OUT / "screen_merged.parquet")
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
