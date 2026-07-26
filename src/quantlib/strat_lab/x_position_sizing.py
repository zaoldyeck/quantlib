"""apex_revcycle_S 部位 sizing 維度實驗(乾淨資料 campaign,2026-07-26)。

問題:S 現行是等權(每檔 1/5 NAV = 0.20)。等權從沒被檢驗過——右尾肥的動量策略
究竟該「重押高分股」(conviction tilt)、還是「重押低波股」(risk parity)、
還是反過來「重押高波股」(波動即右尾)?本檔把 sizing 這一維掃到底。

變體家族(全部只動 entries 的 'weight' 欄,不碰任何 canonical 預設):
  A 信念傾斜  tilt(λ):日內候選第 1~5 名線性加減碼,w_r = (1 + λ(3-r)/2)/5,
                Σw = 1。λ<0 為反向對照(若反向也「贏」→ 該家族是雜訊)。
  B 風險平價  ivol(p):w ∝ (1/σ20)^p 在日內 top-5 正規化(p=1 全量、p=0.5 半量、
                p=0 退化為等權 → 天然高原檢驗)。ivol60 換 60 日窗(feat.lowvol_60,
                與 canonical 特徵同定義,不另造)。ivol_cap 加 [0.10,0.30] 集中度上限。
  C 波動目標  volcap:w = 0.20 × min(1, σ_med/σ_i)(Moreira & Muir 2017 式
                volatility management 的截面版)——只對高波股減碼、留現金,不加碼。
  D ATR 反比  iatr:w ∝ 1/(ATR20/close),與 σ 同向但用真實區間(跳空敏感)。
  E 反向對照  volprop:w ∝ σ20(重押高波)——若 S 的 alpha 來自右尾,這支才該贏。
  F 混合      mix:tilt(0.5) × (1/σ20) 正規化。
  P 恆等檢驗  const020:w ≡ 0.20,必須逐位重現 canonical(weight 管線正確性守護)。

方法論(硬性):任何「最佳」變體都要對 canonical 做配對 moving-block bootstrap
(block=21、n_boot=4000,同一組 block 序列同時重取樣兩條曲線 → 消掉共同市場成分),
報年化 CAGR 差 + 95% CI + P(差≤0);CI 跨 0 即判噪音。KPI 判準 D2:Sortino /
Calmar / MDD / bootstrap 下界須同時 ≥ canonical 才算候選。

Run:  uv run --project . python -m quantlib.strat_lab.x_position_sizing
依賴 cache:是(prep_cached 讀 industry_taxonomy_pit;需 cache 為最新世代)。
"""
from __future__ import annotations

import math

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.metrics import perf_stats, yearly_table
from quantlib.apex.strategy_s import C, DS, prep_cached, run_s_full
from quantlib.apex.validate import block_bootstrap_cagr, daily_returns

TOP_K = 5                 # 日候選名單長度(= n_slots,canonical)
BASE_W = 1.0 / TOP_K      # 等權基準 0.20
HALF_SPLIT = "2020-07-31"  # 前後半段切點(全跨度 2014-10~2026-07 的中位日)
TRADING_DAYS = 252


# ── 風險量測(從 panel 算,全部 backward-looking,無前視)───────────────
def risk_table(panel: pl.DataFrame) -> pl.DataFrame:
    """(date, code, sigma20, atr20) — σ20 = 調整收盤日報酬 20 日標準差;
    ATR20 = 真實區間 / 收盤 的 20 日均值。兩者只用 ≤ 當日資料(決策日 T,T+1 成交)。"""
    p = panel.sort([C, "date"])
    tr = pl.max_horizontal(
        pl.col("high") - pl.col("low"),
        (pl.col("high") - pl.col("close").shift(1).over(C)).abs(),
        (pl.col("low") - pl.col("close").shift(1).over(C)).abs(),
    )
    return p.with_columns([
        (pl.col("close").pct_change().rolling_std(20, min_samples=15)
         .over(C).alias("sigma20")),
        ((tr / pl.col("close")).rolling_mean(20, min_samples=15)
         .over(C).alias("atr20")),
    ]).select(["date", C, "sigma20", "atr20"])


# ── weight 產生器:輸入 df(含 score/rk/風險欄),回傳 weight 表達式 ─────
def _rk() -> pl.Expr:
    """日內分數名次(1 = 最高);與 assemble.entries_and_flags 的 ordinal rank 同義。"""
    return pl.col("score").rank("ordinal", descending=True).over("date")


def _norm_top(expr: pl.Expr) -> pl.Expr:
    """把 expr 在「日內 top-K」正規化成 Σw = 1(非 top-K 的列權重無意義,引擎不取)。"""
    inside = pl.when(pl.col("rk") <= TOP_K).then(expr).otherwise(None)
    return inside / inside.sum().over("date")


def _sig(col: str) -> pl.Expr:
    """風險量測 + 日內中位數補值(新上市窗口不足者不因缺值退回等權而失真)。"""
    x = pl.col(col)
    return x.fill_null(x.median().over("date")).clip(lower_bound=1e-6)


def w_const() -> pl.Expr:
    return pl.lit(BASE_W)


def w_tilt(lam: float) -> pl.Expr:
    """線性信念傾斜:w_r = (1 + λ(3-r)/2)/K。λ=0.5 → [.30,.25,.20,.15,.10]。"""
    return (1.0 + lam * (3.0 - pl.col("rk").cast(pl.Float64)) / 2.0) / TOP_K


def w_ivol(col: str = "sigma20", p: float = 1.0) -> pl.Expr:
    return _norm_top(_sig(col).pow(-p))


def w_ivol_cap(lo: float = 0.10, hi: float = 0.30) -> pl.Expr:
    return w_ivol().clip(lower_bound=lo, upper_bound=hi)


def w_volcap() -> pl.Expr:
    """只減碼不加碼:w = 0.20 × min(1, σ_med(top-K) / σ_i),不足部分留現金。"""
    s = _sig("sigma20")
    med = pl.when(pl.col("rk") <= TOP_K).then(s).otherwise(None).median().over("date")
    return BASE_W * pl.min_horizontal(pl.lit(1.0), med / s)


def w_volprop() -> pl.Expr:
    return _norm_top(_sig("sigma20"))


def w_mix(lam: float = 0.5) -> pl.Expr:
    tilt = 1.0 + lam * (3.0 - pl.col("rk").cast(pl.Float64)) / 2.0
    return _norm_top(tilt / _sig("sigma20"))


VARIANTS: dict[str, pl.Expr] = {
    "P_const020": w_const(),
    "A_tilt+0.25": w_tilt(0.25),
    "A_tilt+0.50": w_tilt(0.50),
    "A_tilt+0.75": w_tilt(0.75),
    "A_tilt-0.25": w_tilt(-0.25),
    "A_tilt-0.50": w_tilt(-0.50),
    "B_ivol20_p1.0": w_ivol("sigma20", 1.0),
    "B_ivol20_p0.5": w_ivol("sigma20", 0.5),
    "B_ivol20_cap": w_ivol_cap(),
    "B_ivol60_p1.0": w_ivol("sigma60", 1.0),
    "C_volcap": w_volcap(),
    "D_iatr20_p1.0": w_ivol("atr20", 1.0),
    "D_iatr20_p0.5": w_ivol("atr20", 0.5),
    "E_volprop20": w_volprop(),
    "E_volprop20_p0.5": _norm_top(_sig("sigma20").pow(0.5)),
    "F_mix_t050_ivol": w_mix(0.5),
}


def make_score_fn(risk: pl.DataFrame, wexpr: pl.Expr):
    """canonical 計分 + 自訂 weight 欄(score 表達式與 strategy_s 逐字同構)。"""
    from quantlib.apex.strategy_s import WREL

    def _fn(df: pl.DataFrame) -> pl.DataFrame:
        expr = None
        for c_, wt in WREL.items():
            term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
            expr = term if expr is None else expr * term
        out = (df.with_columns(expr.alias("score"))
               .join(risk, on=["date", C], how="left")
               # feat.lowvol_60 = -σ60(canonical 特徵定義,不另造第二份)
               .with_columns((-pl.col("lowvol_60")).alias("sigma60"))
               .with_columns(_rk().alias("rk"))
               .with_columns(wexpr.alias("weight")))
        return out
    return _fn


# ── 配對 bootstrap(對高相關曲線最有統計力)───────────────────────────
def paired_block_bootstrap(nav_v: pl.DataFrame, nav_c: pl.DataFrame, *,
                           n_boot: int = 4000, block: int = 21,
                           seed: int = 20260726) -> dict:
    """d = CAGR(variant) − CAGR(canonical),同一組 block 序列同時重取樣兩條日報酬。"""
    rv, rc = daily_returns(nav_v), daily_returns(nav_c)
    if len(rv) != len(rc):
        raise ValueError("配對 bootstrap 需等長 NAV 序列")
    t = len(rv)
    rng = np.random.default_rng(seed)
    n_blocks = math.ceil(t / block)
    diffs = np.empty(n_boot)
    sh_v, sh_c, so_v, so_c = (np.empty(n_boot) for _ in range(4))
    step = 500
    for a in range(0, n_boot, step):
        b = min(a + step, n_boot)
        starts = rng.integers(0, t, size=(b - a, n_blocks))
        idx = ((starts[:, :, None] + np.arange(block)[None, None, :])
               % t).reshape(b - a, -1)[:, :t]
        gv = np.prod(1.0 + rv[idx], axis=1) ** (TRADING_DAYS / t) - 1.0
        gc = np.prod(1.0 + rc[idx], axis=1) ** (TRADING_DAYS / t) - 1.0
        diffs[a:b] = gv - gc
        # 風險平價的主張是「風險調整後更好」,不是 CAGR 更高 → 同一組 block 也測
        # ΔSharpe / ΔSortino,否則等於用對手不主張的指標判它死刑。
        sv, sc_ = rv[idx], rc[idx]
        for arr, buf in ((sv, sh_v), (sc_, sh_c)):
            m = arr.mean(axis=1)
            sd = arr.std(axis=1, ddof=1)
            buf[a:b] = m / sd * math.sqrt(TRADING_DAYS)
        for arr, buf in ((sv, so_v), (sc_, so_c)):
            m = arr.mean(axis=1)
            dn = np.sqrt((np.minimum(arr, 0.0) ** 2).mean(axis=1))
            buf[a:b] = m / dn * math.sqrt(TRADING_DAYS)
    d_sh, d_so = sh_v - sh_c, so_v - so_c
    return {
        "ann_diff": float(diffs.mean()),
        "ci_lo": float(np.percentile(diffs, 2.5)),
        "ci_hi": float(np.percentile(diffs, 97.5)),
        "p_le0": float((diffs <= 0).mean()),
        "d_sharpe": float(d_sh.mean()),
        "d_sharpe_lo": float(np.percentile(d_sh, 2.5)),
        "d_sharpe_hi": float(np.percentile(d_sh, 97.5)),
        "d_sharpe_p_le0": float((d_sh <= 0).mean()),
        "d_sortino": float(d_so.mean()),
        "d_sortino_lo": float(np.percentile(d_so, 2.5)),
        "d_sortino_hi": float(np.percentile(d_so, 97.5)),
        "d_sortino_p_le0": float((d_so <= 0).mean()),
    }


def half_stats(nav: pl.DataFrame) -> tuple[dict, dict]:
    a = nav.filter(pl.col("date") <= pl.lit(HALF_SPLIT).str.to_date())
    b = nav.filter(pl.col("date") >= pl.lit(HALF_SPLIT).str.to_date())
    return perf_stats(a), perf_stats(b)


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    risk = risk_table(panel)

    base_nav, base_tr = run_s_full(panel, feat, elig, DS)
    base_nav = base_nav.sort("date")
    bs = perf_stats(base_nav)
    bboot = block_bootstrap_cagr(base_nav, n_boot=4000, block=21)
    print(f"canonical  CAGR {bs['cagr']*100:7.2f}%  Sortino {bs['sortino']:5.2f}  "
          f"Calmar {bs['calmar']:5.2f}  MDD {bs['mdd']*100:7.2f}%  "
          f"boot2.5% {bboot['ci_lo']*100:6.2f}%  trades {base_tr.height}")

    rows = []
    for name, wexpr in VARIANTS.items():
        nav, tr = run_s_full(panel, feat, elig, DS,
                             _score_fn=make_score_fn(risk, wexpr))
        nav = nav.sort("date")
        st = perf_stats(nav)
        boot = block_bootstrap_cagr(nav, n_boot=4000, block=21)
        pair = paired_block_bootstrap(nav, base_nav)
        ha, hb = half_stats(nav)
        same = bool(np.allclose(nav["nav"].to_numpy(), base_nav["nav"].to_numpy()))
        rows.append({
            "name": name, "cagr": st["cagr"], "sortino": st["sortino"],
            "calmar": st["calmar"], "mdd": st["mdd"], "sharpe": st["sharpe"],
            "ann_vol": st["ann_vol"], "boot_lo": boot["ci_lo"],
            "paired_ann_diff": pair["ann_diff"], "ci_lo": pair["ci_lo"],
            "ci_hi": pair["ci_hi"], "p_le0": pair["p_le0"],
            "n_trades": tr.height, "identical": same,
            "cagr_h1": ha["cagr"], "cagr_h2": hb["cagr"],
            "mdd_h1": ha["mdd"], "mdd_h2": hb["mdd"],
            **{k: pair[k] for k in pair if k.startswith("d_")},
        })
        r = rows[-1]
        print(f"{name:17s} CAGR {r['cagr']*100:7.2f}%  Sor {r['sortino']:5.2f}  "
              f"Cal {r['calmar']:5.2f}  MDD {r['mdd']*100:7.2f}%  "
              f"boot {r['boot_lo']*100:6.2f}%  Δann {r['paired_ann_diff']*100:+7.2f}pp "
              f"[{r['ci_lo']*100:+6.2f},{r['ci_hi']*100:+6.2f}] P(≤0)={r['p_le0']:.3f}"
              + ("  IDENTICAL" if same else ""))
        print(f"{'':17s} 配對 ΔSharpe {r['d_sharpe']:+.3f} "
              f"[{r['d_sharpe_lo']:+.3f},{r['d_sharpe_hi']:+.3f}] P(≤0)={r['d_sharpe_p_le0']:.3f}"
              f"   ΔSortino {r['d_sortino']:+.3f} "
              f"[{r['d_sortino_lo']:+.3f},{r['d_sortino_hi']:+.3f}] "
              f"P(≤0)={r['d_sortino_p_le0']:.3f}")

    out = pl.DataFrame(rows)
    p = paths.OUT_STRAT_LAB / "x_position_sizing.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    out.write_csv(p)
    print(f"\n→ {p}")

    print("\n前後半段(切點 " + HALF_SPLIT + "):")
    for r in rows:
        print(f"{r['name']:17s} H1 CAGR {r['cagr_h1']*100:7.2f}% MDD {r['mdd_h1']*100:6.1f}%"
              f"   H2 CAGR {r['cagr_h2']*100:7.2f}% MDD {r['mdd_h2']*100:6.1f}%")
    ba, bb = half_stats(base_nav)
    print(f"{'canonical':17s} H1 CAGR {ba['cagr']*100:7.2f}% MDD {ba['mdd']*100:6.1f}%"
          f"   H2 CAGR {bb['cagr']*100:7.2f}% MDD {bb['mdd']*100:6.1f}%")

    print("\n逐年報酬(canonical vs 通過 D2 判準者):")
    yb = yearly_table(base_nav)
    print("year       " + "  ".join(f"{y:5d}" for y in yb["year"]))
    print("canonical  " + "  ".join(f"{v*100:+5.0f}" for v in yb["ret"]))
    for r in rows:
        if (r["sortino"] >= bs["sortino"] and r["calmar"] >= bs["calmar"]
                and r["mdd"] >= bs["mdd"] and r["boot_lo"] >= bboot["ci_lo"]
                and not r["identical"]):
            nav, _ = run_s_full(panel, feat, elig, DS,
                                _score_fn=make_score_fn(risk, VARIANTS[r["name"]]))
            yt = yearly_table(nav.sort("date"))
            print(f"{r['name']:11s}" + "  ".join(f"{v*100:+5.0f}" for v in yt["ret"]))


if __name__ == "__main__":
    main()
