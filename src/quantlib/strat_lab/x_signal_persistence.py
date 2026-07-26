"""S 策略維度實驗:訊號時間平滑與持續性(x_signal_persistence)。

**問題**:S 用「當日分數」選股。當日截面分數是否雜訊過大?若對分數做 N 日平滑、
或要求「連續 K 日在榜內」才進場、或延遲進場讓訊號沉澱,是否能提高訊號雜訊比?
反方假設同樣可證偽:S 賺的就是「營收公布後搶快」,任何平滑/延遲都只會遲到。

**變體家族**(全部 PIT——只用 ≤ 決策日的資料):
  A. sm_mean_{W}d   分數的 W 日曆日移動平均(W∈{3,5,8};W=8 ≈ 整個新鮮度視窗均值)
  B. sm_max_{W}d    分數的 W 日滾動最大(要求近期「曾經很強」)
     sm_min_{W}d    分數的 W 日滾動最小(要求「穩定地強」,懲罰忽上忽下)
  C. pers_k{K}_n{N} 需連續 K 個交易日都在當日 top-N 榜內才可進場(K∈{2,3},N∈{5,10})
  D. delay_{L}d     訊號產生後延遲 L 個交易日才下單(L∈{1,2,3};成交仍為次日開盤)
  E. fresh_3d       反向探針:池新鮮度閘 7→3 日(要求更快,測「搶快」是否才是 alpha 源)

**為什麼視窗用「日曆日」而非「列數」**:S 的候選池是 `rev_fresh_days <= 7`,
每檔股票每月只在營收公布後連續約 5-6 個交易日進池。若用「前 N 列」滾動,視窗會跨越
月份把上個月的分數混進來(那是「持續性」不是「平滑」)。用 `rolling_*_by(date, "Wd")`
的時間視窗,W ≤ 8 保證不跨月 burst,語義乾淨。

**方法論**:任何「較佳變體」都必須通過配對 block-bootstrap(block=21、n_boot=4000,
對同一組 block 索引同時重抽 variant 與 canonical 的日報酬 → CAGR 差分佈);95% CI
跨 0 即判噪音。另要求 Sortino/Calmar/MDD/bootstrap 下界同時 ≥ canonical 才算候選。

**結論(2026-07-26,乾淨資料全跨度 2014-10~2026-07,含成本)——整個維度證偽**:
canonical CAGR 82.3% / Sortino 3.28 / Calmar 2.40 / MDD -34.3% / bootstrap 下界 51.3%。
17 個變體**無一**在任何 KPI 上勝出,配對 CAGR 差全部為負(-3.5pp ~ -32.3pp);其中
sm_min_*、pers_k2_n5、pers_k3_*、delay_3d、blend_l50、fresh_3d 的 95% CI 完全落在 0
以下(顯著劣化),其餘 CI 跨 0 但點估計仍為負。劑量反應(λ=0/0.25/0.5)顯示最佳劑量
就是 λ=0 = 不平滑。

**機制(check 模式的結構證據)**:S 的月營收 `avail` 對全市場都是「次月 10 日」→ 候選池
是**全池同步**的每月 burst(每月 10 日後 5-6 個交易日,全史僅 767 個池日)。實際成交日的
`rev_fresh_days` 中位數只有 **2**(全部池日中位 4)——引擎在 burst 前兩天就把「每日 2 檔 /
共 5 席」的額度用完。也就是說 **S 的 alpha 集中在營收公布後的頭兩個交易日**,任何平滑、
持續性確認或延遲都只是把進場推離這個甜蜜點,必然稀釋。反向探針 fresh_3d(要求更快)
同樣劣化 -18.8pp,是因為砍掉 burst 尾端的備選候選(交易數 684→606),讓後續騰出的席位
沒人補;所以最適解不是「更快」而是「快進場 + 保留備選深度」= 現役設定。

**副產品(非 bug 的實測解釋)**:sm_max_5d 與 sm_max_8d 的 NAV 逐位相同,並非實作錯誤——
兩者分數在 9.9% 的列上不同、top-5 名單在 16.7% 的池日上不同,但那些差異全落在 burst
尾端(相異日 fresh 中位 6),而尾端隔日有成交的機率只有 0.8%(全池日平均 54%)。
`check` 子模式會重跑這串證據。

Run:
    uv run --project . python -m quantlib.strat_lab.x_signal_persistence         # 全變體
    uv run --project . python -m quantlib.strat_lab.x_signal_persistence check   # 結構自檢

依賴 cache: 是(quantlib.apex.data.connect + prep_cached 讀 cache.duckdb)。
"""
from __future__ import annotations

import math
import sys
import time
from dataclasses import dataclass
from typing import Callable

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.metrics import perf_stats, trade_stats, yearly_table
from quantlib.apex.strategy_s import C, DS, WREL, prep_cached, run_s_full
from quantlib.apex.validate import block_bootstrap_cagr

TRADING_DAYS = 252
HALF_SPLIT = "2020-07-01"      # 前後半切點(全跨度 2014-10~2026-07 的中位)


# ── 計分基元(逐位複刻 run_s_full 的 canonical 計分,供變體在其上做時間變換)──

def base_score(df: pl.DataFrame, wrel: dict | None = None) -> pl.DataFrame:
    """canonical S 分數:六因子當日 rank-pct 的權重次方連乘。"""
    wrel = wrel or WREL
    expr = None
    for c_, wt in wrel.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    return df.with_columns(expr.alias("score"))


def _rolling_by(kind: str, window_days: int):
    """回傳 _score_fn:先算 canonical 分數,再對每檔做 W 日曆日時間視窗滾動聚合。

    `closed="right"` = 視窗含當日、只看過去 → PIT 合規(無前視)。
    min_samples 預設 1:burst 第一天沒有歷史時退化成當日分數(不強制延遲進場;
    「必須有歷史」的語義由 pers_* 家族單獨測)。
    """
    ws = f"{window_days}d"

    def fn(df: pl.DataFrame) -> pl.DataFrame:
        d = base_score(df).sort([C, "date"])
        col = pl.col("score")
        agg = {"mean": col.rolling_mean_by("date", window_size=ws, closed="right"),
               "max": col.rolling_max_by("date", window_size=ws, closed="right"),
               "min": col.rolling_min_by("date", window_size=ws, closed="right")}[kind]
        return d.with_columns(agg.over(C).alias("score"))

    return fn


def _blend(window_days: int, lam: float):
    """回傳 _score_fn:分數向 W 日移動平均**收縮** λ(劑量反應測試)。

    λ=0 即 canonical、λ=1 即完全平滑。若平滑真的有價值,劑量-反應曲線應在
    某個 λ>0 有峰值;若曲線從 λ=0 起單調下滑,代表最佳劑量就是「不平滑」。
    """
    ws = f"{window_days}d"

    def fn(df: pl.DataFrame) -> pl.DataFrame:
        d = base_score(df).sort([C, "date"])
        sm = pl.col("score").rolling_mean_by("date", window_size=ws, closed="right").over(C)
        return d.with_columns(((1 - lam) * pl.col("score") + lam * sm).alias("score"))

    return fn


def _persistence(k: int, n: int, dmap: pl.DataFrame):
    """回傳 _score_fn:要求連續 k 個交易日都落在當日 top-n 榜內才留為候選。

    dmap = (date, di) 交易日序號表(來自 panel)——「連續」以交易日計,不是日曆日。
    """
    def fn(df: pl.DataFrame) -> pl.DataFrame:
        d = base_score(df).join(dmap, on="date", how="inner")
        top = (d.with_columns(pl.col("score").rank("ordinal", descending=True)
                              .over("date").alias("rk"))
               .filter(pl.col("rk") <= n))
        out = top
        for j in range(1, k):
            prev = top.select([C, (pl.col("di") + j).alias("di")])
            out = out.join(prev, on=[C, "di"], how="semi")
        return out.drop(["rk", "di"])

    return fn


def _delay(lag: int, dmap: pl.DataFrame):
    """回傳 _entries_fn:把候選名單的決策日往後推 lag 個交易日(成交仍為次日開盤)。

    同一檔可能被多個決策日推到同一天 → 保留分數最高者(與引擎「score 高者優先」一致)。
    """
    fwd = dmap.select([pl.col("di"), pl.col("date").alias("date_new")])

    def fn(entries: pl.DataFrame) -> pl.DataFrame:
        return (entries.join(dmap, on="date", how="inner")
                .with_columns((pl.col("di") + lag).alias("di"))
                .drop("date").join(fwd, on="di", how="inner")
                .rename({"date_new": "date"})
                .sort("score", descending=True)
                .unique(subset=["date", C], keep="first")
                .select(["date", C, "score"]))

    return fn


# ── 統計 ────────────────────────────────────────────────────────────────

def _aligned_returns(nav_v: pl.DataFrame, nav_c: pl.DataFrame):
    j = (nav_v.select(["date", pl.col("nav").alias("v")])
         .join(nav_c.select(["date", pl.col("nav").alias("c")]), on="date", how="inner")
         .sort("date"))
    v, c = j["v"].to_numpy(), j["c"].to_numpy()
    return v[1:] / v[:-1] - 1.0, c[1:] / c[:-1] - 1.0


def paired_block_bootstrap(nav_v, nav_c, n_boot=4000, block=21, seed=42, chunk=500) -> dict:
    """配對 moving-block bootstrap:同一組 block 索引同時重抽兩條曲線 → CAGR 差分佈。

    配對(而非各自獨立重抽)是對高相關曲線最有統計力的做法——共同的市場 beta 在
    差分中相消,剩下的才是變體真正的增量。
    """
    rv, rc = _aligned_returns(nav_v, nav_c)
    t = len(rv)
    rng = np.random.default_rng(seed)
    n_blocks = math.ceil(t / block)
    diffs = np.empty(n_boot)
    off = np.arange(block)
    for lo in range(0, n_boot, chunk):
        hi = min(lo + chunk, n_boot)
        starts = rng.integers(0, t, size=(hi - lo, n_blocks))
        idx = ((starts[:, :, None] + off[None, None, :]) % t).reshape(hi - lo, -1)[:, :t]
        gv = np.prod(1.0 + rv[idx], axis=1) ** (TRADING_DAYS / t) - 1.0
        gc = np.prod(1.0 + rc[idx], axis=1) ** (TRADING_DAYS / t) - 1.0
        diffs[lo:hi] = gv - gc
    pv = perf_stats(nav_v)["cagr"]
    pc = perf_stats(nav_c)["cagr"]
    return {"point": pv - pc,
            "boot_median": float(np.percentile(diffs, 50)),
            "ci_lo": float(np.percentile(diffs, 2.5)),
            "ci_hi": float(np.percentile(diffs, 97.5)),
            "p_le0": float((diffs <= 0).mean())}


def _slice(nav: pl.DataFrame, lo: str | None, hi: str | None) -> pl.DataFrame:
    d = nav.sort("date")
    if lo:
        d = d.filter(pl.col("date") >= pl.lit(lo).str.to_date())
    if hi:
        d = d.filter(pl.col("date") < pl.lit(hi).str.to_date())
    return d.with_columns(pl.col("nav") / pl.col("nav").first())


# ── 變體目錄 ────────────────────────────────────────────────────────────

@dataclass
class Variant:
    name: str
    family: str
    note: str
    kw: dict


def build_variants(dmap: pl.DataFrame) -> list[Variant]:
    vs: list[Variant] = []
    for w in (3, 5, 8):
        vs.append(Variant(f"sm_mean_{w}d", "A_smooth", f"分數 {w} 日曆日移動平均",
                          {"_score_fn": _rolling_by("mean", w)}))
    for w in (5, 8):
        vs.append(Variant(f"sm_max_{w}d", "B_shape", f"分數 {w} 日滾動最大(曾經很強)",
                          {"_score_fn": _rolling_by("max", w)}))
        vs.append(Variant(f"sm_min_{w}d", "B_shape", f"分數 {w} 日滾動最小(穩定地強)",
                          {"_score_fn": _rolling_by("min", w)}))
    for k in (2, 3):
        for n in (5, 10):
            vs.append(Variant(f"pers_k{k}_n{n}", "C_persist",
                              f"連續 {k} 交易日在 top-{n} 榜內",
                              {"_score_fn": _persistence(k, n, dmap)}))
    for lag in (1, 2, 3):
        vs.append(Variant(f"delay_{lag}d", "D_delay", f"訊號延遲 {lag} 交易日下單",
                          {"_entries_fn": _delay(lag, dmap)}))
    for lam in (0.25, 0.5):
        vs.append(Variant(f"blend3d_l{int(lam*100)}", "F_dose",
                          f"分數向 3 日均值收縮 λ={lam}(劑量反應)",
                          {"_score_fn": _blend(3, lam)}))
    vs.append(Variant("fresh_3d", "E_faster", "反向探針:池新鮮度閘 7→3 日(更搶快)",
                      {"_fresh_days": 3}))
    return vs


def diag(panel: pl.DataFrame, feat: pl.DataFrame, elig: pl.DataFrame) -> None:
    """結構自檢:確認滾動視窗真的有作用(避免「5d 與 8d 結果相同」其實是 bug)。

    印出:每檔每個營收 burst 的在池交易日數分佈;以及 5d/8d 滾動 max 分數的差異比例
    與「當日 top-5 名單是否被改變」的日數比例。
    """
    df = (feat.filter(pl.col("rev_fresh_days") <= 7)
          .join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
          .drop_nulls(subset=list(WREL))
          .filter(pl.all_horizontal([pl.col(c).is_finite() for c in WREL]))
          .filter(pl.col("cfo_ni_ratio_ttm")
                  >= pl.col("cfo_ni_ratio_ttm").quantile(0.5).over("date")))
    burst = (df.group_by([C, (pl.col("date") - pl.duration(days=pl.col("rev_fresh_days")))
                          .alias("avail")]).len())
    q = burst["len"].quantile
    print(f"[diag] burst 在池交易日數 中位 {burst['len'].median():.0f} "
          f"p10 {q(0.1):.0f} p90 {q(0.9):.0f} max {burst['len'].max()}")
    a = _rolling_by("max", 5)(df).select(["date", C, pl.col("score").alias("s5")])
    b = _rolling_by("max", 8)(df).select(["date", C, pl.col("score").alias("s8")])
    j = a.join(b, on=["date", C], how="inner")
    diff = (j["s5"] - j["s8"]).abs() > 1e-12
    print(f"[diag] max5 vs max8 分數不同的列: {diff.sum():,} / {j.height:,} "
          f"({diff.mean()*100:.2f}%)")
    tops = {}
    for nm, col in (("s5", "s5"), ("s8", "s8")):
        tops[nm] = (j.with_columns(pl.col(col).rank("ordinal", descending=True)
                                   .over("date").alias("rk"))
                    .filter(pl.col("rk") <= 5).group_by("date")
                    .agg(pl.col(C).sort().alias(nm)))
    m = tops["s5"].join(tops["s8"], on="date", how="inner")
    chg = (m["s5"] != m["s8"])
    print(f"[diag] max5/max8 top-5 名單不同的交易日: {chg.sum():,} / {m.height:,} "
          f"({chg.mean()*100:.2f}%)")


# ── 主流程 ──────────────────────────────────────────────────────────────

def check_window_equivalence() -> None:
    """為什麼 sm_max_5d 與 sm_max_8d 的 NAV 逐位相同?——直接驗證,不靠推測。

    假設:兩者分數不同、top-5 名單也常不同,但**實際成交**相同,因為引擎每日最多
    2 個新倉、5 席常滿,只有排名前 1-2 的候選有機會成交;差異都落在排名 3-5。
    """
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    navs, ents = {}, {}
    for w in (5, 8):
        fn = _rolling_by("max", w)
        nav, tr = run_s_full(panel, feat, elig, DS, _score_fn=fn)
        navs[w] = nav
        ents[w] = set(zip(tr["entry_date"].to_list(), tr["company_code"].to_list()))
    a, b = navs[5]["nav"].to_numpy(), navs[8]["nav"].to_numpy()
    print(f"[check] NAV 逐位相同: {bool(np.array_equal(a, b))}  "
          f"max|Δ| {float(np.abs(a-b).max()):.3e}")
    print(f"[check] 實際成交 (entry_date, code) 集合相同: {ents[5] == ents[8]}  "
          f"|5d\\8d|={len(ents[5]-ents[8])} |8d\\5d|={len(ents[8]-ents[5])}")
    # 候選名單層:top-1/top-2/top-5 各有多少日不同
    df = (feat.filter(pl.col("rev_fresh_days") <= 7)
          .join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
          .drop_nulls(subset=list(WREL))
          .filter(pl.all_horizontal([pl.col(c).is_finite() for c in WREL]))
          .filter(pl.col("cfo_ni_ratio_ttm")
                  >= pl.col("cfo_ni_ratio_ttm").quantile(0.5).over("date"))
          .filter(pl.col("date") >= pl.lit(DS).str.to_date()))
    sc = {w: _rolling_by("max", w)(df).with_columns(
        pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
        for w in (5, 8)}
    diff_days = None
    for k in (1, 2, 5):
        t = {w: (s.filter(pl.col("rk") <= k).sort(["date", "rk"]).group_by("date")
                 .agg(pl.col(C).alias("lst"))) for w, s in sc.items()}   # 保序比較
        m = t[5].join(t[8], on="date", how="inner", suffix="_8").sort("date")
        d = (m["lst"] != m["lst_8"])
        print(f"[check] top-{k} 排序名單不同的池日: {d.sum():,} / {m.height:,} "
              f"({d.mean()*100:.2f}%)")
        if k == 5:
            diff_days = m.filter(d)["date"]
    # 決定性檢驗:名單有差異的那些日,隔日引擎到底有沒有成交機會?
    dates = panel["date"].unique().sort()
    nxt = {a: b for a, b in zip(dates[:-1], dates[1:])}
    tr5 = run_s_full(panel, feat, elig, DS, _score_fn=_rolling_by("max", 5))[1]
    fill_days = set(tr5["entry_date"].to_list())
    all_days = m["date"]
    base = sum(1 for d0 in all_days if nxt.get(d0) in fill_days)
    hit = sum(1 for d0 in diff_days if nxt.get(d0) in fill_days)
    print(f"[check] 全部 {len(all_days)} 個池日中隔日有新倉成交的: {base} "
          f"({base/len(all_days)*100:.1f}%);其中名單有差異的 {len(diff_days)} 日命中 {hit} "
          f"({hit/max(len(diff_days),1)*100:.1f}%)")
    # 機制:所有股票的 avail 都是「次月 10 日」→ 全池同步,同一日期上大家的
    # rev_fresh_days 幾乎一樣。故「池日」= 每月 10 日後那 5-6 個交易日的一個 burst。
    fd = (df.group_by("date").agg(pl.col("rev_fresh_days").median().alias("fd"))
          .sort("date"))
    fmap = dict(zip(fd["date"].to_list(), fd["fd"].to_list()))
    dset, fset = set(diff_days.to_list()), {d0 for d0 in all_days if nxt.get(d0) in fill_days}
    for label, ds in (("全部池日", set(all_days.to_list())), ("名單相異日", dset),
                      ("隔日有成交日", fset)):
        v = np.array([fmap[d0] for d0 in ds if d0 in fmap])
        print(f"[check] {label:12s} n={len(v):4d}  rev_fresh_days 中位 {np.median(v):.0f} "
              f"平均 {v.mean():.1f}")
    print("[check] 結論:5d/8d 的差異只出現在 burst 尾端(fresh 大),而引擎在 burst 前段"
          "就把 2 檔/日的額度與 5 席用完 → 尾端沒有成交機會,NAV 自然逐位相同(非 bug)")


def main() -> None:
    t0 = time.time()
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    dates = panel["date"].unique().sort()
    dmap = pl.DataFrame({"date": dates}).with_columns(
        pl.int_range(pl.len()).cast(pl.Int64).alias("di"))
    print(f"[prep] panel {panel.height:,} rows, {dates.len():,} trading days "
          f"({dates[0]} → {dates[-1]})  {time.time()-t0:.1f}s")
    diag(panel, feat, elig)

    t1 = time.time()
    nav_c, tr_c = run_s_full(panel, feat, elig, DS)
    st_c = perf_stats(nav_c)
    print(f"[canonical] {time.time()-t1:.1f}s/run  CAGR {st_c['cagr']*100:.1f}% "
          f"Sortino {st_c['sortino']:.2f} Calmar {st_c['calmar']:.2f} "
          f"MDD {st_c['mdd']*100:.1f}%")

    # parity 自檢:_score_fn=base_score 必須逐位重現 canonical(確認 hook 沒有隱性偏差)
    nav_p, _ = run_s_full(panel, feat, elig, DS, _score_fn=base_score)
    same = bool(np.allclose(nav_p["nav"].to_numpy(), nav_c["nav"].to_numpy(), atol=1e-12))
    print(f"[parity] _score_fn=base_score 重現 canonical: {same}")
    if not same:
        sys.exit("PARITY FAIL — base_score 與 canonical 計分不一致,實驗無效")

    bs_c = block_bootstrap_cagr(nav_c, n_boot=4000, block=21)
    print(f"[canonical] bootstrap CAGR 95% CI [{bs_c['ci_lo']*100:.1f}%, "
          f"{bs_c['ci_hi']*100:.1f}%]")

    rows = []
    for v in build_variants(dmap):
        t2 = time.time()
        nav, trades = run_s_full(panel, feat, elig, DS, **v.kw)
        st = perf_stats(nav)
        ts = trade_stats(trades)
        pb = paired_block_bootstrap(nav, nav_c)
        bs = block_bootstrap_cagr(nav, n_boot=4000, block=21)
        rows.append({"name": v.name, "family": v.family, "note": v.note,
                     "cagr": st["cagr"], "sortino": st["sortino"], "calmar": st["calmar"],
                     "mdd": st["mdd"], "sharpe": st["sharpe"], "boot_lo": bs["ci_lo"],
                     "n_trades": ts.get("n_trades", 0), "win": ts.get("win_rate", 0.0),
                     **{f"pb_{k}": x for k, x in pb.items()},
                     "_nav": nav})
        r = rows[-1]
        print(f"{v.name:14s} CAGR {st['cagr']*100:6.1f}% Sor {st['sortino']:5.2f} "
              f"Cal {st['calmar']:5.2f} MDD {st['mdd']*100:6.1f}% bootLo {bs['ci_lo']*100:6.1f}% "
              f"| Δ {pb['point']*100:+6.1f}pp CI[{pb['ci_lo']*100:+6.1f},{pb['ci_hi']*100:+6.1f}] "
              f"P≤0 {pb['p_le0']:.3f} | n={ts.get('n_trades',0)} ({time.time()-t2:.0f}s)")

    print("\n" + "=" * 118)
    print(f"canonical      CAGR {st_c['cagr']*100:6.1f}% Sor {st_c['sortino']:5.2f} "
          f"Cal {st_c['calmar']:5.2f} MDD {st_c['mdd']*100:6.1f}% "
          f"bootLo {bs_c['ci_lo']*100:6.1f}% n={trade_stats(tr_c).get('n_trades',0)}")

    # 候選判準(D2):Sortino/Calmar/MDD/bootstrap 下界同時 ≥ canonical,且配對 CI 不跨 0
    print("\n── 候選篩選(KPI 全面 ≥ canonical 且配對 CI 不跨 0)──")
    cands = []
    for r in rows:
        kpi_ok = (r["sortino"] >= st_c["sortino"] and r["calmar"] >= st_c["calmar"]
                  and r["mdd"] >= st_c["mdd"] and r["boot_lo"] >= bs_c["ci_lo"])
        sig_ok = r["pb_ci_lo"] > 0
        tag = "候選" if (kpi_ok and sig_ok) else ("KPI過/統計不顯著" if kpi_ok else "淘汰")
        if kpi_ok and sig_ok:
            cands.append(r)
        print(f"  {r['name']:14s} KPI {'✓' if kpi_ok else '✗'}  配對顯著 "
              f"{'✓' if sig_ok else '✗'}  → {tag}")

    focus = cands or sorted(rows, key=lambda r: -r["pb_point"])[:2]
    print("\n── 逐年報酬(canonical vs 表現最好的兩支)──")
    yt_c = yearly_table(nav_c)
    print("year      " + "  ".join(f"{y:>6d}" for y in yt_c["year"]))
    print("canonical " + "  ".join(f"{x*100:+6.0f}" for x in yt_c["ret"]))
    for r in focus:
        yt = yearly_table(r["_nav"])
        print(f"{r['name']:10s}" + "  ".join(f"{x*100:+6.0f}" for x in yt["ret"]))

    print("\n── 前後半段(2014-10~2020-06 / 2020-07~2026-07)──")
    for label, lo, hi in (("H1", None, HALF_SPLIT), ("H2", HALF_SPLIT, None)):
        sc = perf_stats(_slice(nav_c, lo, hi))
        print(f"  {label} canonical  CAGR {sc['cagr']*100:6.1f}%  Sortino {sc['sortino']:5.2f} "
              f"Calmar {sc['calmar']:5.2f}  MDD {sc['mdd']*100:6.1f}%")
        for r in focus:
            s = perf_stats(_slice(r["_nav"], lo, hi))
            pb = paired_block_bootstrap(_slice(r["_nav"], lo, hi), _slice(nav_c, lo, hi))
            print(f"  {label} {r['name']:11s} CAGR {s['cagr']*100:6.1f}%  Sortino {s['sortino']:5.2f} "
                  f"Calmar {s['calmar']:5.2f}  MDD {s['mdd']*100:6.1f}%  "
                  f"Δ {pb['point']*100:+6.1f}pp CI[{pb['ci_lo']*100:+6.1f},{pb['ci_hi']*100:+6.1f}]")

    out = (pl.DataFrame([{k: x for k, x in r.items() if k != "_nav"} for r in rows])
           .sort("pb_point", descending=True))
    with pl.Config(tbl_rows=40, tbl_cols=20, tbl_width_chars=200):
        print("\n", out.select(["name", "family", "cagr", "sortino", "calmar", "mdd",
                                "boot_lo", "pb_point", "pb_ci_lo", "pb_ci_hi",
                                "pb_p_le0", "n_trades"]))
    print(f"\n總耗時 {time.time()-t0:.0f}s")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "check":
        check_window_equivalence()
    else:
        main()
