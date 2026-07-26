"""S 策略維度:**絕對分數門檻 / 空手等待**(乾淨資料 campaign)。

問題:canonical S 永遠持滿 5 檔(相對排名 top-5),即使當天最好的候選也很爛——
強迫持滿在「沒好貨」時可能買到爛股。本 harness 測「有絕對品質底線才進場,否則
空手抱現金」是否提升風險調整後報酬。

五個家族(全部只用 run_s_full 的研究 hooks,不改 canonical 預設):
  A  score_q   分數絕對門檻:候選分數 < 歷史分位數 → 不進場(空手)。
                **PIT 嚴格**:門檻只用「決策日之前」已觀察到的候選分數(擴張窗),
                且需 MIN_OBS 筆歷史才啟用(之前 = canonical 行為)。
  B  abs_*     絕對因子門檻(空手版,select-then-filter):canonical top-5 中不符
                絕對條件者剔除、不遞補。門檻取經濟自然刻度且「真的會咬」(先由
                --mode diag 量出候選的因子分佈再定,避免設出無效門檻):
                  rev_yoy_accel ≥ {5,10,15,20,30} pp(3 月均 YoY − 12 月均 YoY)
                  high_52w ≥ {0.90,0.95,0.99}(距 52 週高 ≤ 10/5/1%)
                  mom_126_5 ≥ {0.20,0.50}(半年動能)
  Bs abs_*_sub 同條件的**遞補版**(filter-then-select):先用絕對條件篩池,再取
                top-5——回答「被剔除的 top-5 邊際股 vs 排名 6-10 但過絕對條件」孰優。
  C  minK      候選數門檻:當日通過絕對條件的候選 < K 檔 → 整天不進場。
  D  sizing    軟版:分數歷史分位數低 → 縮小部位(weight 0.20 → 0.10),不全空手。
  E  breadth   市場水位:全池「營收加速為正」占比低於歷史分位 → 當日不進場。

判準(D2):Sortino / Calmar / MDD / bootstrap 下界必須**同時 ≥ canonical**;
再對日報酬差做配對 block-bootstrap(block=21, n_boot=4000),CI 跨 0 = 噪音級 = 證偽。

── 結論(2026-07-26,32 變體全跑完;**證偽**,負結果落地防重複試錯)────────
canonical:CAGR +82.3% / Sortino 3.28 / Calmar 2.40 / MDD −34.3% / boot_lo +51.3%。
32 個變體**無一**通過出廠標準;最好的 B_accel15(加速 ≥+15pp)年化差僅 +2.8%、
95% CI [−2.8%, +8.3%]、P(差≤0)=0.162 → 噪音級。門檻一旦收緊即單調崩壞
(accel30 −19.0%/yr、breadth50 −16.8%/yr、C_accel20_k5 −21.8%/yr)。

為什麼失效(diag 量出的機制,三層):
 1. **前提不成立**:S 的 top-5 在絕對水位上本來就極好——684 筆成交中 683 筆
    營收加速為正、96% 站在 52 週高的 10% 以內、中位加速 +33pp。「沒好貨還硬買」
    這件事幾乎不發生,絕對門檻沒有東西可擋。
 2. **對照組是現金,不是更好的股票**:S 中位持有僅 16 個交易日,連最差的候選桶
    (分數歷史分位 Q1、加速 5~15pp)每筆平均仍 +2~4.5%,年化等值 +37%~+100%。
    把它換成 0% 的現金,數學上就是穩定毀損複利——空手等待在高換手策略上先天不利。
 3. **遞補也沒用**:Bs 家族(換成排名 6-10 但過絕對門檻者)全部劣化 −2.3~−6.8%/yr,
    證明相對排名 top-5 確實比「絕對條件好但排名差」的替補優。
交易層另有一條獨立證據:加速 <15pp 的成交平均 +1.8% vs ≥15pp 的 +6.6%(Welch
t=3.02)——每筆確實較差,但因 (2) 仍遠勝現金,故無法轉成組合層優勢。

Run:
  uv run --project . python -m quantlib.strat_lab.x_abs_gate_cash --mode diag
  uv run --project . python -m quantlib.strat_lab.x_abs_gate_cash --mode all
  uv run --project . python -m quantlib.strat_lab.x_abs_gate_cash --mode plateau
依賴 cache:是(prep_cached 讀 industry_taxonomy_pit / 價格 panel)。
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.metrics import perf_stats, trade_stats, yearly_table
from quantlib.apex.strategy_s import DS, WREL, prep_cached, run_s_full
from quantlib.apex.validate import block_bootstrap_cagr
from quantlib.strat_lab.s_accelrel_gate import paired_boot

C = "company_code"

#: 擴張窗門檻啟用所需的最少歷史候選筆數。候選只在月營收公布後 ~6 個交易日出現
#: (rev_fresh_days ≤ 7),約 30 筆/月 → 700 筆 ≈ 兩年歷史(橫跨多頭與回檔各一段),
#: 分位數估計才不至於被單一 regime 綁架。啟用前一律 = canonical 行為(不過濾)。
MIN_OBS = 700

#: 前後半段切點(樣本外精神:兩段各自表現,看是否只靠某一段)
SPLIT = "2020-07-01"

FACTOR_COLS = ["rev_yoy_accel", "high_52w", "mom_126_5", "close_pos_20", "rev_seq"]

_G: dict = {}   # 每個工作程序的 panel/feat/elig(initializer 載入一次)


# ── 共用純函式 ──────────────────────────────────────────────────────────

def score_expr(wrel: dict | None = None) -> pl.Expr:
    """canonical S 計分式(rank-pct 幾何加權)——與 strategy_s.run_s_full 內聯式等價。
    hook `_score_fn` 契約要求自行回傳 'score' 欄,故此處複刻該式(權重仍由 WREL 供)。"""
    expr = None
    for c_, wt in (wrel or WREL).items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    return expr.alias("score")


def expanding_quantile(dates: np.ndarray, vals: np.ndarray, p: float,
                       min_obs: int = MIN_OBS, win: int = 0) -> tuple[np.ndarray, np.ndarray]:
    """逐日歷史分位數(**嚴格只用該日之前**的觀測;PIT 無前視)。

    win=0 → 擴張窗(全歷史);win>0 → 只看最近 win 筆觀測(去趨勢版:候選分數水位
    本身逐年上飄〔見 diag〕,擴張窗門檻會被遠古低水位拖住而在近年幾乎不咬)。
    回傳 (unique_dates, threshold);歷史筆數 < min_obs 時門檻 = -inf(等同不過濾)。
    """
    order = np.argsort(dates, kind="stable")
    ds, vs = dates[order], vals[order]
    uniq, starts = np.unique(ds, return_index=True)
    out = np.full(len(uniq), -np.inf)
    for k in range(len(uniq)):
        n = int(starts[k])
        if n >= min_obs:
            lo = max(0, n - win) if win else 0
            out[k] = float(np.quantile(vs[lo:n], p))
    return uniq, out


def expanding_pct(dates: np.ndarray, vals: np.ndarray,
                  min_obs: int = MIN_OBS) -> np.ndarray:
    """每筆觀測在「其之前所有觀測」中的經驗分位(PIT);歷史不足時回 NaN。"""
    order = np.argsort(dates, kind="stable")
    ds, vs = dates[order], vals[order]
    uniq, starts = np.unique(ds, return_index=True)
    res = np.full(len(vs), np.nan)
    bounds = list(starts) + [len(vs)]
    for k in range(len(uniq)):
        n = int(starts[k])
        if n < min_obs:
            continue
        prev = np.sort(vs[:n])
        seg = slice(bounds[k], bounds[k + 1])
        res[seg] = np.searchsorted(prev, vs[seg], side="left") / n
    inv = np.empty_like(res)
    inv[order] = res
    return inv


def _cond_expr(cond: tuple[str, str, float]) -> pl.Expr:
    col, op, v = cond
    return pl.col(col) > v if op == ">" else pl.col(col) >= v


# ── 變體規格(declarative,可跨程序 pickle)───────────────────────────────

@dataclass(frozen=True)
class Variant:
    name: str
    kind: str                                  # canonical|score_q|abs|abs_sub|minK|sizing|breadth
    conds: tuple = ()                          # (col, op, value) 之 tuple
    p: float = 0.0                             # 分位數門檻
    win: int = 0                               # 0=擴張窗;>0=最近 N 筆滾動窗
    min_k: int = 0                             # 當日最少通過候選數
    small_w: float = 0.10                      # sizing 家族的縮小權重
    note: str = ""


#: 滾動窗長度:候選觀測 ~30 筆/月 → 1000 筆 ≈ 近三年(涵蓋一個完整多空循環)
ROLL = 1000

VARIANTS: list[Variant] = [
    # A:分數絕對門檻 — 擴張窗(全歷史)
    Variant("A_exp_q20", "score_q", p=0.20, note="候選分數 < 歷史 P20 → 空手"),
    Variant("A_exp_q35", "score_q", p=0.35, note="< 歷史 P35 → 空手"),
    Variant("A_exp_q50", "score_q", p=0.50, note="< 歷史 P50 → 空手"),
    Variant("A_exp_q65", "score_q", p=0.65, note="< 歷史 P65 → 空手"),
    # A':滾動三年窗(去掉分數水位逐年上飄的趨勢)
    Variant("A_roll_q35", "score_q", p=0.35, win=ROLL, note="< 近三年 P35 → 空手"),
    Variant("A_roll_q50", "score_q", p=0.50, win=ROLL, note="< 近三年 P50 → 空手"),
    Variant("A_roll_q65", "score_q", p=0.65, win=ROLL, note="< 近三年 P65 → 空手"),
    # B:絕對因子門檻(空手版;門檻取「真的會咬」的經濟自然刻度,見 diag 分佈)
    Variant("B_accel10", "abs", conds=(("rev_yoy_accel", ">=", 10.0),),
            note="營收加速 ≥ +10pp"),
    Variant("B_accel20", "abs", conds=(("rev_yoy_accel", ">=", 20.0),),
            note="營收加速 ≥ +20pp"),
    Variant("B_accel30", "abs", conds=(("rev_yoy_accel", ">=", 30.0),),
            note="營收加速 ≥ +30pp"),
    Variant("B_h52_90", "abs", conds=(("high_52w", ">=", 0.90),), note="距 52 週高 ≤10%"),
    Variant("B_h52_95", "abs", conds=(("high_52w", ">=", 0.95),), note="距 52 週高 ≤5%"),
    Variant("B_h52_99", "abs", conds=(("high_52w", ">=", 0.99),), note="幾乎就在 52 週高"),
    Variant("B_mom20", "abs", conds=(("mom_126_5", ">=", 0.20),), note="半年動能 ≥ +20%"),
    Variant("B_mom50", "abs", conds=(("mom_126_5", ">=", 0.50),), note="半年動能 ≥ +50%"),
    Variant("B_accel20_h95", "abs",
            conds=(("rev_yoy_accel", ">=", 20.0), ("high_52w", ">=", 0.95)), note="兩者皆須"),
    # Bs:同條件遞補版(先篩池再取 top-5)
    Variant("Bs_accel20", "abs_sub", conds=(("rev_yoy_accel", ">=", 20.0),),
            note="加速 ≥+20pp 者中取 top-5(遞補)"),
    Variant("Bs_h52_95", "abs_sub", conds=(("high_52w", ">=", 0.95),),
            note="距高 ≤5% 者中取 top-5(遞補)"),
    Variant("Bs_accel20_h95", "abs_sub",
            conds=(("rev_yoy_accel", ">=", 20.0), ("high_52w", ">=", 0.95)),
            note="兩條件過濾後取 top-5(遞補)"),
    # C:候選數門檻(整天不進場)
    Variant("C_accel20_k3", "minK", conds=(("rev_yoy_accel", ">=", 20.0),), min_k=3,
            note="通過候選 <3 → 整天空手"),
    Variant("C_accel20_k5", "minK", conds=(("rev_yoy_accel", ">=", 20.0),), min_k=5,
            note="通過候選 <5 → 整天空手"),
    Variant("C_roll_q50_k3", "minK", p=0.50, win=ROLL, min_k=3,
            note="過近三年 P50 者 <3 → 整天空手"),
    # D:軟版 sizing(不空手,縮小部位)
    Variant("D_size_q50", "sizing", p=0.50, small_w=0.10,
            note="分數 < 歷史 P50 → 部位 20%→10%"),
    Variant("D_size_q35", "sizing", p=0.35, small_w=0.10,
            note="分數 < 歷史 P35 → 部位 20%→10%"),
    # E:市場水位(breadth)
    Variant("E_breadth30", "breadth", p=0.30, note="全池營收加速為正占比 < 歷史 P30 → 空手"),
    Variant("E_breadth50", "breadth", p=0.50, note="占比 < 歷史 P50 → 空手"),
]

#: 第二輪:對第一輪唯二「D2 全過但 CI 跨 0」者(A_exp_q35 / B_accel10)做**高原驗證**——
#: 鄰近門檻若不同號、只有單點好看,那個好看就是雜訊擬合而非結構(§方法論 4)。
PLATEAU: list[Variant] = [
    Variant("A_exp_q25", "score_q", p=0.25, note="高原:< 歷史 P25"),
    Variant("A_exp_q30", "score_q", p=0.30, note="高原:< 歷史 P30"),
    Variant("A_exp_q40", "score_q", p=0.40, note="高原:< 歷史 P40"),
    Variant("A_exp_q45", "score_q", p=0.45, note="高原:< 歷史 P45"),
    Variant("B_accel05", "abs", conds=(("rev_yoy_accel", ">=", 5.0),), note="高原:加速 ≥+5pp"),
    Variant("B_accel15", "abs", conds=(("rev_yoy_accel", ">=", 15.0),), note="高原:加速 ≥+15pp"),
]


# ── hook 工廠 ───────────────────────────────────────────────────────────

def _thr_frame(dates: np.ndarray, vals: np.ndarray, p: float, win: int = 0) -> pl.DataFrame:
    uniq, thr = expanding_quantile(dates, vals, p, win=win)
    return pl.DataFrame({"date": pl.Series(uniq).cast(pl.Date), "_thr": thr})


def _apply_conds(e: pl.DataFrame, feat: pl.DataFrame, conds: tuple) -> pl.DataFrame:
    cols = sorted({c[0] for c in conds})
    e = e.join(feat.select(["date", C, *cols]), on=["date", C], how="left")
    for cond in conds:
        e = e.filter(_cond_expr(cond))
    return e.drop(cols)


def build_hooks(v: Variant, feat: pl.DataFrame, elig: pl.DataFrame) -> dict:
    """把 Variant 轉成 run_s_full 的 keyword hooks(canonical 以外一律只動 hook)。"""
    if v.kind == "canonical":
        return {}

    if v.kind == "score_q":
        def _fn(entries: pl.DataFrame, _p=v.p, _w=v.win) -> pl.DataFrame:
            t = _thr_frame(entries["date"].to_numpy(), entries["score"].to_numpy(), _p, _w)
            return (entries.join(t, on="date", how="left")
                    .filter(pl.col("score") >= pl.col("_thr")).drop("_thr"))
        return {"_entries_fn": _fn}

    if v.kind == "abs":
        def _fn(entries: pl.DataFrame, _f=feat, _c=v.conds) -> pl.DataFrame:
            return _apply_conds(entries, _f, _c)
        return {"_entries_fn": _fn}

    if v.kind == "abs_sub":
        def _sf(df: pl.DataFrame, _c=v.conds) -> pl.DataFrame:
            out = df.with_columns(score_expr())      # 分數用**過濾前**全截面 rank-pct
            for cond in _c:
                out = out.filter(_cond_expr(cond))
            return out
        return {"_score_fn": _sf}

    if v.kind == "minK":
        def _fn(entries: pl.DataFrame, _f=feat, _v=v) -> pl.DataFrame:
            if _v.conds:
                e = _apply_conds(entries, _f, _v.conds)
            else:
                t = _thr_frame(entries["date"].to_numpy(), entries["score"].to_numpy(),
                               _v.p, _v.win)
                e = (entries.join(t, on="date", how="left")
                     .filter(pl.col("score") >= pl.col("_thr")).drop("_thr"))
            return e.filter(pl.len().over("date") >= _v.min_k)
        return {"_entries_fn": _fn}

    if v.kind == "sizing":
        def _fn(entries: pl.DataFrame, _v=v) -> pl.DataFrame:
            pct = expanding_pct(entries["date"].to_numpy(), entries["score"].to_numpy())
            # 歷史不足(NaN)→ 視為通過(canonical 行為)
            w = np.where(np.isnan(pct) | (pct >= _v.p), 0.20, _v.small_w)
            return entries.with_columns(pl.Series("weight", w))
        return {"_entries_fn": _fn}

    if v.kind == "breadth":
        pool = (feat.filter(pl.col("rev_fresh_days") <= 7)
                .join(elig.filter(pl.col("eligible")).select(["date", C]),
                      on=["date", C], how="semi")
                .drop_nulls(subset=["rev_yoy_accel"]))
        br = (pool.group_by("date")
              .agg((pl.col("rev_yoy_accel") > 0).mean().alias("br"))
              .sort("date"))
        bd, bv = br["date"].to_numpy(), br["br"].to_numpy()
        # breadth 是**日級**觀測(每個候選日一筆),歷史門檻改用 24 個月 ≈ 144 筆
        uniq, thr = expanding_quantile(bd, bv, v.p, min_obs=144)
        ok = pl.DataFrame({"date": pl.Series(uniq).cast(pl.Date), "_thr": thr}).join(
            br, on="date", how="inner").filter(pl.col("br") >= pl.col("_thr")).select("date")

        def _fn(entries: pl.DataFrame, _ok=ok) -> pl.DataFrame:
            return entries.join(_ok, on="date", how="semi")
        return {"_entries_fn": _fn}

    raise ValueError(f"unknown kind {v.kind}")


# ── 執行 ────────────────────────────────────────────────────────────────

def _init_worker() -> None:
    con = data.connect()
    p, f, e = prep_cached(con)
    _G["panel"], _G["feat"], _G["elig"] = p, f, e


def run_variant(v: Variant) -> dict:
    panel, feat, elig = _G["panel"], _G["feat"], _G["elig"]
    hooks = build_hooks(v, feat, elig)
    nav, trades = run_s_full(panel, feat, elig, DS, **hooks)
    return {"name": v.name, "note": v.note,
            "nav": nav.to_dict(as_series=False), "trades": trades.to_dict(as_series=False)}


def _occupancy(trades: pl.DataFrame, nav: pl.DataFrame) -> dict:
    """從 trades 重建每日持倉數(引擎 nav 只回 date/nav,故由交易區間還原)。"""
    d = nav["date"].to_numpy()
    cnt = np.zeros(len(d), dtype=int)
    for a, b in zip(trades["entry_date"].to_numpy(), trades["exit_date"].to_numpy()):
        i, j = np.searchsorted(d, a), np.searchsorted(d, b)
        cnt[i:j + 1] += 1
    return {"avg_pos": float(cnt.mean()), "pct_days_empty": float((cnt == 0).mean()),
            "pct_days_full": float((cnt >= 5).mean())}


def _mdd_episode(nav: pl.DataFrame) -> str:
    """最大回撤的峰→谷日期(判斷 MDD 改善是否只是「單一事件被躲掉」)。"""
    v, d = nav["nav"].to_numpy(), nav["date"].to_numpy()
    rm = np.maximum.accumulate(v)
    dd = v / rm - 1.0
    j = int(dd.argmin())
    i = int(np.argmax(v[:j + 1]))
    return f"{str(d[i])[:10]} → {str(d[j])[:10]} ({dd[j]:.1%})"


def _subperiod(nav: pl.DataFrame) -> dict:
    out = {}
    for tag, f in (("H1", pl.col("date") < pl.lit(SPLIT).str.to_date()),
                   ("H2", pl.col("date") >= pl.lit(SPLIT).str.to_date())):
        s = nav.filter(f)
        if s.height < 50:
            continue
        s = s.with_columns(pl.col("nav") / pl.col("nav").first())
        st = perf_stats(s)
        out[tag] = (st["cagr"], st["sortino"], st["mdd"], st["calmar"])
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["diag", "all", "plateau"], default="all")
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()
    todo = PLATEAU if args.mode == "plateau" else VARIANTS

    con = data.connect()
    panel, feat, elig = prep_cached(con)

    if args.mode == "diag":
        diagnostics(panel, feat, elig)
        return

    nav_c, tr_c = run_s_full(panel, feat, elig, DS)
    st_c = perf_stats(nav_c)
    bs_c = block_bootstrap_cagr(nav_c, n_boot=4000)
    occ_c = _occupancy(tr_c, nav_c)
    print("=== canonical S(基準)===")
    print(f"  CAGR {st_c['cagr']:+.1%}  Sortino {st_c['sortino']:.2f}  "
          f"Calmar {st_c['calmar']:.2f}  MDD {st_c['mdd']:.1%}  boot_lo {bs_c['ci_lo']:+.1%}")
    print(f"  平均持倉 {occ_c['avg_pos']:.2f}/5  空手日 {occ_c['pct_days_empty']:.1%}  "
          f"滿倉日 {occ_c['pct_days_full']:.1%}  交易 {tr_c.height}")
    print(f"  最大回撤事件 {_mdd_episode(nav_c)}")
    print()

    with ProcessPoolExecutor(max_workers=args.workers, initializer=_init_worker) as ex:
        results = list(ex.map(run_variant, todo))

    print("=== 變體(全跨度;粗體判準:Sortino/Calmar/MDD/boot_lo 同時 ≥ canonical)===")
    hdr = (f"{'變體':<16}{'CAGR':>8}{'Sortino':>9}{'Calmar':>8}{'MDD':>8}"
           f"{'boot_lo':>9}{'年化差':>9}{'CI下界':>9}{'CI上界':>9}{'P(≤0)':>8}"
           f"{'倉位':>7}{'空手%':>7}{'交易':>6}  判定")
    print(hdr)
    rows = []
    for r in results:
        nav = pl.DataFrame(r["nav"]).with_columns(pl.col("date").cast(pl.Date))
        trades = pl.DataFrame(r["trades"]).with_columns(
            [pl.col("entry_date").cast(pl.Date), pl.col("exit_date").cast(pl.Date)])
        st = perf_stats(nav)
        bs = block_bootstrap_cagr(nav, n_boot=4000)
        pb = paired_boot(nav, nav_c)
        occ = _occupancy(trades, nav)
        d2 = (st["sortino"] >= st_c["sortino"] and st["calmar"] >= st_c["calmar"]
              and st["mdd"] >= st_c["mdd"] and bs["ci_lo"] >= bs_c["ci_lo"])
        sig = pb["ci_lo"] > 0
        verdict = ("候選(D2過+CI>0)" if (d2 and sig) else
                   "D2過但CI跨0" if d2 else "劣化" if pb["ann_diff"] < 0 else "噪音")
        print(f"{r['name']:<16}{st['cagr']:>+8.1%}{st['sortino']:>9.2f}{st['calmar']:>8.2f}"
              f"{st['mdd']:>8.1%}{bs['ci_lo']:>+9.1%}{pb['ann_diff']:>+9.1%}"
              f"{pb['ci_lo']:>+9.1%}{pb['ci_hi']:>+9.1%}{pb['p_le0']:>8.3f}"
              f"{occ['avg_pos']:>7.2f}{occ['pct_days_empty']:>7.1%}{trades.height:>6}  {verdict}")
        rows.append((r, nav, trades, st, bs, pb, d2, sig))

    print("\n=== 候選(D2 全過)的分段與逐年 ===")
    cands = [x for x in rows if x[6]]
    if not cands:
        print("  無:沒有任何變體在 Sortino/Calmar/MDD/boot_lo 四項同時 ≥ canonical。")
    for r, nav, trades, st, bs, pb, _, sig in cands:
        sp = _subperiod(nav)
        spc = _subperiod(nav_c)
        print(f"\n  {r['name']}({r['note']})  最大回撤事件 {_mdd_episode(nav)}")
        for tag in ("H1", "H2"):
            if tag in sp:
                a, b = sp[tag], spc[tag]
                print(f"    {tag}: CAGR {a[0]:+.1%}(canon {b[0]:+.1%}) "
                      f"Sortino {a[1]:.2f}({b[1]:.2f}) MDD {a[2]:.1%}({b[2]:.1%}) "
                      f"Calmar {a[3]:.2f}({b[3]:.2f})")
        yv = yearly_table(nav).join(yearly_table(nav_c), on="year", suffix="_c")
        print("    year " + " ".join(f"{y:>7}" for y in yv["year"]))
        print("    var% " + " ".join(f"{x*100:>+7.1f}" for x in yv["ret"]))
        print("    can% " + " ".join(f"{x*100:>+7.1f}" for x in yv["ret_c"]))
        print(f"    trades {trade_stats(trades)}")


def diagnostics(panel: pl.DataFrame, feat: pl.DataFrame, elig: pl.DataFrame) -> None:
    """機制先驗:分數/絕對因子水位到底能不能預測實現報酬?不能就先驗證偽。"""
    nav, trades = run_s_full(panel, feat, elig, DS)
    print(f"canonical: {perf_stats(nav)}")

    # 候選(每日 top-5)重建
    pool = (feat.filter(pl.col("rev_fresh_days") <= 7)
            .join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
            .drop_nulls(subset=list(WREL))
            .filter(pl.all_horizontal([pl.col(c).is_finite() for c in WREL]))
            .filter(pl.col("cfo_ni_ratio_ttm")
                    >= pl.col("cfo_ni_ratio_ttm").quantile(0.5).over("date")))
    scored = pool.with_columns(score_expr())
    ranked = scored.with_columns(
        pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
    ent = ranked.filter(pl.col("rk") <= 5).filter(pl.col("date") >= pl.lit(DS).str.to_date())

    dts = panel["date"].unique().sort().to_list()
    nxt = pl.DataFrame({"date": dts[:-1], "fill_date": dts[1:]})
    ent = ent.join(nxt, on="date", how="left")

    n_days = ent["date"].n_unique()
    csz = scored.group_by("date").len()
    print(f"\n候選日數 {n_days}(全交易日 {len(dts)});每日截面池大小 "
          f"中位 {csz['len'].median():.0f} / P10 {csz['len'].quantile(0.1):.0f} / "
          f"P90 {csz['len'].quantile(0.9):.0f}")

    ent = ent.sort("date").with_columns(
        pl.Series("score_pct", expanding_pct(
            ent.sort("date")["date"].to_numpy(), ent.sort("date")["score"].to_numpy())))

    # 分數逐年水位(絕對門檻是否只是 regime 代理?)
    yr = (ent.with_columns(pl.col("date").dt.year().alias("y")).group_by("y")
          .agg([pl.col("score").mean().alias("mean"), pl.col("score").max().alias("max"),
                pl.len().alias("n")]).sort("y"))
    print("\n候選分數逐年水位(mean/max/n):")
    for y, m, mx, n in yr.iter_rows():
        print(f"  {y}  mean {m:.4f}  max {mx:.4f}  n {n}")

    # 實現報酬 vs 進場時的分數歷史分位 / 絕對因子
    fills = (trades.filter(pl.col("exit_reason") != "open")
             .join(ent.select([pl.col("fill_date").alias("entry_date"), C, "score",
                               "score_pct", *FACTOR_COLS]),
                   on=["entry_date", C], how="inner"))
    print(f"\n已平倉交易 {trades.filter(pl.col('exit_reason') != 'open').height},"
          f"對上候選 {fills.height}")

    # ann_eq = 把「該桶平均每筆報酬 / 該桶中位持有天數」換算成年化——這是**空手等待
    # 的真正對照組**:換成現金就是 0%。任何桶只要 ann_eq 遠高於 0,把它換成現金
    # 就必然毀損複利,絕對門檻在數學上就不可能贏。
    agg = [pl.col("ret_net").mean().alias("avg"), pl.col("ret_net").median().alias("med"),
           (pl.col("ret_net") > 0).mean().alias("win"),
           pl.col("days_held").median().alias("hold"), pl.len().alias("n")]

    def _ann(avg: float, hold: float) -> float:
        return (1.0 + avg) ** (252.0 / max(hold, 1.0)) - 1.0

    q = (fills.filter(pl.col("score_pct").is_not_null() & pl.col("score_pct").is_not_nan())
         .with_columns((pl.col("score_pct") * 5).floor().clip(0, 4).alias("bkt"))
         .group_by("bkt").agg(agg).sort("bkt"))
    print("\n【機制檢驗 1】進場分數的歷史分位 → 實現報酬(五等分);ann_eq 對照現金 0%:")
    for b, a, m, w, h, n in q.iter_rows():
        print(f"  Q{int(b)+1}  avg {a:+.2%}  med {m:+.2%}  win {w:.1%}  "
              f"持有 {h:.0f}d  ann_eq {_ann(a, h):+.0%}  n {n}")

    print("\n【分佈】候選(top-5)與已成交倉位的絕對因子水位分位數:")
    for col in ("rev_yoy_accel", "high_52w", "mom_126_5", "close_pos_20"):
        for tag, src in (("cand", ent), ("fill", fills)):
            s = src[col].drop_nulls()
            qs = [float(s.quantile(q)) for q in (0.05, 0.25, 0.5, 0.75, 0.95)]
            print(f"  {col:<15}{tag}  P5 {qs[0]:+.3f}  P25 {qs[1]:+.3f}  "
                  f"P50 {qs[2]:+.3f}  P75 {qs[3]:+.3f}  P95 {qs[4]:+.3f}")

    # 機制的**獨立**檢驗:不看 NAV 路徑,直接問「被門檻剔掉的那些成交,實現報酬
    # 是不是真的比較差」。若否,任何 NAV 上的優勢都只是「剛好少做到幾筆賠錢單」的
    # 路徑運氣,而非結構性 edge。
    for col, cuts in (("rev_yoy_accel", [-1e9, 5.0, 10.0, 15.0, 20.0, 30.0, 50.0, 1e9]),
                      ("mom_126_5", [-1e9, 0.0, 0.20, 0.50, 1.0, 1e9]),
                      ("high_52w", [-1e9, 0.90, 0.95, 0.99, 1e9])):
        g = (fills.drop_nulls(col)
             .with_columns(pl.col(col).cut(cuts[1:-1], left_closed=True).alias("b"))
             .group_by("b").agg(agg).sort("b"))
        print(f"\n【機制檢驗】{col} 分組 → 實現報酬(ann_eq 對照現金 0%):")
        for b, a, m, w, h, n in g.iter_rows():
            print(f"  {b!s:<18} avg {a:+.2%}  med {m:+.2%}  win {w:.1%}  "
                  f"持有 {h:.0f}d  ann_eq {_ann(a, h):+.0%}  n {n}")

    # 交易層 Welch t 檢定:被「加速 ≥+15pp」門檻剔掉的成交,平均報酬真的較差嗎?
    # (與 NAV 配對 bootstrap 互相獨立的第二條證據線;門檻本身是回測內挑的,故此
    #  t 值同樣是 in-sample,只能當量級參考、不能當顯著性宣稱。)
    for thr in (10.0, 15.0, 20.0):
        lo = fills.filter(pl.col("rev_yoy_accel") < thr)["ret_net"].to_numpy()
        hi = fills.filter(pl.col("rev_yoy_accel") >= thr)["ret_net"].to_numpy()
        se = np.sqrt(lo.var(ddof=1) / len(lo) + hi.var(ddof=1) / len(hi))
        print(f"\n【Welch t】加速 <{thr:.0f}pp(n={len(lo)}, avg {lo.mean():+.2%}, "
              f"sd {lo.std(ddof=1):.2%}) vs ≥{thr:.0f}pp(n={len(hi)}, avg {hi.mean():+.2%}, "
              f"sd {hi.std(ddof=1):.2%}) → 差 {hi.mean()-lo.mean():+.2%}, t = "
              f"{(hi.mean()-lo.mean())/se:.2f}")


if __name__ == "__main__":
    main()
