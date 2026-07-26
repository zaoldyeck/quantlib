"""S 計分「聚合形態」實驗:同一組六因子資訊,換不同的組合方式。

**問的問題**:S 現在把六個因子的百分位 rank 做「幾何加權乘積」(Π rank_i^w_i)。
資訊完全相同的前提下,換成算術平均 / z-score / 木桶短板 / 門檻篩後等權,選出來的
股票會不同嗎?會更好嗎?——這是**聚合形態**的實驗,不是換因子、不是調權重
(權重 ±20% 與剪因子先前已證偽)。

**理論骨架(不做無腦網格)**:(a)(c)(e) 三類其實是同一個家族——**廣義冪平均**
  M_p(r) = ( Σ w_i·r_i^p / Σ w_i )^(1/p)
  p → 0   幾何平均 = canonical(與 Π r^w 逐位同序,乘冪 1/Σw 是單調變換)
  p = 1   算術加權平均(任務的 (a))
  p = 2,4 凸聚合,強調「有沒有一項特別突出」(短板可被長板補償)
  p = −1  調和平均;p → −∞  木桶短板 min-rank(任務的 (e))
  p 就是**互補性參數**:p 越小越要求「六項都不差」,p 越大越允許「一項超強補其他」。
  掃 p 是掃一條有學理意義的軸(高原驗證),不是撿尖峰。
其餘三類各自獨立:
  (b) z-score:用因子**原始值**標準化(rank 丟掉了「贏多少」,z 保留量級)+ winsor ±kσ
  (d) rank 正規化寫法:average-rank/n(canonical) vs ordinal-rank/n vs (rank−0.5)/n
      vs 常態分位變換(van der Waerden,把均勻 rank 拉成常態尾距)
  (f) 排除法:先要求各因子 rank > q 才入選,再用 canonical 排序(門檻 vs 連續補償)

**方法論**:每個變體與 canonical 做**配對 block-bootstrap**(d_t = 日報酬差,block=21,
n_boot=4000)。CI 跨 0 = 噪音級即判證偽;另要求 Sortino/Calmar/MDD/bootstrap 下界
同時 ≥ canonical 才算候選;候選再看前後半段與逐年。
(d) 類的「等價改寫」(ordinal / mid-rank)同時充當**噪音地板**:它們與 canonical 的
差距,就是「純寫法差異」能製造多少 CAGR 波動——別的變體要比這個大才值得談。

Run: uv run --project . python -m quantlib.strat_lab.x_score_aggregation
依賴 cache:是(prep_cached 讀 industry_taxonomy_pit / daily_quote)。
"""
from __future__ import annotations

import polars as pl
from scipy.special import ndtri

from quantlib.apex import data
from quantlib.apex.metrics import perf_stats, yearly_table
from quantlib.apex.strategy_s import C, DS, WREL, prep_cached, run_s_full
from quantlib.apex.validate import block_bootstrap_cagr
from quantlib.strat_lab.s_accelrel_gate import paired_boot   # 配對 bootstrap 唯一真源

SUM_W = sum(WREL.values())
MID = "2020-07-01"          # 前後半段切點(跨度 2014-10~2026-07 的近中點)


# ── 聚合形態:每個 _score_fn 收「過濾後含全因子欄的 df」、回含 'score' 欄 ──────

def _pct(method: str = "average", denom: str = "n") -> dict[str, pl.Expr]:
    """六因子的截面百分位 rank 表達式。denom: n = rank/n(canonical)、
    n1 = rank/(n+1)(常態變換用,避免 Φ⁻¹(1)=∞)、mid = (rank−0.5)/n。"""
    out = {}
    for c in WREL:
        r = pl.col(c).rank(method=method)
        if denom == "n":
            e = r / pl.len()
        elif denom == "n1":
            e = r / (pl.len() + 1)
        elif denom == "mid":
            e = (r - 0.5) / pl.len()
        else:
            raise ValueError(denom)
        out[c] = e.over("date")
    return out


def geo(method: str = "average", denom: str = "n"):
    """幾何加權乘積(canonical 形態;method/denom 可換 rank 正規化寫法)。"""
    def f(df: pl.DataFrame) -> pl.DataFrame:
        p = _pct(method, denom)
        expr = None
        for c, w in WREL.items():
            t = p[c] ** w
            expr = t if expr is None else expr * t
        return df.with_columns(expr.alias("score"))
    return f


def pmean(p: float):
    """廣義冪平均 M_p(p=0 走幾何,與 canonical 同序)。p<0 時 M_p 仍對每個 rank
    單調遞增(r↑ → r^p↓ → 和↓ → 取 1/p<0 次方 ↑),不需翻號。"""
    if p == 0:
        return geo()

    def f(df: pl.DataFrame) -> pl.DataFrame:
        pc = _pct()
        s = None
        for c, w in WREL.items():
            t = w * (pc[c] ** p)
            s = t if s is None else s + t
        return df.with_columns(((s / SUM_W) ** (1.0 / p)).alias("score"))
    return f


def minrank(df: pl.DataFrame) -> pl.DataFrame:
    """木桶短板:取六因子中最差的 rank(冪平均 p → −∞ 的極限;權重失效)。"""
    pc = _pct()
    return df.with_columns(pl.min_horizontal(list(pc.values())).alias("score"))


def zscore(winsor: float | None):
    """因子**原始值**截面 z-score(可 winsor 到 ±kσ)後加權算術平均。
    rank 只保留次序、z 保留『贏多少』——對右尾極端值的敏感度不同。"""
    def f(df: pl.DataFrame) -> pl.DataFrame:
        s = None
        for c, w in WREL.items():
            z = ((pl.col(c) - pl.col(c).mean()) / pl.col(c).std()).over("date")
            if winsor is not None:
                z = z.clip(-winsor, winsor)
            t = w * z
            s = t if s is None else s + t
        return df.with_columns((s / SUM_W).alias("score"))
    return f


def normal_score(df: pl.DataFrame) -> pl.DataFrame:
    """van der Waerden 常態分位變換:Φ⁻¹(rank/(n+1)) 後加權算術平均
    (把均勻 rank 拉回常態尾距——尾端間距被拉開,中段被壓平)。"""
    pc = _pct(denom="n1")
    tmp = df.with_columns([e.alias(f"_p_{c}") for c, e in pc.items()])
    tmp = tmp.with_columns([
        pl.col(f"_p_{c}").map_batches(lambda s: pl.Series(ndtri(s.to_numpy())),
                                      return_dtype=pl.Float64).alias(f"_q_{c}")
        for c in WREL])
    s = None
    for c, w in WREL.items():
        t = w * pl.col(f"_q_{c}")
        s = t if s is None else s + t
    return tmp.with_columns((s / SUM_W).alias("score"))


def gate(q: float, need: int = 6):
    """排除法:要求至少 need 個因子 rank > q 才有資格,合格者再用 canonical 排序。
    (門檻式『不能有短板』vs 幾何式『短板可被長板部分補償』的對照)"""
    def f(df: pl.DataFrame) -> pl.DataFrame:
        pc = _pct()
        cnt = None
        for c in WREL:
            t = (pc[c] > q).cast(pl.Int32)
            cnt = t if cnt is None else cnt + t
        out = geo()(df.with_columns(cnt.alias("_cnt")))
        return out.filter(pl.col("_cnt") >= need)
    return f


def gate_soft(q: float):
    """軟門檻:主排序 = 通過 q 的因子個數,同數者以 canonical 分數決勝
    (完全不篩掉任何人,只把『幾項達標』提到第一順位)。"""
    def f(df: pl.DataFrame) -> pl.DataFrame:
        pc = _pct()
        cnt = None
        for c in WREL:
            t = (pc[c] > q).cast(pl.Int32)
            cnt = t if cnt is None else cnt + t
        out = geo()(df.with_columns(cnt.alias("_cnt")))
        tie = (pl.col("score").rank() / pl.len()).over("date")
        return out.with_columns((pl.col("_cnt").cast(pl.Float64) + tie).alias("score"))
    return f


# ── 變體清單 ────────────────────────────────────────────────────────────────
VARIANTS: list[tuple[str, object]] = [
    # (a)(c)(e) 冪平均家族:p = 互補性參數(小 = 要求六項都不差)
    ("pmean p=-8", pmean(-8.0)),
    ("pmean p=-4", pmean(-4.0)),
    ("pmean p=-2", pmean(-2.0)),
    ("pmean p=-1 (調和)", pmean(-1.0)),
    ("pmean p=-0.5", pmean(-0.5)),
    ("pmean p=+0.5", pmean(0.5)),
    ("pmean p=+1 (算術)", pmean(1.0)),
    ("pmean p=+2", pmean(2.0)),
    ("pmean p=+4", pmean(4.0)),
    ("min-rank (p→-∞)", minrank),
    # (b) z-score(原始值標準化)
    ("z 原始值 winsor3", zscore(3.0)),
    ("z 原始值 winsor2", zscore(2.0)),
    ("z 原始值 無 winsor", zscore(None)),
    # (d) rank 正規化寫法(等價改寫 → 同時是噪音地板)
    ("幾何 ordinal rank", geo(method="ordinal")),
    ("幾何 (rank-.5)/n", geo(denom="mid")),
    ("常態分位 vdW", normal_score),
    # (f) 排除法 / 門檻
    ("門檻 全>0.4", gate(0.4)),
    ("門檻 全>0.5", gate(0.5)),
    ("門檻 5/6>0.5", gate(0.5, need=5)),
    ("軟門檻 cnt>0.5", gate_soft(0.5)),
]


def order_stat(k: int):
    """次序統計量聚合:取六個 rank 由小到大的第 k 個(k=1 木桶短板、k=2 修剪短板、
    k=3/4 中位、k=6 最強項)。這是『互補性』的另一種參數化——冪平均是連續的,
    次序統計量是離散的,兩條路徑若指向同一結論,結論才穩。"""
    def f(df: pl.DataFrame) -> pl.DataFrame:
        pc = _pct()
        tmp = df.with_columns([e.alias(f"_r_{c}") for c, e in pc.items()])
        cols = [f"_r_{c}" for c in WREL]
        return tmp.with_columns(
            pl.concat_list(cols).list.sort().list.get(k - 1).alias("score"))
    return f


def hierarchical(w_rev: float = 0.5):
    """階層聚合(拓樸不同,非權重微調):先把六因子分成『營收軸』(rev_yoy_accel/
    rev_seq/accel_rel)與『價格軸』(high_52w/close_pos_20/mom_126_5)各自幾何平均,
    再把兩軸幾何加權。等於強制兩類資訊各占一半話語權,而不是六個因子平起平坐。"""
    rev_ax = ("rev_yoy_accel", "rev_seq", "accel_rel")
    px_ax = ("high_52w", "close_pos_20", "mom_126_5")

    def f(df: pl.DataFrame) -> pl.DataFrame:
        pc = _pct()
        blocks = {}
        for nm, ax in (("rev", rev_ax), ("px", px_ax)):
            e = None
            for c in ax:
                t = pc[c] ** (1.0 / len(ax))
                e = t if e is None else e * t
            blocks[nm] = e
        return df.with_columns(
            (blocks["rev"] ** w_rev * blocks["px"] ** (1 - w_rev)).alias("score"))
    return f


STAGE2: list[tuple[str, object]] = [
    # 冪平均在 0 附近的細網(確認峰在 canonical 而非鄰近)
    ("pmean p=-0.25", pmean(-0.25)),
    ("pmean p=+0.25", pmean(0.25)),
    # 次序統計量家族(互補性的離散參數化)
    ("次序 k=2 (修剪短板)", order_stat(2)),
    ("次序 k=3 (中位偏低)", order_stat(3)),
    ("次序 k=4 (中位偏高)", order_stat(4)),
    ("次序 k=6 (取最強項)", order_stat(6)),
    # 階層拓樸(營收軸 vs 價格軸 各半)
    ("階層 營收/價格 50:50", hierarchical(0.5)),
    ("階層 營收 60%", hierarchical(0.6)),
]


def _kpi(nav: pl.DataFrame) -> dict:
    st = perf_stats(nav.sort("date"))
    st["boot_lo"] = block_bootstrap_cagr(nav, n_boot=2000)["ci_lo"]
    return st


def _sub(nav: pl.DataFrame, lo: str | None, hi: str | None) -> dict:
    q = nav.sort("date")
    if lo:
        q = q.filter(pl.col("date") >= pl.lit(lo).str.to_date())
    if hi:
        q = q.filter(pl.col("date") < pl.lit(hi).str.to_date())
    q = q.with_columns(pl.col("nav") / pl.col("nav").first())
    return perf_stats(q)


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)

    nav_c, tr_c = run_s_full(panel, feat, elig, DS)
    k_c = _kpi(nav_c)
    print("=== S 計分聚合形態實驗(全跨度 2014-10~2026-07,含成本)===")
    print(f"  canonical(幾何加權乘積): CAGR {k_c['cagr']:+.1%}  Sortino {k_c['sortino']:.2f}  "
          f"Calmar {k_c['calmar']:.2f}  MDD {k_c['mdd']:+.1%}  boot下界 {k_c['boot_lo']:+.1%}  "
          f"trades {len(tr_c)}")

    # selfcheck:冪平均 p→0 分支必須逐位重現 canonical(rank 管線抄錯即 fail-loud)
    nav0, _ = run_s_full(panel, feat, elig, DS, _score_fn=geo())
    d0 = (nav_c.join(nav0, on="date", suffix="_b")
          .select((pl.col("nav") - pl.col("nav_b")).abs().max()).item())
    assert d0 is not None and d0 < 1e-12, f"geo() 未重現 canonical(max diff {d0})——計分管線抄錯"
    print("  [selfcheck] geo() 逐位重現 canonical NAV ✓\n")

    print(f"  {'變體':<22}{'CAGR':>8}{'Sortino':>9}{'Calmar':>8}{'MDD':>8}{'boot下界':>10}"
          f"{'交易':>6}{'年化差':>9}{'95% CI':>19}{'P(≤0)':>8}")
    print(f"  {'canonical':<22}{k_c['cagr']:>+7.1%}{k_c['sortino']:>9.2f}{k_c['calmar']:>8.2f}"
          f"{k_c['mdd']:>+7.1%}{k_c['boot_lo']:>+9.1%}{len(tr_c):>6}")

    rows = []
    for stage, vs in (("", VARIANTS), ("stage2", STAGE2)):
        if stage:
            print("  ── 第二輪:次序統計量家族 / 階層拓樸 / p 細網 ──")
        for name, fn in vs:
            nav, tr = run_s_full(panel, feat, elig, DS, _score_fn=fn)
            k = _kpi(nav)
            pb = paired_boot(nav, nav_c)
            ci = f"[{pb['ci_lo']:+.1%},{pb['ci_hi']:+.1%}]"
            print(f"  {name:<22}{k['cagr']:>+7.1%}{k['sortino']:>9.2f}{k['calmar']:>8.2f}"
                  f"{k['mdd']:>+7.1%}{k['boot_lo']:>+9.1%}{len(tr):>6}"
                  f"{pb['ann_diff']:>+8.1%}{ci:>19}{pb['p_le0']:>8.3f}", flush=True)
            rows.append((name, k, pb, nav))

    # 判準:四項 KPI 同時 ≥ canonical 且配對 CI 下界 > 0
    print("\n=== 候選判定(Sortino/Calmar/MDD/boot下界 皆 ≥ canonical 且配對 CI 不跨 0)===")
    cands = [r for r in rows
             if r[1]["sortino"] >= k_c["sortino"] and r[1]["calmar"] >= k_c["calmar"]
             and r[1]["mdd"] >= k_c["mdd"] and r[1]["boot_lo"] >= k_c["boot_lo"]
             and r[2]["ci_lo"] > 0]
    if not cands:
        near = [r for r in rows if r[2]["ci_lo"] > 0]
        print("  無變體同時通過四項 KPI + 配對顯著 → 聚合形態維度證偽,canonical 不動。")
        if near:
            print("  (配對 CI 不跨 0 但 KPI 未全過:" + ", ".join(r[0] for r in near) + ")")
    else:
        for name, k, pb, nav in cands:
            print(f"\n  ★ 候選 {name}: CAGR {k['cagr']:+.1%} / Sortino {k['sortino']:.2f} / "
                  f"Calmar {k['calmar']:.2f} / 年化差 {pb['ann_diff']:+.1%} "
                  f"CI [{pb['ci_lo']:+.1%},{pb['ci_hi']:+.1%}]")
            for lab, lo, hi in (("前半 2014-10~2020-06", None, MID),
                                ("後半 2020-07~2026-07", MID, None)):
                sv, sc = _sub(nav, lo, hi), _sub(nav_c, lo, hi)
                print(f"    {lab}: CAGR {sv['cagr']:+.1%}(canonical {sc['cagr']:+.1%})  "
                      f"MDD {sv['mdd']:+.1%}(canonical {sc['mdd']:+.1%})")
            yv = yearly_table(nav).rename({"ret": "ret_v"}).select(["year", "ret_v"])
            yc = yearly_table(nav_c).rename({"ret": "ret_c"}).select(["year", "ret_c"])
            y = yv.join(yc, on="year").with_columns((pl.col("ret_v") - pl.col("ret_c")).alias("d"))
            print("    逐年:" + "  ".join(
                f"{r['year']}:{r['ret_v']:+.0%}({r['d']:+.0%})" for r in y.iter_rows(named=True)))
            print(f"    正差年數 {int((y['d'] > 0).sum())}/{len(y)}"
                  "(只靠一兩年 = regime 依賴,不算穩)")

    # 前後半段的形態脊線:canonical 的優勢若只在某一半成立,就是全樣本假象
    print("\n=== 形態脊線的前後半段一致性(全樣本是 in-sample 篩選,分段看是否同向)===")
    ridge = ["canonical", "pmean p=-4", "pmean p=-1 (調和)", "pmean p=+1 (算術)",
             "pmean p=+4", "min-rank (p→-∞)", "次序 k=6 (取最強項)",
             "z 原始值 winsor3", "階層 營收/價格 50:50"]
    byname = {"canonical": nav_c} | {r[0]: r[3] for r in rows}
    print(f"  {'形態':<22}{'全跨度':>9}{'前半':>9}{'後半':>9}")
    for nm in ridge:
        if nm not in byname:
            continue
        n = byname[nm]
        print(f"  {nm:<22}{perf_stats(n.sort('date'))['cagr']:>+8.1%}"
              f"{_sub(n, None, MID)['cagr']:>+8.1%}{_sub(n, MID, None)['cagr']:>+8.1%}")

    # 噪音地板:等價改寫(ordinal / mid-rank)造成的 CAGR 波動
    floor = [abs(r[1]["cagr"] - k_c["cagr"]) for r in rows
             if r[0] in ("幾何 ordinal rank", "幾何 (rank-.5)/n")]
    if floor:
        print(f"\n  噪音地板(rank 等價改寫造成的 |ΔCAGR|):{max(floor):.1%}"
              "——低於此值的差異不必解讀。")


if __name__ == "__main__":
    main()
