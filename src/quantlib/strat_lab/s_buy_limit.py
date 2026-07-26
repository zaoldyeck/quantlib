"""S 買單掛限價:全史 12 年、真實進場、引擎內模擬——「該掛什麼價」的正式答案。

## 使用者的問題(2026-07-26)
「與其研究哪個時點下單,我倒希望可以掛限價單,幫助系統決定能掛什麼價格。」

## 為什麼是日 K 全史而不是 1 分 K 兩個月
「掛在前收 −x% 會不會成交」的判定條件就是「當日**最低價** ≤ 限價」——日 K 的 low
即完整判定,不需要盤中資料。1 分 K 只多告訴我們「幾點成交」,而那不影響成交與否
與成交價上界。故本研究用**全部 12 年、689 筆真實 S 進場**,而非 1 分 K 的兩個月
35 筆(且那兩個月剛好是下跌期,樣本偏誤明顯)。

## 為什麼必須在引擎內模擬(這是關鍵)
掛低價沒成交,**不等於「那筆報酬 0」**——席位空著,隔天輪到下一個候選填,資金
繼續運轉。這個連鎖效應(未成交 → 席位釋出 → 候選遞補 → 整條 NAV 路徑改變)只有
引擎算得出來。用「成交率 × 成交者報酬」的簡化算法會系統性低估掛低價的表現。

## 掛價語義(engine.ExecSpec.buy_limit)
限價 = **決策日收盤** × (1 + x)。必須用決策日收盤而非隔日開盤:S 的計畫在盤前產生,
下單當下開盤價還不存在,拿開盤價當基準是前視偏差。
成交條件 low ≤ 限價;成交價 min(開盤, 限價);限價成交**不加滑價**(限價即價格上界)。

## 六段(把各效應逐一拆開)
1. 買單掛價掃描(沒撈到就不買)——含 `+9%` 必成交臂,分離「省滑價」的會計貢獻
2. vs 必成交臂的直接比較——實盤真正要選的是「掛多高」,不是「vs 市價假設」
3. 波動度自適應掛價(ATR 版)——檢驗「折價幅度沒調好」這個替代解釋
4. 賣單掛價(沒撈到就扛著)——風險不對稱,不能只靠推理封口
5. 買單 + **收盤價保底**(實盤 `BUY_PATIENT` 的真實語義)——**結論在此翻轉**
6. 賣單 + 收盤價保底(實盤 `SELL_EXIT` 的真實語義)

⚠ 第 5 段的「一律收盤買」必須用 `buy_limit=-0.99 + fallback_close` 做,**不能**用
`fill_at="next_close"`——後者會把賣單也搬到收盤,混進第二個變因(初版即踩此坑,
測出 −11.3% 的假結論;隔離後真值是 +0.01%)。

結論見 `docs/strategy_research/limit_order_verdict.md`。

Run: uv run --project . python -m quantlib.strat_lab.s_buy_limit
依賴 cache: 是(prep_cached)。長任務,建議背景跑。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExecSpec
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import C as Cc
from quantlib.apex.strategy_s import DS, prep_cached, run_s_full

OFFSETS = (0.09, 0.03, 0.02, 0.01, 0.005, 0.0, -0.005, -0.01, -0.015, -0.02, -0.03, -0.05)


def _kpi(nav: pl.DataFrame) -> dict:
    s = perf_stats(nav)
    return {k: float(s[k]) for k in ("cagr", "sortino", "mdd", "calmar")}


def _paired(nav_a: pl.DataFrame, nav_b: pl.DataFrame, n_boot=4000, block=21, seed=42) -> dict:
    """配對 block bootstrap:兩條高度相關 NAV 的日報酬差,年化後取 95% CI。"""
    j = (nav_a.select(["date", pl.col("nav").alias("na")])
         .join(nav_b.select(["date", pl.col("nav").alias("nb")]), on="date", how="inner").sort("date")
         .with_columns((pl.col("na") / pl.col("na").shift(1)
                        - pl.col("nb") / pl.col("nb").shift(1)).alias("d")).drop_nulls())
    d = j["d"].to_numpy()
    T = len(d)
    rng = np.random.default_rng(seed)
    st = np.array([np.concatenate([d[i:i + block]
                                   for i in rng.integers(0, T - block, T // block + 1)])[:T].mean() * 252
                   for _ in range(n_boot)])
    lo, hi = np.percentile(st, [2.5, 97.5])
    return {"ann": float(d.mean() * 252), "lo": float(lo), "hi": float(hi),
            "p_le0": float(np.mean(st <= 0))}


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)

    base_nav, base_tr = run_s_full(panel, feat, elig, DS)
    b = _kpi(base_nav)
    print(f"canonical(市價 next_open + 滑價 0.1%):CAGR {b['cagr']:+.2%}  "
          f"Sortino {b['sortino']:.2f}  MDD {b['mdd']:.2%}  Calmar {b['calmar']:.2f}  "
          f"交易 {base_tr.height}\n", flush=True)

    print("=== 買單掛限價 = 決策日收盤 ×(1+x);全史 2014-10~ 引擎內模擬 ===")
    print(f"  {'掛價 x':>9}{'交易數':>7}{'CAGR':>10}{'Sortino':>9}{'MDD':>9}{'Calmar':>8}"
          f"{'vs canonical 年化差 (95% CI)':>32}")
    rows, navs = [], {}
    for x in OFFSETS:
        nav, tr = run_s_full(panel, feat, elig, DS, _exec_spec=ExecSpec(buy_limit=x))
        k = _kpi(nav)
        p = _paired(nav, base_nav)
        tag = f"{x:+.1%}" + ("*" if x >= 0.09 else "")
        print(f"  {tag:>9}{tr.height:>7}{k['cagr']:>+9.2%}{k['sortino']:>9.2f}"
              f"{k['mdd']:>9.2%}{k['calmar']:>8.2f}"
              f"   {p['ann']:>+7.2%} [{p['lo']:+.2%}, {p['hi']:+.2%}]"
              f" {'✓' if p['lo'] > 0 else ' '}", flush=True)
        rows.append({"x": x, "trades": tr.height, **k, **{f"p_{a}": v for a, v in p.items()}})
        navs[x] = nav

    print(f"\n  * +9% = 限價高到幾乎必成交 → 等同「市價但不加 0.1% 滑價」的對照,"
          f"用來分離『省滑價』與『省價差』兩種效應。")
    print(f"  ✓ = 配對 block bootstrap 95% CI 下界 > 0(勝過 canonical 且統計顯著)")

    # ── 第二段:真正的決策比較——各掛價 vs「必成交」臂 ────────────────────
    # 對 canonical 比會混入滑價假設差異;實盤真正要選的是「掛多高的限價」,
    # 故基準換成 +9%(必成交、零滑價)那條。
    print("\n=== vs 必成交臂(+9%);這才是實盤『要掛多高』的決策比較 ===")
    print(f"  {'掛價 x':>9}{'成交/689':>10}{'CAGR':>10}"
          f"{'年化差 vs 必成交 (95% CI)':>30}")
    for r in rows:
        if r["x"] >= 0.09:
            continue
        p = _paired(navs[r["x"]], navs[0.09])
        print(f"  {r['x']:>+8.1%}{r['trades']:>10}{r['cagr']:>+9.2%}"
              f"   {p['ann']:>+7.2%} [{p['lo']:+.2%}, {p['hi']:+.2%}]"
              f" {'✗ 顯著較差' if p['hi'] < 0 else ''}")
        r["vs_fill_ann"], r["vs_fill_lo"], r["vs_fill_hi"] = p["ann"], p["lo"], p["hi"]

    # ── 第三段:波動度自適應掛價(高波動股容忍較深折價)────────────────────
    # 假設:固定 −1% 對日振幅 2% 的股票是深折價、對振幅 8% 的股票只是雜訊。
    # 掛價 = 前收 × (1 − k × ATR20/前收)。ATR 一律 shift(1)(掛單在盤前,只能用昨日以前)。
    print("\n=== 波動度自適應掛價:限價 = 前收 ×(1 − k × ATR20/前收)===")
    pv = (panel.sort([Cc, "date"])
          .with_columns(
              pl.max_horizontal(
                  pl.col("high") - pl.col("low"),
                  (pl.col("high") - pl.col("close").shift(1).over(Cc)).abs(),
                  (pl.col("low") - pl.col("close").shift(1).over(Cc)).abs(),
              ).alias("_tr"))
          .with_columns((pl.col("_tr").rolling_mean(20).over(Cc)
                         / pl.col("close")).shift(1).over(Cc).alias("_atrp")))
    print(f"  {'k':>9}{'成交/689':>10}{'CAGR':>10}{'Sortino':>9}"
          f"{'年化差 vs 必成交 (95% CI)':>30}")
    for k in (0.25, 0.5, 1.0, 1.5):
        # ATR 尚未暖機(前 20 根)時退回「必成交」掛價 +9%——缺資料不該靜默變成
        # 最嚴格的掛價(那會把暖機期的單全部漏掉,看起來像規則變差)。
        pk = pv.with_columns(
            pl.when(pl.col("_atrp").is_null()).then(pl.lit(0.09))
            .otherwise(-(k * pl.col("_atrp")).clip(0.0, 0.09)).alias("_off"))
        nav, tr = run_s_full(pk, feat, elig, DS, _exec_spec=ExecSpec(buy_limit_col="_off"))
        kk = _kpi(nav)
        p = _paired(nav, navs[0.09])
        print(f"  {k:>9.2f}{tr.height:>10}{kk['cagr']:>+9.2%}{kk['sortino']:>9.2f}"
              f"   {p['ann']:>+7.2%} [{p['lo']:+.2%}, {p['hi']:+.2%}]"
              f" {'✗ 顯著較差' if p['hi'] < 0 else ''}", flush=True)
        rows.append({"x": f"atr_k{k}", "trades": tr.height, **kk,
                     "vs_fill_ann": p["ann"], "vs_fill_lo": p["lo"], "vs_fill_hi": p["hi"]})

    # ── 第四段:賣單也掛限價?——風險不對稱,不能只靠推理封口 ────────────────
    # 買單沒成交只是少買一檔;賣單沒成交是「該出場的部位繼續扛」。引擎在未成交時
    # 把出場理由掛回 pending_exit 隔日重掛(沿用跌停鎖死的重試路徑),完整重現這個風險。
    print("\n=== 賣單掛限價 = 決策日收盤 ×(1+y);未成交則隔日重掛(部位續抱)===")
    print(f"  {'掛價 y':>9}{'交易數':>7}{'CAGR':>10}{'Sortino':>9}{'MDD':>9}"
          f"{'年化差 vs canonical (95% CI)':>32}")
    for y in (-0.09, -0.02, 0.0, 0.01, 0.02, 0.03):
        nav, tr = run_s_full(panel, feat, elig, DS, _exec_spec=ExecSpec(sell_limit=y))
        kk = _kpi(nav)
        p = _paired(nav, base_nav)
        tag = f"{y:+.1%}" + ("*" if y <= -0.09 else "")
        print(f"  {tag:>9}{tr.height:>7}{kk['cagr']:>+9.2%}{kk['sortino']:>9.2f}"
              f"{kk['mdd']:>9.2%}   {p['ann']:>+7.2%} [{p['lo']:+.2%}, {p['hi']:+.2%}]"
              f" {'✓' if p['lo'] > 0 else ('✗ 顯著較差' if p['hi'] < 0 else '')}", flush=True)
        rows.append({"x": f"sell{y:+.3f}", "trades": tr.height, **kk,
                     "vs_fill_ann": p["ann"], "vs_fill_lo": p["lo"], "vs_fill_hi": p["hi"]})
    print("  * −9% = 掛低到必成交(等同市價賣但不付滑價)")

    # ── 第五段:實盤 BUY_PATIENT 的真實語義 = 撈低 +「收盤價保底」──────────
    # live/execute.py 派工未帶 --patience → 吃 CLI 預設 "price" → BUY_PATIENT
    # (passive_rounds=10**6、盤中永不跨價、收盤未竟由盤後定價 14:30 撮合 = 收盤價收尾)。
    # 那不是「沒撈到就不買」,而是「沒撈到就用收盤價買」——必須單獨量,不能拿第一段代替。
    print("\n=== 實盤 BUY_PATIENT 形態:掛低撈價,未觸價則以當日收盤價成交 ===")
    print(f"  {'掛價 x':>9}{'交易數':>7}{'CAGR':>10}{'Sortino':>9}{'MDD':>9}"
          f"{'年化差 vs 必成交 (95% CI)':>30}")
    # 「一律收盤買」的乾淨隔離:限價低到永遠碰不到 → 每筆都走收盤保底,而**賣方不動**
    #(直接用 fill_at="next_close" 會把賣單也移到收盤,那是另一個變因,不可混談)
    nav_c, tr_c = run_s_full(panel, feat, elig, DS,
                             _exec_spec=ExecSpec(buy_limit=-0.99,
                                                 buy_limit_fallback_close=True))
    kc, pc_ = _kpi(nav_c), _paired(nav_c, navs[0.09])
    print(f"  {'一律收盤買':>9}{tr_c.height:>7}{kc['cagr']:>+9.2%}{kc['sortino']:>9.2f}"
          f"{kc['mdd']:>9.2%}   {pc_['ann']:>+7.2%} [{pc_['lo']:+.2%}, {pc_['hi']:+.2%}]"
          f" {'✗ 顯著較差' if pc_['hi'] < 0 else ''}", flush=True)
    rows.append({"x": "buy_all_close", "trades": tr_c.height, **kc,
                 "vs_fill_ann": pc_["ann"], "vs_fill_lo": pc_["lo"], "vs_fill_hi": pc_["hi"]})
    for x in (0.0, -0.01, -0.02, -0.03, -0.05):
        nav, tr = run_s_full(panel, feat, elig, DS,
                             _exec_spec=ExecSpec(buy_limit=x, buy_limit_fallback_close=True))
        kk = _kpi(nav)
        p = _paired(nav, navs[0.09])
        print(f"  {x:>+8.1%}{tr.height:>7}{kk['cagr']:>+9.2%}{kk['sortino']:>9.2f}"
              f"{kk['mdd']:>9.2%}   {p['ann']:>+7.2%} [{p['lo']:+.2%}, {p['hi']:+.2%}]"
              f" {'✗ 顯著較差' if p['hi'] < 0 else ''}", flush=True)
        rows.append({"x": f"patient{x:+.3f}", "trades": tr.height, **kk,
                     "vs_fill_ann": p["ann"], "vs_fill_lo": p["lo"], "vs_fill_hi": p["hi"]})

    # ── 第六段:賣方的對稱形態(實盤 SELL_EXIT = 撈高 + 收盤保底)─────────────
    # 第四段的賣單限價「沒撈到就扛著」不是實盤形態;實盤有盤後定價收尾,當天一定出場。
    # 兩者的差別正是第五段在買方揭露的關鍵:致命的從來不是掛價,是「沒成交」。
    print("\n=== 實盤 SELL_EXIT 形態:掛高撈價,未觸價則以當日收盤價成交 ===")
    print(f"  {'掛價 y':>9}{'交易數':>7}{'CAGR':>10}{'Sortino':>9}{'MDD':>9}"
          f"{'年化差 vs 必成交 (95% CI)':>30}")
    nav_sc, tr_sc = run_s_full(panel, feat, elig, DS,
                               _exec_spec=ExecSpec(sell_limit=9.0,
                                                   sell_limit_fallback_close=True))
    ks, ps_ = _kpi(nav_sc), _paired(nav_sc, navs[0.09])
    print(f"  {'一律收盤賣':>9}{tr_sc.height:>7}{ks['cagr']:>+9.2%}{ks['sortino']:>9.2f}"
          f"{ks['mdd']:>9.2%}   {ps_['ann']:>+7.2%} [{ps_['lo']:+.2%}, {ps_['hi']:+.2%}]"
          f" {'✗ 顯著較差' if ps_['hi'] < 0 else ''}", flush=True)
    rows.append({"x": "sell_all_close", "trades": tr_sc.height, **ks,
                 "vs_fill_ann": ps_["ann"], "vs_fill_lo": ps_["lo"], "vs_fill_hi": ps_["hi"]})
    for y in (0.0, 0.01, 0.02, 0.03, 0.05):
        nav, tr = run_s_full(panel, feat, elig, DS,
                             _exec_spec=ExecSpec(sell_limit=y,
                                                 sell_limit_fallback_close=True))
        kk = _kpi(nav)
        p = _paired(nav, navs[0.09])
        print(f"  {y:>+8.1%}{tr.height:>7}{kk['cagr']:>+9.2%}{kk['sortino']:>9.2f}"
              f"{kk['mdd']:>9.2%}   {p['ann']:>+7.2%} [{p['lo']:+.2%}, {p['hi']:+.2%}]"
              f" {'✗ 顯著較差' if p['hi'] < 0 else ''}", flush=True)
        rows.append({"x": f"sellpat{y:+.3f}", "trades": tr.height, **kk,
                     "vs_fill_ann": p["ann"], "vs_fill_lo": p["lo"], "vs_fill_hi": p["hi"]})

    from quantlib import paths
    fp = paths.OUT_STRAT_LAB / "s_buy_limit.csv"
    fp.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, strict=False).write_csv(fp)
    print(f"\n  明細 → {fp}")


if __name__ == "__main__":
    main()
