"""x_pool_depth — S 策略「候選池深度 × 遞補節奏」維度實驗(乾淨資料 campaign)。

用途
----
S(apex_revcycle_S)canonical 規格為 `_top_k=5` = `n_slots=5`、`max_new_per_day=2`:
每日只看前 5 名、最多開 2 檔新倉。機制假設是——名單與席次同深時,只要當日前幾名
被漲停擋單 / 已持有 / 停牌 / 現金不足,該席次當天就空轉;把名單加深(top_k > slots)
可讓次順位遞補,提高資金利用率;放寬(或收緊)`max_new_per_day` 則改變建倉節奏。

本檔測三件事(維度自身,不重測已證偽的 slots):
  (a) 深度:_top_k ∈ {5, 8, 10, 15, 20} 配 n_slots=5
  (b) 節奏:max_new_per_day ∈ {1, 2, 3, None}
  (c) 兩者交互(5×4 全格,用來看「高原」而非挑尖峰)

方法論(硬性)
--------------
- 判準 D2:候選必須 Sortino / Calmar / MDD / bootstrap 下界 **同時 ≥ canonical**。
- 任何「最好的變體」一律做配對 moving-block bootstrap(block=21、n_boot=4000),
  對同一組重抽索引同時算 variant 與 canonical 的 CAGR,取差 → 年化差 + 95% CI +
  P(差≤0)。**CI 跨 0 判噪音級 = 證偽**。
- 另報前後半段(2014-10~2020-06 / 2020-07~2026-07)與逐年報酬,檢查 regime 一致性。
- 機制診斷:每格報「進場成交筆數」與「席次占用率」——深度若真的解除塞車,成交筆數
  必須上升;若不升,機制假設本身即被否定。

第二階段(grid 結果逼出來的):深度完全無效,因為「名單深」不是綁定約束——S 的池閘
`rev_fresh_days <= 7` 讓候選只在月營收公布後的窗口存在,大多數交易日候選數為 0,
席次空著也沒人可補。真正對應的機制是**遞補節奏的時間維度**:讓當日未成交的候選在
其後 k 個交易日仍可遞補(carry-forward),這才是「名單用完了」的解法。因此加測
carry ∈ {1,3,5,10} 交易日(fresh cohort 恆優先於 stale cohort,以 score − age 排序,
無門檻魔術數字)。

Run
---
    uv run --project . python -m quantlib.strat_lab.x_pool_depth diag   # 機制診斷(秒級)
    uv run --project . python -m quantlib.strat_lab.x_pool_depth grid   # (a)(b)(c) 全格
    uv run --project . python -m quantlib.strat_lab.x_pool_depth carry  # 候選延壽遞補

依賴 cache: 是(prep_cached 讀 cache.duckdb;引擎走 apex.engine.simulate)。
輸出: var/out/strat_lab/x_pool_depth_grid.csv、x_pool_depth_carry.csv
"""
from __future__ import annotations

import math
import sys
import time

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.engine import PortSpec
from quantlib.apex.metrics import perf_stats, yearly_table
from quantlib.apex.strategy_s import DS, prep_cached, run_s_full

TRADING_DAYS = 252
SPLIT = "2020-07-01"          # 前後半段切點(跨度中位,非最佳化產物)
TOP_KS = [5, 8, 10, 15, 20]
MAX_NEWS: list[int | None] = [1, 2, 3, None]


# ── 統計工具 ────────────────────────────────────────────────────────────────
def _daily_rets(nav: pl.DataFrame) -> np.ndarray:
    v = nav.sort("date")["nav"].to_numpy()
    return v[1:] / v[:-1] - 1.0


def paired_block_bootstrap(nav_v: pl.DataFrame, nav_b: pl.DataFrame, *,
                           n_boot: int = 4000, block: int = 21,
                           seed: int = 42, chunk: int = 500) -> dict:
    """配對 circular moving-block bootstrap:同一組重抽索引同時作用於 variant 與
    baseline 的日報酬,回「年化 CAGR 差」的分佈。對高相關曲線比獨立重抽有力得多。"""
    rv, rb = _daily_rets(nav_v), _daily_rets(nav_b)
    if len(rv) != len(rb):
        raise ValueError("variant/baseline 日報酬長度不一致,無法配對")
    t = len(rv)
    rng = np.random.default_rng(seed)
    n_blocks = math.ceil(t / block)
    diffs = np.empty(n_boot)
    off = np.arange(block)[None, None, :]
    for s in range(0, n_boot, chunk):
        n = min(chunk, n_boot - s)
        starts = rng.integers(0, t, size=(n, n_blocks))
        idx = ((starts[:, :, None] + off) % t).reshape(n, -1)[:, :t]
        gv = np.prod(1.0 + rv[idx], axis=1) ** (TRADING_DAYS / t) - 1.0
        gb = np.prod(1.0 + rb[idx], axis=1) ** (TRADING_DAYS / t) - 1.0
        diffs[s:s + n] = gv - gb
    return {
        "ann_diff": float(np.mean(diffs)),
        "ci_lo": float(np.percentile(diffs, 2.5)),
        "ci_hi": float(np.percentile(diffs, 97.5)),
        "p_le0": float((diffs <= 0).mean()),
    }


def block_bootstrap_lower(nav: pl.DataFrame, *, n_boot: int = 4000, block: int = 21,
                          seed: int = 42, chunk: int = 500) -> float:
    """單曲線 CAGR bootstrap 的 95% 下界(與 apex.validate.block_bootstrap_cagr 同法,
    此處分塊計算避免 4000×2858 索引一次成形的記憶體尖峰)。"""
    r = _daily_rets(nav)
    t = len(r)
    rng = np.random.default_rng(seed)
    n_blocks = math.ceil(t / block)
    out = np.empty(n_boot)
    off = np.arange(block)[None, None, :]
    for s in range(0, n_boot, chunk):
        n = min(chunk, n_boot - s)
        starts = rng.integers(0, t, size=(n, n_blocks))
        idx = ((starts[:, :, None] + off) % t).reshape(n, -1)[:, :t]
        out[s:s + n] = np.prod(1.0 + r[idx], axis=1) ** (TRADING_DAYS / t) - 1.0
    return float(np.percentile(out, 2.5))


# ── 機制診斷 ────────────────────────────────────────────────────────────────
def occupancy(trades: pl.DataFrame, dates: np.ndarray, n_slots: int = 5) -> dict:
    """由 trades 的 [entry_date, exit_date) 區間還原每日持倉數,量「席次空轉」程度。"""
    ei = np.searchsorted(dates, trades["entry_date"].to_numpy())
    xi = np.searchsorted(dates, trades["exit_date"].to_numpy())
    delta = np.zeros(len(dates) + 1)
    np.add.at(delta, ei, 1.0)
    np.add.at(delta, np.minimum(xi, len(dates)), -1.0)
    npos = np.cumsum(delta)[:len(dates)]
    return {
        "n_fills": int(trades.height),
        "mean_npos": float(npos.mean()),
        "frac_days_full": float((npos >= n_slots).mean()),
        "frac_days_le3": float((npos <= 3).mean()),
    }


def pool_size_by_day(panel, feat, elig) -> pl.DataFrame:
    """借 `_score_fn` 鉤子攔截「引擎自己算出來的過濾後候選池」,量每日候選數。
    不複製任何過濾邏輯(score 內容在此無意義,只取 df 的 date 分佈)。"""
    grab: dict[str, pl.DataFrame] = {}

    def _cap(df: pl.DataFrame) -> pl.DataFrame:
        grab["df"] = df
        return df.with_columns(pl.lit(0.0).alias("score"))

    run_s_full(panel, feat, elig, DS, _score_fn=_cap)
    return (grab["df"].filter(pl.col("date") >= pl.lit(DS).str.to_date())
            .group_by("date").len().rename({"len": "n_cand"}).sort("date"))


def carry_entries_fn(dates: np.ndarray, k: int):
    """候選延壽遞補:當日 top_k 名單在其後 k 個交易日仍可被引擎取用。
    排序鍵 = score − age(score ∈ (0,1],age 為整數天)——保證「今日新鮮名單」
    永遠排在「昨日殘留名單」之前,不引入任何門檻魔術數字。同檔多次入榜取最新。"""
    def _fn(entries: pl.DataFrame) -> pl.DataFrame:
        di = np.searchsorted(dates, entries["date"].to_numpy())
        base = entries.with_columns(pl.Series("di", di))
        outs = [base.with_columns(pl.lit(0).alias("age"))]
        for a in range(1, k + 1):
            outs.append(base.with_columns(pl.lit(a).alias("age")))
        allrows = (pl.concat(outs)
                   .with_columns((pl.col("di") + pl.col("age")).alias("dj"))
                   .filter(pl.col("dj") < len(dates)))
        allrows = (allrows
                   .with_columns(pl.Series("date2", dates[allrows["dj"].to_numpy()]))
                   # 同一 (日, 檔) 若同時來自新鮮與殘留,只留最新鮮那筆
                   .sort(["date2", "company_code", "age"])
                   .unique(subset=["date2", "company_code"], keep="first")
                   .with_columns((pl.col("score") - pl.col("age")).alias("score2")))
        return allrows.select([pl.col("date2").alias("date"), "company_code",
                               pl.col("score2").alias("score")])
    return _fn


def half_stats(nav: pl.DataFrame) -> tuple[dict, dict]:
    a = nav.filter(pl.col("date") < pl.lit(SPLIT).str.to_date()).sort("date")
    b = nav.filter(pl.col("date") >= pl.lit(SPLIT).str.to_date()).sort("date")
    return perf_stats(a), perf_stats(b)


# ── 主流程 ──────────────────────────────────────────────────────────────────
def _report_variant(name, nav, nav_b, base):
    """分段(前/後半)+ 逐年對照,檢查 regime 一致性。"""
    h1, h2 = half_stats(nav)
    b1, b2 = half_stats(nav_b)
    print(f"\n--- {name} 分段 ---")
    print(f"  2014-10~2020-06: CAGR {h1['cagr']:+.4f} (canon {b1['cagr']:+.4f}) "
          f"Sortino {h1['sortino']:.2f} (canon {b1['sortino']:.2f}) "
          f"MDD {h1['mdd']:.3f} (canon {b1['mdd']:.3f})")
    print(f"  2020-07~2026-07: CAGR {h2['cagr']:+.4f} (canon {b2['cagr']:+.4f}) "
          f"Sortino {h2['sortino']:.2f} (canon {b2['sortino']:.2f}) "
          f"MDD {h2['mdd']:.3f} (canon {b2['mdd']:.3f})")
    yv = yearly_table(nav).rename({"ret": "ret_v", "mdd": "mdd_v"})
    yb = yearly_table(nav_b).rename({"ret": "ret_c", "mdd": "mdd_c"})
    yy = yv.join(yb, on="year").with_columns(
        (pl.col("ret_v") - pl.col("ret_c")).alias("dret"))
    print("  逐年 Δret:", ", ".join(
        f"{r['year']}:{r['dret']:+.3f}" for r in yy.iter_rows(named=True)))
    print(f"  逐年勝出年數 {int((yy['dret'] > 0).sum())}/{yy.height}")


def stage_diag(panel, feat, elig, nav_b, tr_b) -> None:
    """機制診斷:候選池每日大小 vs 席次空轉——回答「深度是不是綁定約束」。"""
    nav_dates = np.sort(nav_b["date"].to_numpy())
    ps = pool_size_by_day(panel, feat, elig)
    full = pl.DataFrame({"date": nav_dates}).join(ps, on="date", how="left").with_columns(
        pl.col("n_cand").fill_null(0))
    n = full.height
    nc = full["n_cand"].to_numpy()
    print(f"\n=== 候選池每日大小(全 {n} 交易日)===")
    for th in (0, 1, 3, 5, 8, 10, 20):
        lbl = "= 0" if th == 0 else f">= {th}"
        v = (nc == 0).mean() if th == 0 else (nc >= th).mean()
        print(f"  候選數 {lbl:>5}: {v * 100:6.2f}% 的交易日")
    print(f"  候選數 中位數 {np.median(nc):.0f} / 平均 {nc.mean():.2f} / 最大 {nc.max()}")

    # 席次空轉與「當日有無候選」交叉
    ei = np.searchsorted(nav_dates, tr_b["entry_date"].to_numpy())
    xi = np.searchsorted(nav_dates, tr_b["exit_date"].to_numpy())
    delta = np.zeros(n + 1)
    np.add.at(delta, ei, 1.0)
    np.add.at(delta, np.minimum(xi, n), -1.0)
    npos = np.cumsum(delta)[:n]
    idle = npos < 5
    print(f"\n=== 席次空轉({idle.mean() * 100:.1f}% 的交易日 n_pos<5)===")
    print(f"  空轉且當日候選數 = 0 : {(idle & (nc == 0)).sum():5d} 天 "
          f"({(idle & (nc == 0)).sum() / max(idle.sum(), 1) * 100:.1f}% 的空轉日)")
    print(f"  空轉且當日候選 > 5   : {(idle & (nc > 5)).sum():5d} 天 "
          f"({(idle & (nc > 5)).sum() / max(idle.sum(), 1) * 100:.1f}% 的空轉日)")
    print("  → 前者才是綁定約束(沒人可選);後者才是 top_k 加深救得到的情境。")


def stage_carry(panel, feat, elig, nav_b, tr_b, base, base_lo, base_occ) -> None:
    """候選延壽遞補:當日未成交名單在其後 k 個交易日仍可補位。"""
    dates = np.sort(panel["date"].unique().to_numpy())
    nav_dates = np.sort(nav_b["date"].to_numpy())
    rows, navs = [], {}
    for k in (1, 3, 5, 10):
        nav, tr = run_s_full(panel, feat, elig, DS,
                             _entries_fn=carry_entries_fn(dates, k))
        nav = nav.sort("date")
        navs[k] = nav
        st = perf_stats(nav)
        lo = block_bootstrap_lower(nav)
        occ = occupancy(tr, nav_dates)
        pb = paired_block_bootstrap(nav, nav_b)
        rows.append({"carry_days": k, "cagr": st["cagr"], "sortino": st["sortino"],
                     "calmar": st["calmar"], "mdd": st["mdd"], "boot_lo": lo, **occ,
                     **pb,
                     "pass_d2": bool(st["sortino"] >= base["sortino"]
                                     and st["calmar"] >= base["calmar"]
                                     and st["mdd"] >= base["mdd"] and lo >= base_lo)})
        r = rows[-1]
        sig = "跨0(噪音)" if r["ci_lo"] <= 0 <= r["ci_hi"] else (
            "顯著優" if r["ci_lo"] > 0 else "顯著劣")
        print(f"carry={k:2d}d  CAGR {r['cagr']:.4f} Sortino {r['sortino']:.3f} "
              f"Calmar {r['calmar']:.3f} MDD {r['mdd']:.4f} boot_lo {r['boot_lo']:.4f} "
              f"fills={r['n_fills']:3d} npos={r['mean_npos']:.2f} "
              f"D2={'Y' if r['pass_d2'] else 'n'} | Δann {r['ann_diff']:+.4f} "
              f"CI[{r['ci_lo']:+.4f},{r['ci_hi']:+.4f}] P(Δ≤0)={r['p_le0']:.3f} {sig}")
    df = pl.DataFrame(rows)
    paths.OUT_STRAT_LAB.mkdir(parents=True, exist_ok=True)
    out = paths.OUT_STRAT_LAB / "x_pool_depth_carry.csv"
    df.write_csv(out)
    print(f"\n[carry] -> {out}")
    best = df.sort("ann_diff", descending=True).head(1)
    for row in best.iter_rows(named=True):
        _report_variant(f"carry={row['carry_days']}d", navs[row["carry_days"]], nav_b, base)


def main() -> None:
    stage = sys.argv[1] if len(sys.argv) > 1 else "grid"
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    dates = np.sort(panel["date"].unique().to_numpy())

    t0 = time.time()
    nav_b, tr_b = run_s_full(panel, feat, elig, DS)
    nav_b = nav_b.sort("date")
    base = perf_stats(nav_b)
    base_lo = block_bootstrap_lower(nav_b)
    base_occ = occupancy(tr_b, np.sort(nav_b["date"].to_numpy()))
    print(f"[canonical] top_k=5 max_new=2  CAGR {base['cagr']:.4f} Sortino {base['sortino']:.3f} "
          f"Calmar {base['calmar']:.3f} MDD {base['mdd']:.4f} boot_lo {base_lo:.4f}")
    print(f"           fills={base_occ['n_fills']} mean_npos={base_occ['mean_npos']:.3f} "
          f"full%={base_occ['frac_days_full']:.3f} le3%={base_occ['frac_days_le3']:.3f} "
          f"({time.time() - t0:.1f}s)")

    if stage == "parity":
        # 守護:carry=0 的 _entries_fn 必須逐位重現 canonical(否則 carry 的差異
        # 可能來自 entries 重組 artefact,而非延壽本身)。
        nav0, _ = run_s_full(panel, feat, elig, DS,
                             _entries_fn=carry_entries_fn(dates, 0))
        d = float(np.abs(nav0.sort("date")["nav"].to_numpy()
                         - nav_b["nav"].to_numpy()).max())
        print(f"[parity] carry=0 vs canonical  max|ΔNAV| = {d:.3e}  "
              f"{'PASS' if d == 0.0 else 'FAIL'}")
        return
    if stage == "diag":
        stage_diag(panel, feat, elig, nav_b, tr_b)
        print(f"\n[done] {time.time() - t0:.1f}s")
        return
    if stage == "carry":
        stage_carry(panel, feat, elig, nav_b, tr_b, base, base_lo, base_occ)
        print(f"\n[done] {time.time() - t0:.1f}s")
        return

    nav_dates = np.sort(nav_b["date"].to_numpy())
    rows: list[dict] = []
    navs: dict[tuple, pl.DataFrame] = {(5, 2): nav_b}
    for tk in TOP_KS:
        for mn in MAX_NEWS:
            if (tk, mn) == (5, 2):
                st, lo, occ = base, base_lo, base_occ
            else:
                nav, tr = run_s_full(
                    panel, feat, elig, DS, _top_k=tk,
                    _port_spec=PortSpec(n_slots=5, max_new_per_day=mn,
                                        capital=3_000_000.0))
                nav = nav.sort("date")
                navs[(tk, mn)] = nav
                st = perf_stats(nav)
                lo = block_bootstrap_lower(nav)
                occ = occupancy(tr, nav_dates)
            rows.append({
                "top_k": tk, "max_new": -1 if mn is None else mn,
                "cagr": st["cagr"], "sortino": st["sortino"], "calmar": st["calmar"],
                "mdd": st["mdd"], "sharpe": st["sharpe"], "boot_lo": lo,
                **occ,
                "pass_d2": bool(st["sortino"] >= base["sortino"]
                                and st["calmar"] >= base["calmar"]
                                and st["mdd"] >= base["mdd"]
                                and lo >= base_lo),
            })
            r = rows[-1]
            print(f"top_k={tk:2d} max_new={str(mn):>4}  CAGR {r['cagr']:.4f} "
                  f"Sortino {r['sortino']:.3f} Calmar {r['calmar']:.3f} MDD {r['mdd']:.4f} "
                  f"boot_lo {r['boot_lo']:.4f} fills={r['n_fills']:3d} "
                  f"npos={r['mean_npos']:.2f} D2={'Y' if r['pass_d2'] else 'n'}")

    grid = pl.DataFrame(rows)
    paths.OUT_STRAT_LAB.mkdir(parents=True, exist_ok=True)
    out = paths.OUT_STRAT_LAB / "x_pool_depth_grid.csv"
    grid.write_csv(out)
    print(f"\n[grid] -> {out}")

    # ── 配對 bootstrap:全格都做(不挑事後最佳,避免選擇偏誤;CI 跨 0 = 噪音)──
    print("\n=== 配對 block-bootstrap(d = variant − canonical,block=21,n_boot=4000)===")
    pb_rows: list[dict] = []
    for (tk, mn), nav in navs.items():
        if (tk, mn) == (5, 2):
            continue
        pb = paired_block_bootstrap(nav, nav_b)
        pb_rows.append({"top_k": tk, "max_new": -1 if mn is None else mn, **pb})
        sig = "跨0(噪音)" if pb["ci_lo"] <= 0 <= pb["ci_hi"] else (
            "顯著優" if pb["ci_lo"] > 0 else "顯著劣")
        print(f"top_k={tk:2d} max_new={str(mn):>4}  Δann {pb['ann_diff']:+.4f} "
              f"CI[{pb['ci_lo']:+.4f},{pb['ci_hi']:+.4f}] P(Δ≤0)={pb['p_le0']:.3f}  {sig}")
    pb_df = pl.DataFrame(pb_rows)
    grid = grid.join(pb_df, on=["top_k", "max_new"], how="left")
    grid.write_csv(out)

    # ── 候選深究:D2 全過 且 配對 CI 下界 > 0 者 ─────────────────────────────
    cand = grid.filter(pl.col("pass_d2") & (pl.col("ci_lo") > 0)).sort("cagr", descending=True)
    print(f"\n=== 候選(D2 全過 ∧ 配對 CI 不跨 0):{cand.height} 個 ===")
    if cand.height == 0:
        # 沒有候選時,仍報「CAGR 最高」與「配對統計最強」兩者的分段表現供機制解讀
        show = grid.filter(pl.col("top_k") != 5).sort("ann_diff", descending=True).head(2)
    else:
        show = cand.head(3)
    for row in show.iter_rows(named=True):
        tk, mn = row["top_k"], (None if row["max_new"] == -1 else row["max_new"])
        _report_variant(f"top_k={tk} max_new={mn}", navs[(tk, mn)], nav_b, base)

    print(f"\n[done] {time.time() - t0:.1f}s, {len(navs)} variants")


if __name__ == "__main__":
    main()
