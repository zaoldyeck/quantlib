"""M02 meta-study:refit **頻率** vs OOS(固定 3 年最優窗)。

問題(使用者 2026-07-21):M01 證了「用過去 3 年研發最優(窗長)」,但 refit 的
**頻率**(多久重選一次 config)從沒回測過——每月?每季?每半年?每年?幾月
refit 最好?「一年一次(下次 2027-07)」是 M01 設計裡寫死的假設,不是量出來的。

方法:reuse M01 的 24-config 全期連續 NAV(net of 成本);固定 trailing 3 年窗,
在不同 refit 頻率下,每個 refit 點用近 3 年 KPI 選最優 config、部署其未來報酬到
下一個 refit 點,串接成連續策略;比較各頻率 OOS CAGR/Sharpe/MDD/P5。**config
切換時計入保守全額換手成本**(不換則連續持有、零切換成本)。另掃「年更的起始
月份」看 refit 月份是否有差。

判準(誠實界定):這是**參數層 meta-study**——研發自由度以 24-config 網格代理
(與 M01 同限);量的是「在這族策略內,多久重選一次最好」,非全策略空間。連續
NAV 切片近似(切換時的建倉過渡以 switch_cost 概括)。

Run: uv run --project research python -m research.apex.experiments.m02_refit_frequency
依賴 cache:是(經 m01.prep)。
"""
from __future__ import annotations

import time
from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data
from research.apex.experiments.m01_window_length import GRID, prep, run_config, seg

C = "company_code"
SIM_START = Date(2007, 1, 1)        # 延到 TWSE+TPEx 都在的最早乾淨起點(日報價 2004/TPEx 2007);
                                    # 用月初界避免「永不」基準的 3 年前窗差 1 天被跳過
REFIT_FIRST = Date(2010, 1, 1)      # 首個 refit 點(3 年前 = 2007,窗內已有資料)
W_YEARS = 3
SWITCH_COST = 0.004                 # config 切換保守全額換手(賣稅 0.3% + 手續 0.0285%×2 ≈ 0.36%,取 0.4%)
BOOT_SEED = 20260721


def _sub_years(d: Date, y: int) -> Date:
    try:
        return Date(d.year - y, d.month, d.day)
    except ValueError:                       # 2/29
        return Date(d.year - y, d.month, 28)


def _add_months(d: Date, m: int) -> Date:
    idx = (d.month - 1) + m
    return Date(d.year + idx // 12, idx % 12 + 1, 1)


def _rets(nav: pl.DataFrame, a: Date, b: Date) -> pl.DataFrame:
    s = nav.filter((pl.col("date") >= a) & (pl.col("date") < b)).sort("date")
    return (s.with_columns((pl.col("nav") / pl.col("nav").shift(1) - 1).alias("ret"))
            .drop_nulls().select(["date", "ret"]))


def _p5(r: np.ndarray, rng: np.random.Generator, block: int = 20, n: int = 300) -> float:
    """block-bootstrap 年化報酬 5% 下界(apex 主 KPI 的簡版)。"""
    if len(r) < block * 3:
        return float("nan")
    nb = len(r) // block
    starts_pool = len(r) - block
    cagrs = []
    for _ in range(n):
        idx = rng.integers(0, starts_pool, size=nb)
        sample = np.concatenate([r[s:s + block] for s in idx])
        nav = float(np.prod(1 + sample))
        yrs = len(sample) / 252
        cagrs.append(nav ** (1 / yrs) - 1 if nav > 0 else -1.0)
    return float(np.percentile(cagrs, 5))


def _metrics(full: pl.DataFrame, rng: np.random.Generator) -> dict:
    r = full["ret"].to_numpy()
    if len(r) < 100:
        return {}
    nav = np.cumprod(1 + r)
    yrs = len(r) / 252
    peak = np.maximum.accumulate(nav)
    return {
        "cagr": float(nav[-1] ** (1 / yrs) - 1),
        "sharpe": float(r.mean() / r.std() * np.sqrt(252)) if r.std() > 0 else 0.0,
        "mdd": float((nav / peak - 1).min()),
        "p5": _p5(r, rng),
        "final_x": float(nav[-1]),
    }


def refit_sim(navs, data_end, freq_m, kpi, start_month=1, switch_cost=SWITCH_COST,
              first_refit=REFIT_FIRST):
    """在 freq_m 個月一次的 refit 頻率下串接連續策略,回 metrics + 換手次數。
    first_refit 可覆寫以做前/後半段切分(檢驗最佳月份的一致性)。"""
    points, t = [], Date(first_refit.year, start_month, 1)
    while t < data_end:
        if t >= first_refit:
            points.append(t)
        t = _add_months(t, freq_m)
    segs, prev = [], None
    n_switch = 0
    for i, tp in enumerate(points):
        b = points[i + 1] if i + 1 < len(points) else data_end
        a = _sub_years(tp, W_YEARS)
        if a < SIM_START:
            continue
        tr = [seg(nv, a, tp) for nv in navs]
        if any(x is None for x in tr):
            continue
        pick = int(np.argmax([x[kpi] for x in tr]))
        sr = _rets(navs[pick], tp, b)
        if switch_cost and prev is not None and pick != prev and len(sr):
            n_switch += 1
            d0 = sr["date"][0]
            sr = sr.with_columns(pl.when(pl.col("date") == d0)
                                 .then(pl.col("ret") - switch_cost)
                                 .otherwise(pl.col("ret")).alias("ret"))
        segs.append(sr)
        prev = pick
    if not segs:
        return {}
    rng = np.random.default_rng(BOOT_SEED)
    m = _metrics(pl.concat(segs).sort("date"), rng)
    m["switches"] = n_switch
    return m


def main() -> None:
    t0 = time.time()
    con = data.connect()
    latest = data.latest_date(con).isoformat()
    panel, feat = prep(con, prep_start="2006-06-01", end=latest)
    elig_map = {adv: (data.eligibility(panel, min_adv=adv)
                      .filter(pl.col("eligible")).select(["date", C]))
                for adv in [5e6, 20e6]}
    print(f"prep {time.time()-t0:.0f}s;跑 {len(GRID)} configs 全期連續 NAV(2007→{latest})…")
    navs = []
    for i, cfg in enumerate(GRID):
        navs.append(run_config(panel, feat, elig_map, cfg, sim_start="2007-01-02"))
        if (i + 1) % 6 == 0:
            print(f"  {i+1}/{len(GRID)}  ({time.time()-t0:.0f}s)")
    data_end = navs[0]["date"][-1]
    print(f"config NAV 完成({time.time()-t0:.0f}s);OOS 串接窗 {REFIT_FIRST}→{data_end}\n")

    rows = []
    for kpi in ["cagr", "sharpe"]:
        for freq_m, name in [(1, "每月"), (3, "每季"), (6, "每半年"), (12, "每年"), (9999, "永不(凍結首選)")]:
            m = refit_sim(navs, data_end, freq_m, kpi)
            if m:
                rows.append({"KPI": kpi, "頻率": name, **m})
    res = pl.DataFrame(rows)
    with pl.Config(tbl_rows=30, float_precision=3):
        print("=== refit 頻率對照(3 年窗;含切換成本 0.4%/次)===")
        print(res.select(["KPI", "頻率", "cagr", "sharpe", "mdd", "p5", "final_x", "switches"]))

    print("\n=== 年更『起始月份』掃描:全窗 vs 前半 vs 後半(檢驗最佳月是真訊號還是雜訊)===")
    mid = Date(2018, 1, 1)
    mrows = []
    for mo in range(1, 13):
        full = refit_sim(navs, data_end, 12, "cagr", start_month=mo)
        early = refit_sim(navs, mid, 12, "cagr", start_month=mo, first_refit=REFIT_FIRST)
        late = refit_sim(navs, data_end, 12, "cagr", start_month=mo, first_refit=mid)
        if full:
            mrows.append({"refit月": mo, "全窗CAGR": full["cagr"], "全窗P5": full["p5"],
                          "前半CAGR": (early or {}).get("cagr"),
                          "後半CAGR": (late or {}).get("cagr")})
    md = pl.DataFrame(mrows)
    with pl.Config(tbl_rows=13, float_precision=3):
        print(md.select(["refit月", "全窗CAGR", "全窗P5", "前半CAGR", "後半CAGR"]))

    def top3(col):
        return set(md.drop_nulls(col).sort(col, descending=True)["refit月"][:3].to_list())
    e3, l3 = top3("前半CAGR"), top3("後半CAGR")
    print(f"\n全窗最佳3月={sorted(top3('全窗CAGR'))}｜前半最佳3月={sorted(e3)}｜後半最佳3月={sorted(l3)}")
    print(f"前後半重疊={sorted(e3 & l3)} → "
          + ("有重疊,月份效應可能真實" if e3 & l3 else "零重疊 → 最佳月是雜訊,不可依賴,維持現行月份"))

    res.write_parquet("research/apex/ledger/m02_refit_frequency.parquet")
    print(f"\ntotal {time.time()-t0:.0f}s;結果 → research/apex/ledger/m02_refit_frequency.parquet")


if __name__ == "__main__":
    main()
