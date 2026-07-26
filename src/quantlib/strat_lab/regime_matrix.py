"""策略 × 年度報酬矩陣:「不同時期有不同的最強策略」這個前提到底成不成立?

使用者的想法「每個時期用該時期最強的策略」隱含一個前提:**不同時期的最強策略不同**。
`strategy_rotation` 已證輪動大輸固定(追績效效應),本檔直接檢驗那個**前提**:
把 10 個結構迥異的策略 × 每個年度的報酬列成矩陣,看:
1. 每年的冠軍是誰?S_EVENT 拿下幾年?
2. S 在「它不是冠軍」的年份,離冠軍差多遠?(= 輪動理論上能賺到的最大空間)
3. 分 regime(TAIEX 年報酬正/負)看:S 是否在多頭/空頭都能撐?
若 S 幾乎年年前段班、且輸的年份差距小 → 前提不成立,輪動注定無利可圖(還要付切換成本+選錯風險)。

Run: uv run --project . python -m quantlib.strat_lab.regime_matrix
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import PortSpec
from quantlib.apex.strategy_s import DS, prep_cached, run_s_full
from quantlib.strat_lab.strategy_rotation import BOOKS, _sfn


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    print(f"[matrix] 跑 {len(BOOKS)} 策略…", flush=True)
    navs = {}
    for name, (cols, inv, ex, kw) in BOOKS.items():
        nav, _ = run_s_full(panel, feat, elig, DS,
                            _score_fn=_sfn(cols, inv), _wrel={c: 1.0 for c in cols},
                            _exit_spec=ex, _port_spec=PortSpec(n_slots=5, max_new_per_day=2), **kw)
        navs[name] = nav.sort("date").with_columns(pl.col("date").dt.year().alias("y"))

    # 大盤年報酬(regime 標記)
    tw = con.execute("""SELECT date, close FROM market_index
                        WHERE market='twse' AND name='發行量加權股價指數' AND close > 0
                        ORDER BY date""").pl().with_columns(pl.col("date").dt.year().alias("y"))
    mkt = (tw.group_by("y").agg([pl.col("close").first().alias("o"), pl.col("close").last().alias("c")])
           .with_columns((pl.col("c") / pl.col("o") - 1).alias("mkt")).sort("y"))

    years = sorted(set(navs["S_EVENT"]["y"].to_list()))
    names = list(BOOKS)
    print("\n=== 策略 × 年度報酬矩陣(同引擎/同成本;粗體=當年冠軍)===")
    hdr = f"  {'年':>6}{'大盤':>8}" + "".join(f"{n[:8]:>10}" for n in names)
    print(hdr)
    ranks_s, gaps = [], []
    for y in years:
        row = {}
        for n in names:
            g = navs[n].filter(pl.col("y") == y)
            row[n] = float(g["nav"][-1] / g["nav"][0] - 1) if g.height >= 40 else float("nan")
        vals = {k: v for k, v in row.items() if v == v}
        if not vals:
            continue
        champ = max(vals, key=vals.get)
        m = mkt.filter(pl.col("y") == y)["mkt"]
        mk = float(m[0]) if m.len() else float("nan")
        line = f"  {y:>6}{mk:>+7.0%}"
        for n in names:
            v = row[n]
            s = f"{v:+.0%}" if v == v else "--"
            line += f"{('*' + s) if n == champ else s:>10}"
        print(line)
        srt = sorted(vals.items(), key=lambda kv: -kv[1])
        rk = [k for k, _ in srt].index("S_EVENT") + 1 if "S_EVENT" in vals else None
        if rk:
            ranks_s.append((y, rk, vals["S_EVENT"], vals[champ], champ, mk))
            gaps.append(vals[champ] - vals["S_EVENT"])

    print("\n=== S_EVENT 的年度名次(共 %d 年,10 策略中)===" % len(ranks_s))
    print(f"  {'年':>6}{'名次':>6}{'S 報酬':>10}{'當年冠軍':>12}{'冠軍報酬':>10}{'差距':>9}{'大盤':>8}")
    for y, rk, sv, cv, ch, mk in ranks_s:
        print(f"  {y:>6}{rk:>6}{sv:>+9.0%}{ch:>12}{cv:>+9.0%}{cv - sv:>+8.0%}{mk:>+7.0%}")
    r = np.array([x[1] for x in ranks_s])
    print(f"\n  S 平均名次 {r.mean():.1f}/10;拿下冠軍 {int((r == 1).sum())}/{len(r)} 年;"
          f"前三名 {int((r <= 3).sum())}/{len(r)} 年")
    print(f"  S 落後冠軍的平均差距 {np.mean(gaps):+.1%}(= 完美輪動的理論上限空間,尚未扣選錯風險/切換成本)")
    neg = [x for x in ranks_s if x[5] == x[5] and x[5] < 0]
    pos = [x for x in ranks_s if x[5] == x[5] and x[5] >= 0]
    if neg:
        print(f"  空頭年(大盤跌,{len(neg)} 年):S 平均報酬 {np.mean([x[2] for x in neg]):+.1%}、"
              f"平均名次 {np.mean([x[1] for x in neg]):.1f}")
    if pos:
        print(f"  多頭年({len(pos)} 年):S 平均報酬 {np.mean([x[2] for x in pos]):+.1%}、"
              f"平均名次 {np.mean([x[1] for x in pos]):.1f}")
    print("\n  判讀:S 若年年前段班且空頭年也撐得住 → 『不同時期最強策略不同』的前提不成立,"
          "輪動先天無利可圖。")


if __name__ == "__main__":
    main()
