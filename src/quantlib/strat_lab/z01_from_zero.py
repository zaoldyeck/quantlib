"""B2 從零線:不以 S 為起點,用已驗因子庫自由構造策略——並回答「S 的 alpha 住在哪」。

**設計原則(from-zero,非 S 微調)**:從乾淨資料 IC 證據庫挑因子(高 IC + 高 decile spread),
用最小結構假設構書:每日對**全 eligible universe**(不設 S 的營收新鮮池)排名、top-5 slot、
出場用已高原驗證的 trail35(+time60 as 持有地平線;非 S 的事件式 30/15)。

四書 + 一個對照,刻意設計來**分解 S 的 alpha 來源**:
- Z1 純動能雙因子:high_52w × close_pos_20(兩個最強價格因子)
- Z2 動能×低波:× lowvol_60(防禦內建於分數)
- Z3 動能×品質穩定:× gm_vol8_neg(毛利穩定,q01 冠軍;季報 PIT asof)
- Z4 營收×動能(無新鮮閘):rev_yoy_accel × high_52w,天天可進——**拆解:alpha 在因子還是在
  「營收剛公布」的事件時機?**
- Z5 = Z4 + 新鮮閘(≤7 日)= 對照錨(等於精簡版 S,無 cfo 閘/無其餘因子)
判準:D2(Sortino/Calmar/MDD/下界)vs S(82.3%/3.28/2.40/-34.3%/+51.8%)。

Run: uv run --project . python -m quantlib.strat_lab.z01_from_zero
依賴 cache:是。
"""
from __future__ import annotations

import polars as pl

from quantlib.apex import data
from quantlib.apex.assemble import entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import C, DS, prep_cached
from quantlib.apex.validate import block_bootstrap_cagr

#: 季報申報截止(月)——gm_vol8 的 PIT 可用日(同 fresh_v1_qual_mom 慣例)
_DEADLINE = {1: (0, 5, 22), 2: (0, 8, 21), 3: (0, 11, 21), 4: (1, 4, 7)}


def _gm_vol8(con) -> pl.DataFrame:
    """毛利率 8 季穩定度(負波動=高分),avail=申報截止日。回 (avail, C, gm_vol8_neg)。"""
    rq = con.sql("SELECT company_code, year, quarter, gross_margin_q FROM raw_quarterly "
                 "WHERE gross_margin_q IS NOT NULL").pl().sort([C, "year", "quarter"])
    rq = rq.with_columns(
        (-pl.col("gross_margin_q").rolling_std(8).over(C)).alias("gm_vol8_neg"))
    import datetime as _dt
    rows = rq.drop_nulls(subset=["gm_vol8_neg"]).to_dicts()
    out = [{"avail": _dt.date(r["year"] + _DEADLINE[r["quarter"]][0],
                              _DEADLINE[r["quarter"]][1], _DEADLINE[r["quarter"]][2]),
            C: r[C], "gm_vol8_neg": r["gm_vol8_neg"]} for r in rows]
    return pl.DataFrame(out).sort("avail")


def _book(panel, feat, elig, cols: list[str], *, fresh_gate: int | None, start: str) -> pl.DataFrame:
    df = (feat.join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
          .drop_nulls(subset=cols)
          .filter(pl.all_horizontal([pl.col(c).is_finite() for c in cols])))
    if fresh_gate is not None:
        df = df.filter(pl.col("rev_fresh_days") <= fresh_gate)
    expr = None
    for c_ in cols:
        term = (pl.col(c_).rank() / pl.len()).over("date")
        expr = term if expr is None else expr * term
    sc = (df.with_columns(expr.alias("score")).select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(start).str.to_date()))
    entries, _ = entries_and_flags(sc, 5, 10**9)
    res = simulate(panel, entries, exit_flags=None, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=60),
                   start=__import__("datetime").date.fromisoformat(start))
    return (res.nav.select(["date", "nav"]).sort("date")
            .with_columns(pl.col("nav") / pl.col("nav").first()))


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    gm = _gm_vol8(con)
    feat = (feat.sort("date")
            .join_asof(gm, left_on="date", right_on="avail", by=C, strategy="backward")
            .sort([C, "date"]))

    books = {
        "Z1 mom2(52wH×cpos)": (["high_52w", "close_pos_20"], None),
        "Z2 mom2×lowvol": (["high_52w", "close_pos_20", "lowvol_60"], None),
        "Z3 mom2×gm穩定": (["high_52w", "close_pos_20", "gm_vol8_neg"], None),
        "Z4 rev×mom 無新鮮閘": (["rev_yoy_accel", "high_52w"], None),
        "Z5 rev×mom +新鮮閘7": (["rev_yoy_accel", "high_52w"], 7),
    }
    print("=== 從零構書(全 eligible、top5、trail35+time60;S 基準 82.3%/3.28/2.40/-34.3%/+51.8%)===")
    print(f"  {'書':<22}{'CAGR':>8}{'Sortino':>9}{'Calmar':>8}{'MDD':>8}{'下界':>9}")
    for name, (cols, gate) in books.items():
        nav = _book(panel, feat, elig, cols, fresh_gate=gate, start=DS)
        st = perf_stats(nav)
        boot = block_bootstrap_cagr(nav)
        print(f"  {name:<22}{st['cagr']:>+7.1%}{st['sortino']:>9.2f}{st['calmar']:>8.2f}"
              f"{st['mdd']:>+7.1%}{boot['ci_lo']:>+8.1%}", flush=True)
    print("\n  判讀:Z4 vs Z5 = 「營收新鮮事件閘」的 alpha 貢獻;Z1-Z3 = 純因子書天花板。"
          "任一書超 S → 進出廠閘門;否則證偽:alpha 在事件時機,不在因子本身。")


if __name__ == "__main__":
    main()
