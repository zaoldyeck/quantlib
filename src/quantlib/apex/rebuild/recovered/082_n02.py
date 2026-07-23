"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T21:55:39.960Z(工具 Bash)
涵蓋 trials(8):n02_m42xConsol-n10, n02_m42xConsol-n12, n02_m42xCxH-n10, n02_m42xCxH-n12, n02_m42xH52-n10, n02_m42xH52-n12, n02_m63xConsol-n10, n02_m63xConsol-n12
"""
"""N02 — 動能 × 品質軸(月頻/分散/無停損規格)。"""
import itertools, time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.apex.experiments.g01_ml_ranker import C, W3_START, kpi, prep
from quantlib.apex.experiments.n01_momentum_single import month_firsts

t0 = time.time()
con, panel, feat = prep()
elig = (data.eligibility(panel, min_adv=5_000_000.0)
        .filter(pl.col("eligible")).select(["date", C]))
w3 = [d for d in panel.select("date").unique().sort("date")["date"].to_list()
      if d >= Date.fromisoformat(W3_START)]
rebals = month_firsts(w3)
base = (panel.sort([C, "date"])
        .with_columns([
            (pl.col("close").shift(5) / pl.col("close").shift(5 + 42) - 1).over(C).alias("mom42"),
            (pl.col("close").shift(5) / pl.col("close").shift(5 + 63) - 1).over(C).alias("mom63"),
            ((pl.col("close").rolling_max(60) - pl.col("close").rolling_min(60))
             / (pl.col("close").rolling_mean(60) + 1e-9)).over(C).alias("consol"),
            (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
        ])
        .select(["date", C, "mom42", "mom63", "consol", "h52"]))
SCORES = {
    "m42xConsol": ["mom42", "consol"],
    "m42xH52": ["mom42", "h52"],
    "m42xCxH": ["mom42", "consol", "h52"],
    "m63xConsol": ["mom63", "consol"],
}
rows = []
for sname, axes in SCORES.items():
    for n in [10, 12]:
        day = (base.filter(pl.col("date").is_in(rebals))
               .join(elig, on=["date", C], how="semi").drop_nulls(subset=axes))
        expr = None
        for a in axes:
            term = (pl.col(a).rank() / pl.len()).over("date")
            expr = term if expr is None else expr * term
        day = day.with_columns(expr.alias("score")).with_columns(
            pl.col("score").rank(descending=True).over("date").alias("rk"))
        entries = day.filter(pl.col("rk") <= n).select(["date", C, "score"])
        flags = (day.select(["date", C])
                 .join(day.filter(pl.col("rk") <= n * 2).select(["date", C]),
                       on=["date", C], how="anti"))
        res = simulate(panel, entries, exit_flags=flags, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=n),
                       exit_spec=ExitSpec(loser_time_stop=50),
                       start=Date.fromisoformat(W3_START))
        k = kpi(res.nav)
        name = f"{sname}-n{n}"
        ledger.log_trial(family="n_line", name=f"n02_{name}", hypothesis="動能×品質軸月頻",
                         config={"axes": axes, "n": n}, window=f"{W3_START}..2026-07-09",
                         metrics={kk: float(vv) for kk, vv in k.items()},
                         batch="N02", curve=res.nav)
        rows.append({"cell": name, **{kk: round(vv, 3) for kk, vv in k.items()}})
print(pl.DataFrame(rows).sort("p5", descending=True))
print(f"\n對照:N01 最佳 63.0/12.8;S 96.0/45.9;total {time.time()-t0:.0f}s")
