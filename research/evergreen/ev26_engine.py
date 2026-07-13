"""EV26 — 引擎重優化戰役基礎設施:參數化 harvest + KPI 報表。

所有變體共用:membership(YYYY-MM join 修復版)→ 可參數化 sc 構造
(排位軸/濾網/權重/regime 停新倉)→ 可參數化 exit_flags(池籍 rotation
/regime 清倉)→ simulate(PortSpec/ExitSpec 全開)。
紀律:train(2022-07~2025-06)KPI v3;OOS 只在定版後由 ev26_final 驗一次。

Run(單變體煙測): uv run --project research python -m research.evergreen.ev26_engine
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import kpi
from research.evergreen.harvest import build_feats, monthly_membership

C = "company_code"
TRAIN_END = Date(2025, 6, 30)


@dataclass(frozen=True)
class EngineSpec:
    # 排位/濾網/權重
    h120_gate: float = 0.7
    mom_gate: bool = False              # mom > 0 才可進場
    axes: tuple = ("conv", "h52", "mom")
    weight_mode: str = "conv"           # conv | equal
    weight_clip: tuple = (0.10, 0.30)
    # regime guard(大盤:發行量加權指數)
    regime: str | None = None           # None | "halt_new" | "flatten"
    regime_ma: int = 120                # 大盤收盤 < MA_n 觸發
    # 席位/出場
    n_slots: int = 5
    max_new: int = 2
    trail: float = 0.35
    lts: int | None = 30
    time_stop: int | None = None
    profit_recycle: tuple | None = None
    pool_months: int = 4                # 池籍長度(monthly_membership POOL_MONTHS)


class Lab:
    """載入一次,跑多變體。"""

    def __init__(self) -> None:
        con = data.connect()
        self.panel = data.common_stocks(
            data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=300))
        self.dates_all = (self.panel.select("date").unique()
                          .sort("date")["date"].to_list())
        self.feats = build_feats(self.panel)
        import duckdb
        raw = duckdb.connect("research/cache.duckdb", read_only=True)
        self.taiex = (raw.sql("SELECT date, close FROM market_index "
                              "WHERE name = '發行量加權股價指數' ORDER BY date").pl())
        self.regs = {n: pl.read_parquet(
            f"research/evergreen/data/registry_{n}.parquet") for n in ["v1", "v2"]}
        self._memb_cache: dict = {}

    def membership(self, reg_name: str, pool_months: int) -> pl.DataFrame:
        key = (reg_name, pool_months)
        if key not in self._memb_cache:
            import research.evergreen.harvest as hv
            old = hv.POOL_MONTHS
            hv.POOL_MONTHS = pool_months
            try:
                self._memb_cache[key] = monthly_membership(
                    self.regs[reg_name], self.dates_all, Date(2022, 7, 1))
            finally:
                hv.POOL_MONTHS = old
        return self._memb_cache[key]

    def bad_regime_days(self, ma: int) -> set:
        t = self.taiex.with_columns(
            pl.col("close").rolling_mean(ma).alias("ma"))
        return set(t.filter(pl.col("close") < pl.col("ma"))["date"].to_list())

    def run(self, reg_name: str, spec: EngineSpec):
        memb = self.membership(reg_name, spec.pool_months)

        def rank(c):
            return (pl.col(c).rank() / pl.len()).over("date")

        sc = (memb.join(self.feats, on=["date", C], how="left")
              .filter(pl.col("h120").fill_null(0) > spec.h120_gate))
        if spec.mom_gate:
            sc = sc.filter(pl.col("mom") > 0)
        expr = None
        for a in spec.axes:
            expr = rank(a) if expr is None else expr * rank(a)
        sc = sc.with_columns(expr.alias("score"))
        if spec.weight_mode == "conv":
            sc = sc.with_columns(
                ((pl.col("conv") / pl.col("conv").mean().over("date")) / 5)
                .clip(*spec.weight_clip).alias("weight"))
        else:
            sc = sc.with_columns(pl.lit(1.0 / spec.n_slots).alias("weight"))
        sc = (sc.select(["date", C, "score", "weight"]).drop_nulls()
              .sort(["date", "score", C], descending=[False, True, False]))

        days = [d for d in self.dates_all if d >= Date(2022, 7, 1)]
        bad = self.bad_regime_days(spec.regime_ma) if spec.regime else set()
        if spec.regime:  # 壞 regime 停發新倉
            sc = sc.filter(~pl.col("date").is_in(list(bad)))
        all_codes = memb[C].unique().to_list()
        flag = (pl.DataFrame({"date": days})
                .join(pl.DataFrame({C: all_codes}), how="cross")
                .join(memb.select(["date", C]), on=["date", C], how="anti"))
        if spec.regime == "flatten":  # 壞 regime 清倉
            bad_df = (pl.DataFrame({"date": [d for d in days if d in bad]})
                      .join(pl.DataFrame({C: all_codes}), how="cross"))
            flag = pl.concat([flag, bad_df]).unique(subset=["date", C])
        flag = flag.sort(["date", C])

        res = simulate(self.panel, sc, exit_flags=flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=spec.n_slots,
                                          max_new_per_day=spec.max_new),
                       exit_spec=ExitSpec(trailing_stop=spec.trail,
                                          loser_time_stop=spec.lts,
                                          time_stop=spec.time_stop,
                                          profit_recycle=spec.profit_recycle),
                       start=Date(2022, 7, 1))
        nav = res.nav.sort("date")
        tr = kpi(nav.filter(pl.col("date") <= TRAIN_END))
        oos = nav.filter(pl.col("date") > TRAIN_END)
        return {"cagr": tr["cagr"], "p5": tr["p5"], "mdd": tr["mdd"],
                "martin": tr.get("martin", float("nan")),
                "oos": float(oos["nav"][-1] / oos["nav"][0] - 1),
                "trades": res.trades.height}


def fmt(name: str, k: dict) -> str:
    return (f"{name}:CAGR {k['cagr']:7.1%}  P5 {k['p5']:6.1%}  "
            f"MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}  "
            f"OOS {k['oos']:+7.1%}  tr {k['trades']}")


def main() -> None:
    lab = Lab()
    for rn in ["v1", "v2"]:
        k = lab.run(rn, EngineSpec())
        print(fmt(f"{rn} 基準(凍結參數)", k))


if __name__ == "__main__":
    main()
