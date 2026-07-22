"""EV45 裁決:Fable 哲學版 vs 原版(Opus 哲學)同月池品質對比。

度量(EV28 pilot 同尺):各月標記自站位次交易日起 fwd63 報酬,
h120>0.7 濾後等權平均。原版對照列兩份——EV28 pilot 與 EV29 registry
同月標記(同提示詞不同 run,顯示 run-to-run 變異幫助解讀 ±3pp 判準)。

判準(預註冊):Fable 版 ≥ 原版 +3pp → 換提示詞;±3pp 平手留原版;
≤ −3pp 原版留任。

Run: uv run --project research python -m research.evergreen.ev45_verdict
依賴 cache: 是
"""
from __future__ import annotations

from datetime import date as Date

import duckdb
import polars as pl
from research import paths

STANCE = {"2023-08": Date(2023, 8, 11), "2025-04": Date(2025, 4, 11)}
C = "company_code"


def fwd63(con, codes: list[str], stance: Date) -> pl.DataFrame:
    if not codes:
        return pl.DataFrame(schema={"code": pl.Utf8, "fwd63": pl.Float64,
                                    "h120": pl.Float64})
    ph = ",".join("?" * len(codes))
    q = f"""
    WITH px AS (
      SELECT company_code, date, closing_price,
             ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date) rn
      FROM daily_quote WHERE company_code IN ({ph}) AND date >= ?
    ), h AS (
      SELECT company_code,
             max(closing_price) FILTER (WHERE date BETWEEN ? AND ?) hi120,
             max(closing_price) FILTER (WHERE date = ?) c0
      FROM daily_quote WHERE company_code IN ({ph}) GROUP BY company_code
    )
    SELECT p1.company_code code,
           p2.closing_price / p1.closing_price - 1 AS fwd63,
           h.c0 / h.hi120 AS h120
    FROM px p1
    JOIN px p2 ON p1.company_code = p2.company_code AND p2.rn = p1.rn + 63
    JOIN h ON h.company_code = p1.company_code
    WHERE p1.rn = 2  -- 站位次交易日
    """
    from datetime import timedelta
    lo = stance - timedelta(days=200)
    return con.execute(q, codes + [stance.isoformat(), lo.isoformat(),
                                   stance.isoformat(), stance.isoformat()]
                       + codes).pl()


def pool_metric(con, codes: list[str], stance: Date) -> tuple[float, int, int]:
    df = fwd63(con, list(dict.fromkeys(codes)), stance)
    n0 = df.height
    df = df.filter(pl.col("h120") > 0.7)
    if not df.height:
        return float("nan"), 0, n0
    return float(df["fwd63"].mean()), df.height, n0


def main() -> None:
    con = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    fable = {}
    for ym in STANCE:
        import json, os
        p = f"research/evergreen/data/ev45_pilot/{ym}.json"
        if os.path.exists(p):
            fable[ym] = [x["code"] for x in json.load(open(p))["labels"]]
    pilot = pl.read_parquet("research/evergreen/data/ev28_pilot_labels.parquet")
    reg = pl.read_parquet("research/evergreen/data/registry_v3.parquet")

    print(f"{'月':8s} {'臂':22s} {'fwd63 均':>9s} {'濾後/總':>8s}")
    tot = {"fable": [], "pilot": [], "registry": []}
    for ym, d in STANCE.items():
        arms = {
            "Fable 哲學(EV45)": fable.get(ym, []),
            "原版 pilot(EV28)": pilot.filter(pl.col("month") == ym)["code"].to_list(),
            "原版 registry(EV29)": reg.filter(pl.col("month") == ym)["code"].to_list(),
        }
        for name, codes in arms.items():
            m, k, n0 = pool_metric(con, codes, d)
            key = ("fable" if "Fable" in name
                   else "pilot" if "pilot" in name else "registry")
            if k:
                tot[key].append(m)
            print(f"{ym:8s} {name:22s} {m:>9.1%} {k:>4d}/{n0}")
    print()
    for k, v in tot.items():
        if v:
            print(f"兩月平均 {k:10s}: {sum(v) / len(v):.1%}")


if __name__ == "__main__":
    main()
