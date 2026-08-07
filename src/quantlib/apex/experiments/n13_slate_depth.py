"""N13 — 候選名單深度 top_k × 建倉節流 max_new_per_day:有沒有在漏接火箭?

**為什麼這是還沒動過的旋鈕**:S 現行 `_top_k = 5` 恰好等於席位數 `n_slots = 5`
——**名單上沒有任何備選**。任何一檔當日買不到(漲停鎖死、無成交、現金不足),
那個席位當天就空著,不會由第 6 名遞補。而 S 的候選正是微型股營收火箭,
**漲停鎖死在它們身上是常態不是例外**(引擎的 E01 精準鎖死偵測就是為此而生)。

N07 量到 S 的報酬集中度極高:最賺的 10%(67 筆)佔全部正報酬的 54.1%。
**在這種分佈下,漏接一筆火箭的代價遠大於多接一筆平庸倉的代價。** 這條旋鈕動的不是
訊號品質,是**執行完整度**——與前面九批(出場、資金分配、計分)全部不同軸。

**臂**:
  K 軸 top_k ∈ {5(現行), 6, 8, 10, 15} —— 名單加深 = 有備選遞補
  M 軸 max_new_per_day ∈ {1, 2(現行), 3, 5} —— 節流放寬 = 同日可多建倉

兩軸各自單獨掃(不做全網格)。**注意 top_k 放大不等於持股變多**:席位仍是 5,
只是排隊的人變多;真正改變的是「第 1-5 名買不到時,第 6 名有沒有機會」。

**判準**:CAGR 與 Sharpe 同時不劣於現行,且勝出方向須過前後窗一致性(切點 2021-07-01)。

**已知反向風險**:名單加深會讓分數較低的候選有機會進場,若第 6-15 名品質明顯較差,
稀釋效應會蓋過補位收益。這正是要量的東西。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n13_slate_depth
"""
from __future__ import annotations

import polars as pl

from quantlib import paths
from quantlib.apex import data, metrics
from quantlib.apex.engine import PortSpec
from quantlib.apex.strategy_s import prep_cached, run_s_full

FULL = "2015-01-01"
SPLIT = "2021-07-01"
OUT = paths.OUT / "apex" / "n13_slate"


def _row(label: str, nav, trades) -> dict:
    st = metrics.summarize(nav, trades)
    reasons = st.get("exit_reasons", {}) or {}
    n = max(1, sum(reasons.values()))
    return {"arm": label, "cagr": st["cagr"], "sharpe": st["sharpe"],
            "sortino": st["sortino"], "mdd": st["mdd"], "calmar": st["calmar"],
            "n_trades": st.get("n_trades", 0),
            "win_rate": round(st.get("win_rate", 0), 3),
            "signal占比": round(reasons.get("signal", 0) / n, 3)}


def run_axis(panel, feat, elig, name: str,
             arms: list[tuple[str, dict, dict]]) -> None:
    rows_full, rows_a, rows_b = [], [], []
    for label, run_kw, port_kw in arms:
        ps = PortSpec(n_slots=5, max_new_per_day=port_kw.get("max_new_per_day", 2))
        nav, tr = run_s_full(panel, feat, elig, FULL, _port_spec=ps, **run_kw)
        rows_full.append(_row(label, nav, tr))
        cut = pl.lit(SPLIT).str.to_date()
        na = nav.filter(pl.col("date") < cut)
        rows_a.append(_row(label, na.with_columns(pl.col("nav") / pl.col("nav").first()),
                           tr.filter(pl.col("entry_date") < cut)))
        nb, trb = run_s_full(panel, feat, elig, SPLIT, _port_spec=ps, **run_kw)
        rows_b.append(_row(label, nb, trb))
        print(f"  {label} 完成")

    cols = ["arm", "cagr", "sharpe", "sortino", "mdd", "calmar",
            "n_trades", "win_rate", "signal占比"]
    for tag, rows in (("全窗", rows_full), ("前窗", rows_a), ("後窗", rows_b)):
        t = pl.DataFrame(rows)
        base = t.filter(pl.col("arm").str.contains("現行"))
        t = t.with_columns([
            ((pl.col("cagr") - base["cagr"][0]) * 100).round(2).alias("ΔCAGR_pp"),
            (pl.col("sharpe") - base["sharpe"][0]).round(3).alias("ΔSharpe")])
        print("\n" + "=" * 82)
        print(f"【{name} 軸 / {tag}】")
        print("=" * 82)
        with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=190, float_precision=4):
            print(t.select(cols + ["ΔCAGR_pp", "ΔSharpe"]))
        t.write_parquet(OUT / f"{name}_{tag}.parquet")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)

    print("\n### K 軸:候選名單深度 top_k(席位仍為 5,只是排隊的人變多)")
    run_axis(panel, feat, elig, "K_topk", [
        (f"top_k {v}{'(現行)' if v == 5 else ''}", {"_top_k": v}, {})
        for v in (5, 6, 8, 10, 15)])

    print("\n### M 軸:建倉節流 max_new_per_day")
    run_axis(panel, feat, elig, "M_maxnew", [
        (f"max_new {v}{'(現行)' if v == 2 else ''}", {}, {"max_new_per_day": v})
        for v in (1, 2, 3, 5)])

    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
