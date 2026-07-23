"""EV54 — 三策略融資槓桿 overlay(預註冊見 LEDGER.md EV54;S 側=F13)。

訊號引擎全凍結,槓桿住 NAV overlay 層(src/quantlib/trading/margin_sim,制度常數
第一手查證)。選擇紀律:train 選 max P5(KPI v3),OOS 只驗不選;收編標準
= 兩折 OOS P5 均 ≥ 未槓桿 baseline。

Run: uv run --project . python -m quantlib.evergreen.ev54_margin
依賴: tri dashboard 快取(先跑過 quantlib.tri.pnl_dashboard)+ cache 最新
"""
from __future__ import annotations

from datetime import date as Date
from pathlib import Path

import polars as pl

from quantlib.evergreen.ev36_walkforward import kpis_full
from quantlib.trading.margin_sim import constant_leverage, vol_target_leverage
from quantlib import paths

NT = Path(f"{paths.REPORTS / "tri"}/_navtrade_cache")
FOLDS = [("折1", Date(2024, 7, 10), Date(2024, 7, 11), Date(2025, 7, 10)),
         ("折2", Date(2025, 7, 10), Date(2025, 7, 11), Date(2026, 7, 9))]
TRAIN_START = {"S": Date(2022, 7, 11), "Serenity": Date(2022, 7, 11),
               "Evergreen": Date(2023, 7, 11)}   # Evergreen 用 wf 誠實線(首 OOS 起)
CONST_L = (1.25, 1.5, 1.75, 2.0, 2.5)
VT = [(s, 20, c) for s in (0.15, 0.20, 0.25, 0.30) for c in (1.5, 2.0)]


def load_series() -> dict[str, pl.DataFrame]:
    out = {}
    for nm, f in (("S", "s_nav.parquet"), ("Serenity", "serenity_nav.parquet")):
        out[nm] = (pl.read_parquet(NT / f)
                   .with_columns(pl.col("date").cast(pl.Date))
                   .select(["date", "nav"]).sort("date"))
    from quantlib.apex import data
    from quantlib.evergreen.engine import walkforward_nav_cached
    con = data.connect()
    try:
        eg = walkforward_nav_cached(con, data.latest_date(con))
    finally:
        con.close()
    out["Evergreen"] = (eg.filter(~pl.col("in_sample"))
                        .select(["date", "nav"]).sort("date"))
    return out


def seg(df: pl.DataFrame, a: Date, b: Date) -> pl.DataFrame:
    return df.filter((pl.col("date") >= a) & (pl.col("date") <= b))


def run_cfg(win: pl.DataFrame, cfg: dict) -> tuple[dict, int]:
    """overlay 對窗口獨立起槓桿;回 (kpis_full, 斷頭次數)。"""
    if cfg["kind"] == "base":
        return kpis_full(win), 0
    if cfg["kind"] == "const":
        nav, forced = constant_leverage(win, cfg["L"])
        return kpis_full(nav), forced
    nav = vol_target_leverage(win, cfg["sigma"], cfg["lb"], cfg["cap"])
    return kpis_full(nav), 0


def tag(cfg: dict) -> str:
    if cfg["kind"] == "base":
        return "L=1 無槓桿"
    if cfg["kind"] == "const":
        return f"融資 L={cfg['L']}(月頻再槓桿)"
    return f"vol-target σ{cfg['sigma']:.0%}/cap{cfg['cap']}"


def main() -> None:
    series = load_series()
    grid = ([{"kind": "const", "L": L} for L in CONST_L]
            + [{"kind": "vt", "sigma": s, "lb": lb, "cap": c} for s, lb, c in VT])
    base = {"kind": "base"}
    verdicts = {}
    for nm, df in series.items():
        print(f"\n{'=' * 12} {nm}(序列 {df['date'][0]} ~ {df['date'][-1]}){'=' * 12}")
        ok_folds = 0
        for fname, t1, o0, o1 in FOLDS:
            tr_win = seg(df, TRAIN_START[nm], t1)
            oos_win = seg(df, o0, o1)
            if tr_win.height < 120 or oos_win.height < 60:
                print(f"  {fname}: 窗口不足,略過")
                continue
            rows = []
            for cfg in grid:
                k, forced = run_cfg(tr_win, cfg)
                rows.append((cfg, k, forced))
            rows.sort(key=lambda x: -x[1]["p5"])
            kb_tr, _ = run_cfg(tr_win, base)
            kb_oos, _ = run_cfg(oos_win, base)
            best_cfg, kbest_tr, f_tr = rows[0]
            kbest_oos, f_oos = run_cfg(oos_win, best_cfg)
            print(f"\n  {fname}(train →{t1} / OOS {o0}~{o1})train top-3(P5 尺):")
            for cfg, k, fo in rows[:3]:
                print(f"    {tag(cfg):32s} CAGR {k['cagr']:7.1%} MDD {k['mdd']:6.1%}"
                      f" P5 {k['p5']:7.1%}" + (f" 斷頭{fo}次" if fo else ""))
            print(f"    {'(對照)L=1 無槓桿':30s} CAGR {kb_tr['cagr']:7.1%}"
                  f" MDD {kb_tr['mdd']:6.1%} P5 {kb_tr['p5']:7.1%}")
            held = kbest_oos["p5"] >= kb_oos["p5"]
            ok_folds += held
            print(f"  → OOS 對決:選中 {tag(best_cfg)}"
                  + (f"(OOS 斷頭 {f_oos} 次)" if f_oos else ""))
            print(f"    無槓桿 OOS: CAGR {kb_oos['cagr']:7.1%} MDD {kb_oos['mdd']:6.1%}"
                  f" P5 {kb_oos['p5']:7.1%}")
            print(f"    槓桿版 OOS: CAGR {kbest_oos['cagr']:7.1%} MDD {kbest_oos['mdd']:6.1%}"
                  f" P5 {kbest_oos['p5']:7.1%}  "
                  f"{'✓ P5 勝' if held else '✗ P5 輸(槓桿無增量)'}")
        verdicts[nm] = ok_folds
        print(f"  【{nm} 判定】兩折 OOS P5 勝 {ok_folds}/2 → "
              + ("收編候選(待使用者裁決)" if ok_folds == 2 else "不採(收編標準未過)"))
    print("\n" + "=" * 50)
    for nm, ok in verdicts.items():
        print(f"  {nm}: {'✓ 收編候選' if ok == 2 else f'✗ 不採({ok}/2)'}")


if __name__ == "__main__":
    main()
