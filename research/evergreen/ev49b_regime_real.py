"""EV49b — Regime 真切換引擎(預註冊見 LEDGER.md EV49 上界結果段)。

上界(EV49)兩折同勝 → 本輪落地為可實現形態:
- regime:TAIEX vs MA120,翻轉需連續 confirm 日確認(防抖,{1,3,5})
- 兩引擎 config = 各折 train 段內 top-1(EV49 產物,無前視)
- 分段模擬:OOS 依 regime 切段,每段獨立 simulate(資金 = 前段末 NAV ×
  (1 − 0.004 × 投入比)近似段界全清倉成本),段內完整引擎行為
判準:真切換兩折 OOS 皆勝單引擎 → 收編;否則 regime 線止於上界記錄。

Run: uv run --project research python -m research.evergreen.ev49b_regime_real
依賴 cache: 是
"""
from __future__ import annotations

import duckdb
import numpy as np
import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import C, seg_kpi
from research.evergreen.ev38_exhaust import FOLDS, LabX, bench

# EV49 各折 train 段內 top-1(攻擊 / 防禦)
CFGS = {
    "折1": {
        True: dict(gate="none", pool_months=2, h120=0.0, trail=0.30, lts=30,
                   n_slots=5, max_new=2),
        False: dict(gate="inst5", pool_months=3, h120=0.0, trail=0.30, lts=45,
                    n_slots=5, max_new=2),
    },
    "折2": {
        True: dict(gate="none", pool_months=2, h120=0.0, trail=0.40, lts=30,
                   n_slots=5, max_new=2),
        False: dict(gate="inst5", pool_months=3, h120=0.6, trail=0.40, lts=45,
                    n_slots=5, max_new=2),
    },
}
MONO = {  # EV49 全窗單引擎對照(該折 train 全窗 top-1)
    "折1": dict(gate="none", pool_months=3, h120=0.0, trail=0.30, lts=45,
                n_slots=6, max_new=1),
    "折2": dict(gate="inst5", pool_months=3, h120=0.6, trail=0.30, lts=45,
                n_slots=5, max_new=2),
}


def regime_segments(confirm: int, d0, d1) -> list[tuple]:
    raw = duckdb.connect("research/cache.duckdb", read_only=True)
    idx = (raw.execute("SELECT date, close FROM market_index "
                       "WHERE name = '發行量加權股價指數' ORDER BY date").pl()
           .with_columns(pl.col("close").rolling_mean(120).alias("ma"))
           .drop_nulls()
           .with_columns((pl.col("close") > pl.col("ma")).alias("raw_att")))
    rows = idx.filter((pl.col("date") >= d0) & (pl.col("date") <= d1)).to_dicts()
    segs, cur, start, streak = [], None, None, 0
    pend = None
    for r in rows:
        s = r["raw_att"]
        if cur is None:
            cur, start = s, r["date"]
            continue
        if s != cur:
            pend = s if pend is None or pend != s else pend
            streak += 1
            if streak >= confirm:
                segs.append((start, r["date"], cur))
                cur, start, streak, pend = s, r["date"], 0, None
        else:
            streak, pend = 0, None
    segs.append((start, rows[-1]["date"], cur))
    return segs


def run_seg(lab: LabX, cfg: dict, start, end, capital: float) -> tuple:
    memb, pool_flag = lab.memb(cfg["pool_months"])

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > cfg["h120"]))
    if cfg["gate"] != "none":
        sc = sc.filter(pl.col(cfg["gate"]).fill_null(False))
    sc = (sc.with_columns((rank("h52") * rank("h120")).alias("score"))
          .with_columns(pl.lit(1.0 / cfg["n_slots"]).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                   exit_flags=pool_flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=cfg["n_slots"],
                                      max_new_per_day=cfg["max_new"],
                                      capital=capital),
                   exit_spec=ExitSpec(trailing_stop=cfg["trail"],
                                      loser_time_stop=cfg["lts"]),
                   start=start)
    nav = res.nav.sort("date").filter(
        (pl.col("date") >= start) & (pl.col("date") <= end))
    inv_ratio = float((nav["invested"][-1] / nav["nav"][-1])
                      if nav.height else 0.0)
    end_nav = float(nav["nav"][-1]) if nav.height else capital
    return nav.select(["date", "nav"]), end_nav, inv_ratio


def main() -> None:
    lab = LabX()
    for fold in FOLDS:
        b = bench(fold)
        for confirm in (1, 3, 5):
            segs = regime_segments(confirm, fold["o0"], fold["o1"])
            cap, parts = 1_000_000.0, []
            for (s0, s1, att) in segs:
                nav, end_nav, inv = run_seg(lab, CFGS[fold["name"]][att],
                                            s0, s1, cap)
                if nav.height:
                    parts.append(nav.with_columns(pl.col("nav") / pl.lit(1.0)))
                cap = end_nav * (1 - 0.004 * inv)  # 段界全清倉成本近似
            full = pl.concat(parts).sort("date").unique(subset="date",
                                                        keep="last")
            # renormalize 為連續 NAV(段界資金已在 cap 傳遞,nav 欄需重建)
            # 各段 nav 以自身 capital 起步,直接串接即為絕對 NAV
            k = seg_kpi(full)
            n_sw = len(segs) - 1
            print(f"{fold['name']} confirm={confirm}:真切換 CAGR {k['cagr']:7.1%} "
                  f"MDD {k['mdd']:6.1%}(切換 {n_sw} 次)"
                  + "".join(f" | {nm} {v['cagr']:+.1%}"
                            for nm, v in b.items() if v))
        print(f"  對照 單引擎 OOS:折1 94.5/−33.7、折2 467.3/−21.0;"
              f"上界:折1 112.4、折2 865.7")




def train_confirm_select() -> None:
    """收編前最後一關:confirm 由各折 train 窗自選(分段模擬 train 段)。"""
    from research.evergreen.ev36_walkforward import kpis_full
    lab = LabX()
    for fold in FOLDS:
        picks = {}
        for confirm in (1, 3, 5):
            segs = regime_segments(confirm, fold["t0"], fold["t1"])
            cap, parts = 1_000_000.0, []
            for (s0, s1, att) in segs:
                nav, end_nav, inv = run_seg(lab, CFGS[fold["name"]][att],
                                            s0, s1, cap)
                if nav.height:
                    parts.append(nav)
                cap = end_nav * (1 - 0.004 * inv)
            full = pl.concat(parts).sort("date").unique(subset="date", keep="last")
            k = kpis_full(full)
            picks[confirm] = k["p5"]
            print(f"{fold['name']} train confirm={confirm}: P5 {k['p5']:.1%}")
        best = max(picks, key=picks.get)
        print(f"→ {fold['name']} train 自選 confirm = {best}")


if __name__ == "__main__":
    train_confirm_select()
