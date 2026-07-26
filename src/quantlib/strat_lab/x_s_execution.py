"""S(apex_revcycle_S)**執行維度**變體 harness — 成交時點 / 最短持有 / 當日出場 / 滑價韌性。

問題:S 現行是「T 收盤決策 → T+1 開盤成交」、min_hold_days=1、門檻型出場一律隔日。
這條線只動**執行層**(不動選股、不動因子、不動出場門檻值),測四件事:

  (a) fill_at ∈ {next_open, next_close, next_mid}
      機制假設:S 賺動能,強勢股常在 T+1 開盤跳空;若跳空已把一段報酬吃掉,
      改收盤進場(或半開半收)可能拿到更好的成本基礎——也可能相反(動能日內續強)。
      本檔另附「跳空 vs 日內」分解實測(_gap_decomposition)直接量出方向。
  (b) min_hold_days ∈ {1,2,3,5} ∪ {10,16,20,25}
      機制假設:trail 35% 對剛進場的部位太敏感,一根長黑就出場 = 把雜訊當訊號。
      拉長最短持有等於給部位一個「不被當日雜訊掃出」的緩衝。
      **第一輪實測發現 2/3/5 完全不綁定**(逐位等同 canonical):S 的出場結構
      在 5 天內根本觸發不了——signal 出場要 rev_fresh_days ≥ 26 而進場要 ≤ 7
      (至少 19 天才可能),time_loser 要 15 天,trail 35% 幾乎不觸發。要真的
      測這個維度必須用**會綁定**的值(≥ 10,涵蓋 time_loser=15 這條主力出場),
      故第二輪補 {10,16,20,25}。只掃不綁定的值 = 假裝測過。
  (c) same_day_exit=True
      門檻型出場(trail / time / loser_time)的觸發線在盤中事先已知,實盤可掛 MOC
      當日收盤成交,不必等隔日。假設:少等一天 = 少一天的續跌。
  (d) slippage ∈ {0.0005, 0.001(現行), 0.002, 0.003, 0.005}
      不是「改進」而是**韌性測試**:S 的 edge 在多大的執行摩擦下才被吃光。

方法論(硬性):
  * 任何變體都對 canonical 做**配對 moving-block bootstrap**(共用 block 索引,
    對高相關曲線統計力最強):對每個 resample 算 CAGR_variant − CAGR_canonical,
    報年化差點估、95% CI、P(差 ≤ 0)。**CI 跨 0 = 噪音,判證偽**。
  * KPI 判準(D2):Sortino / Calmar / MDD / bootstrap 5% 下界必須**同時 ≥ canonical**
    才算候選,單看 CAGR 高不算。
  * 另報逐年報酬與前後半段(2014-10~2020-06 / 2020-07~2026-07)看 regime 一致性。

Run:
    uv run --project . python -m quantlib.strat_lab.x_s_execution

依賴 cache: 是(prep_cached 讀 cache.duckdb;首次會算 ~31s 特徵後落磁碟快取)。
輸出:終端表格 + var/out/strat_lab/x_s_execution.json(完整數字,可重跑復現)。
"""
from __future__ import annotations

import json
import math
from dataclasses import replace

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec
from quantlib.apex.metrics import perf_stats, trade_stats, turnover_ann, yearly_table
from quantlib.apex.strategy_s import DS, prep_cached, run_s_full
from quantlib.apex.validate import block_bootstrap_cagr

C = "company_code"
TRADING_DAYS = 252

# canonical S 規格(strategy_s.run_s_full 的預設,這裡顯式寫出好做 replace 衍生)
CANON_EXEC = ExecSpec()                                             # next_open, slip 0.001
CANON_PORT = PortSpec(n_slots=5, max_new_per_day=2)                 # min_hold_days=1
CANON_EXIT = ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15)

SPLIT = "2020-07-01"     # 前後半段切點(全跨度 2014-10~2026-07 的近似中點)

# ── 變體清單(預註冊;跑之前就定好,不事後追加以免挑尖峰)──────────────────
VARIANTS: list[tuple[str, str, dict]] = [
    # (name, group, run_s_full kwargs)
    ("fill_next_close", "a_fill",
     {"_exec_spec": replace(CANON_EXEC, fill_at="next_close")}),
    ("fill_next_mid", "a_fill",
     {"_exec_spec": replace(CANON_EXEC, fill_at="next_mid")}),

    # 不綁定區(第一輪已證逐位等同 canonical,保留當「出場結構最短反應時間」的證據)
    ("min_hold_2", "b_hold", {"_port_spec": replace(CANON_PORT, min_hold_days=2)}),
    ("min_hold_5", "b_hold", {"_port_spec": replace(CANON_PORT, min_hold_days=5)}),
    # 綁定區(真正測「給部位緩衝」假設;10 起壓到 time_loser=15 這條主力出場)
    ("min_hold_10", "b_hold", {"_port_spec": replace(CANON_PORT, min_hold_days=10)}),
    ("min_hold_16", "b_hold", {"_port_spec": replace(CANON_PORT, min_hold_days=16)}),
    ("min_hold_20", "b_hold", {"_port_spec": replace(CANON_PORT, min_hold_days=20)}),
    ("min_hold_25", "b_hold", {"_port_spec": replace(CANON_PORT, min_hold_days=25)}),

    ("same_day_exit", "c_sde", {"_exit_spec": replace(CANON_EXIT, same_day_exit=True)}),

    # 組合(預註冊,非事後追加):執行時點的兩個自由度同時動
    ("close+sde", "e_combo",
     {"_exec_spec": replace(CANON_EXEC, fill_at="next_close"),
      "_exit_spec": replace(CANON_EXIT, same_day_exit=True)}),
    ("close+hold2", "e_combo",
     {"_exec_spec": replace(CANON_EXEC, fill_at="next_close"),
      "_port_spec": replace(CANON_PORT, min_hold_days=2)}),

    # 滑價韌性(不是改進候選,是壓力測試)
    ("slip_0.0005", "d_slip", {"_exec_spec": replace(CANON_EXEC, slippage=0.0005)}),
    ("slip_0.002", "d_slip", {"_exec_spec": replace(CANON_EXEC, slippage=0.002)}),
    ("slip_0.003", "d_slip", {"_exec_spec": replace(CANON_EXEC, slippage=0.003)}),
    ("slip_0.005", "d_slip", {"_exec_spec": replace(CANON_EXEC, slippage=0.005)}),
]


# ── 統計:配對 moving-block bootstrap ────────────────────────────────────
def _daily_rets(nav: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    s = nav.sort("date")
    v = s["nav"].to_numpy()
    return s["date"].to_numpy()[1:], v[1:] / v[:-1] - 1.0


def paired_block_bootstrap(nav_var: pl.DataFrame, nav_base: pl.DataFrame,
                           n_boot: int = 4000, block: int = 21, seed: int = 7,
                           batch: int = 500) -> dict:
    """共用 block 索引的配對 bootstrap:每個 resample 同時重採兩條曲線的同一段日期,
    算 CAGR 差。共用索引保留兩曲線的日對日相關(相關 ~0.99 時統計力遠高於各自獨立
    重採),因此能對「小但一致的差異」給出有意義的 CI。"""
    da, ra = _daily_rets(nav_var)
    db, rb = _daily_rets(nav_base)
    if len(da) != len(db) or not (da == db).all():
        raise ValueError("兩條 NAV 的日期軸不一致,配對 bootstrap 無效")
    t = len(ra)
    rng = np.random.default_rng(seed)
    n_blocks = math.ceil(t / block)
    diffs = np.empty(n_boot)
    done = 0
    while done < n_boot:
        n = min(batch, n_boot - done)
        starts = rng.integers(0, t, size=(n, n_blocks))
        idx = ((starts[:, :, None] + np.arange(block)[None, None, :])
               % t).reshape(n, -1)[:, :t]
        ga = np.prod(1.0 + ra[idx], axis=1) ** (TRADING_DAYS / t) - 1.0
        gb = np.prod(1.0 + rb[idx], axis=1) ** (TRADING_DAYS / t) - 1.0
        diffs[done:done + n] = ga - gb
        done += n
    # 點估用實際全樣本 CAGR 差(不是 bootstrap 中位數——後者含重採偏誤)
    pa = perf_stats(nav_var)["cagr"]
    pb = perf_stats(nav_base)["cagr"]
    return {
        "point": pa - pb,
        "ci_lo": float(np.percentile(diffs, 2.5)),
        "ci_hi": float(np.percentile(diffs, 97.5)),
        "boot_median": float(np.percentile(diffs, 50)),
        "p_le0": float((diffs <= 0).mean()),
    }


def _renorm(nav: pl.DataFrame, lo: str | None = None, hi: str | None = None) -> pl.DataFrame:
    out = nav.sort("date")
    if lo:
        out = out.filter(pl.col("date") >= pl.lit(lo).str.to_date())
    if hi:
        out = out.filter(pl.col("date") < pl.lit(hi).str.to_date())
    return out.with_columns(pl.col("nav") / pl.col("nav").first())


# ── 機制診斷:跳空 vs 日內分解 ───────────────────────────────────────────
def gap_decomposition(panel: pl.DataFrame, trades: pl.DataFrame) -> dict:
    """canonical 實際成交的那些 (code, fill_date) 上,拆開兩段報酬:
       gap    = 成交日 open / 前一交易日 close − 1   (隔夜跳空:決策日收盤→成交開盤)
       intra  = 成交日 close / open − 1              (日內:next_close 比 next_open 多等的那段)

    讀法(這是整條 fill_at / same_day_exit 結論的機制來源,不靠推測):
      進場側 intra < 0 → 收盤買比較便宜(對 next_close 有利,幅度 = −intra)
      出場側 intra < 0 → 收盤賣比較差  (對 next_open  有利,幅度 = −intra)
      出場側 gap  > 0 → 決策日收盤就賣(same_day_exit)會放棄這段隔夜跳空
    附 t 統計量:平均值除以標準誤,|t| < 2 視為與 0 無異(不可據以改規則)。"""
    px = (panel.select(["date", C, "open", "close"]).sort([C, "date"])
          .with_columns(pl.col("close").shift(1).over(C).alias("prev_close"))
          .with_columns([
              (pl.col("open") / pl.col("prev_close") - 1).alias("gap"),
              (pl.col("close") / pl.col("open") - 1).alias("intra"),
          ]))
    out: dict = {}
    for side, dcol in (("entry", "entry_date"), ("exit", "exit_date")):
        j = (trades.select([C, pl.col(dcol).alias("date")])
             .join(px.select(["date", C, "gap", "intra"]), on=["date", C], how="inner")
             .drop_nulls())
        if not j.height:
            continue
        rec = {"n": j.height}
        for col in ("gap", "intra"):
            x = j[col].to_numpy()
            se = float(np.std(x, ddof=1) / math.sqrt(len(x)))
            rec |= {f"{col}_mean": float(np.mean(x)), f"{col}_med": float(np.median(x)),
                    f"{col}_t": float(np.mean(x) / se) if se > 0 else 0.0}
        rec["intra_pos_rate"] = float((j["intra"] > 0).mean())
        out[side] = rec
    return out


def hold_distribution(trades: pl.DataFrame) -> dict:
    """已平倉交易的 days_held 分位——用來判斷某個 min_hold_days 到底綁不綁得到。
    若 min_hold ≤ min(days_held),該變體必然逐位等同 canonical(掃了也是白掃)。"""
    c = trades.filter(pl.col("exit_reason") != "open")["days_held"].to_numpy()
    return {"n": int(len(c)), "min": int(c.min()), "p1": float(np.percentile(c, 1)),
            "p5": float(np.percentile(c, 5)), "p10": float(np.percentile(c, 10)),
            "median": float(np.median(c)), "p90": float(np.percentile(c, 90))}


def per_trade_delta(var_tr: pl.DataFrame, base_tr: pl.DataFrame) -> dict:
    """變體 vs canonical 的**每筆交易淨報酬**平均差——把 CAGR 差還原成單筆執行成本,
    確認曲線層的差異真的來自成交價、而不是選股路徑分岔。"""
    a = var_tr.filter(pl.col("exit_reason") != "open")["ret_net"].to_numpy()
    b = base_tr.filter(pl.col("exit_reason") != "open")["ret_net"].to_numpy()
    return {"mean_var": float(a.mean()), "mean_base": float(b.mean()),
            "mean_delta_pp": float((a.mean() - b.mean()) * 100),
            "n_var": int(len(a)), "n_base": int(len(b))}


# ── 主流程 ──────────────────────────────────────────────────────────────
def kpi(nav: pl.DataFrame, trades: pl.DataFrame) -> dict:
    st = perf_stats(nav)
    ts = trade_stats(trades)
    # turnover_ann 的分母是 NAV,分子是**元計價**的 trades.cost;run_s_full 回傳的 nav
    # 已歸一化成 1.0 起,直接餵會得到 10^7 量級的假值 → 先還原成元(× 起始資本)。
    nav_dollars = nav.with_columns(pl.col("nav") * CANON_PORT.capital)
    return {
        "cagr": st["cagr"], "sortino": st["sortino"], "sharpe": st["sharpe"],
        "mdd": st["mdd"], "calmar": st["calmar"], "ann_vol": st["ann_vol"],
        "n_trades": ts.get("n_trades", 0), "win_rate": ts.get("win_rate"),
        "med_days_held": ts.get("med_days_held"),
        "turnover_ann": turnover_ann(trades, nav_dollars),
        "exit_reasons": ts.get("exit_reasons", {}),
    }


def main(only_group: str | None = None) -> None:
    """only_group:只跑某一組變體(a_fill / b_hold / c_sde / d_slip / e_combo),
    供補測單一維度時免整輪重跑;None = 全跑。"""
    con = data.connect()
    panel, feat, elig = prep_cached(con)

    base_nav, base_tr = run_s_full(panel, feat, elig, DS)
    base = kpi(base_nav, base_tr)
    base_boot = block_bootstrap_cagr(base_nav, n_boot=4000, block=21)
    base["boot_lo"] = base_boot["ci_lo"]
    base["h1_cagr"] = perf_stats(_renorm(base_nav, hi=SPLIT))["cagr"]
    base["h2_cagr"] = perf_stats(_renorm(base_nav, lo=SPLIT))["cagr"]
    base["h1_sortino"] = perf_stats(_renorm(base_nav, hi=SPLIT))["sortino"]
    base["h2_sortino"] = perf_stats(_renorm(base_nav, lo=SPLIT))["sortino"]
    base["per_trade_mean"] = per_trade_delta(base_tr, base_tr)["mean_base"]

    print("=" * 100)
    print("S 執行維度 harness — canonical 基準")
    print(f"window {base_nav['date'][0]} → {base_nav['date'][-1]}   "
          f"CAGR {base['cagr']*100:.1f}%  Sortino {base['sortino']:.2f}  "
          f"Calmar {base['calmar']:.2f}  MDD {base['mdd']*100:.1f}%  "
          f"boot5% {base['boot_lo']*100:+.1f}%")
    print(f"trades {base['n_trades']}  win {base['win_rate']*100:.1f}%  "
          f"medHold {base['med_days_held']:.0f}d  turnover {base['turnover_ann']:.1f}x")
    print(f"exits {base['exit_reasons']}")

    gd = gap_decomposition(panel, base_tr)
    print("\n── 機制診斷:成交日跳空 vs 日內(canonical 實際成交樣本)──")
    for side, v in gd.items():
        print(f"  {side:5s} n={v['n']:5d}  "
              f"gap {v['gap_mean']*100:+.3f}% (med {v['gap_med']*100:+.3f}%, t={v['gap_t']:+.2f})"
              f"   intraday {v['intra_mean']*100:+.3f}% (med {v['intra_med']*100:+.3f}%,"
              f" t={v['intra_t']:+.2f}, 上漲率 {v['intra_pos_rate']*100:.1f}%)")

    hd = hold_distribution(base_tr)
    print(f"\n── canonical 持有天數分佈:min {hd['min']}  p1 {hd['p1']:.0f}  p5 {hd['p5']:.0f}  "
          f"p10 {hd['p10']:.0f}  med {hd['median']:.0f}  p90 {hd['p90']:.0f}")
    print(f"   → min_hold_days ≤ {hd['min']} 必然不綁定(逐位等同 canonical)")

    rows = []
    todo = [v for v in VARIANTS if only_group is None or v[1] == only_group]
    for name, group, kw in todo:
        nav, tr = run_s_full(panel, feat, elig, DS, **kw)
        k = kpi(nav, tr)
        k["per_trade"] = per_trade_delta(tr, base_tr)
        pb = paired_block_bootstrap(nav, base_nav)
        bb = block_bootstrap_cagr(nav, n_boot=4000, block=21)
        k |= {
            "name": name, "group": group,
            "boot_lo": bb["ci_lo"],
            "paired_point": pb["point"], "paired_lo": pb["ci_lo"],
            "paired_hi": pb["ci_hi"], "paired_p_le0": pb["p_le0"],
            "h1_cagr": perf_stats(_renorm(nav, hi=SPLIT))["cagr"],
            "h2_cagr": perf_stats(_renorm(nav, lo=SPLIT))["cagr"],
            "h1_sortino": perf_stats(_renorm(nav, hi=SPLIT))["sortino"],
            "h2_sortino": perf_stats(_renorm(nav, lo=SPLIT))["sortino"],
        }
        # D2 判準:四項同時 ≥ canonical 才算候選
        k["d2_pass"] = bool(
            k["sortino"] >= base["sortino"] and k["calmar"] >= base["calmar"]
            and k["mdd"] >= base["mdd"] and k["boot_lo"] >= base["boot_lo"])
        k["sig"] = bool(k["paired_lo"] > 0)     # CI 不跨 0(正向)
        rows.append(k)
        print(f"  ran {name:16s} CAGR {k['cagr']*100:7.2f}%  "
              f"Δ {k['paired_point']*100:+7.2f}pp  P(Δ≤0) {k['paired_p_le0']:.3f}")

    print("\n" + "=" * 118)
    hdr = (f"{'variant':16s} {'CAGR':>8s} {'Sortino':>8s} {'Calmar':>7s} {'MDD':>8s} "
           f"{'boot5%':>8s} {'Δann':>8s} {'95%CI':>17s} {'P(Δ≤0)':>7s} {'D2':>3s} {'trades':>7s} {'hold':>5s}")
    print(hdr)
    print(f"{'CANONICAL':16s} {base['cagr']*100:7.2f}% {base['sortino']:8.2f} "
          f"{base['calmar']:7.2f} {base['mdd']*100:7.2f}% {base['boot_lo']*100:7.2f}% "
          f"{'—':>8s} {'—':>17s} {'—':>7s} {'—':>3s} {base['n_trades']:7d} "
          f"{base['med_days_held']:5.0f}")
    print("-" * 118)
    for k in rows:
        ci = f"[{k['paired_lo']*100:+.1f},{k['paired_hi']*100:+.1f}]"
        print(f"{k['name']:16s} {k['cagr']*100:7.2f}% {k['sortino']:8.2f} "
              f"{k['calmar']:7.2f} {k['mdd']*100:7.2f}% {k['boot_lo']*100:7.2f}% "
              f"{k['paired_point']*100:+7.2f}pp {ci:>17s} {k['paired_p_le0']:7.3f} "
              f"{'Y' if k['d2_pass'] else 'n':>3s} {k['n_trades']:7d} {k['med_days_held']:5.0f}")

    print("\n── 前後半段(regime 一致性;各段自行歸一)──")
    print(f"{'variant':16s} {'H1 CAGR':>9s} {'H1 Sort':>8s} {'H2 CAGR':>9s} {'H2 Sort':>8s}")
    print(f"{'CANONICAL':16s} {base['h1_cagr']*100:8.2f}% {base['h1_sortino']:8.2f} "
          f"{base['h2_cagr']*100:8.2f}% {base['h2_sortino']:8.2f}")
    for k in rows:
        print(f"{k['name']:16s} {k['h1_cagr']*100:8.2f}% {k['h1_sortino']:8.2f} "
              f"{k['h2_cagr']*100:8.2f}% {k['h2_sortino']:8.2f}")

    print("\n── 每筆交易淨報酬差(把 CAGR 差還原成單筆執行成本)──")
    print(f"{'CANONICAL':16s} mean ret_net {base['per_trade_mean']*100:+.3f}%")
    for k in rows:
        p = k["per_trade"]
        print(f"{k['name']:16s} mean ret_net {p['mean_var']*100:+.3f}%  "
              f"Δ {p['mean_delta_pp']:+.3f}pp/筆  (n={p['n_var']})")

    print("\n── 出場理由分佈 + 換手(min_hold / same_day_exit 的機制證據)──")
    print(f"{'CANONICAL':16s} turnover {base['turnover_ann']:5.2f}x  {base['exit_reasons']}")
    for k in rows:
        print(f"{k['name']:16s} turnover {k['turnover_ann']:5.2f}x  {k['exit_reasons']}")

    suffix = f"_{only_group}" if only_group else ""
    outp = paths.OUT_STRAT_LAB / f"x_s_execution{suffix}.json"
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(
        {"canonical": base, "gap_decomposition": gd, "hold_distribution": hd,
         "variants": rows, "yearly_canonical": yearly_table(base_nav).to_dicts()},
        indent=2, default=str, ensure_ascii=False))
    print(f"\n寫出 {outp}")


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else None)
