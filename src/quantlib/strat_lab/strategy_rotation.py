"""策略輪動對決:「每個時期都用該時期最強的策略」vs「一直用同一個策略」。

## 使用者的問題(2026-07-24 最終澄清)
「不是針對 S,是**從頭研發**——**每個不同時期的市場都用該時期最強的策略去交易**。」
這不是換參數、也不是換因子,而是 **strategy rotation / regime-adaptive strategy selection**:
維持一個結構迥異的策略池,每期依近期表現挑最強的來交易。本檔直接測它成不成立。

## 策略池(10 個結構迥異的策略,同一引擎/同一 universe/同一成本 → 可比)
用 `run_s_full` 的 hooks 統一驅動(同 ExecSpec 成本、同 next_open 成交、同 5 席等權):
| 代號 | 選股邏輯 | 池 | 出場 |
|---|---|---|---|
| S_EVENT | S 六因子(營收事件) | 營收新鮮 ≤7 + cfo 閘 | 訊號時效 26 天 + trail35/time30/15 |
| MOM2 | high_52w × close_pos_20 | 全 eligible | trail35 + time60 |
| MOM_LV | + lowvol_60(低波動) | 全 eligible | trail35 + time60 |
| REV_MOM | rev_yoy_accel × high_52w | 全 eligible(**無**新鮮閘) | trail35 + time60 |
| BREAKOUT | donchian_60(60 日新高突破) | 全 eligible | trail25 + time40 |
| RANGE | range_pos_60(區間位置) | 全 eligible | trail35 + time60 |
| QUALITY | cfo_ni_ratio_ttm × 低波 | 全 eligible | trail35 + time120 |
| YIELD | dy(殖利率,價值代理) | 全 eligible | trail35 + time120 |
| LOWVOL | lowvol_60 單因子(防禦) | 全 eligible | trail35 + time120 |
| CONTRA | 反向動能(1−high_52w) | 全 eligible | trail35 + time60 |

## 輪動規則(每期重選,PIT)
每 **季/年** 初,依**前 L 個月**各策略的 Sortino(D2 主尺)排名,選 top-1(另測 top-3 等權),
交易下一期;期末再選。切換成本以保守 **0.5%/次** 計(平倉+建倉的摩擦)。
對照:① always-S_EVENT ② 等權持有全部 10 策略(不輪動)③ **事後**最佳單一策略(上界參考)。

## 判讀
輪動若勝 always-S 且勝等權 → 「用當期最強策略」成立(使用者的想法對);
若輸 → 近期表現對下期無預測力(= 追績效效應),固定最強策略是對的。

Run: uv run --project . python -m quantlib.strat_lab.strategy_rotation
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExitSpec, PortSpec
from quantlib.apex.metrics import perf_stats
from quantlib.apex.strategy_s import DS, WREL, prep_cached, run_s_full

_OPEN = dict(_fresh_days=9999, _cfo_q=0.0, _stale_days=99999)   # 取消 S 專屬的池閘與時效出場


def _sfn(cols: list[str], inverse: tuple = ()):
    def fn(df: pl.DataFrame) -> pl.DataFrame:
        expr = None
        for c_ in cols:
            r = (pl.col(c_).rank() / pl.len()).over("date")
            if c_ in inverse:
                r = 1.0 - r
            expr = r if expr is None else expr * r
        return df.with_columns(expr.alias("score"))
    return fn


#: name -> (score 因子, 反向因子, exit_spec, 額外 kwargs)
BOOKS: dict[str, tuple] = {
    "S_EVENT":  (list(WREL), (), ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15), {}),
    "MOM2":     (["high_52w", "close_pos_20"], (), ExitSpec(trailing_stop=0.35, time_stop=60), _OPEN),
    "MOM_LV":   (["high_52w", "close_pos_20", "lowvol_60"], (), ExitSpec(trailing_stop=0.35, time_stop=60), _OPEN),
    "REV_MOM":  (["rev_yoy_accel", "high_52w"], (), ExitSpec(trailing_stop=0.35, time_stop=60), _OPEN),
    "BREAKOUT": (["donchian_60", "high_52w"], (), ExitSpec(trailing_stop=0.25, time_stop=40), _OPEN),
    "RANGE":    (["range_pos_60"], (), ExitSpec(trailing_stop=0.35, time_stop=60), _OPEN),
    "QUALITY":  (["cfo_ni_ratio_ttm", "lowvol_60"], (), ExitSpec(trailing_stop=0.35, time_stop=120), _OPEN),
    "YIELD":    (["dy"], (), ExitSpec(trailing_stop=0.35, time_stop=120), _OPEN),
    "LOWVOL":   (["lowvol_60"], (), ExitSpec(trailing_stop=0.35, time_stop=120), _OPEN),
    "CONTRA":   (["high_52w", "close_pos_20"], ("high_52w", "close_pos_20"),
                 ExitSpec(trailing_stop=0.35, time_stop=60), _OPEN),
}

SWITCH_COST = 0.005     # 每次換策略的保守摩擦(平倉+建倉)


def _daily_returns(nav: pl.DataFrame) -> pl.DataFrame:
    return (nav.sort("date")
            .with_columns((pl.col("nav") / pl.col("nav").shift(1) - 1).alias("r"))
            .drop_nulls().select(["date", "r"]))


def _sortino(r: np.ndarray) -> float:
    if len(r) < 20:
        return -9.0
    d = r[r < 0]
    return float(r.mean() / d.std() * np.sqrt(252)) if len(d) > 2 and d.std() > 0 else 0.0


def _rotate(rets: dict[str, pl.DataFrame], dates: list, period: str, lookback_m: int,
            top_n: int = 1) -> tuple[np.ndarray, list]:
    """每期初依前 lookback_m 月 Sortino 選 top_n 策略等權持有。回 (日報酬序列, 選擇紀錄)。"""
    wide = None
    for name, df in rets.items():
        c = df.rename({"r": name})
        wide = c if wide is None else wide.join(c, on="date", how="inner")
    wide = wide.sort("date")
    ds = wide["date"].to_list()
    names = [c for c in wide.columns if c != "date"]
    mat = wide.select(names).to_numpy()
    # 期界
    bounds = []
    for i, d in enumerate(ds):
        if i == 0:
            continue
        prev = ds[i - 1]
        if (period == "Y" and d.year != prev.year) or (period == "Q" and (d.month - 1) // 3 != (prev.month - 1) // 3):
            bounds.append(i)
    out = np.zeros(len(ds))
    picks, prev_pick = [], None
    for bi, start in enumerate(bounds):
        end = bounds[bi + 1] if bi + 1 < len(bounds) else len(ds)
        lb_start = max(0, start - int(lookback_m * 21))
        if lb_start >= start - 20:
            continue
        sc = [_sortino(mat[lb_start:start, j]) for j in range(len(names))]
        order = np.argsort(-np.array(sc))[:top_n]
        chosen = [names[j] for j in order]
        picks.append((ds[start], chosen, [round(sc[j], 2) for j in order]))
        seg = mat[start:end, order].mean(axis=1)
        if prev_pick is not None and set(chosen) != set(prev_pick) and len(seg):
            seg = seg.copy()
            seg[0] -= SWITCH_COST
        prev_pick = chosen
        out[start:end] = seg
    return out, picks


def _cagr(r: np.ndarray, n_days: int) -> float:
    nz = r[r != 0]
    if len(nz) < 50:
        return float("nan")
    return float(np.prod(1 + r) ** (252 / max(n_days, 1)) - 1)


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    print(f"[rot] 跑 {len(BOOKS)} 個結構迥異的策略(同引擎/同成本)…", flush=True)
    navs, rets = {}, {}
    for name, (cols, inv, ex, kw) in BOOKS.items():
        nav, _ = run_s_full(panel, feat, elig, DS,
                            _score_fn=_sfn(cols, inv), _wrel={c: 1.0 for c in cols},
                            _exit_spec=ex, _port_spec=PortSpec(n_slots=5, max_new_per_day=2), **kw)
        navs[name] = nav.sort("date")
        rets[name] = _daily_returns(navs[name])
        st = perf_stats(navs[name])
        print(f"  {name:<10} CAGR {st['cagr']:>+8.1%}  Sortino {st['sortino']:>5.2f}  MDD {st['mdd']:>+7.1%}", flush=True)

    common = None
    for df in rets.values():
        common = df.select("date") if common is None else common.join(df.select("date"), on="date", how="inner")
    dates = common.sort("date")["date"].to_list()
    rets = {k: v.join(common, on="date", how="semi").sort("date") for k, v in rets.items()}
    nd = len(dates)

    print(f"\n=== 策略輪動 vs 固定(共同期間 {dates[0]}~{dates[-1]},{nd} 交易日;切換成本 {SWITCH_COST:.1%}/次)===")
    print(f"  {'規則':<28}{'CAGR':>10}{'Sortino':>10}{'MDD':>9}")
    base_r = rets["S_EVENT"]["r"].to_numpy()
    def _show(tag, r):
        mdd = float((np.minimum.accumulate(np.cumprod(1 + r) / np.maximum.accumulate(np.cumprod(1 + r))) - 1).min())
        print(f"  {tag:<28}{_cagr(r, nd):>+9.1%}{_sortino(r):>10.2f}{mdd:>+8.1%}")
    _show("固定 S_EVENT(對照)", base_r)
    eq = np.mean(np.column_stack([rets[k]["r"].to_numpy() for k in BOOKS]), axis=1)
    _show("等權持有全部 10 策略", eq)
    best_name = max(BOOKS, key=lambda k: _sortino(rets[k]["r"].to_numpy()))
    _show(f"事後最佳單一({best_name},上界)", rets[best_name]["r"].to_numpy())

    all_picks = {}
    for period, plab in (("Y", "每年"), ("Q", "每季")):
        for lb in (6, 12, 24):
            for tn in (1, 3):
                r, picks = _rotate(rets, dates, period, lb, tn)
                _show(f"{plab}重選 top{tn}(看前 {lb} 月)", r)
                all_picks[(period, lb, tn)] = picks

    print("\n=== 每年重選 top1(看前 12 月)的實際選擇 ===")
    for d, ch, sc in all_picks.get(("Y", 12, 1), [])[:12]:
        print(f"  {d}: {ch[0]:<10}(前期 Sortino {sc[0]})")
    print("\n  判讀:輪動若勝『固定 S_EVENT』且勝『等權』→「每期用當期最強」成立;"
          "若輸 → 近期表現對下期無預測力(追績效效應),固定最強策略才對。")


if __name__ == "__main__":
    main()
