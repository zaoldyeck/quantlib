"""EV62:前瞻視窗(#6)定案——真實 NAV 模擬,重疊部位 + 逐筆成本。

## 為什麼 EV60 的年化不夠

EV60 用「每站位前 15 檔的平均報酬」再年化來比較 horizon。那有兩個近似:

1. **重疊部位**:站位是月頻,持有 60~120 交易日 ⇒ 同時有 3~6 批部位在場。
   「平均單期報酬 ^ (250/H)」假設可以連續複利,但重疊配置的實際複利路徑不是那樣。
2. **交易成本未入模**:H 越短換手越兇。而且富邦費率有**最低收費**(整股 20 元、
   零股 1 元),小額單的實質費率會發散——這對「短持有期是否真的比較好」是決定性的,
   因為短持有期意味著更多筆、每筆更小。

本模組把兩件事都做實:逐日標記 NAV、逐筆計費(費率唯一真源 `execsim.broker_fee`)。

## 資本規模不是魔術數字,是使用者事實——但它會改變答案

最低收費讓成本佔比隨單筆金額變小而發散,所以**最佳 horizon 可能取決於帳戶大小**:
資本越小,短持有期被固定成本吃掉的越多。故本模組**掃資本規模**並各自報結果,
而不是挑一個數字。這同時處理了流動性:小資本下流動性不綁,NAV 拿得到;資本變大時
一併報出**隱含參與率**(單筆金額 ÷ ADV20),讓「這個 NAV 買不買得到」變成看得見的數字。

Run: uv run --project . python -m quantlib.evergreen.ev62_horizon_nav
依賴 cache: 是。長任務,建議背景跑。
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.evergreen import ev57_station_sample as ev57
from quantlib.evergreen.ev57_station_sample import _panel, _stations, _universe
from quantlib.evergreen.ev60_threshold_study import (FEATURES, TOP_N, TRAIN_END,
                                                     _rich_features)
from quantlib.apex.validate import block_bootstrap_cagr
from quantlib.evergreen.ev36_walkforward import seg_kpi
from quantlib.execsim.broker_fee import FubonFeeSchedule, MonthlyFeeMeter

OUT = paths.OUT / "ev62_horizon_nav.json"

#: 掃描的前瞻視窗。EV60 已把 h=20 因可交易性作廢、h=250 明顯較差,故收斂到此區間。
HORIZONS = (40, 60, 80, 100, 120)
#: 掃描的資本規模(新台幣)。不是挑一個——最低收費讓最佳 horizon 取決於帳戶大小,
#: 所以答案本來就是「視資本而定」,報成一張表才誠實。
CAPITALS = (300_000, 1_000_000, 3_000_000, 10_000_000)
#: 標籤定義:實際入帳報酬(EV60 導出,見 ev60_no_magic_numbers.md)。
#: 門檻不再寫死——**(h, θ) 必須聯合掃**:θ=30% 是在 h=120 下、用近似指標選的;
#: h=60 是在 θ 寫死 0.30 下、用真實 NAV 選的。兩者從未在同一個指標上一起比過,
#: 而那正是「定案」的最後一哩。
LABEL_TRAIL = 0.25
THRESHOLDS = (0.20, 0.30, 0.50)


def _fit(u: pl.DataFrame, label_col: str, thr: float) -> pl.DataFrame:
    import lightgbm as lgb
    u = u.with_columns((pl.col(label_col) >= thr).cast(pl.Int8).alias("y"))
    tr, te = u.filter(pl.col("date") <= TRAIN_END), u.filter(pl.col("date") > TRAIN_END)
    if tr["y"].sum() < 50 or te["y"].sum() < 20:
        return pl.DataFrame()
    m = lgb.train(
        {"objective": "binary", "learning_rate": 0.05, "num_leaves": 31,
         "min_data_in_leaf": 200, "feature_fraction": 0.8, "bagging_fraction": 0.8,
         "bagging_freq": 1, "verbose": -1, "seed": 20260806},
        lgb.Dataset(tr.select(FEATURES).to_pandas(), label=tr["y"].to_numpy()),
        num_boost_round=200)
    return te.with_columns(pl.Series("score", m.predict(te.select(FEATURES).to_pandas())))


def simulate(panel: pl.DataFrame, picks: pl.DataFrame, horizon: int, capital: float,
             trail: float = 0.25, n_slots: int = TOP_N) -> dict:
    """逐日 NAV,重疊部位,逐筆計費。

    配置:每個站位對每檔投入 `NAV / (n_slots × cohorts)`,`cohorts = ceil(H / 站位間距)`
    ——這樣同時在場的部位總額約等於 NAV,不會因重疊而超額槓桿或閒置。站位間距由
    實際站位日算出(不寫死 21),因為交易日曆有休市與補班。

    出場:自峰值回撤 `trail` 或滿 `horizon` 交易日,先到先出。**逐日重放**是專案的
    出場語義鐵律——只看期末價會把中途已觸發的出場當成沒發生。
    """
    C = "company_code"
    ser = panel.sort([C, "date"]).select([C, "date", "close", "trade_value"])
    by_code: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for (code,), g in ser.group_by([C], maintain_order=True):
        by_code[code] = (g["date"].to_numpy(), g["close"].to_numpy().astype(float))

    stations = sorted(picks["date"].unique().to_list())
    if len(stations) < 3:
        return {}
    gap = np.median(np.diff([np.datetime64(s) for s in stations]).astype("timedelta64[D]")
                    .astype(int)) * (250 / 365.25)          # 站位間距換算交易日
    cohorts = max(1, math.ceil(horizon / max(gap, 1)))
    per_pos = capital / (n_slots * cohorts)

    fee = MonthlyFeeMeter(FubonFeeSchedule())
    all_days = sorted(panel["date"].unique().to_list())
    day_idx = {d: i for i, d in enumerate(all_days)}
    open_pos: list[dict] = []
    cash = capital
    nav_path: list[tuple[Date, float]] = []
    costs = {"commission": 0.0, "tax": 0.0}
    n_trades = 0
    part_rates: list[float] = []
    # polars 的 group_by 回傳的鍵是 **tuple**,不是純值。寫成 `for s, g in`
    # 會讓 key 變成 `(date,)`,而查表用純 date ⇒ 永遠落空、零筆交易,
    # 而模擬照樣「成功」跑完並回報 CAGR 0.00%——無聲失效。
    by_station = {s: g for (s,), g in picks.group_by("date")}

    for d in all_days:
        if d < stations[0]:
            continue
        # 1) 到期或觸發停損的部位先平倉
        still: list[dict] = []
        for p in open_pos:
            dates, px = by_code[p["code"]]
            i = int(np.searchsorted(dates, np.datetime64(d)))
            if i >= len(px) or dates[i] != np.datetime64(d):
                still.append(p)
                continue
            p["peak"] = max(p["peak"], px[i])
            held = day_idx[d] - p["i0"]
            hit = px[i] <= p["peak"] * (1 - trail)
            if hit or held >= horizon:
                gross = p["shares"] * px[i]
                c = fee.commission(d, gross, shares=p["shares"])
                tx = fee.sell_tax(gross)
                cash += gross - c - tx
                costs["commission"] += c
                costs["tax"] += tx
                n_trades += 1
            else:
                still.append(p)
        open_pos = still

        # 2) 站位日開新倉(以站位次一交易日收盤成交,對齊 EV60 的進場語義)
        if d in day_idx and day_idx[d] > 0 and all_days[day_idx[d] - 1] in by_station:
            g = by_station[all_days[day_idx[d] - 1]]
            for r in g.iter_rows(named=True):
                dates, px = by_code.get(r["company_code"], (None, None))
                if dates is None:
                    continue
                i = int(np.searchsorted(dates, np.datetime64(d)))
                if i >= len(px) or dates[i] != np.datetime64(d) or px[i] <= 0:
                    continue
                budget = min(per_pos, cash)
                shares = math.floor(budget / px[i])
                if shares <= 0:
                    continue
                gross = shares * px[i]
                c = fee.commission(d, gross, shares=shares)
                if gross + c > cash:
                    continue
                cash -= gross + c
                costs["commission"] += c
                n_trades += 1
                if r.get("adv20"):
                    part_rates.append(gross / r["adv20"])
                open_pos.append({"code": r["company_code"], "shares": shares,
                                 "i0": day_idx[d], "peak": px[i]})

        # 3) 逐日標記 NAV
        mv = 0.0
        for p in open_pos:
            dates, px = by_code[p["code"]]
            i = int(np.searchsorted(dates, np.datetime64(d)))
            mv += p["shares"] * px[min(i, len(px) - 1)]
        nav_path.append((d, cash + mv))

    if len(nav_path) < 50:
        return {}
    nav = np.array([v for _, v in nav_path])
    yrs = (nav_path[-1][0] - nav_path[0][0]).days / 365.25
    cagr = (nav[-1] / nav[0]) ** (1 / yrs) - 1 if yrs > 0 and nav[-1] > 0 else -1.0
    dd = nav / np.maximum.accumulate(nav) - 1
    r = np.diff(nav) / nav[:-1]
    # **決勝指標用專案自己的**:`p5` = block bootstrap CAGR 的 5% 下界,
    # live_config 記載 selection_metric = "p5(EV44 裁決,KPI v3 主尺)"。
    # 用既有指標而不是我另挑一個——那才是「不引入新的選擇」。
    # 它同時吃到報酬水準與路徑不確定性,正好治本次的非單調(雜訊)問題。
    kpi = seg_kpi(pl.DataFrame({"date": [x for x, _ in nav_path], "nav": nav}))
    p5 = float(block_bootstrap_cagr(
        pl.DataFrame({"date": [x for x, _ in nav_path], "nav": nav}), n_boot=500)["ci_lo"])
    return {"cagr": round(float(cagr), 4), "mdd": round(float(dd.min()), 4),
            "martin": round(kpi["martin"], 3), "p5": round(p5, 4),
            "sharpe": round(float(r.mean() / r.std() * math.sqrt(250)), 3) if r.std() else None,
            "final_nav": round(float(nav[-1]), 0), "years": round(yrs, 2),
            "cohorts": cohorts, "per_position_ntd": round(per_pos, 0),
            "n_trades": n_trades,
            "cost_pct_of_capital": round((costs["commission"] + costs["tax"]) / capital, 4),
            "cost_drag_annual": round(((costs["commission"] + costs["tax"]) / capital) / yrs, 4)
            if yrs > 0 else None,
            "median_participation": round(float(np.median(part_rates)), 4) if part_rates else None}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizons", default=",".join(map(str, HORIZONS)))
    ap.add_argument("--capitals", default=",".join(map(str, CAPITALS)))
    ap.add_argument("--thresholds", default=",".join(map(str, THRESHOLDS)))
    a = ap.parse_args()

    con = data.connect()
    res: dict[str, dict] = {}
    for h in [int(x) for x in a.horizons.split(",")]:
        ev57.FWD_DAYS = h
        print(f"\n── horizon {h} 交易日 ──", flush=True)
        panel = _panel(con)
        u = _universe(con, _rich_features(panel), _stations(panel))
        from quantlib.evergreen.ev60_threshold_study import _trail_returns
        tr = _trail_returns(panel, u.select(["company_code", "date"]), (LABEL_TRAIL,))
        if tr.is_empty():
            continue
        u = u.join(tr, on=["company_code", "date"], how="left")
        for thr in [float(x) for x in a.thresholds.split(",")]:
            scored = _fit(u, f"trail{int(LABEL_TRAIL * 100)}", thr)
            if scored.is_empty():
                continue
            base = float(scored["y"].mean())
            picks = (scored.sort(["date", "score"], descending=[False, True])
                     .group_by("date", maintain_order=True).head(TOP_N)
                     .select(["company_code", "date", "adv20"]))
            for cap in [int(x) for x in a.capitals.split(",")]:
                k = simulate(panel, picks, h, float(cap))
                if not k:
                    continue
                k["base_rate"] = round(base, 4)
                res[f"h={h},thr={thr},cap={cap}"] = k
                print(f"  θ={thr:>4.0%} 基準率 {base:>6.2%}  資本 {cap:>10,}"
                      f"  CAGR {k['cagr']:>7.2%}  MDD {k['mdd']:>7.2%}"
                      f"  **p5 {k['p5']:>7.2%}**  Martin {k['martin']:>6.2f}"
                      f"  成本/年 {k['cost_drag_annual']:.2%}")

    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\n→ {OUT}")
    # 依專案主尺 p5 決勝,不用 CAGR——CAGR 在本次掃描上非單調(h=80 反低於 h=100),
    # 那是雜訊的跡象;p5 把路徑不確定性算進去,正是為這種情況設計的。
    best = max(res.items(), key=lambda kv: kv[1]["p5"]) if res else (None, None)
    print(f"依 p5(專案主尺)最佳:{best[0]} → {best[1]}")


if __name__ == "__main__":
    main()
