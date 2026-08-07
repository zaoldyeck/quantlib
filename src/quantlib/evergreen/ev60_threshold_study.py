"""EV60:暴漲門檻與母體選擇的論證——把 `SURGE_MIN = 0.80` 從拍腦袋變成量出來的。

## 這支要回答的問題,以及為什麼原本的問法是錯的

EV57 用 `SURGE_MIN = 0.80`,理由寫的是「沿用『大幅上漲』語義以與舊工作可比」。
那不是出處,是慣例。而依專案鐵律,**說不出「這個值從哪個量測得來」的數字就是魔術
數字**,要嘛量出來,要嘛刪掉。

但直接問「多少 % 才算暴漲」是問錯了——那是語義問題,沒有客觀答案。門檻的**職責**
才是可判定的:它是蒸餾的**訓練目標**,而訓練目標的好壞只有一個標準——

    **拿它當目標訓練出來的判別力,套到下游選股之後,賺不賺錢。**

所以本研究把門檻掃過一遍,對每個候選值問同一組可量測的問題:

1. **母體基準率** p(θ) —— 盲判的機率錨,也決定樣本夠不夠分層。
2. **樣本充足性** —— regime × 流動性層各格能不能填滿配額。填不滿的門檻直接出局,
   這是硬約束不是目標。
3. **樣本外可學性** —— 用**只含站位當日可得資訊**的量化特徵訓練一個固定超參的模型,
   train 2008-2015 / test 2016-2021,報 OOS AUC。這是「這個類別有沒有結構」的**下界**:
   量化模型學得到的,任何判別者都學得到;量化模型學不到的,質化**未必**學不到。
   (這條假設是本研究最大的限制,寫在這裡而不是藏在結論裡。)
4. **可捕捉率** —— 標籤說「120 日內最高漲 θ」,但策略吃不到最高點。實測「標籤報酬」
   與「一般化出場實際拿到的報酬」的比值。**比值低的門檻是在標記一種賺不到的現象。**
5. **決定性指標:同技能下的池子報酬** —— 用第 3 步的模型在 OOS 每個站位選前 N 檔,
   實際算那個池子的報酬。**這是唯一同時吃到「稀有度、可學性、可捕捉率」三件事的
   單一數字**,而且單位是錢。掃 θ 取最大者。

## 為什麼標籤與報酬的口徑分歧本身就是個發現

現行標籤是「未來 120 交易日**區間最高**還原報酬 ≥ 80%」。而下游引擎用移動停利/停損/
時間停損出場——一檔漲到 +80% 再全部吐回去的股票,**標籤算正例,實際賺不到**。
標籤與付款的口徑不同,是量化系統的頭號無聲殺手。第 4 步就是量這件事。

Run:
  uv run --project . python -m quantlib.evergreen.ev60_threshold_study
  uv run --project . python -m quantlib.evergreen.ev60_threshold_study --universe-sweep
依賴 cache: 是。長任務,建議背景跑。
"""
from __future__ import annotations

import argparse
import json
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.evergreen import ev57_station_sample as ev57
from quantlib.evergreen.ev58_tables import auc as _auc_nk
from quantlib.evergreen.ev57_station_sample import _panel, _stations, _universe

FWD_DAYS = ev57.FWD_DAYS

OUT = paths.OUT / "ev60_threshold_study.json"

#: 候選門檻。下界 20% 是「稱不上大幅上漲」的區域,上界 200% 是樣本已稀少到分層填不滿
#: 的區域——兩端刻意跨過可用範圍,好讓最佳值落在內部而不是撞邊界(撞邊界代表掃描範圍
#: 選錯,結論不可信)。
THRESHOLDS = (0.20, 0.30, 0.40, 0.50, 0.60, 0.80, 1.00, 1.25, 1.50, 2.00)

#: train/test 以**時間**切,不是隨機切。隨機切會讓同一個站位的鄰近觀測同時出現在
#: 兩邊,OOS AUC 被洩漏撐高——而我們正是要用它當「可學性下界」。
TRAIN_END = Date(2015, 12, 31)

#: 只用站位當日可得的量化特徵。**任何含未來資訊的欄位都不得進來**,否則整個
#: 「可學性」的量測失去意義。
FEATURES = ("ret20", "ret60", "ret120", "pct_below_250d_high",
            "adv_dec", "mom_dec", "turnover20", "vol60", "log_adv20")

#: 每站位選幾檔——對齊下游標記 Agent 的實際預算(上限 15 檔)。
TOP_N = 15
#: 一般化出場:自站位次日進場,以區間高點回撤 TRAIL 出場,否則持有到 FWD_DAYS。
#: 25% 不是從 Evergreen 抄來的參數,是為了讓「可捕捉率」有個中性的量尺;
#: 第 4 步同時報 0%(不停損,持有到期)與 15%/35% 當敏感度,結論不靠單一值。
TRAILS = (0.15, 0.25, 0.35)


def _rich_features(panel: pl.DataFrame) -> pl.DataFrame:
    """在 EV57 的前瞻/前期特徵之外,補齊「站位當日可得」的量化特徵與實際出場報酬。"""
    C = "company_code"
    p = ev57._features(panel).with_columns([
        (pl.col("close") / pl.col("close").shift(20).over(C) - 1).alias("ret20"),
        (pl.col("close") / pl.col("close").shift(60).over(C) - 1).alias("ret60"),
        (pl.col("close") / pl.col("close").shift(120).over(C) - 1).alias("ret120"),
        (pl.col("close") / pl.col("close").rolling_max(250, min_samples=60).over(C) - 1)
        .alias("pct_below_250d_high"),
        (pl.col("close") / pl.col("close").shift(1).over(C) - 1)
        .rolling_std(60, min_samples=40).over(C).alias("vol60"),
        # 前瞻**終值**報酬:與「區間最高」對照,量的是「漲上去有沒有留住」
        (pl.col("close").shift(-ev57.FWD_DAYS).over(C) / pl.col("close") - 1).alias("fwd_end_ret"),
    ])
    return p.with_columns([
        (pl.col("trade_value") / pl.col("adv20")).alias("turnover20"),
        pl.col("adv20").log1p().alias("log_adv20"),
    ])


def _trail_returns(panel: pl.DataFrame, keys: pl.DataFrame,
                   trails: tuple[float, ...]) -> pl.DataFrame:
    """對每個 (code, station) 算各種移動停利出場的實際報酬。**一次預處理,全部向量化。**

    第一版每列做一次 polars filter,245k 列 = 245k 次 DataFrame 掃描,踩中專案的
    極速鐵律「迴圈內禁止重複預處理」。改法:每檔的收盤價**只轉成 numpy 一次**,
    站位以 searchsorted 定位,窗內用 `np.maximum.accumulate` 求峰值、`argmax` 求
    首次觸發——每個 key 變成 O(窗長) 的純 numpy,沒有任何 DataFrame 操作。

    語義不變:進場價 = 站位次一交易日收盤;峰值由價格歷史逐日重算(不是期末快照);
    自峰值回撤 `trail` 即出場,否則持有到第 `FWD_DAYS` 日。逐日重放是專案的出場鐵律
    ——只看期末價會把「中途已觸發」的出場當成沒發生。
    """
    C = "company_code"
    cols = {f"trail{int(t * 100)}": [] for t in trails}
    out_code: list[str] = []
    out_date: list[Date] = []
    ser = panel.sort([C, "date"]).select([C, "date", "close"])
    by_code: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for (code,), g in ser.group_by([C], maintain_order=True):
        by_code[code] = (g["date"].to_numpy(), g["close"].to_numpy().astype(float))

    for code, d in keys.iter_rows():
        pair = by_code.get(code)
        if pair is None:
            continue
        dates, px = pair
        i = int(np.searchsorted(dates, np.datetime64(d)))
        if i >= len(px) or dates[i] != np.datetime64(d):
            continue
        w = px[i + 1: i + 1 + ev57.FWD_DAYS]
        if w.size < 2 or w[0] <= 0:
            continue
        peak = np.maximum.accumulate(w)
        out_code.append(code)
        out_date.append(d)
        for tr in trails:
            if tr <= 0:
                cols[f"trail{int(tr * 100)}"].append(w[-1] / w[0] - 1.0)
                continue
            below = w <= peak * (1 - tr)
            k = int(np.argmax(below)) if below.any() else -1
            cols[f"trail{int(tr * 100)}"].append(w[k] / w[0] - 1.0)
    if not out_code:
        return pl.DataFrame()
    return pl.DataFrame({C: out_code, "date": out_date, **cols})


def _fit_score(u: pl.DataFrame, thr: float, col: str = "fwd_max_ret") -> pl.DataFrame:
    """固定超參的 LightGBM:train 2008-2015 → 預測 2016-2021。

    超參固定、特徵固定、切分固定,**唯一變動的是訓練目標 θ**——這樣掃出來的差異
    才歸因得到門檻本身,而不是調參。
    """
    import lightgbm as lgb

    u = u.with_columns((pl.col(col) >= thr).cast(pl.Int8).alias("y"))
    tr = u.filter(pl.col("date") <= TRAIN_END)
    te = u.filter(pl.col("date") > TRAIN_END)
    if tr["y"].sum() < 50 or te["y"].sum() < 20:
        return pl.DataFrame()
    m = lgb.train(
        {"objective": "binary", "learning_rate": 0.05, "num_leaves": 31,
         "min_data_in_leaf": 200, "feature_fraction": 0.8, "bagging_fraction": 0.8,
         "bagging_freq": 1, "verbose": -1, "seed": 20260806},
        lgb.Dataset(tr.select(FEATURES).to_pandas(), label=tr["y"].to_numpy()),
        num_boost_round=200)
    return te.with_columns(pl.Series("score", m.predict(te.select(FEATURES).to_pandas())))


def _auc(score: np.ndarray, y: np.ndarray) -> float:
    """AUC——**轉呼叫 `ev58_tables.auc`,不自己再實作一次。**

    第一版在這裡寫了第二份 AUC。同一個統計量兩份實作必然漂移(其中一份修了平手
    處理、另一份沒有),而漂移之後兩邊的數字看起來都合理、卻不可比——這正是專案
    「引擎唯一真源」鐵律要防的事,統計原語同樣適用。
    """
    return _auc_nk(list(map(float, score)), list(map(int, y)))[0]


def study(u: pl.DataFrame, ret_cols: list[str], col: str = "fwd_max_ret") -> list[dict]:
    """掃門檻。`col` 是**標籤定義**——換成 `trail25` 就是拿「實際入帳報酬」當目標。"""
    rows = []
    for thr in THRESHOLDS:
        base = float((u[col] >= thr).mean())
        pos = u.filter(pl.col(col) >= thr)
        cells = (pos.group_by(["regime", "tier"]).agg(pl.len().alias("n"))
                 if "regime" in u.columns else
                 pos.group_by("tier").agg(pl.len().alias("n")))
        min_cell = int(cells["n"].min()) if cells.height else 0

        scored = _fit_score(u, thr, col)
        row = {"threshold": thr, "base_rate": round(base, 5),
               "n_positive": pos.height, "min_cell": min_cell,
               "cells_filled_18": bool(min_cell >= 18)}
        # 可捕捉率:標籤說的漲幅,實際出場拿得到多少
        for rc in ret_cols:
            got = pos[rc].drop_nulls()
            if got.len():
                row[f"capture_{rc}_median"] = round(float(got.median()), 4)
                row[f"capture_{rc}_ratio"] = round(float(got.median()) / thr, 3)
        if scored.height:
            y = scored["y"].to_numpy()
            row["oos_auc"] = round(_auc(scored["score"].to_numpy(), y), 4)
            row["oos_n"] = scored.height
            row["oos_pos"] = int(y.sum())
            # 決定性指標:同一個模型在每個站位選前 N 檔,那個池子實際賺多少
            picks = (scored.sort(["date", "score"], descending=[False, True])
                     .group_by("date", maintain_order=True).head(TOP_N))
            row["picked_per_station"] = round(picks.height / picks["date"].n_unique(), 1)
            row["hit_rate_at_topN"] = round(float(picks["y"].mean()), 4)
            # **可交易性**:報酬再高,買不到就不是報酬。短持有期的模型很容易去挑
            # 流動性極差的小型股——那裡的「報酬」是報價跳動,不是可成交的價格。
            # 專案鐵律:「好到不像話」的數字優先假設是自己的設計錯了。
            row["pick_adv_dec_median"] = float(picks["adv_dec"].median())
            row["pick_adv_dec_p25"] = float(picks["adv_dec"].quantile(0.25))
            row["pick_bottom_decile_share"] = round(float((picks["adv_dec"] == 0).mean()), 4)
            row["pick_adv20_median_ntd"] = round(float(picks["adv20"].median()), 0)
            row["lift_at_topN"] = round(float(picks["y"].mean()) / base, 2) if base else None
            for rc in ret_cols:
                v = picks[rc].drop_nulls()
                if v.len():
                    m = float(v.mean())
                    row[f"pool_{rc}_mean"] = round(m, 4)
                    row[f"pool_{rc}_median"] = round(float(v.median()), 4)
                    # **年化**:不同 horizon 的單期報酬不可直接比——期數不同。
                    # 一年約 250 個交易日(實測 2008-2021 年均 246,取整 250)。
                    # 這是本報表自己差點犯的混口徑:h=250 的 18.25% 看起來遠勝
                    # h=60 的 8.45%,年化後卻是 18.3% vs 40.2%,結論完全相反。
                    row[f"pool_{rc}_ann"] = round((1 + m) ** (250 / ev57.FWD_DAYS) - 1, 4)
        rows.append(row)
    return rows


# ------------------------------------------------------------------ 標籤定義掃描

#: 候選標籤定義。**這才是「選什麼標的」的核心問題**——現行標籤是「120 日內區間最高
#: 報酬 ≥ θ」,但下游用移動停利出場:一檔漲到 +80% 再全部吐回去,標籤算正例、實際
#: 賺不到。標籤與付款的口徑不同,是量化系統的頭號無聲殺手。
LABEL_DEFS = {
    "max_120d": "fwd_max_ret",        # 現行:區間最高(策略吃不到)
    "end_120d": "fwd_end_ret",        # 終值:漲上去有沒有留住
    "trail25": "trail25",             # 一般化出場實際拿到的
}


def label_sweep(u: pl.DataFrame, ret_cols: list[str]) -> list[dict]:
    """同一個門檻下,換不同的**標籤定義**,看哪一種訓練出來的池子最賺錢。

    比較必須在同一個「稀有度」上進行,否則比到的是門檻不是定義。故每個定義各自
    取分位數門檻,讓正例比例對齊現行標籤在 θ=0.8 的基準率。
    """
    target = float((u["fwd_max_ret"] >= 0.80).mean())
    rows = []
    for name, col in LABEL_DEFS.items():
        if col not in u.columns:
            continue
        v = u[col].drop_nulls()
        if v.is_empty():
            continue
        thr = float(v.quantile(1 - target))
        uu = u.with_columns((pl.col(col) >= thr).cast(pl.Int8).alias("y"))
        tr, te = (uu.filter(pl.col("date") <= TRAIN_END),
                  uu.filter(pl.col("date") > TRAIN_END))
        if tr["y"].sum() < 50 or te["y"].sum() < 20:
            continue
        import lightgbm as lgb
        m = lgb.train(
            {"objective": "binary", "learning_rate": 0.05, "num_leaves": 31,
             "min_data_in_leaf": 200, "feature_fraction": 0.8, "bagging_fraction": 0.8,
             "bagging_freq": 1, "verbose": -1, "seed": 20260806},
            lgb.Dataset(tr.select(FEATURES).to_pandas(), label=tr["y"].to_numpy()),
            num_boost_round=200)
        te = te.with_columns(pl.Series("score", m.predict(te.select(FEATURES).to_pandas())))
        picks = (te.sort(["date", "score"], descending=[False, True])
                 .group_by("date", maintain_order=True).head(TOP_N))
        row = {"label": name, "column": col, "matched_threshold": round(thr, 4),
               "base_rate": round(float(uu["y"].mean()), 5),
               "oos_auc": round(_auc(te["score"].to_numpy(), te["y"].to_numpy()), 4)}
        for rc in ret_cols:
            w = picks[rc].drop_nulls()
            if w.len():
                row[f"pool_{rc}_mean"] = round(float(w.mean()), 4)
        rows.append(row)
    return rows


# ------------------------------------------------------------------ 負例硬度帶

#: 候選負例帶(前瞻區間最高報酬落在此區間者)。判準與正例門檻**恰好相反**:
#: 正例門檻要選「量化模型分得出來」的(有結構才學得到);負例帶要選「量化模型
#: **分不出來**」的——量化分得出來的對比,質化層學不到任何新東西,那種負例只是
#: 在教一件下游引擎本來就會的事。
NEG_BANDS = ((0.00, 0.10), (0.00, 0.15), (0.10, 0.25), (0.15, 0.30),
             (0.25, 0.50), (0.30, 0.60), (0.50, 0.80))


def negative_band_study(u: pl.DataFrame, pos_thr: float = 0.80) -> list[dict]:
    """對每個候選負例帶,量「量化特徵把它與正例分開的能力」。

    AUC 越接近 0.5,代表**量化層在這組對比上使不上力**,判別必須來自質化資訊——
    那正是我們要交給 LLM 去學的東西。AUC 高的帶則相反:對比太容易,蒸餾出來的
    「判別力」有一大半下游引擎自己就有,白花標記成本。
    """
    pos = u.filter(pl.col("fwd_max_ret") >= pos_thr)
    out = []
    for lo, hi in NEG_BANDS:
        neg = u.filter((pl.col("fwd_max_ret") >= lo) & (pl.col("fwd_max_ret") < hi))
        if neg.height < 200:
            continue
        both = pl.concat([pos.with_columns(pl.lit(1).alias("y")),
                          neg.with_columns(pl.lit(0).alias("y"))], how="diagonal")
        X = both.select(FEATURES).to_numpy()
        y = both["y"].to_numpy()
        ok = ~np.isnan(X).any(axis=1)
        X, y = X[ok], y[ok]
        # 單變量最大 AUC 當「量化可分性」的保守上界:多變量模型會過擬合小樣本,
        # 而我們要的是「有沒有一個現成的量化維度就能分開」這個更嚴格的問題。
        aucs = {FEATURES[i]: _auc(X[:, i], y) for i in range(X.shape[1])}
        best = max(aucs.items(), key=lambda kv: abs(kv[1] - 0.5))
        out.append({"band": f"[{lo:.0%}, {hi:.0%})", "n_neg": int(neg.height),
                    "n_pos": int(pos.height),
                    "hardest_is_best": True,
                    "max_univariate_auc": round(best[1], 4),
                    "driver": best[0],
                    "all_auc": {k: round(v, 3) for k, v in sorted(
                        aucs.items(), key=lambda kv: -abs(kv[1] - 0.5))[:4]}})
    return out


# ------------------------------------------------------------------ 配對殘留稽核

def confound_audit(u: pl.DataFrame) -> list[dict]:
    """量現行樣本的正負例之間,**還有哪個量化特徵單獨就能分開**。

    這是配對品質的直接檢驗,而且判準明確:配對的目的是把量化捷徑堵死,好讓蒸餾器
    只能從質化資訊找判別力。若某個特徵仍能單獨分出正負,蒸餾器就會學到它——而那個
    特徵下游引擎本來就有,等於花標記成本去學一件已經會的事,還會讓報告上的判別力
    看起來比真實的質化貢獻高。

    讀法:AUC 離 0.5 越遠 = 殘留越嚴重。0.5 附近 = 該維度已被配對控制乾淨。
    """
    from quantlib import paths as _p
    pos = pl.read_csv(_p.OUT / "evergreen_ev57_positives.csv",
                      schema_overrides={"company_code": pl.Utf8}).select(["company_code", "date"])
    neg = pl.read_csv(_p.OUT / "evergreen_ev57_negatives.csv",
                      schema_overrides={"company_code": pl.Utf8}).select(["company_code", "date"])
    key = pl.concat([pos.with_columns(pl.lit(1).alias("y")),
                     neg.with_columns(pl.lit(0).alias("y"))])
    key = key.with_columns(pl.col("date").cast(pl.Date))
    j = key.join(u, on=["company_code", "date"], how="inner")
    out = []
    for f in FEATURES:
        if f not in j.columns:
            continue
        x = j[f].to_numpy().astype(float)
        y = j["y"].to_numpy()
        ok = ~np.isnan(x)
        a = _auc(x[ok], y[ok])
        out.append({"feature": f, "auc": round(a, 4), "n": int(ok.sum()),
                    "abs_dev": round(abs(a - 0.5), 4),
                    "in_matching_key": f in ("adv_dec", "mom_dec")})
    return sorted(out, key=lambda r: -r["abs_dev"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trail", type=float, default=None,
                    help="只算單一 trail(預設全掃 %s)" % (TRAILS,))
    ap.add_argument("--labels", action="store_true", help="加跑標籤定義掃描")
    ap.add_argument("--negatives", action="store_true", help="加跑負例硬度帶掃描")
    ap.add_argument("--confound", action="store_true", help="稽核現行 EV57 樣本的配對殘留")
    ap.add_argument("--horizon", type=int, default=None,
                    help="覆寫前瞻視窗交易日數(預設 %d);掃它才知道 120 是不是最好" % ev57.FWD_DAYS)
    ap.add_argument("--label-col", default="fwd_max_ret",
                    help="用哪個欄位當標籤定義(fwd_max_ret | fwd_end_ret | trail25)")
    a = ap.parse_args()

    if a.horizon:
        # 覆寫模組級常數:EV57 的前瞻特徵、本模組的終值與出場重放全都讀它,
        # 改一處即整條鏈一致——分頭傳參很容易漏掉一處,而漏掉的那處會讓 horizon
        # 掃描比到的是兩個不同視窗的混合。
        ev57.FWD_DAYS = a.horizon
    con = data.connect()
    print(f"載入還原價面板…(前瞻視窗 {ev57.FWD_DAYS} 交易日)", flush=True)
    panel = _panel(con)
    st = _stations(panel)
    feat = _rich_features(panel)
    u = _universe(con, feat, st)
    print(f"站位母體 {u.height:,}(站位 {u['date'].n_unique()} 個)", flush=True)

    keys = u.select(["company_code", "date"])
    ret_cols = ["fwd_max_ret", "fwd_end_ret"]
    trails = (a.trail,) if a.trail is not None else TRAILS
    print(f"  重放出場 {['%.0f%%' % (x * 100) for x in trails]}…", flush=True)
    r = _trail_returns(panel, keys, trails)
    if r.height:
        u = u.join(r, on=["company_code", "date"], how="left")
        ret_cols += [f"trail{int(x * 100)}" for x in trails]

    rows = study(u, ret_cols, a.label_col)
    OUT.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"\n=== 門檻掃描(標籤定義 = {a.label_col})===")
    hdr = f"{'θ':>6}{'基準率':>9}{'正例':>8}{'最小格':>8}{'OOS AUC':>10}{'top15 命中':>11}{'lift':>7}"
    for rc in ret_cols:
        hdr += f"{'池' + rc.replace('fwd_', '').replace('_ret', '') + '年化':>15}"
    print(hdr)
    for r in rows:
        line = (f"{r['threshold']:>6.0%}{r['base_rate']:>9.2%}{r['n_positive']:>8,}"
                f"{r['min_cell']:>8}{r.get('oos_auc', float('nan')):>10.4f}"
                f"{r.get('hit_rate_at_topN', float('nan')):>11.2%}"
                f"{r.get('lift_at_topN', float('nan')):>7.2f}")
        for rc in ret_cols:
            line += f"{r.get(f'pool_{rc}_ann', float('nan')):>15.2%}"
        print(line)
    extra = {}
    if a.labels:
        extra["label_sweep"] = label_sweep(u, ret_cols)
        print("\n=== 標籤定義掃描(稀有度對齊 θ=80% 的基準率)===")
        print(f"{'標籤':>10}{'對應門檻':>10}{'基準率':>9}{'OOS AUC':>10}"
              + "".join(f"{'池' + rc.replace('fwd_', '').replace('_ret', ''):>13}" for rc in ret_cols))
        for r in extra["label_sweep"]:
            print(f"{r['label']:>10}{r['matched_threshold']:>10.2%}{r['base_rate']:>9.2%}"
                  f"{r['oos_auc']:>10.4f}"
                  + "".join(f"{r.get(f'pool_{rc}_mean', float('nan')):>13.2%}" for rc in ret_cols))
    if a.negatives:
        extra["negative_bands"] = negative_band_study(u)
        print("\n=== 負例硬度帶(**AUC 越接近 0.5 越好**——量化分不出來,質化才有事做)===")
        print(f"{'帶':>14}{'負例數':>9}{'最強單變量 AUC':>16}  主導特徵")
        for r in extra["negative_bands"]:
            print(f"{r['band']:>14}{r['n_neg']:>9,}{r['max_univariate_auc']:>16.4f}  {r['driver']}")
    if a.confound:
        extra["confound"] = confound_audit(u)
        print("\n=== 現行 EV57 樣本的配對殘留(AUC 離 0.5 越遠 = 量化捷徑還在)===")
        print(f"{'特徵':>22}{'AUC':>9}{'|偏離|':>9}{'已在配對鍵':>12}")
        for r in extra["confound"]:
            print(f"{r['feature']:>22}{r['auc']:>9.4f}{r['abs_dev']:>9.4f}"
                  f"{'是' if r['in_matching_key'] else '**否**':>12}")
    if extra:
        OUT.write_text(json.dumps({"thresholds": rows, **extra},
                                  ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\n→ {OUT}")


if __name__ == "__main__":
    main()
