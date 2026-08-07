"""EV57:站位錨定的蒸餾樣本——標籤、門檻、配對、分層全部由量測導出。

## 每個設計決定的出處(完整盤點見 `docs/strategy_research/ev60_no_magic_numbers.md`)

本模組不留任何說不出出處的數字。三類:**界限**(交易所公告、資料起始日)、
**導出**(repo 內可重跑的量測)、**刪除**(讓不變式接手)。

### 標籤 = 實際入帳報酬,不是區間最高報酬(EV60 導出)
下游用移動停利出場,一檔漲到 +80% 再全部吐回去,舊標籤算正例、**實際賺不到**。
標籤與付款的口徑不同,是量化系統的頭號無聲殺手。稀有度對齊後實測:

    舊標籤(區間最高 @80%)  OOS AUC 0.689(最高)  池報酬 6.03%(最低)
    實際入帳標籤            OOS AUC 0.622(最低)  池報酬 8.40%(最高)

**最好預測的目標最不賺錢。** 優化錯的目標,預測得越準賠越多。

### 門檻 20%、視窗 60 交易日(EV60/EV62,(h, θ) 聯合掃描 + 真實 NAV)
判準是專案主尺 `p5`(block bootstrap CAGR 5% 下界,`live_config.selection_metric`)。
θ 單峰內部最佳:5/10/15/**20**/30/50% 的 p5 為 6.08/6.53/7.15/**8.44**/5.61/1.82%,
CAGR、MDD、Martin 同時最佳。h=20 因**可交易性**作廢(46.5% 選股落在最低流動性
十分位、ADV20 中位僅 73 萬元——那種報酬買不到)。

### 配對 = 傾向分數,不是手挑的鍵(EV60 稽核)
手挑配對鍵本身就是魔術數字的來源。而且實測漏了最強的:舊樣本稽核顯示鍵內的
`adv_dec` 0.4989 / `mom_dec` 0.4999(控制到近乎完美),**沒進鍵的 `vol60` 殘留
0.5781**。傾向分數配對讓任何量化特徵依構造都分不開兩臂 ⇒ 蒸餾器測到的是
「相對於整個量化層的增量」。**逐折外樣本**,否則等於用未來資訊配對。
配對在 (站位 × 流動性組) 格內進行——分層是設計的一部分,配對必須尊重它。

### 抽樣層剔除未還原公司行動(2026-08-06)
單日還原報酬 > 當日制度上限(2015-06 前 7%、後 10%)⇒ 該日有公司行動未被還原。
**零參數,物理界限直接當偵測器。** 不擋的話機械跳空會被算成暴漲,而它只製造假正例
不製造假沉寂(舊樣本實測正例 6.5% 受影響、負例僅 1.4%)。

### 一檔股票只出現一次
不加的話實測 432 檔裡 60 檔重複、32 檔同時當過正負例。後果三個:PIT 破功(做同一檔
的晚站位時查到的材料留在上下文裡)、臂別露餡、有效樣本數灌水。

## 刪掉的參數(不是替它們找理由,是讓不變式接手)
`NEAR_MISS` / `QUIET_MAX`(硬度帶邊界沒有出處 → 改由配對後的分布量出來)、
手挑配對鍵、`PRIOR_DAYS`、十分位切點。

Run: uv run --project . python -m quantlib.evergreen.ev57_station_sample [--explore]
依賴 cache: 是。長任務,建議背景跑。
"""
from __future__ import annotations

import argparse
from datetime import date as Date

import polars as pl

from quantlib import paths, prices
from quantlib.apex import data

C = "company_code"
#: 每個數字的出處見 `docs/strategy_research/ev60_no_magic_numbers.md`。
#: 那份文件把方法論裡的每一個影響行為的數字逐個判定為「界限 / 導出 / 魔術」,
#: 並把魔術的量出來或刪掉。以下常數全部落在前兩類。

#: **界限**:tpex 資料自 2007-07 起,加 250 交易日暖身。
ERA_START = Date(2008, 1, 1)
#: **導出**:下游標記期起點;前瞻窗越過它就與標記期重疊,哲學不再是樣本外。
FWD_MUST_END_BEFORE = Date(2022, 7, 1)
#: **導出**(EV62,真實 NAV + 專案主尺 p5):前瞻視窗 60 交易日。
#: 掃 20/40/60/80/100/120/250,h=20 因可交易性作廢(46.5% 選股落在最低流動性十分位、
#: ADV20 中位僅 73 萬元),h=250 明顯較差;60 的 p5 5.61% 為最高。
FWD_DAYS = 60
#: **導出**:由 `FWD_MUST_END_BEFORE` 與 `FWD_DAYS` 反推——前瞻窗必須在標記期之前結束。
#: h 從 120 縮到 60,可用的最後站位因此往後延(先前寫死 2021-12-31 是舊 h 的產物)。
ERA_END = Date(2022, 3, 31)
#: **導出**(EV60/EV62):標籤 = 站位次日進場、移動停利 `LABEL_TRAIL` 出場、
#: 期滿 `FWD_DAYS` 平倉的**實際入帳報酬** ≥ `SURGE_MIN`。
#:
#: 為什麼不是「區間最高報酬 ≥ 80%」(舊定義):下游用移動停利出場,一檔漲到 +80%
#: 再全部吐回去,舊標籤算正例、實際賺不到。**標籤與付款的口徑不同,是量化系統的
#: 頭號無聲殺手。** 稀有度對齊後實測:舊標籤 AUC 最高(0.689)卻最不賺(池 6.03%),
#: 實際入帳標籤 AUC 最低(0.622)卻最賺(8.40%)。
#: 門檻 20% 為 (h, θ) 聯合掃描在真實 NAV 上的**內部最佳**(單峰:5/10/15/20/30/50%
#: 的 p5 為 6.08/6.53/7.15/**8.44**/5.61/1.82%),CAGR、MDD、Martin 同時最佳。
LABEL_TRAIL = 0.25
SURGE_MIN = 0.20
#: **界限**:台股個股單日漲跌幅由 7% 放寬至 10% 的生效日(交易所公告)。
LIMIT_ERA_SPLIT = Date(2015, 6, 1)
#: **導出**(EV61,檢力):最小可偵測 lift 1.5、α .05、power .80、12 格
#: (4 regime × 3 流動性組)⇒ 每格 16。`lift = 1.5` 是政策參數不是資料事實
#: (低於它的規則對 15 檔的月度池子改變不了選誰),1.2/2.0 的對應值見該文件。
PER_CELL = 16
#: **導出**(EV61,配額可填滿的前提下基準率極差最大):極差 49.4%,
#: 對照舊設定(250/750/−0.25)的 ~34% ——舊設定在分層品質上留了 15pp 沒拿。
REGIME_R12M_WIN, REGIME_DD_WIN, REGIME_CRASH = 250, 1000, -0.30
#: **導出**(EV61,等頻十分位 + 相鄰組信賴區間重疊即合併):資料支持三組
#: {十分位 0} / {十分位 1} / {十分位 2-9}。舊切點(≥7 高 / ≥3 中)把毫無差異的 2-9
#: 切成兩半,又把真正有差異的 0 與 1 併進同一組。
LIQ_GROUPS = ((0, 0), (1, 1), (2, 9))


def _panel(con) -> pl.DataFrame:
    fr = []
    for m in ("twse", "tpex"):
        f = prices.fetch_adjusted_panel(
            con, ERA_START.isoformat(), FWD_MUST_END_BEFORE.isoformat(),
            # 暖機取 310 個交易日:最長的站位當日特徵是 `rolling_max(250)`,
            # 加 60 日的 `vol60` 緩衝。`PRIOR_DAYS` 已隨配對改傾向分數而刪除。
            market=m, include_extra_history_days=310)
        if not f.is_empty():
            fr.append(f.select([C, "date", "close", "trade_value"])
                      .with_columns(pl.lit(m).alias("market")))
    return (pl.concat(fr).unique(subset=[C, "date"], keep="first")
            .filter(pl.col("close") > 0).sort([C, "date"]))


#: regime 的 3 年回撤需 750 個交易日暖機;不足則 rolling_max 回 null、該段全掉進
#: `otherwise` 分支。2026-08-05 抓到的實證:原本只給 180 天暖機,結果 **2008 金融海嘯
#: 被標成「修正」(249 天、零天崩跌),而 2010-2011 被標成「崩跌」(139/247 天)**
#: ——標籤整整落後真實崩盤兩年,分層等於照著錯的標籤配額。
REGIME_WARMUP_DAYS = 1200          # 日曆天,≈ 750+ 個交易日
REGIME_PROXY_MARKET = "twse"       # 只用 twse:tpex 2007-07 才有,混用會讓指數在該日不連續


def _regime(con, r12m_win: int = REGIME_R12M_WIN, dd_win: int = REGIME_DD_WIN,
            crash: float = REGIME_CRASH, mania: float = 0.30, bull: float = 0.0) -> pl.DataFrame:
    """市場 regime 標籤(等權 twse proxy)。

    為什麼要這個維度:EV55 量到蒸餾期(≤2021-12)**崩跌 regime 只有 23 個站位**,
    而各 regime 的暴漲基準率差很多(崩跌 17.6% / 修正 6.4% / 多頭 5.9% / 狂熱 7.5%)。
    純按「年」分層會讓最稀缺、也最有資訊量的崩跌樣本過薄——而哲學的第十道判別
    正是「宏觀 regime 時點」。故 regime 一律標記,並在報告中列出配額分布。
    """
    warm = (Date.fromordinal(ERA_START.toordinal() - REGIME_WARMUP_DAYS)).isoformat()
    px = prices.fetch_adjusted_panel(con, warm, FWD_MUST_END_BEFORE.isoformat(),
                                     market=REGIME_PROXY_MARKET,
                                     include_extra_history_days=0)
    ret = (px.select([C, "date", "close"]).filter(pl.col("close") > 0).sort([C, "date"])
           .with_columns((pl.col("close") / pl.col("close").shift(1).over(C) - 1).alias("r"))
           .filter(pl.col("r").is_between(-0.2, 0.2)))     # 濾停板級雜訊
    mkt = (ret.group_by("date").agg(pl.col("r").mean().alias("mr")).sort("date")
           .with_columns((1 + pl.col("mr")).cum_prod().alias("idx")))
    # 窗長與門檻**參數化**,好讓 EV61 掃描它們並把值導出來;預設值仍是現行設定,
    # 呼叫端不改就行為不變。單一真源:定義只有這一份,掃描與生產共用。
    return mkt.with_columns([
        (pl.col("idx") / pl.col("idx").shift(r12m_win) - 1).alias("r12m"),
        (pl.col("idx") / pl.col("idx").rolling_max(dd_win) - 1).alias("dd3y"),
    ]).with_columns(
        pl.when(pl.col("dd3y") < crash).then(pl.lit("1崩跌"))
          .when(pl.col("r12m") > mania).then(pl.lit("4狂熱"))
          .when(pl.col("r12m") > bull).then(pl.lit("3多頭"))
          .otherwise(pl.lit("2修正")).alias("regime")).select(["date", "regime"])


def _stations(panel: pl.DataFrame) -> list[Date]:
    """月度站位日 = 當月 10 日之後的第一個交易日(與 live 標記日曆同構)。"""
    d = panel.select("date").unique().sort("date")
    return (d.with_columns([pl.col("date").dt.strftime("%Y-%m").alias("ym"),
                            pl.col("date").dt.day().alias("dd")])
            .filter(pl.col("dd") > 10)
            .group_by("ym").agg(pl.col("date").min())
            .sort("date")["date"].to_list())


def _features(panel: pl.DataFrame) -> pl.DataFrame:
    """站位當日可得的量化特徵 + **實際入帳報酬**標籤。

    標籤從「區間最高報酬」改為「移動停利實際出場的報酬」,是 EV60 量出來的:
    下游用移動停利出場,一檔漲到 +80% 再吐回去,舊標籤算正例、實際賺不到。
    口徑對齊之後,同一個技能下的池子多賺 39%。

    量化特徵在這裡就算好,因為**傾向分數配對要用它們**——配對必須把量化捷徑堵死,
    才能讓蒸餾器測到的判別力是「相對於整個量化層的增量」。
    """
    p = panel.with_columns([
        pl.col("date").shift(-FWD_DAYS).over(C).alias("fwd_end_date"),
        pl.col("trade_value").rolling_mean(20, min_samples=20).over(C).alias("adv20"),
        # 站位當日可得的特徵——**任何一欄都不得含未來**
        (pl.col("close") / pl.col("close").shift(20).over(C) - 1).alias("ret20"),
        (pl.col("close") / pl.col("close").shift(60).over(C) - 1).alias("ret60"),
        (pl.col("close") / pl.col("close").shift(120).over(C) - 1).alias("ret120"),
        (pl.col("close") / pl.col("close").rolling_max(250, min_samples=60).over(C) - 1)
        .alias("pct_below_250d_high"),
        (pl.col("close") / pl.col("close").shift(1).over(C) - 1)
        .rolling_std(60, min_samples=40).over(C).alias("vol60"),
    ])
    return p.with_columns([
        (pl.col("trade_value") / pl.col("adv20")).alias("turnover20"),
        pl.col("adv20").log1p().alias("log_adv20"),
        # `prior_ret` 只為報表的混淆因子檢核而留;配對不再用它(改傾向分數)
        (pl.col("close") / pl.col("close").shift(120).over(C) - 1).alias("prior_ret"),
    ])


def _realized_returns(panel: pl.DataFrame, keys: pl.DataFrame,
                      trail: float = LABEL_TRAIL) -> pl.DataFrame:
    """站位次日進場、自峰值回撤 `trail` 或期滿 `FWD_DAYS` 出場的**實際入帳報酬**。

    逐日重放而非期末快照——專案的出場語義鐵律:只看期末價會把「中途已觸發」的出場
    當成沒發生。每檔收盤價只轉 numpy 一次、站位以 searchsorted 定位,窗內全向量化。
    """
    import numpy as np

    ser = panel.sort([C, "date"]).select([C, "date", "close"])
    by_code = {code: (g["date"].to_numpy(), g["close"].to_numpy().astype(float))
               for (code,), g in ser.group_by([C], maintain_order=True)}
    codes, dates_out, rets = [], [], []
    for code, d in keys.iter_rows():
        pair = by_code.get(code)
        if pair is None:
            continue
        dts, px = pair
        i = int(np.searchsorted(dts, np.datetime64(d)))
        if i >= len(px) or dts[i] != np.datetime64(d):
            continue
        w = px[i + 1: i + 1 + FWD_DAYS]
        if w.size < 2 or w[0] <= 0:
            continue
        peak = np.maximum.accumulate(w)
        below = w <= peak * (1 - trail)
        k = int(np.argmax(below)) if below.any() else -1
        codes.append(code)
        dates_out.append(d)
        rets.append(w[k] / w[0] - 1.0)
    return pl.DataFrame({C: codes, "date": dates_out, "realized_ret": rets})


def _unadjusted_action_days(con) -> pl.DataFrame:
    """還原價漏還原公司行動的「檔 × 日」——由**物理界限**偵測,零參數。

    台股個股單日漲跌幅上限 2015-06-01 前 7%、後 10%(交易所公告)。真實交易不可能
    超過它 ⇒ 超過即代表該日有公司行動未被還原(減資、面額變更、合併換股、股票分割)。

    為什麼要在抽樣層擋掉:機械跳空會被 `fwd_max_ret` 算成暴漲,而它**只製造假正例、
    不製造假沉寂**——實測正例 6.5% 受影響、負例僅 1.4%,差 4.6 倍。留著等於在正例臂
    摻進一批「漲幅是換股比例」的樣本,而蒸餾器會認真去替它們找消息面解釋。

    為什麼不修資料就好:`capital_reduction` 端點只回溯到 twse 2011 / tpex 2013
    (2026-08-06 逐年實測的界限),而 2011 補齊後該年仍有 201 筆違反——**減資不是唯一
    成因**,股票分割與面額變更根本不在那張表裡。修不到的部分只能誠實剔除。
    """
    import importlib
    scan = importlib.import_module("quantlib.audits.09_unadjusted_action_scan").scan
    fr = [scan(con, m, ERA_START.isoformat(), FWD_MUST_END_BEFORE.isoformat())
          for m in ("twse", "tpex")]
    fr = [f for f in fr if not f.is_empty()]
    return (pl.concat(fr).select([C, "date"]).unique()
            if fr else pl.DataFrame({C: [], "date": []}))


def _universe(con, feat: pl.DataFrame, stations: list[Date]) -> pl.DataFrame:
    """站位日 × 可執行母體,附流動性與前期動能的**當日橫斷面十分位**。

    十分位在站位當日的橫斷面上算 —— 絕對金額/絕對漲幅在 2008 與 2021 意義不同,
    分位自我正規化。配對就配在同一格上,混淆因子當場被控制掉。
    """
    first = con.sql("""
        SELECT company_code,
               make_date(min(year*100+month)//100, min(year*100+month)%100, 1) AS first_rev
        FROM operating_revenue GROUP BY company_code""").pl()
    u = (feat.filter(pl.col("date").is_in(stations))
         .filter((pl.col("date") >= ERA_START) & (pl.col("date") <= ERA_END)
                 & pl.col("fwd_end_date").is_not_null()
                 & (pl.col("fwd_end_date") < FWD_MUST_END_BEFORE)
                 & pl.col("realized_ret").is_not_null()
                 & pl.col("prior_ret").is_not_null()
                 & pl.col("adv20").is_not_null()
                 & pl.col(C).str.contains(r"^[0-9]{4}$"))
         .join(first, on=C, how="inner")
         .filter(pl.col("date") >= pl.col("first_rev")).drop("first_rev"))
    u = u.with_columns([
        ((pl.col("adv20").rank("ordinal").over("date") * 10 - 1)
         // pl.len().over("date")).alias("adv_dec"),
        # `mom_dec` 只留給報表的混淆因子檢核;**配對已改傾向分數,不再用它當鍵**
        ((pl.col("prior_ret").rank("ordinal").over("date") * 10 - 1)
         // pl.len().over("date")).alias("mom_dec"),
    ])
    # **剔除前瞻窗內含未還原公司行動的站位**。用 as-of 而非逐列迴圈:對每個 (檔, 站位)
    # 找該檔下一個「壞日」,落在前瞻窗內就剔除。
    bad = _unadjusted_action_days(con).rename({"date": "bad_date"}).sort("bad_date")
    if bad.height:
        n0 = u.height
        u = (u.sort("date")
             .join_asof(bad, left_on="date", right_on="bad_date", by=C, strategy="forward")
             .with_columns(pl.col("bad_date").alias("_bad"))
             .filter(pl.col("_bad").is_null()
                     | (pl.col("_bad") > pl.col("fwd_end_date")))
             .drop(["bad_date", "_bad"]))
        print(f"  剔除前瞻窗含未還原公司行動的站位:{n0:,} → {u.height:,}"
              f"(-{n0 - u.height:,})", flush=True)

    tax = con.sql("SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
                  "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
    return (u.sort("date")
            .join_asof(tax.sort("effective_date"), left_on="date",
                       right_on="effective_date", by=C, strategy="backward")
            .with_columns([
                pl.col("industry").fill_null("(未分類)"),
                pl.when(pl.col("date") >= LIMIT_ERA_SPLIT).then(pl.lit("10%"))
                  .otherwise(pl.lit("7%")).alias("limit_era"),
                # 分組由 EV61 導出:等頻十分位 + 相鄰組信賴區間重疊即合併 ⇒
                # {十分位 0} / {十分位 1} / {十分位 2-9}。舊切點(≥7/≥3)把毫無差異的
                # 2-9 切成兩半,又把真正有差異的 0 與 1 併起來。
                pl.when(pl.col("adv_dec") <= LIQ_GROUPS[0][1]).then(pl.lit("D0"))
                  .when(pl.col("adv_dec") <= LIQ_GROUPS[1][1]).then(pl.lit("D1"))
                  .otherwise(pl.lit("D2-9")).alias("tier"),
                pl.col("date").dt.year().alias("y"),
            ]))


#: 傾向分數要用的量化特徵——**與 EV60 的可學性下界代理同一組**,單一真源。
PROPENSITY_FEATURES = ("ret20", "ret60", "ret120", "pct_below_250d_high",
                       "adv_dec", "mom_dec", "turnover20", "vol60", "log_adv20")


def _propensity(u: pl.DataFrame, seed: int, folds: int = 5) -> pl.DataFrame:
    """對每個「檔 × 站位」算傾向分數 = 量化模型預測的正例機率,**逐折外樣本**。

    為什麼要它:手挑配對鍵(站位 × 流動性 × 動能 × 產業)本身就是魔術數字的來源
    ——挑哪幾個維度、各切幾分位,全是選擇。而且實測漏了最強的那個:對舊樣本稽核,
    配對鍵內的 `adv_dec` 0.4989 / `mom_dec` 0.4999(控制到近乎完美),**沒進鍵的
    `vol60` 殘留 0.5781**、`ret60` 0.5418、`ret20` 0.5410。蒸餾器會把波動率學成
    「判別力」,而下游引擎本來就有它。

    傾向分數配對讓**任何量化特徵都無法分開兩臂(依構造)**,蒸餾器測到的判別力
    因此是「相對於整個量化層的增量」——那才是質化系統該被評價的東西。而且它沒有
    任何要挑的維度或分位數。

    **逐折外樣本是必要的**:拿全期資料 fit 出來的分數去挑負例,等於用未來資訊配對。
    按時間切折、每折用其他折訓練——折界由分位決定,不由人挑年份。
    """
    import lightgbm as lgb
    import numpy as np

    u = u.with_columns((pl.col("realized_ret") >= SURGE_MIN).cast(pl.Int8).alias("_y"))
    d = u["date"].to_numpy()
    edges = np.quantile(d.astype("datetime64[D]").astype(int),
                        np.linspace(0, 1, folds + 1))
    fold = np.clip(np.searchsorted(edges[1:-1],
                                   d.astype("datetime64[D]").astype(int)), 0, folds - 1)
    score = np.full(u.height, np.nan)
    X = u.select(PROPENSITY_FEATURES).to_pandas()
    y = u["_y"].to_numpy()
    for k in range(folds):
        tr, te = fold != k, fold == k
        if y[tr].sum() < 50 or te.sum() == 0:
            continue
        m = lgb.train(
            {"objective": "binary", "learning_rate": 0.05, "num_leaves": 31,
             "min_data_in_leaf": 200, "feature_fraction": 0.8, "bagging_fraction": 0.8,
             "bagging_freq": 1, "verbose": -1, "seed": seed},
            lgb.Dataset(X[tr], label=y[tr]), num_boost_round=200)
        score[te] = m.predict(X[te])
    return u.with_columns(pl.Series("propensity", score)).drop("_y")


def _match(pos: pl.DataFrame, u: pl.DataFrame, seed: int) -> pl.DataFrame:
    """負例 = **同站位、傾向分數最接近的非正例**。

    「非正例」的定義就是標籤本身(`realized_ret < SURGE_MIN`),不再有 `near_miss` /
    `quiet` 兩檔手設硬度帶——那兩個帶的邊界(25~50% / <15%)沒有出處。硬度改為
    **量出來的維度**:配對完成後記錄每對的傾向分數差與負例的實際報酬,由統計表呈現。
    """
    import numpy as np

    neg_pool = u.filter((pl.col("realized_ret") < SURGE_MIN)
                        & pl.col("propensity").is_not_null())
    out, used = [], set()
    rng = np.random.default_rng(seed)
    # **配對在(站位 × 流動性組)格內進行**,不是只在站位內。
    # 只配站位的話實測 `adv_dec` 正例中位 1.0 vs 負例 5.0——因為正例被分層強制均分到
    # 三個流動性組,而負例按自然分布落點。傾向分數平衡的是**分數**不是每個特徵,
    # 兩檔分數相同可以來自完全不同的特徵組合。
    # 這不是把手挑的配對鍵加回來:流動性組**本來就是分層維度**,配對尊重分層是
    # 設計的一致性要求,不是新增一個要挑的鍵。
    by_station = {k: g for k, g in neg_pool.group_by(["date", "tier"])}
    for t in pos.sort("date").iter_rows(named=True):
        g = by_station.get((t["date"], t["tier"]))
        if g is None or t["propensity"] is None:
            continue
        cand = g.filter(~pl.col(C).is_in(list(used)) & (pl.col(C) != t[C]))
        if cand.is_empty():
            continue
        gap = (cand["propensity"].to_numpy() - t["propensity"]).__abs__()
        j = int(np.argmin(gap + rng.random(len(gap)) * 1e-12))   # 平手隨機打散
        hit = cand.row(j, named=True)
        used.add(hit[C])
        out.append({**hit, "matched_to": f"{t[C]}@{t['date']}",
                    "propensity_gap": float(gap[j])})
    return pl.DataFrame(out) if out else pl.DataFrame()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--explore", action="store_true")
    ap.add_argument("--per-cell", type=int, default=PER_CELL,
                    help="每 (regime × 流動性組) 抽幾檔正例;預設由檢力導出(EV61)")
    ap.add_argument("--seed", type=int, default=20260806)
    a = ap.parse_args()

    con = data.connect()
    print("載入還原價面板…", flush=True)
    panel = _panel(con)
    st = _stations(panel)
    print(f"  {panel.height:,} 列;月度站位 {len(st)} 個({st[0]} ~ {st[-1]})", flush=True)
    feat = _features(panel)
    base = (feat.filter(pl.col("date").is_in(st))
            .select([C, "date"]))
    print("  重放實際入帳報酬(逐日,移動停利 %.0f%%)…" % (LABEL_TRAIL * 100), flush=True)
    lab = _realized_returns(panel, base, LABEL_TRAIL)
    feat = feat.join(lab, on=[C, "date"], how="left")

    u = (_universe(con, feat, st)
         .join(_regime(con), on="date", how="left")
         .with_columns(pl.col("regime").fill_null("2修正")))
    print(f"  站位母體觀測 {u.height:,}")
    print("  regime 站位分布:", dict(
        u.group_by("regime").agg(pl.col("date").n_unique().alias("n"))
         .sort("regime").iter_rows()))

    print(f"\n=== 母體基準率(標籤 = 實際入帳報酬,{FWD_DAYS} 交易日)===")
    for thr in (0.10, SURGE_MIN, 0.30, 0.50):
        mark = " ←採用" if abs(thr - SURGE_MIN) < 1e-9 else ""
        print(f"  實際入帳報酬 ≥ {thr:>4.0%} → {(u['realized_ret'] >= thr).mean():.2%}{mark}")

    pos_all = u.filter(pl.col("realized_ret") >= SURGE_MIN)
    print(f"\n正例母體 {pos_all.height:,};prior120 中位 {pos_all['prior_ret'].median():+.1%}")
    if a.explore:
        print(pos_all.group_by(["y", "tier"]).agg(pl.len().alias("n"))
              .pivot(on="tier", index="y", values="n").sort("y"))
        return

    print("\n計算傾向分數(逐折外樣本)…", flush=True)
    u = _propensity(u, a.seed)
    pos_all = u.filter(pl.col("realized_ret") >= SURGE_MIN
                       ).filter(pl.col("propensity").is_not_null())

    # **一檔股票在整個樣本裡只准出現一次**(見 2026-08-05 的量測):不加的話實測
    # 432 檔裡 60 檔重複、32 檔同時當過正負例,而同一檔的兩個時點會讓 PIT 破功
    # (做晚站位查到的材料留在上下文裡)、臂別露餡、有效樣本數灌水。
    n_cell = max(a.per_cell, 1)
    ranked = (pos_all.sample(fraction=1.0, shuffle=True, seed=a.seed)
              .with_columns(pl.int_range(pl.len()).over(["regime", "tier", "y"]).alias("_rr"))
              .sort(["_rr"]).drop("_rr"))
    used_codes: set[str] = set()
    cell_n: dict[tuple, int] = {}
    keep = []
    for r in ranked.iter_rows(named=True):
        cell = (r["regime"], r["tier"])
        if cell_n.get(cell, 0) >= n_cell or r[C] in used_codes:
            continue
        used_codes.add(r[C])
        cell_n[cell] = cell_n.get(cell, 0) + 1
        keep.append(r)
    pos = pl.DataFrame(keep).sort(["regime", "tier", C])

    neg = _match(pos, u.filter(~pl.col(C).is_in(list(used_codes))), a.seed)
    print(f"\n正例 {pos.height} 檔;負例 {neg.height} 檔")
    print("  配額(regime × 流動性組):")
    print(pos.group_by(["regime", "tier"]).agg(pl.len().alias("n"))
          .pivot(on="tier", index="regime", values="n").sort("regime"))
    print(f"  正例年份分布: {dict(sorted(pos.group_by('y').agg(pl.len()).iter_rows()))}")

    print("\n=== 配對品質(傾向分數應把量化捷徑堵死)===")
    if neg.height:
        print(f"  傾向分數差:中位 {neg['propensity_gap'].median():.5f}"
              f"  p90 {neg['propensity_gap'].quantile(0.9):.5f}")
        print(f"  負例實際入帳報酬:中位 {neg['realized_ret'].median():+.1%}"
              f"  p10 {neg['realized_ret'].quantile(0.1):+.1%}"
              f"  p90 {neg['realized_ret'].quantile(0.9):+.1%}")
        print("  ↑ 硬度不再由手設的 near_miss/quiet 帶決定,而是**量出來的分布**")
        for f in ("vol60", "ret20", "ret60", "prior_ret", "adv_dec"):
            if f in pos.columns and f in neg.columns:
                print(f"  {f:<12}正例中位 {pos[f].median():>9.4f}"
                      f"  負例中位 {neg[f].median():>9.4f}")

    cols = [C, "date", "market", "industry", "limit_era", "regime", "tier", "y",
            "adv_dec", "mom_dec", "realized_ret", "prior_ret", "propensity"]
    pos.select([c for c in cols if c in pos.columns]).write_csv(
        paths.OUT / "evergreen_ev57_positives.csv")
    neg.select([c for c in cols + ["matched_to", "propensity_gap"]
                if c in neg.columns]).write_csv(
        paths.OUT / "evergreen_ev57_negatives.csv")
    print(f"\n  正例 → {paths.OUT / 'evergreen_ev57_positives.csv'}")
    print(f"  負例 → {paths.OUT / 'evergreen_ev57_negatives.csv'}")


if __name__ == "__main__":
    main()
