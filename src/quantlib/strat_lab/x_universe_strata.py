"""x_universe_strata — S 策略「Universe 分層」維度實驗(乾淨資料 campaign)。

用途
----
S(apex_revcycle_S)的 universe 只有流動性/價格/掛牌長度三道閘
(`data.eligibility(panel, min_adv=5_000_000)`,內含 `raw_close >= 10`、
掛牌滿 60 根 bar)。**沒有任何產業、市值、波動的分層**。本檔測「把某一層從池裡
拿掉,整體會不會更好」。

機制假設
--------
S 的核心訊號是**月營收年增加速度**(`rev_yoy_accel` = 近 3 月 YoY 均 − 近 12 月 YoY 均)
與同業相對加速(`accel_rel`)。這個訊號的前提是「月營收 = 當期經營動能的即時代理」。
這個前提在不同層並不等價:

  (a) 產業語義:金融保險的「營收」是利息+手續費+投資損益(可為負、受評價影響),
      建材營造是完工比例/交屋認列(月營收是工程里程碑的鋸齒,不是動能)。這兩類
      的 rev_yoy_accel 在學理上就不是動能代理 → 若它們進得了池,貢獻的是雜訊。
      反向:電子鏈月營收 = 拉貨/出貨,是最乾淨的動能代理 → 「只做電子」可能更強。
  (b) 市值:小型股營收基期小、單筆訂單就能讓 YoY 加速度爆衝(訊號雜訊比差),
      但小型股同時是動能溢酬最大的地方 → 雙向都可能,必須測。
  (c) 波動:S 是「營收動能 + 突破」複合,高波動是 alpha 來源還是風險?排高波動
      應該砍 upside(若砍不到 = 高波動層本來就沒貢獻)。
  (d) 價格:canonical 已有 raw_close >= 10 的低價股閘;本檔測「再拉高門檻」
      (15/20/30 元)——低價股跳動幅度(tick size)相對大、假突破多。

實作
----
一律在 **elig 層**過濾:`eligible := eligible AND NOT 屬於被排除層`,再傳給
`run_s_full`。**排除是「正面識別」語義**——屬性缺漏(無產業分類、無股本)的個股
一律保留,避免「資料缺漏」偽裝成「分層 alpha」。

PIT 紀律
--------
- 產業:`industry_taxonomy_pit` 以 `effective_date <= 決策日` 的最新一筆 asof(專案鐵律)。
- 市值:股本 `capital_stock` 取自 raw_quarterly,套用與 `apex.assemble` 相同的法定
  生效日(Q1→5/15、Q2→8/14、Q3→11/14、Q4→次年 3/31)後 asof;
  市值 = `raw_close × capital_stock × 100`(capital_stock 單位為千元、面額 10 元
  → 股數 = capital_stock × 1000 / 10)。
- 波動:60 日還原收盤日報酬標準差(僅用過去資料的 rolling)。
- 市值/波動的分層一律用**當日橫截面分位**(只在 eligible 名單內計算),不用絕對
  數字門檻——避免通膨/市場整體漲跌造成的門檻漂移(那會是隱形的前視/漂移參數)。

方法論(硬性)
--------------
- 判準 D2:候選必須 Sortino / Calmar / MDD / bootstrap 下界 **同時 ≥ canonical**。
- 每個變體都做配對 moving-block bootstrap(block=21、n_boot=4000):同一組重抽
  索引同時作用於變體與 canonical 的日報酬 → 年化 CAGR 差 + 95% CI + P(差≤0)。
  **CI 跨 0 = 噪音級 = 證偽**。
- 分位家族(bot10/20/30)當**高原**看,不挑尖峰;通過 D2 者另報前後半段與逐年。
- 診斷階段先報「canonical 的成交在各層的分佈與貢獻」——若某層根本沒交易,
  排除它必然無效(機制上先否定,不必被雜訊誤導)。

結論(2026-07-26 乾淨資料,全跨度 2014-10~2026-07,含成本;canonical =
CAGR +82.3% / Sortino 3.284 / Calmar 2.397 / MDD -34.3% / boot 下界 +51.3%)
------------------------------------------------------------------------
**維度證偽:22 個分層變體,0 個通過 D2,0 個配對 CI 全正。**
最好的一個(排除建材營造)年化 +0.52pp、95% CI [-5.4, +6.7]pp、P(差≤0)=0.43,
且 Sortino 3.231 < canonical 3.284 → 噪音級。

安慰劑(隨機永久排除同比例個股,35 次)給出一條**廣度稅**:每排掉池的 1%,
年化 CAGR 損約 0.49pp(排 20% → -11.7pp),而「同樣排 20%、換個隨機種子」的
純運氣尺度是 ±9pp。這解釋了為什麼所有分層都變差——**S 的池本來就窄**
(`rev_fresh_days<=7` 讓候選只在月營收公布窗口存在),任何再收窄都直接打到
5 席的填充品質。也代表:任何 universe 過濾要被驗成改進,效果必須大於這條稅,
本維度沒有任何一個做到。

扣掉廣度稅後仍有兩個**機制事實**(對日後設計有用,雖不構成可上線的改進):
  · **波動是 S 的 alpha 載體**:排掉高波動前 20%,實際 -48.5pp,遠超廣度稅預期的
    -11.7pp(excess -36.8pp,z=-4.1);top30、mid_only 同向(z=-3.2/-3.1)。
    診斷面吻合:波動第 5 分位吃下 323/688 筆成交、平均 +8.6%,第 1 分位僅 +1.1%。
    → **任何降波動的 universe 或風控設計都會直接砍掉 S 的報酬來源**。
  · **低波動層近乎零貢獻**:排掉低波動 70% 只損 -8.4pp(廣度稅預期 -38.1pp,
    z=+3.3)——但仍是負的。「無害」不等於「有益」,不可上線。
  · 產業無資訊:電子鏈與其他傳產的每筆平均報酬幾乎相同(+6.39% vs +6.38%),
    「只做電子」與隨機排同比例無異(z=+0.05);金融、營建雖然貢獻近零
    (24/39 筆、合計 +0.24/+0.12,對比全體 +40.3),排掉也賺不回來(z≈0)——
    遞補上來的候選並不比它們好。
  · 市值、價格門檻:全部 |z|<2,與隨機排除無異。

Run
---
    uv run --project . python -m quantlib.strat_lab.x_universe_strata diag    # 分層歸因診斷
    uv run --project . python -m quantlib.strat_lab.x_universe_strata grid    # 18 個分層變體
    uv run --project . python -m quantlib.strat_lab.x_universe_strata stage2  # 安慰劑對照 + 機制正向測
    uv run --project . python -m quantlib.strat_lab.x_universe_strata breadth # 扣廣度稅(只讀 CSV)
    uv run --project . python -m quantlib.strat_lab.x_universe_strata all

依賴 cache: 是(prep_cached 讀 cache.duckdb + industry_taxonomy_pit + raw_quarterly)。
輸出: var/out/strat_lab/x_universe_strata_{diag,grid,stage2,breadth}.csv
"""
from __future__ import annotations

import math
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.metrics import perf_stats, yearly_table
from quantlib.apex.strategy_s import DS, prep_cached, run_s_full

C = "company_code"
TRADING_DAYS = 252
SPLIT = "2020-07-01"        # 前後半段切點(跨度中位,非最佳化產物)
OUT = paths.OUT / "strat_lab"

# 產業群組(依 TWSE/TPEx 官方分類名;taxonomy 含新舊世代名稱,一併列入)
FIN = {"金融保險", "金融業"}
CONSTR = {"建材營造", "水泥窯製營造"}
NONSTD = {"管理股票", "存託憑證", "綜合"}
ELEC = {"電子工業", "半導體業", "電子零組件業", "電子", "光電業",
        "電腦及週邊設備業", "其他電子業", "通信網路業", "資訊服務業",
        "電子通路業", "數位雲端"}


# ── 統計工具(與 x_pool_depth 同法:配對重抽對高相關曲線最有力)──────────────
def _daily_rets(nav: pl.DataFrame) -> np.ndarray:
    v = nav.sort("date")["nav"].to_numpy()
    return v[1:] / v[:-1] - 1.0


def paired_block_bootstrap(nav_v: pl.DataFrame, nav_b: pl.DataFrame, *,
                           n_boot: int = 4000, block: int = 21,
                           seed: int = 42, chunk: int = 500) -> dict:
    """配對 circular moving-block bootstrap → 年化 CAGR 差的分佈。"""
    rv, rb = _daily_rets(nav_v), _daily_rets(nav_b)
    if len(rv) != len(rb):
        raise ValueError("variant/baseline 日報酬長度不一致,無法配對")
    t = len(rv)
    rng = np.random.default_rng(seed)
    n_blocks = math.ceil(t / block)
    diffs = np.empty(n_boot)
    off = np.arange(block)[None, None, :]
    for s in range(0, n_boot, chunk):
        n = min(chunk, n_boot - s)
        starts = rng.integers(0, t, size=(n, n_blocks))
        idx = ((starts[:, :, None] + off) % t).reshape(n, -1)[:, :t]
        diffs[s:s + n] = (np.prod(1.0 + rv[idx], axis=1) ** (TRADING_DAYS / t)
                          - np.prod(1.0 + rb[idx], axis=1) ** (TRADING_DAYS / t))
    return {"ann_diff": float(np.mean(diffs)),
            "ci_lo": float(np.percentile(diffs, 2.5)),
            "ci_hi": float(np.percentile(diffs, 97.5)),
            "p_le0": float((diffs <= 0).mean())}


def block_bootstrap_lower(nav: pl.DataFrame, *, n_boot: int = 4000, block: int = 21,
                          seed: int = 42, chunk: int = 500) -> float:
    """單曲線 CAGR bootstrap 95% 下界(分塊算,避免索引矩陣記憶體尖峰)。"""
    r = _daily_rets(nav)
    t = len(r)
    rng = np.random.default_rng(seed)
    n_blocks = math.ceil(t / block)
    out = np.empty(n_boot)
    off = np.arange(block)[None, None, :]
    for s in range(0, n_boot, chunk):
        n = min(chunk, n_boot - s)
        starts = rng.integers(0, t, size=(n, n_blocks))
        idx = ((starts[:, :, None] + off) % t).reshape(n, -1)[:, :t]
        out[s:s + n] = np.prod(1.0 + r[idx], axis=1) ** (TRADING_DAYS / t) - 1.0
    return float(np.percentile(out, 2.5))


# ── 分層屬性表(PIT)───────────────────────────────────────────────────────
def attrs_cached(con, panel: pl.DataFrame, elig: pl.DataFrame) -> pl.DataFrame:
    """build_attrs 的磁碟快取(極速鐵律:昂貴衍生物必快取)。key 含 cache.duckdb
    mtime → 資料世代一變即失效。同時是子行程的載入來源(避免把 3M 列 frame
    pickle 給 6 個 worker)。"""
    import hashlib
    import os
    key = hashlib.md5(f"attrs_{os.path.getmtime(paths.CACHE_DB)}".encode()).hexdigest()[:12]
    f = paths.CACHE_DIR / "prep_cache" / f"x_univ_attrs_{key}.parquet"
    f.parent.mkdir(parents=True, exist_ok=True)
    if f.exists():
        return pl.read_parquet(f)
    a = build_attrs(con, panel, elig)
    a.write_parquet(f)
    return a


def build_attrs(con, panel: pl.DataFrame, elig: pl.DataFrame) -> pl.DataFrame:
    """(date, code, industry, mktcap, mktcap_pct, vol60, vol60_pct, raw_close)。

    分位一律在**當日 eligible 名單內**計算(決策當下真正的橫截面),缺屬性者
    分位為 null → 下游「正面識別才排除」的語義自然保留它。
    """
    # 產業:PIT asof(專案鐵律,禁止用 operating_revenue.industry)
    tax = (con.sql("SELECT company_code, effective_date, industry "
                   "FROM industry_taxonomy_pit WHERE industry IS NOT NULL "
                   "ORDER BY effective_date").pl().sort("effective_date"))
    # 股本:法定生效日(與 apex.assemble 的 q_avail 完全同款,避免兩套 PIT 語義)
    rq = (pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
          .sort([C, "year", "quarter"])
          .with_columns(
              pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
              .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
              .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
              .otherwise(pl.date(pl.col("year") + 1, 3, 31)).alias("q_avail"))
          .select([C, "q_avail", "capital_stock"]).drop_nulls()
          .filter(pl.col("capital_stock") > 0)
          .unique(subset=[C, "q_avail"], keep="last")
          .sort("q_avail"))

    base = (panel.sort([C, "date"])
            .with_columns((pl.col("close") / pl.col("close").shift(1) - 1).over(C).alias("_ret"))
            .with_columns(pl.col("_ret").rolling_std(60).over(C).alias("vol60"))
            .select(["date", C, "raw_close", "vol60"])
            .sort("date"))
    base = (base.join_asof(tax, left_on="date", right_on="effective_date", by=C,
                           strategy="backward")
            .sort("date")
            .join_asof(rq, left_on="date", right_on="q_avail", by=C,
                       strategy="backward", tolerance="400d")
            # 股本單位 = 千元、面額 10 元 → 股數 = capital_stock × 100
            .with_columns((pl.col("raw_close") * pl.col("capital_stock") * 100.0)
                          .alias("mktcap")))

    e = elig.filter(pl.col("eligible")).select(["date", C])
    a = (base.join(e, on=["date", C], how="semi")
         .with_columns([
             (pl.col("mktcap").rank() / pl.col("mktcap").is_not_null().sum())
             .over("date").alias("mktcap_pct"),
             (pl.col("vol60").rank() / pl.col("vol60").is_not_null().sum())
             .over("date").alias("vol60_pct"),
         ]))
    return a.select(["date", C, "industry", "mktcap", "mktcap_pct",
                     "vol60", "vol60_pct", "raw_close"])


# ── 變體定義:每個 = 「被排除層」的 polars 條件(正面識別才排除)────────────
def _in(names: set[str]) -> pl.Expr:
    return pl.col("industry").is_in(list(names))


VARIANTS: dict[str, pl.Expr] = {
    # (a) 產業語義
    "IND_excl_fin":            _in(FIN),
    "IND_excl_constr":         _in(CONSTR),
    "IND_excl_fin_constr":     _in(FIN | CONSTR),
    "IND_excl_fin_constr_nonstd": _in(FIN | CONSTR | NONSTD),
    "IND_elec_only":           pl.col("industry").is_not_null() & ~_in(ELEC),
    "IND_excl_elec":           _in(ELEC),
    # (b) 市值分層(當日橫截面分位;高原家族 bot10/20/30)
    "MC_excl_bot10":           pl.col("mktcap_pct") <= 0.10,
    "MC_excl_bot20":           pl.col("mktcap_pct") <= 0.20,
    "MC_excl_bot30":           pl.col("mktcap_pct") <= 0.30,
    "MC_excl_top20":           pl.col("mktcap_pct") > 0.80,
    "MC_mid_only":             (pl.col("mktcap_pct") <= 0.20) | (pl.col("mktcap_pct") > 0.80),
    # (c) 波動分層
    "VOL_excl_top20":          pl.col("vol60_pct") > 0.80,
    "VOL_excl_top30":          pl.col("vol60_pct") > 0.70,
    "VOL_excl_bot20":          pl.col("vol60_pct") <= 0.20,
    "VOL_mid_only":            (pl.col("vol60_pct") <= 0.20) | (pl.col("vol60_pct") > 0.80),
    # (d) 價格門檻(canonical 已有 >= 10;此處只測再拉高)
    "PX_min15":                pl.col("raw_close") < 15.0,
    "PX_min20":                pl.col("raw_close") < 20.0,
    "PX_min30":                pl.col("raw_close") < 30.0,
}


# 第二階段(grid 結果逼出來的):
#   ① 安慰劑對照 —— grid 顯示「排掉任何一層都變差」,但這可能只是**池變小本身**的
#      代價(候選少 → 每日排名前 5 的品質下降),與該層是不是雜訊無關。用「隨機
#      排掉同比例、且**對個股永久生效**」的安慰劑(與分層排除同為持久性排除,不是
#      逐日churn)量出這條「廣度稅」基線;分層排除只有**顯著劣於安慰劑**才叫
#      「該層是負貢獻」,只有**顯著優於安慰劑**才叫「該層是雜訊、排掉有益」。
#   ② 機制正向測 —— 診斷顯示 alpha 集中在高波動層(vol 第 5 分位 323/688 筆、
#      平均 +8.6% vs 第 1 分位 +1.1%)。既然排高波動大傷,反向「砍掉低波動尾巴、
#      把資金逼進高波動」是這個機制唯一還沒被否定的方向,用高原家族測。
def _rand_excl(frac: float, seed: int) -> pl.Expr:
    """對個股永久生效的隨機排除(hash 個股代號,與日期無關 → 與分層排除同為
    持久性,不會退化成「延後一兩天進場」的溫和干擾)。"""
    return (pl.col(C).hash(seed=seed) % 10_000) < int(frac * 10_000)


STAGE2: dict[str, pl.Expr] = {
    "VOL_excl_bot30":  pl.col("vol60_pct") <= 0.30,
    "VOL_excl_bot50":  pl.col("vol60_pct") <= 0.50,
    "VOL_excl_bot70":  pl.col("vol60_pct") <= 0.70,
    "MC_excl_top50":   pl.col("mktcap_pct") > 0.50,
}
# 安慰劑帶:同一比例多個種子——單一種子只是一次抽樣,分層效果要跟**分佈**比,不是跟一個點比。
#      比例覆蓋到 70%,因為 VOL_excl_bot70 排掉 70% 的池——安慰劑若只到 40%,
#      對它就是外插,z 會被高估(廣度稅若飽和,外插會低估預期損失)。
STAGE2.update({f"PLACEBO_{int(f*100)}_s{i}": _rand_excl(f, 1000 * i + int(f * 100))
               for f in (0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.70) for i in range(1, 6)})


def elig_excluding(elig: pl.DataFrame, attrs: pl.DataFrame, cond: pl.Expr) -> pl.DataFrame:
    """把「被排除層」從 eligible 拿掉(缺屬性者不被排除)。"""
    drop = attrs.filter(cond.fill_null(False)).select(["date", C]).with_columns(
        pl.lit(True).alias("_drop"))
    return (elig.join(drop, on=["date", C], how="left")
            .with_columns((pl.col("eligible") & pl.col("_drop").is_null()).alias("eligible"))
            .drop("_drop"))


# ── 執行 ────────────────────────────────────────────────────────────────────
_G: dict = {}


def _init():
    """子行程自行從磁碟快取載入(prep_cached ~1.5s、attrs parquet ~1s),
    不透過 pickle 傳 3M 列 frame。"""
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    _G.update(panel=panel, feat=feat, elig=elig, attrs=attrs_cached(con, panel, elig))


def _all_variants() -> dict[str, pl.Expr]:
    return {**VARIANTS, **STAGE2}


def _run_one(name: str) -> tuple[str, pl.DataFrame, pl.DataFrame]:
    e2 = elig_excluding(_G["elig"], _G["attrs"], _all_variants()[name])
    nav, tr = run_s_full(_G["panel"], _G["feat"], e2, DS)
    return name, nav, tr


def _pool_frac(elig: pl.DataFrame, attrs: pl.DataFrame, cond: pl.Expr) -> float:
    """被排除層佔「日 × eligible 檔」樣本的比例(量這個變體到底動了多少池)。"""
    tot = attrs.height
    return float(attrs.filter(cond.fill_null(False)).height / tot) if tot else 0.0


def half_stats(nav: pl.DataFrame) -> tuple[dict, dict]:
    a = nav.filter(pl.col("date") < pl.lit(SPLIT).str.to_date()).sort("date")
    b = nav.filter(pl.col("date") >= pl.lit(SPLIT).str.to_date()).sort("date")
    return perf_stats(a), perf_stats(b)


def stage_diag(panel, feat, elig, attrs, trades) -> None:
    """canonical 成交在各層的分佈與貢獻——機制先驗:沒交易的層,排除它必然無效。"""
    ent = (trades.select([C, "entry_date", "ret_net", "days_held"])
           .join(attrs.rename({"date": "entry_date"}), on=["entry_date", C], how="left"))
    print("\n=== 診斷 1:canonical 689 筆成交的產業分佈 ===")
    g = (ent.with_columns(
            pl.when(_in(FIN)).then(pl.lit("金融"))
            .when(_in(CONSTR)).then(pl.lit("營建"))
            .when(_in(NONSTD)).then(pl.lit("非標準"))
            .when(_in(ELEC)).then(pl.lit("電子鏈"))
            .when(pl.col("industry").is_null()).then(pl.lit("(無分類)"))
            .otherwise(pl.lit("其他傳產")).alias("grp"))
         .group_by("grp").agg([pl.len().alias("n"),
                               pl.col("ret_net").mean().alias("mean_ret"),
                               pl.col("ret_net").median().alias("med_ret"),
                               (pl.col("ret_net") > 0).mean().alias("winrate"),
                               pl.col("ret_net").sum().alias("sum_ret")])
         .sort("sum_ret", descending=True))
    print(g)

    print("\n=== 診斷 2:逐產業(≥8 筆)===")
    g2 = (ent.group_by("industry").agg([pl.len().alias("n"),
                                        pl.col("ret_net").mean().alias("mean_ret"),
                                        (pl.col("ret_net") > 0).mean().alias("winrate"),
                                        pl.col("ret_net").sum().alias("sum_ret")])
          .filter(pl.col("n") >= 8).sort("sum_ret", descending=True))
    with pl.Config(tbl_rows=40):
        print(g2)

    print("\n=== 診斷 3:市值 / 波動 五分位 ===")
    for col in ("mktcap_pct", "vol60_pct"):
        q = (ent.filter(pl.col(col).is_not_null())
             .with_columns((pl.col(col) * 5).ceil().cast(pl.Int32).clip(1, 5).alias("q"))
             .group_by("q").agg([pl.len().alias("n"),
                                 pl.col("ret_net").mean().alias("mean_ret"),
                                 (pl.col("ret_net") > 0).mean().alias("winrate"),
                                 pl.col("ret_net").sum().alias("sum_ret")])
             .sort("q"))
        print(f"-- {col}(1=最小/最低,5=最大/最高)--")
        print(q)

    print("\n=== 診斷 4:各變體會從池裡拿掉多少 ===")
    rows = [{"variant": k, "pool_excluded_frac": _pool_frac(elig, attrs, v)}
            for k, v in _all_variants().items()]
    with pl.Config(tbl_rows=40):
        print(pl.DataFrame(rows))
    OUT.mkdir(parents=True, exist_ok=True)
    g2.write_csv(OUT / "x_universe_strata_diag.csv")


def stage_grid(panel, feat, elig, attrs, nav_b, tr_b, names: list[str] | None = None,
               tag: str = "grid") -> pl.DataFrame:
    base = perf_stats(nav_b)
    base_lo = block_bootstrap_lower(nav_b)
    print(f"\ncanonical: CAGR {base['cagr']:+.4f} Sortino {base['sortino']:.3f} "
          f"Calmar {base['calmar']:.3f} MDD {base['mdd']:.4f} boot_lo {base_lo:+.4f} "
          f"trades {tr_b.height}")

    names = names or list(VARIANTS)
    allv = _all_variants()
    navs: dict[str, pl.DataFrame] = {}
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=6, initializer=_init) as ex:
        for name, nav, tr in ex.map(_run_one, names):
            navs[name] = nav
            navs[name + "__trades"] = tr
    print(f"[{time.time() - t0:.1f}s] {len(names)} 變體跑完")

    rows = []
    for name in names:
        nav, tr = navs[name], navs[name + "__trades"]
        st = perf_stats(nav)
        lo = block_bootstrap_lower(nav)
        pb = paired_block_bootstrap(nav, nav_b)
        d2 = (st["sortino"] >= base["sortino"] and st["calmar"] >= base["calmar"]
              and st["mdd"] >= base["mdd"] and lo >= base_lo)
        rows.append({
            "variant": name,
            "pool_excl_frac": round(_pool_frac(elig, attrs, allv[name]), 4),
            "n_trades": tr.height, "cagr": st["cagr"], "sortino": st["sortino"],
            "calmar": st["calmar"], "mdd": st["mdd"], "boot_lo": lo,
            "d_cagr": st["cagr"] - base["cagr"],
            "paired_ann_diff": pb["ann_diff"], "ci_lo": pb["ci_lo"], "ci_hi": pb["ci_hi"],
            "p_le0": pb["p_le0"], "d2_pass": bool(d2),
            "sig": bool(pb["ci_lo"] > 0 or pb["ci_hi"] < 0),
        })
    res = pl.DataFrame(rows).sort("paired_ann_diff", descending=True)
    OUT.mkdir(parents=True, exist_ok=True)
    res.write_csv(OUT / f"x_universe_strata_{tag}.csv")
    with pl.Config(tbl_rows=40, tbl_cols=20, fmt_str_lengths=40):
        print(res)

    # 通過 D2 或配對顯著為正者 → 補分段/逐年一致性
    cands = res.filter(pl.col("d2_pass") | (pl.col("ci_lo") > 0))["variant"].to_list()
    for name in cands:
        nav = navs[name]
        h1, h2 = half_stats(nav)
        b1, b2 = half_stats(nav_b)
        print(f"\n--- {name} 分段 ---")
        print(f"  2014-10~2020-06: CAGR {h1['cagr']:+.4f} (canon {b1['cagr']:+.4f}) "
              f"Sortino {h1['sortino']:.2f} ({b1['sortino']:.2f}) MDD {h1['mdd']:.3f} ({b1['mdd']:.3f})")
        print(f"  2020-07~2026-07: CAGR {h2['cagr']:+.4f} (canon {b2['cagr']:+.4f}) "
              f"Sortino {h2['sortino']:.2f} ({b2['sortino']:.2f}) MDD {h2['mdd']:.3f} ({b2['mdd']:.3f})")
        yv = yearly_table(nav).rename({"ret": "ret_v", "mdd": "mdd_v"})
        yb = yearly_table(nav_b).rename({"ret": "ret_c", "mdd": "mdd_c"})
        yy = yv.join(yb, on="year").with_columns((pl.col("ret_v") - pl.col("ret_c")).alias("dret"))
        print("  逐年 Δret:", ", ".join(f"{r['year']}:{r['dret']:+.3f}"
                                        for r in yy.iter_rows(named=True)))
        print(f"  逐年勝出年數 {int((yy['dret'] > 0).sum())}/{yy.height}")
    if not cands:
        print("\n(無變體通過 D2,也無配對 CI 全正 → 全維度證偽)")
    return res


def stage_breadth() -> pl.DataFrame:
    """第三階段:把「廣度稅」從「分層效果」裡扣掉。

    grid 顯示排掉任何一層都變差,但安慰劑(隨機排掉同比例個股)也一樣變差——
    代表大部分損失來自**候選變少**本身,與該層的品質無關。作法:
      1. 用安慰劑點 (排除比例 f, 年化 CAGR 差 d) 過原點迴歸出廣度稅斜率 b
         (f=0 時 d=0 是定義上的真值,故不含截距);殘差標準差 = 純運氣的尺度。
      2. 每個分層變體的 excess = d − b·f,z = excess / 殘差 sd。
         |z| < 2 → 與隨機排除無異(該層沒有特殊性,結論:此分層無資訊)。
         z < −2  → 排掉它比隨機排更痛(該層是 alpha 載體,**不可排**)。
         z > +2  → 排掉它比隨機排便宜(該層確實是雜訊,但仍需 d > 0 才算改進)。
    """
    g = pl.read_csv(OUT / "x_universe_strata_grid.csv")
    s2 = pl.read_csv(OUT / "x_universe_strata_stage2.csv")
    allr = pl.concat([g, s2], how="vertical_relaxed")
    pb = allr.filter(pl.col("variant").str.starts_with("PLACEBO"))
    f = pb["pool_excl_frac"].to_numpy()
    d = pb["paired_ann_diff"].to_numpy()
    b = float((f * d).sum() / (f * f).sum())
    resid = d - b * f
    sd = float(resid.std(ddof=1))
    print(f"\n=== 廣度稅基線(安慰劑 n={len(f)})===")
    print(f"  斜率 b = {b:+.4f}(每排除池的 1%,年化 CAGR 損 {abs(b) / 100:.4f} → "
          f"排 20% 期望 {b * 0.2:+.3f})")
    print(f"  安慰劑殘差 sd = {sd:.4f}(這就是「同樣排 20%,換個隨機種子」的純運氣尺度)")
    print(pb.select(["variant", "pool_excl_frac", "paired_ann_diff"])
          .sort("pool_excl_frac"))

    # 中心值改用「最近比例桶的安慰劑實測均值」——不假設廣度稅是線性的
    # (0→70% 跨度大,線性外插會系統性偏誤);離散尺度仍用跨桶合併殘差 sd
    # (每桶只有 5 個種子,單桶 sd 太不穩)。
    buck = (pb.with_columns((pl.col("pool_excl_frac") * 10).round().alias("_b"))
            .group_by("_b").agg([pl.col("pool_excl_frac").mean().alias("bf"),
                                 pl.col("paired_ann_diff").mean().alias("bm")])
            .sort("bf"))
    bf, bm = buck["bf"].to_numpy(), buck["bm"].to_numpy()
    print("\n  安慰劑分桶均值(排除比例 → 年化 CAGR 差):",
          ", ".join(f"{x:.0%}→{y:+.3f}" for x, y in zip(bf, bm)))

    def _expected(fr: float) -> float:
        return float(bm[int(np.argmin(np.abs(bf - fr)))])

    out = (allr.filter(~pl.col("variant").str.starts_with("PLACEBO"))
           .with_columns(pl.col("pool_excl_frac").map_elements(
               _expected, return_dtype=pl.Float64).alias("placebo_exp"))
           .with_columns(
               (pl.col("paired_ann_diff") - pl.col("placebo_exp")).alias("excess"))
           .with_columns((pl.col("excess") / sd).alias("z_vs_placebo"))
           .with_columns(pl.when(pl.col("z_vs_placebo") < -2).then(pl.lit("alpha 載體(不可排)"))
                         .when(pl.col("z_vs_placebo") > 2).then(pl.lit("該層是雜訊(排掉比隨機便宜)"))
                         .otherwise(pl.lit("與隨機排除無異")).alias("verdict"))
           .select(["variant", "pool_excl_frac", "paired_ann_diff", "placebo_exp",
                    "excess", "z_vs_placebo", "verdict"])
           .sort("z_vs_placebo", descending=True))
    with pl.Config(tbl_rows=40, tbl_width_chars=200, fmt_str_lengths=40):
        print("\n=== 扣掉廣度稅後的分層效果 ===")
        print(out)
    out.write_csv(OUT / "x_universe_strata_breadth.csv")
    return out


def main() -> None:
    stage = sys.argv[1] if len(sys.argv) > 1 else "all"
    if stage == "breadth":       # 只讀前兩階段 CSV,不需引擎
        stage_breadth()
        return
    con = data.connect()
    t0 = time.time()
    panel, feat, elig = prep_cached(con)
    attrs = attrs_cached(con, panel, elig)
    nav_b, tr_b = run_s_full(panel, feat, elig, DS)
    print(f"[{time.time() - t0:.1f}s] prep + attrs + canonical done "
          f"(attrs {attrs.height} 列;產業覆蓋 "
          f"{1 - attrs['industry'].null_count() / attrs.height:.3f}、市值覆蓋 "
          f"{1 - attrs['mktcap'].null_count() / attrs.height:.3f})")
    if stage in ("diag", "all"):
        stage_diag(panel, feat, elig, attrs, tr_b)
    if stage in ("grid", "all"):
        stage_grid(panel, feat, elig, attrs, nav_b, tr_b)
    if stage in ("stage2", "all"):
        stage_grid(panel, feat, elig, attrs, nav_b, tr_b,
                   names=list(STAGE2), tag="stage2")
    if stage == "all":
        stage_breadth()


if __name__ == "__main__":
    main()
