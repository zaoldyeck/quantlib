"""apex_revcycle_S 官方 replay 引擎(唯一真源)。

S 的特徵組裝(prep)+ 規格回測(run_s,STRATEGY.md §4-§6,5 檔 20%、trail 35%、
時間止損 30/15、無絕對停損、6 因子)。生產與研究路徑(tri.pnl_dashboard s_nav、
s01 分佈診斷、chart 對比圖)一律 import 這份,禁止各自重寫(2026-07-20:從
experiments/chart_s_vs_benchmarks 搬到正式模組,消除「策略引擎住在畫圖檔」壞味道)。
冠軍規格與研發史見 apex/STRATEGY.md、apex/REPORT.md。

依賴 cache: 是(prep 讀 industry_taxonomy_pit;run_s 走 apex.engine.simulate)。
"""
from __future__ import annotations

import hashlib
import os
from datetime import date as Date

import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DS = "2014-10-31"          # 特徵 panel 載入起點(正2 上市日;固定錨,非資料截止)
WREL = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0,
        "mom_126_5": 0.5, "rev_seq": 0.5, "accel_rel": 0.5}


def prep(con, end: str | None = None):
    """S 的特徵組裝(六軸 + PIT 環比/同業相對加速)。end 預設 = cache 最新日
    (動態,見 data.latest_date);LIVE 儀表板重用本函式時必須傳入同一 end。"""
    de = end or data.latest_date(con).isoformat()
    panel, feat, _ = build_features(con, DS, de)
    rev = (data.load_monthly_revenue(con, de)
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("avail"),
               # 分母護欄(2026-07-23 稽核 D-apex-s-live):前三月營收合計為 0 時
               # ratio=+inf、0/0=NaN,兩者皆非 null → 不被 drop_nulls 剔除,polars rank
               # 會把 inf/NaN 排到近頂/絕對頂(建設股認列跳動,6 列)。未定義的成長率
               # 該 null 掉,與 close_pos_20 對 high==low 的 when/else None 同款範式。
               pl.when(pl.col("monthly_revenue").rolling_sum(3).shift(3) > 0)
               .then(pl.col("monthly_revenue").rolling_sum(3)
                     / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
               .otherwise(None)
               .over(C).alias("rev_seq"),
           ])
           .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance="70d")
            .sort([C, "date"]))
    tax = con.sql(
        "SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
        "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
    fx = (feat.select(["date", C, "rev_yoy_accel"]).drop_nulls().sort("date")
          .join_asof(tax.sort("effective_date"), left_on="date",
                     right_on="effective_date", by=C, strategy="backward")
          .drop_nulls(subset=["industry"]))
    ind_med = fx.group_by(["date", "industry"]).agg(
        pl.col("rev_yoy_accel").median().alias("m"))
    rel = (fx.join(ind_med, on=["date", "industry"], how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("m")).alias("accel_rel"))
           .select(["date", C, "accel_rel"]))
    feat = feat.join(rel, on=["date", C], how="left")
    elig = data.eligibility(panel, min_adv=5_000_000.0)
    return panel, feat, elig


def prep_cached(con, end: str | None = None):
    """prep 的磁碟快取版(極速鐵律:昂貴衍生物必快取)。key 含 cache.duckdb mtime——資料世代
    一變即失效重算。特徵組裝 ~31s → 迭代研究首次算完後 ~0.3s 秒回。生產路徑仍用 prep(要當下
    最新);純研究掃參/掃變體用本函式免每次重算。"""
    de = end or data.latest_date(con).isoformat()
    key = hashlib.md5(f"{de}_{os.path.getmtime(paths.CACHE_DB)}".encode()).hexdigest()[:12]
    cdir = paths.CACHE_DIR / "prep_cache"
    cdir.mkdir(parents=True, exist_ok=True)
    fs = {n: cdir / f"s_{n}_{key}.parquet" for n in ("panel", "feat", "elig")}
    if all(f.exists() for f in fs.values()):
        return tuple(pl.read_parquet(fs[n]) for n in ("panel", "feat", "elig"))
    panel, feat, elig = prep(con, de)
    for n, df in zip(("panel", "feat", "elig"), (panel, feat, elig)):
        df.write_parquet(fs[n])
    return panel, feat, elig


def score_pool(feat, elig, *, fresh_days: int = 7, cfo_q: float = 0.5,
               wrel: dict | None = None, score_fn=None) -> pl.DataFrame:
    """S 的候選池過濾 + 計分(唯一真源)——回傳含 `score` 欄的逐日逐股 df。

    從 run_s_full 抽出的可重用單元:回測、live、以及研究層(條件機率地圖、overlay
    對決)一律呼叫本函式,**禁止各自複製這段過濾/計分**。抽出的動機是實證的——研究層
    複製一份計分式後,任何一方改動都會讓對照組悄悄不再是 S(n03 七臂對決的 ΔCAGR
    會整批作廢);守護見 apex/tests/test_n03_overlay_parity.py。

    score_fn 不為 None 時由呼叫端自訂計分(收「過濾後含全因子欄的 df」、回傳含
    'score' 欄的 df),過濾邏輯仍走本函式,確保池籍不分岔。
    """
    wrel = wrel or WREL
    df = (feat.filter(pl.col("rev_fresh_days") <= fresh_days)
          .join(elig.filter(pl.col("eligible")).select(["date", C]),
                on=["date", C], how="semi")
          .drop_nulls(subset=list(wrel))
          # defense-in-depth(2026-07-23 稽核 D-apex-s-live):任何因子若殘留 inf/NaN
          # (drop_nulls 不剔除 NaN/inf),rank 會把它排到頂端污染選股。六因子一律要求
          # 有限值,關掉此陷阱(rev_seq 護欄已治本,此為第二道防線)。
          .filter(pl.all_horizontal([pl.col(c).is_finite() for c in wrel]))
          .filter(pl.col("cfo_ni_ratio_ttm")
                  >= pl.col("cfo_ni_ratio_ttm").quantile(cfo_q).over("date")))
    return score_fn(df) if score_fn is not None else canonical_score(df, wrel)


def canonical_score(df: pl.DataFrame, wrel: dict | None = None) -> pl.DataFrame:
    """S 的計分式(唯一真源):六因子逐日截面 rank 百分位的幾何加權乘積。

    獨立成函式是為了讓自訂 score_fn 能在「canonical 分數之上」疊加 overlay 而不必
    複製公式——複製品漂移會讓對照組不再是 S(見 score_pool docstring)。
    """
    expr = None
    for c_, wt in (wrel or WREL).items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    return df.with_columns(expr.alias("score"))


def run_s_full(panel, feat, elig, start: str, *,
               _exit_spec: "ExitSpec | None" = None,
               _port_spec: "PortSpec | None" = None,
               _exec_spec: "ExecSpec | None" = None,
               _fresh_days: int = 7, _stale_days: int = 26, _cfo_q: float = 0.5,
               _wrel: dict | None = None,
               _score_fn=None, _entries_fn=None, _top_k: int = 5,
               ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """S 規格回測(STRATEGY.md §4-§6),回 (歸一化 NAV, 交易明細 trades)。
    trades = TRADE_SCHEMA(進出場日/ret_net ROI/days_held/exit_reason;open=當下持有)。

    底線參數皆為**研究用 hooks(預設 = canonical S 規格,生產行為逐位不變)**,供 strat_lab
    變體實驗傳入,不改官方規格:
      _exit_spec/_port_spec/_exec_spec  出場/組合/執行(fill_at、min_hold_days、profit_recycle…)
      _fresh_days/_stale_days/_cfo_q    池新鮮度閘 / 陳舊出場 / cfo_ni 閘分位
      _wrel                             因子權重 dict(取代 monkeypatch WREL——後者非執行緒安全)
      _score_fn(df) -> df               自訂計分:收「過濾後含全因子欄的 df」、回傳含 'score' 欄
                                        (涵蓋聚合形態〔幾何/算術/z-score〕、非線性、時間平滑)
      _entries_fn(entries) -> entries   自訂進場後處理:絕對分數門檻、自訂 'weight' 欄(sizing)
      _top_k                            每日候選名單長度(與 n_slots 分離;>slots = 有備選)
    """
    wrel = _wrel or WREL
    scored = score_pool(feat, elig, fresh_days=_fresh_days, cfo_q=_cfo_q,
                        wrel=wrel, score_fn=_score_fn)
    keep = ["date", C, "score"] + (["weight"] if "weight" in scored.columns else [])
    sc = (scored.select(keep)
          .filter(pl.col("date") >= pl.lit(start).str.to_date()))
    entries, _ = entries_and_flags(sc, _top_k, 10**9)
    if "weight" in sc.columns:   # entries_and_flags 只留 score;把 weight 接回來
        entries = entries.join(sc.select(["date", C, "weight"]), on=["date", C], how="left")
    if _entries_fn is not None:
        entries = _entries_fn(entries)
    stale = (feat.filter(pl.col("rev_fresh_days") >= _stale_days).select(["date", C])
             .filter(pl.col("date") >= pl.lit(start).str.to_date()))
    res = simulate(panel, entries, exit_flags=stale, exec_spec=_exec_spec or ExecSpec(),
                   port_spec=_port_spec or PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=_exit_spec or ExitSpec(trailing_stop=0.35, time_stop=30,
                                                    loser_time_stop=15),
                   start=Date.fromisoformat(start))
    nav = (res.nav.select(["date", "nav"]).sort("date")
           .with_columns(pl.col("nav") / pl.col("nav").first()))
    trades = res.trades.filter(pl.col("entry_date") >= pl.lit(start).str.to_date())
    return nav, trades


def run_s(panel, feat, elig, start: str) -> pl.DataFrame:
    """run_s_full 的 NAV-only 薄包裝(既有 chart/s01/s_nav 呼叫者相容)。"""
    return run_s_full(panel, feat, elig, start)[0]
