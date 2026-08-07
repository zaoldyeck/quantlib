"""N02 — 均值回歸「跌久必漲」直接檢驗(社群貼文假設的自有樣本測試)。

**假設來源**:2026-08 社群貼文(自稱前 HFT 量化交易員的「真正的均值回歸」教學),
四個步驟:(1) 用對數報酬建模而非價格 (2) 自迴歸 + 方向編碼探索「跌久必漲」的
統計機率 (3) IS 75% / OOS 25% 時序分割 (4) 勝率 / 複利報酬 / 年化 Sharpe / 淨值曲線。

**本檔忠實實作該假設**(不是稻草人版):對數報酬、方向編碼(連續下跌 run length)、
方向 × 幅度二維切分、pooled AR(1)、IS/OOS 75/25 分割、勝率+複利+Sharpe+淨值。
在此之上補三項貼文缺的把關(見下),因為缺了它們的結論不能拿來改策略。

**補的三項把關(貼文沒有,但少了會得到假陽性)**:
  1. **還原價**:未還原收盤在除權息日製造假下跌 → 把除權息缺口偽裝成「連續下跌」
     訊號,且次日「反彈」是還原假象。台股 7-8 月除權息旺季會系統性汙染。
  2. **日均 t 統計**:pooled t 把同一天上千檔的截面相關當成獨立樣本,t 值灌水
     一個數量級。正確做法是先取每日截面均值再對日序列做 t(Fama-MacBeth 式)。
     本檔兩個都報,差距本身就是貼文方法的體檢報告。
  3. **台股來回成本 35.7 bp**(手續費 2×2.85bp 二折 + 證交稅 30bp)。日頻均值回歸
     的毛利必須先跨過這個門檻才有討論價值。

**判準**(沿用 apex F01 家法,比貼文嚴):條件效果需 IS/OOS 同號 + 日均 |t| ≥ 3
+ run length 單調 + 扣 35.7 bp 後仍為正,四項全過才算可用。

**結論(2026-08-07,定案;完整報告 docs/strategy_research/meanrev_post_verdict.md)**:
前三項全過、第四項全敗。均值回歸在台股**是真的**——剔除漲跌停日後 Spearman 秩相關
IC −0.0382(逐日 t −17.09、65.3% 交易日為負),強度高於 S 現役六因子之一的
close_pos_20(h5 IC 0.024);劑量反應單調(連跌 ≥1/2/3/4 日的次日毛邊際
−0.93/+1.75/+4.54/+8.01 bp)。但最好一格 8.01 bp 只有成本門檻 35.7 bp 的 1/4.5,
12 組組態扣成本後淨 CAGR 全負。**訊號是真的,值 8 bp;證交稅收 30 bp。**

⚠ **本檔最值得記住的方法教訓**:OLS 自迴歸原始係數 +0.0462(t +78)看起來是「動量」,
與上述結論完全相反——那是 ±10% 漲跌停日(僅佔 2.11% 列)當槓桿點主導迴歸的假象。
價格有硬邊界的市場,一律用秩相關(離群免疫)裁定訊號方向,OLS 係數只能參考。

依賴 cache: 是(prices.fetch_adjusted_panel 還原價面板 + daily_quote)。
run: uv run --project . python -m quantlib.apex.experiments.n02_meanrev_runlength
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data

C = "company_code"
DS = "2014-10-31"                 # 與 S 同起點(apex.strategy_s.DS)
COST_RT = 0.00357                 # 來回成本:手續費 2×0.0285% + 證交稅 0.3%
OUT = paths.OUT / "apex" / "n02_meanrev"


def _tstat(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    if x.size < 3:
        return float("nan")
    return float(x.mean() / (x.std(ddof=1) / np.sqrt(x.size)))


def build(min_adv: float = 5_000_000.0) -> pl.DataFrame:
    """回傳逐日逐股的方向編碼 panel:log 報酬、連跌/連漲 run length、前瞻報酬。

    只留 eligible 日(20 日中位成交值 ≥ min_adv、價 ≥ 10、掛牌 ≥ 60 根)——
    否則結果會被無法成交的雞蛋水餃股主導。
    """
    con = data.connect()
    end = data.latest_date(con).isoformat()
    panel = data.common_stocks(data.load_panel(con, DS, end))
    elig = data.eligibility(panel, min_adv=min_adv)

    df = (
        panel.select(["date", C, "close", "raw_close"])
        .sort([C, "date"])
        .with_columns(
            (pl.col("close").log() - pl.col("close").log().shift(1)).over(C).alias("r")
        )
        .drop_nulls("r")
    )
    # 方向編碼:down = r < 0。run length = 結束於 t 的連續下跌天數(今日非跌則 0)。
    # 作法:以「非下跌日」的累計數當群組 id,群組內下跌日累計 = run length。
    df = df.with_columns(
        [
            (pl.col("r") < 0).alias("dn"),
            (pl.col("r") > 0).alias("up"),
        ]
    ).with_columns(
        [
            (~pl.col("dn")).cum_sum().over(C).alias("_gd"),
            (~pl.col("up")).cum_sum().over(C).alias("_gu"),
        ]
    ).with_columns(
        [
            pl.col("dn").cum_sum().over([C, "_gd"]).alias("dn_run"),
            pl.col("up").cum_sum().over([C, "_gu"]).alias("up_run"),
        ]
    )
    # 幅度軸:該連跌段的累計對數跌幅(貼文「把收益率拆成方向和幅度」)
    df = df.with_columns(
        pl.when(pl.col("dn"))
        .then(pl.col("r").cum_sum().over([C, "_gd"]))
        .otherwise(None)
        .alias("dn_cum")
    )
    # 前瞻報酬(對數;fwd1 = 次日,fwd5/fwd21 = 未來 5/21 日累計)
    df = df.with_columns(
        [
            pl.col("r").shift(-1).over(C).alias("fwd1"),
            (pl.col("r").shift(-1).rolling_sum(5).shift(-4)).over(C).alias("fwd5"),
            (pl.col("r").shift(-1).rolling_sum(21).shift(-20)).over(C).alias("fwd21"),
        ]
    )
    # 截面超額(個股自己的均值回歸,剝掉大盤共同成分)
    df = df.with_columns(
        [
            (pl.col("fwd1") - pl.col("fwd1").median().over("date")).alias("fwd1_x"),
            (pl.col("r") - pl.col("r").median().over("date")).alias("r_x"),
        ]
    )
    return (
        df.join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
        .drop(["_gd", "_gu"])
        .sort(["date", C])
    )


def _split(df: pl.DataFrame, frac: float = 0.75) -> tuple[pl.DataFrame, pl.DataFrame]:
    """貼文的 IS 75% / OOS 25% 時序分割(按交易日,非按列——按列會洩漏)。"""
    days = df.select("date").unique().sort("date")["date"].to_list()
    cut = days[int(len(days) * frac)]
    return df.filter(pl.col("date") < cut), df.filter(pl.col("date") >= cut)


def bucket_stats(df: pl.DataFrame, run_col: str, tgt: str, max_run: int = 6) -> pl.DataFrame:
    """依 run length 分桶:樣本數、勝率、平均前瞻報酬、pooled t、日均 t。

    pooled t = 貼文方法(把同日上千檔當獨立樣本);日均 t = 先取每日截面均值再對
    日序列做 t(修掉截面相關)。兩者並列,差距即為貼文顯著性灌水的量測。
    """
    d = df.filter(pl.col(tgt).is_not_null()).with_columns(
        pl.min_horizontal(pl.col(run_col), pl.lit(max_run)).alias("bk")
    )
    rows = []
    for bk in sorted(d["bk"].unique().to_list()):
        s = d.filter(pl.col("bk") == bk)
        v = s[tgt].to_numpy()
        dm = (
            s.group_by("date").agg(pl.col(tgt).mean().alias("m")).sort("date")["m"].to_numpy()
        )
        rows.append(
            {
                "run": int(bk),
                "n": int(v.size),
                "win_rate": float((v > 0).mean()),
                "mean_bp": float(v.mean() * 1e4),
                "t_pooled": _tstat(v),
                "t_daily": _tstat(dm),
                "n_days": int(dm.size),
            }
        )
    return pl.DataFrame(rows)


def ar1(df: pl.DataFrame, xcol: str = "r", ycol: str = "fwd1") -> dict:
    """pooled AR(1):fwd1 = a + b·r。b < 0 = 均值回歸,b > 0 = 動量。"""
    s = df.select([xcol, ycol]).drop_nulls()
    x, y = s[xcol].to_numpy(), s[ycol].to_numpy()
    b, a = np.polyfit(x, y, 1)
    resid = y - (a + b * x)
    se = np.sqrt((resid**2).sum() / (x.size - 2) / ((x - x.mean()) ** 2).sum())
    return {"beta": float(b), "t": float(b / se), "n": int(x.size),
            "half_life_days": float(-np.log(2) / np.log(1 + b)) if -1 < b < 0 else float("nan")}


def strategy_nav(df: pl.DataFrame, min_run: int, hold: int = 1) -> dict:
    """貼文步驟 4:把訊號做成等權組合,報勝率 / 複利 / 年化 Sharpe / 淨值。

    每日等權買進 dn_run ≥ min_run 的全部個股,持有 hold 日。成本按每次換手
    收 COST_RT(hold=1 即每日全額換手,台股證交稅在此原形畢露)。
    """
    tgt = f"fwd{hold}"
    d = df.filter((pl.col("dn_run") >= min_run) & pl.col(tgt).is_not_null())
    daily = d.group_by("date").agg(
        [pl.col(tgt).mean().alias("ret_log"), pl.len().alias("n")]
    ).sort("date")
    if daily.is_empty():
        return {"min_run": min_run, "hold": hold, "n_days": 0}
    r = np.expm1(daily["ret_log"].to_numpy()) / hold          # 分攤成每日簡單報酬
    r_net = r - COST_RT / hold                                # 換手成本按持有期分攤
    ann = 252
    out = {}
    for tag, rr in (("gross", r), ("net", r_net)):
        nav = np.cumprod(1 + rr)
        yrs = len(rr) / ann
        out[f"cagr_{tag}"] = float(nav[-1] ** (1 / yrs) - 1)
        out[f"sharpe_{tag}"] = float(rr.mean() / rr.std(ddof=1) * np.sqrt(ann))
        out[f"mdd_{tag}"] = float((nav / np.maximum.accumulate(nav) - 1).min())
    out.update(
        {"min_run": min_run, "hold": hold, "n_days": len(r),
         "win_rate_daily": float((r > 0).mean()),
         "avg_names": float(daily["n"].mean()),
         "gross_edge_bp": float(r.mean() * 1e4),
         "cost_hurdle_bp": COST_RT / hold * 1e4}
    )
    return out


def robustness() -> None:
    """兩個對 AR(1) 結論的攻擊面,從已存 panel 重算(不需重建)。

    (1) **漲跌停鎖死**:台股 ±10% 漲跌停會製造人為的正自相關——當日鎖死的價格沒調整完,
        隔日繼續走。若剔除 |r| ≥ 9.5% 的日子後 b 仍顯著為正,則正自相關不是停板假象。
    (2) **逐年一致性**:單一年份(如 2020 崩跌反彈)可能主導 pooled 迴歸。
    """
    df = pl.read_parquet(OUT / "panel.parquet")
    lim = df.filter(pl.col("r").abs() < np.log(1.095))
    print("\n" + "=" * 78)
    print("【R1】剔除疑似漲跌停日(|log r| ≥ 9.5%)後的 AR(1)")
    print("=" * 78)
    print(f"剔除 {df.height - lim.height:,} 列 ({(1 - lim.height / df.height):.2%})")
    for tag, sub in (("含停板", df), ("剔停板", lim)):
        raw, xs = ar1(sub, "r", "fwd1"), ar1(sub, "r_x", "fwd1_x")
        print(f"{tag}  raw b={raw['beta']:+.5f} t={raw['t']:+7.1f} | "
              f"截面超額 b={xs['beta']:+.5f} t={xs['t']:+7.1f}")

    print("\n" + "=" * 78)
    print("【R2】逐年 AR(1)(截面超額;b>0 = 動量,b<0 = 均值回歸)")
    print("=" * 78)
    rows = []
    for y in sorted(lim.select(pl.col("date").dt.year().alias("y"))["y"].unique().to_list()):
        s = lim.filter(pl.col("date").dt.year() == y)
        a = ar1(s, "r_x", "fwd1_x")
        rows.append({"year": y, "beta": round(a["beta"], 5), "t": round(a["t"], 1), "n": a["n"]})
    print(pl.DataFrame(rows))
    print(f"\n負 beta 年數: {sum(1 for r in rows if r['beta'] < 0)} / {len(rows)}")

    # R3 — OLS 的 beta 被 ±10% 停板當槓桿點主導(2.1% 的列決定符號)。Spearman 秩相關
    # 對離群完全免疫,是「訊號到底有沒有方向」的裁判(即因子 IC 的定義)。
    print("\n" + "=" * 78)
    print("【R3】Spearman 秩相關 IC(離群免疫;負 = 均值回歸),逐日算再對日序列做 t")
    print("=" * 78)
    for tag, sub in (("含停板", df), ("剔停板", lim)):
        d = sub.select(["date", "r_x", "fwd1_x"]).drop_nulls()
        ic = (d.group_by("date")
              .agg(pl.corr(pl.col("r_x").rank(), pl.col("fwd1_x").rank()).alias("ic"))
              .sort("date"))
        v = ic["ic"].to_numpy()
        print(f"{tag}  mean IC={np.nanmean(v):+.4f}  t_daily={_tstat(v):+6.2f}  "
              f"負 IC 日佔比={np.nanmean(v < 0):.1%}  n_days={np.isfinite(v).sum()}")

    # R4 — 剔停板後重跑桶表與組合網格:符號翻轉是否讓貼文的策略「起死回生」?
    print("\n" + "=" * 78)
    print("【R4】剔停板後的連跌桶表(截面超額次日報酬)")
    print("=" * 78)
    for tag, sub in (("IS", _split(lim)[0]), ("OOS", _split(lim)[1])):
        print(f"\n--- {tag} ---")
        print(bucket_stats(sub, "dn_run", "fwd1_x"))

    print("\n" + "=" * 78)
    print("【R5】剔停板後的組合網格(毛 vs 扣 35.7bp)")
    print("=" * 78)
    res = [strategy_nav(lim, mr, h) for mr in (1, 2, 3, 4) for h in (1, 5, 21)]
    tab = pl.DataFrame([r for r in res if r.get("n_days")])
    with pl.Config(tbl_cols=-1, tbl_width_chars=200, tbl_rows=-1, float_precision=4):
        print(tab.select(["min_run", "hold", "gross_edge_bp", "cost_hurdle_bp",
                          "cagr_gross", "sharpe_gross", "cagr_net", "sharpe_net"]))
    tab.write_parquet(OUT / "strategy_grid_exlimit.parquet")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    df = build()
    print(f"panel: {df.height:,} 列 / {df['date'].n_unique():,} 交易日 / "
          f"{df[C].n_unique():,} 檔  [{df['date'].min()} ~ {df['date'].max()}]")
    df.write_parquet(OUT / "panel.parquet")

    is_, oos = _split(df)
    print(f"\nIS {is_['date'].min()}~{is_['date'].max()} ({is_.height:,} 列) | "
          f"OOS {oos['date'].min()}~{oos['date'].max()} ({oos.height:,} 列)")

    print("\n" + "=" * 78)
    print("【1】AR(1):次日對數報酬 ~ 今日對數報酬(b<0 = 均值回歸)")
    print("=" * 78)
    for tag, sub in (("ALL", df), ("IS", is_), ("OOS", oos)):
        raw = ar1(sub, "r", "fwd1")
        xs = ar1(sub, "r_x", "fwd1_x")
        print(f"{tag:4s} raw  b={raw['beta']:+.5f} t={raw['t']:+8.1f}  | "
              f"截面超額 b={xs['beta']:+.5f} t={xs['t']:+8.1f}  n={raw['n']:,}")

    print("\n" + "=" * 78)
    print("【2】方向編碼:連跌 N 日後的次日報酬(run=0 即今日非跌,當基準)")
    print("=" * 78)
    for tag, sub in (("IS", is_), ("OOS", oos)):
        for tgt in ("fwd1", "fwd1_x"):
            b = bucket_stats(sub, "dn_run", tgt)
            print(f"\n--- {tag} / {tgt} ---")
            print(b)
            b.write_parquet(OUT / f"buckets_{tag}_{tgt}.parquet")

    print("\n" + "=" * 78)
    print("【3】對照組:連漲 N 日後的次日報酬(動量側,檢查訊號是否只是雜訊對稱)")
    print("=" * 78)
    for tag, sub in (("IS", is_), ("OOS", oos)):
        print(f"\n--- {tag} / fwd1_x ---")
        print(bucket_stats(sub, "up_run", "fwd1_x"))

    print("\n" + "=" * 78)
    print("【4】幅度軸:連跌 ≥2 日,按累計跌幅五分位(貼文的『方向 × 幅度』)")
    print("=" * 78)
    deep = df.filter(pl.col("dn_run") >= 2).with_columns(
        (pl.col("dn_cum").rank("ordinal").over("date")
         / pl.len().over("date") * 5).ceil().cast(pl.Int32).alias("q")
    )
    for tag, sub in (("IS", _split(deep)[0]), ("OOS", _split(deep)[1])):
        rows = []
        for q in range(1, 6):
            s = sub.filter(pl.col("q") == q).drop_nulls("fwd1_x")
            dm = s.group_by("date").agg(pl.col("fwd1_x").mean().alias("m"))["m"].to_numpy()
            rows.append({"q(跌最深=1)": q, "n": s.height,
                         "mean_bp": float(s["fwd1_x"].mean() * 1e4),
                         "win_rate": float((s["fwd1_x"] > 0).mean()),
                         "t_daily": _tstat(dm)})
        print(f"\n--- {tag} ---")
        print(pl.DataFrame(rows))

    print("\n" + "=" * 78)
    print("【5】貼文步驟 4:做成組合的勝率 / 複利 / 年化 Sharpe(毛 vs 扣 35.7bp)")
    print("=" * 78)
    res = []
    for mr in (1, 2, 3, 4):
        for hold in (1, 5, 21):
            res.append(strategy_nav(df, mr, hold))
    tab = pl.DataFrame([r for r in res if r.get("n_days")])
    cols = ["min_run", "hold", "avg_names", "gross_edge_bp", "cost_hurdle_bp",
            "cagr_gross", "sharpe_gross", "cagr_net", "sharpe_net", "mdd_net"]
    with pl.Config(tbl_cols=-1, tbl_width_chars=200, float_precision=4):
        print(tab.select(cols))
    tab.write_parquet(OUT / "strategy_grid.parquet")
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
