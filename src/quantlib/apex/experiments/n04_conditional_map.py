"""N04 — S 持倉的條件期望地圖(把「方向 × 幅度 × 條件機率」套到趨勢側)。

**問題**:S 的出場是三個寫死的常數——trail 35%、time 30、輸家 time 15。這三個數字
是網格掃出來的,不是從條件分佈讀出來的。而且 35% 這種**價格百分比**門檻,對年化波動
20% 與 70% 的股票是完全不同的機率事件:同一條線在前者是 3 個標準差的極端事件、在後者
是家常便飯。「用對數收益率建模而不是價格」真正在講的就是這件事——**狀態要標準化才能比**。

**本檔做什麼**:不猜規則、不掃參數。先把 S 的候選在進場後 60 個交易日的路徑全部攤開,
對每一個「持有中的日子」編碼狀態,再讀出**條件期望**:

  狀態(方向 × 幅度,全部標準化):
    k      持有天數
    z_dd   距持有期峰值的回落 ÷ 進場時 20 日日波動(σ)——「幅度」
    pct_dd 同上但用原始百分比(對照組:證明標準化真的比較好用)
    run    連續上漲/下跌天數——「方向」
    z_cum  進場以來累計對數報酬 ÷ σ

  條件目標:E[未來 10 日對數報酬 | 狀態]、P(未來 10 日為正 | 狀態)

**判準**:期望值由正轉負的那條線,就是統計上該砍的位置。把它跟 S 現行的
trail 35% / time 30 / loser 15 對照,才知道現行規則是站在期望值的哪一側。

**紀律**:t 統計一律先取「同一觀測日的截面平均」再對日序列做——同一天的持倉不是
獨立樣本(N02 已量到 pooled t 會灌水一個數量級,連符號都能翻)。IS/OOS 依進場日
75/25 切分,檢查地圖在體制轉換下是否還成立。

**PIT**:σ 用進場日(含)為止的 20 日報酬算;路徑只往前走,不回看。
候選定義 = S 官方 `score_pool` + top-5,首次進榜才算一次進場(重複入榜不重複計)。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n04_conditional_map
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.assemble import entries_and_flags
from quantlib.apex.strategy_s import prep_cached, score_pool

C = "company_code"
START = "2015-01-01"
H = 60                      # 路徑長度(交易日);涵蓋 time_stop 30 有餘裕
FWD = 10                    # 條件目標地平線(交易日)
TOP_K = 5
OUT = paths.OUT / "apex" / "n04_condmap"


def _tstat(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    return float(x.mean() / (x.std(ddof=1) / np.sqrt(x.size))) if x.size >= 3 else float("nan")


def build_paths() -> pl.DataFrame:
    """S 候選首次進榜 → 未來 60 日路徑,逐日附上標準化狀態與前瞻報酬。"""
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    sc = (score_pool(feat, elig).select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(START).str.to_date()))
    entries, _ = entries_and_flags(sc, TOP_K, 10**9)
    # 榜內名次(1 = 當日最高分):用來檢驗「等權 20% × 5 席」有沒有浪費排名資訊
    entries = entries.with_columns(
        pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))

    # 首次進榜才算一次進場:連續在榜的第 2、3…天不重複開倉(與 S 的實際語義一致,
    # 也避免高度重疊的偽樣本把統計量灌水)。
    days = sc.select("date").unique().sort("date").with_row_index("di")
    e = (entries.join(days, on="date")
         .sort([C, "di"])
         .with_columns((pl.col("di") - pl.col("di").shift(1).over(C)).alias("gap"))
         .filter(pl.col("gap").is_null() | (pl.col("gap") > 1))
         .select(["date", C, "rk"]))

    p = (panel.select(["date", C, "close"]).sort([C, "date"])
         .with_columns([
             pl.int_range(pl.len()).over(C).alias("i"),
             (pl.col("close").log() - pl.col("close").log().shift(1)).over(C).alias("r"),
         ])
         .with_columns(pl.col("r").rolling_std(20).over(C).alias("sig")))

    ent = (e.join(p.select(["date", C, "i", "close", "sig"]), on=["date", C])
           .rename({"i": "i0", "close": "c0", "date": "entry_date"})
           .filter(pl.col("sig") > 0)
           .with_row_index("eid"))

    path = (ent.select(["eid", C, "entry_date", "i0", "c0", "sig", "rk"])
            .with_columns(pl.int_ranges(1, H + 1).alias("k"))
            .explode("k")
            .with_columns((pl.col("i0") + pl.col("k")).alias("i"))
            .join(p.select([C, "i", "date", "close"]).rename({"date": "obs_date"}),
                  on=[C, "i"], how="inner")
            .sort(["eid", "k"]))

    lg = pl.col("close").log()
    path = path.with_columns([
        (lg - pl.col("c0").log()).alias("cum"),
        # 峰值下限 = 進場價(Exit Semantics Contract:回測 peak_close = entry_close)
        pl.max_horizontal(pl.col("c0"), pl.col("close").cum_max().over("eid")).alias("peak"),
        # 路徑內日報酬:k=1 那天的前一根是進場日收盤
        (lg - pl.coalesce(lg.shift(1).over("eid"), pl.col("c0").log())).alias("lr"),
    ]).with_columns([
        (lg - pl.col("peak").log()).alias("dd"),
        (lg.shift(-FWD).over("eid") - lg).alias("fwd"),
    ]).with_columns([
        (pl.col("dd") / pl.col("sig")).alias("z_dd"),
        (pl.col("cum") / pl.col("sig")).alias("z_cum"),
        (pl.col("lr") < 0).alias("dn"),
    ]).with_columns((~pl.col("dn")).cum_sum().over("eid").alias("_g")) \
      .with_columns(pl.col("dn").cum_sum().over(["eid", "_g"]).alias("dn_run")) \
      .drop("_g")
    return path.drop_nulls("fwd")


def _cond(df: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """條件統計:樣本數、E[fwd] (bp)、P(fwd>0)、逐日 t。"""
    rows = []
    for key, s in df.group_by(by, maintain_order=True):
        v = s["fwd"].to_numpy()
        dm = s.group_by("obs_date").agg(pl.col("fwd").mean().alias("m"))["m"].to_numpy()
        rows.append(dict(zip(by, key)) | {
            "n": int(v.size), "E_fwd_bp": round(float(v.mean()) * 1e4, 1),
            "P_up": round(float((v > 0).mean()), 3), "t_daily": round(_tstat(dm), 2)})
    return pl.DataFrame(rows).sort(by)


def _bucket_zdd(col: str, edges: list[float]) -> pl.Expr:
    """回落分桶(0 = 在峰值上;數字越大代表回落越深)。"""
    e = pl.when(pl.col(col) >= -1e-9).then(pl.lit(0))
    for j, cut in enumerate(edges, start=1):
        e = e.when(pl.col(col) > cut).then(pl.lit(j))
    return e.otherwise(pl.lit(len(edges) + 1)).alias("bk")


def tail_risk() -> None:
    """條件分佈的尾巴(從已存 paths.parquet 重算,不需重建)。

    **為什麼一定要看這個**:【1】只給了條件**期望**,而停損的價值從來不在期望值——
    在避開左尾。若深回落桶的期望為正但左尾災難性,停損就仍然值得留;若左尾也還好,
    停損就是在正期望區砍倉。只看均值會把這兩種完全相反的情況混為一談。

    量三個:E[fwd]、左尾條件均值 CVaR5(最差 5% 的平均)、以及 E ÷ |CVaR5|
    (每承擔一單位左尾換到多少期望報酬——這才是「該不該續抱」的判準)。
    """
    p = pl.read_parquet(OUT / "paths.parquet")
    days = p.select("entry_date").unique().sort("entry_date")["entry_date"].to_list()
    cut = days[int(len(days) * 0.75)]
    ZE = [-0.5, -1.0, -2.0, -3.0, -5.0]
    lab = ["在峰值"] + [f"至 {c}σ" for c in ZE] + ["更深"]
    print("=" * 78)
    print(f"【7】條件分佈的尾巴:σ 回落桶 → E[fwd{FWD}] / 左尾 CVaR5 / 兩者之比")
    print("=" * 78)
    for tag, sub in (("IS", p.filter(pl.col("entry_date") <= cut)),
                     ("OOS", p.filter(pl.col("entry_date") > cut))):
        rows = []
        d = sub.with_columns(_bucket_zdd("z_dd", ZE))
        for bk in range(len(lab)):
            v = d.filter(pl.col("bk") == bk)["fwd"].to_numpy()
            if v.size < 200:
                continue
            cvar = float(np.mean(np.sort(v)[: max(1, int(v.size * 0.05))]))
            e = float(v.mean())
            rows.append({"bk": bk, "桶": lab[bk], "n": int(v.size),
                         "E_bp": round(e * 1e4, 1), "CVaR5_bp": round(cvar * 1e4, 1),
                         "P_跌逾10%": round(float((v < np.log(0.9)).mean()), 3),
                         "E÷|CVaR5|": round(e / abs(cvar), 3) if cvar < 0 else None})
        print(f"\n--- {tag} ---")
        with pl.Config(tbl_rows=-1, tbl_width_chars=140):
            print(pl.DataFrame(rows))


def rocket_prob() -> None:
    """右尾機率:進場狀態 → P(60 日內最大漲幅 ≥ +50%)。

    **為什麼這比期望值更切題**:S 是 5 席集中的右尾策略,組合績效由少數火箭的幅度
    決定,不由平均值決定。【6】量到低波候選的 E÷σ 較好,但 N05 端到端測試顯示減碼
    高波倉會砍掉 16~54pp 的 CAGR——若高波候選的**火箭機率**顯著較高,這個矛盾就有了
    量化的解釋,而不是只能說「大概是右尾」。

    火箭定義用**持有期內最大漲幅**(不是期末報酬):部位在路徑中觸及 +50% 時,trailing
    與止盈都可能已把它變現,期末值會低估這種倉的實際貢獻。
    """
    p = pl.read_parquet(OUT / "paths.parquet")
    ent = (p.group_by("eid").agg([
        pl.col("entry_date").first(), pl.col("sig").first(), pl.col("rk").first(),
        pl.col("cum").max().alias("max_cum"), pl.col("cum").min().alias("min_cum"),
    ]))
    days = ent.select("entry_date").unique().sort("entry_date")["entry_date"].to_list()
    cut = days[int(len(days) * 0.75)]
    print("=" * 78)
    print(f"【8】右尾機率:進場 σ 五分位 → P(持有 {H} 日內最大漲幅 ≥ +50% / +30%)")
    print("=" * 78)
    for tag, sub in (("IS", ent.filter(pl.col("entry_date") <= cut)),
                     ("OOS", ent.filter(pl.col("entry_date") > cut))):
        d = sub.with_columns(
            (pl.col("sig").rank("ordinal") / pl.len() * 5).ceil().cast(pl.Int32).alias("σ五分位"))
        t = (d.group_by("σ五分位").agg([
            pl.len().alias("n"),
            (pl.col("sig").median() * 1e4).round(0).alias("σ_bp"),
            (pl.col("max_cum") >= pl.lit(1.5).log()).mean().round(4).alias("P_火箭50"),
            (pl.col("max_cum") >= pl.lit(1.3).log()).mean().round(4).alias("P_漲30"),
            (pl.col("min_cum") <= pl.lit(0.7).log()).mean().round(4).alias("P_跌30"),
            (pl.col("max_cum").exp() - 1).median().round(4).alias("中位最大漲幅"),
        ]).sort("σ五分位"))
        print(f"\n--- {tag} ---")
        with pl.Config(tbl_rows=-1, tbl_width_chars=140):
            print(t)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    path = build_paths()
    path.write_parquet(OUT / "paths.parquet")
    n_ent = path["eid"].n_unique()
    print(f"進場樣本 {n_ent:,} 筆 | 持倉日觀測 {path.height:,} 列 | "
          f"進場期間 {path['entry_date'].min()} ~ {path['entry_date'].max()}")

    sig_pct = path.group_by("eid").first()["sig"].to_numpy()
    q = np.nanpercentile(sig_pct, [10, 50, 90])
    print("\n" + "=" * 78)
    print("【0】為什麼「固定 35% trail」是機率上不一致的門檻")
    print("=" * 78)
    print(f"進場時 20 日日波動 σ 分佈:P10={q[0]:.2%} / 中位={q[1]:.2%} / P90={q[2]:.2%}")
    for tag, s in (("P10 低波股", q[0]), ("中位股", q[1]), ("P90 高波股", q[2])):
        print(f"  {tag:10s} σ={s:.2%} → 35% 回落 = {abs(np.log(0.65)) / s:5.1f} 個 σ")
    print("同一條 35% 的線,對低波股是罕見的極端事件、對高波股是常態波動——"
          "它在不同標的上代表完全不同的機率。")

    is_cut = path.select("entry_date").unique().sort("entry_date")["entry_date"].to_list()
    cut = is_cut[int(len(is_cut) * 0.75)]
    print(f"\nIS 進場 ≤ {cut} | OOS 進場 > {cut}")
    IS = path.filter(pl.col("entry_date") <= cut)
    OOS = path.filter(pl.col("entry_date") > cut)

    print("\n" + "=" * 78)
    print(f"【1】幅度軸:距峰值回落 → 未來 {FWD} 日期望報酬(σ 標準化 vs 原始 %)")
    print("=" * 78)
    ZE = [-0.5, -1.0, -2.0, -3.0, -5.0]          # σ 單位
    PE = [-0.03, -0.07, -0.15, -0.25, -0.35]     # 原始對數百分比
    for name, col, edges in (("σ 標準化", "z_dd", ZE), ("原始 %", "dd", PE)):
        print(f"\n--- {name} 分桶(bk 0 = 在峰值;越大回落越深) ---")
        lab = ["在峰值"] + [f"至 {c}" for c in edges] + ["更深"]
        for tag, sub in (("IS", IS), ("OOS", OOS)):
            t = _cond(sub.with_columns(_bucket_zdd(col, edges)), ["bk"])
            t = t.with_columns(pl.col("bk").map_elements(
                lambda b: lab[int(b)], return_dtype=pl.Utf8).alias("桶"))
            print(f"{tag}:")
            print(t.select(["bk", "桶", "n", "E_fwd_bp", "P_up", "t_daily"]))

    print("\n" + "=" * 78)
    print(f"【2】時間軸 × 輸贏:持有天數 → 未來 {FWD} 日期望(對照 time30 / loser15)")
    print("=" * 78)
    kb = (pl.when(pl.col("k") <= 5).then(pl.lit("01-05"))
          .when(pl.col("k") <= 10).then(pl.lit("06-10"))
          .when(pl.col("k") <= 15).then(pl.lit("11-15"))
          .when(pl.col("k") <= 20).then(pl.lit("16-20"))
          .when(pl.col("k") <= 30).then(pl.lit("21-30"))
          .when(pl.col("k") <= 45).then(pl.lit("31-45"))
          .otherwise(pl.lit("46-60")).alias("k_bk"))
    for tag, sub in (("IS", IS), ("OOS", OOS)):
        d = sub.with_columns([kb, (pl.col("cum") > 0).alias("赢")])
        print(f"\n--- {tag} ---")
        with pl.Config(tbl_rows=-1):
            print(_cond(d, ["k_bk", "赢"]))

    print("\n" + "=" * 78)
    print(f"【3】方向軸:連續下跌天數 → 未來 {FWD} 日期望(持倉中的方向編碼)")
    print("=" * 78)
    for tag, sub in (("IS", IS), ("OOS", OOS)):
        d = sub.with_columns(pl.min_horizontal(pl.col("dn_run"), pl.lit(4)).alias("dn_bk"))
        print(f"\n--- {tag} ---")
        print(_cond(d, ["dn_bk"]))

    print("\n" + "=" * 78)
    print("【4】交叉表:持有天數 × σ 回落 → 期望值換號的那條線在哪")
    print("=" * 78)
    print("(欄 = 回落桶:0 在峰值 / 1 至 −0.5σ / 2 至 −1σ / 3 至 −2σ / 4 至 −3σ / "
          "5 至 −5σ / 6 更深;值 = 未來 10 日期望對數報酬 bp,n<200 的格留白)")
    for tag, sub in (("IS", IS), ("OOS", OOS)):
        d = sub.with_columns([kb, _bucket_zdd("z_dd", ZE)])
        x = _cond(d, ["k_bk", "bk"]).filter(pl.col("n") >= 200)
        print(f"\n--- {tag} ---")
        with pl.Config(tbl_rows=-1, tbl_width_chars=140):
            print(x.pivot(values="E_fwd_bp", index="k_bk", on="bk",
                          aggregate_function="first").sort("k_bk"))
        x.write_parquet(OUT / f"cross_k_zdd_{tag}.parquet")

    # ── 資金分配軸:等權 20% × 5 席,是另一個「用點估計取代條件分佈」的地方 ──
    print("\n" + "=" * 78)
    print(f"【5】榜內名次 → 未來 {FWD} 日期望(等權有沒有浪費排名資訊?)")
    print("=" * 78)
    for tag, sub in (("IS", IS), ("OOS", OOS)):
        print(f"\n--- {tag} ---")
        with pl.Config(tbl_rows=-1):
            print(_cond(sub, ["rk"]))

    print("\n" + "=" * 78)
    print(f"【6】進場波動分層 → 未來 {FWD} 日期望與風險調整後期望(σ 五分位)")
    print("=" * 78)
    for tag, sub in (("IS", IS), ("OOS", OOS)):
        d = sub.with_columns(
            (pl.col("sig").rank("ordinal") / pl.len() * 5).ceil().cast(pl.Int32).alias("σ五分位"))
        t = _cond(d, ["σ五分位"])
        med = (d.group_by("σ五分位").agg(pl.col("sig").median().alias("σ中位"))
               .with_columns((pl.col("σ中位") * 1e4).round(0).alias("σ_bp")).drop("σ中位"))
        t = (t.join(med, on="σ五分位")
             .with_columns((pl.col("E_fwd_bp") / pl.col("σ_bp")).round(3).alias("E÷σ")))
        print(f"\n--- {tag} ---")
        with pl.Config(tbl_rows=-1):
            print(t)
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
