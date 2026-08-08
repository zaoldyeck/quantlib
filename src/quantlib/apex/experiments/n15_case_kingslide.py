"""N15 — 個案診斷:S 為什麼選不到 2059 川湖(King Slide)。

**問題來源**:使用者 2026-08 主觀交易川湖大賺,問 S 為什麼選不到。逐層回答三件事:
(1) **gate 層**——川湖有沒有進過 S 的候選池(fresh≤7 / 流動性 / 六因子完整 / cfo 閘);
(2) **rank 層**——在池內排第幾、被哪個因子綁死(top_k=5 才進得了場);
(3) **結構層**——就算每月完美選中,S 的月中切片節奏(公布後買、下次公布前 5 天出清)
    能吃到川湖行情的多少;空手的公布窗(每月 6-10 日前後)貢獻了多少。

診斷手法:(1)(2) 鏡射 score_pool 的過濾鏈做**逐步歸因**(生產計分唯一真源仍是
strategy_s.score_pool/canonical_score,本檔只讀不改);(3) 用還原價收盤對收盤,
每月「11 日後首個交易日進、次月 5 日前最後交易日出」串接,對照同錨買進抱住。

依賴 cache: 是(prep_cached;凍結於 cache 最新日)。
run: uv run --project . python -m quantlib.apex.experiments.n15_case_kingslide
"""
from __future__ import annotations

import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.strategy_s import WREL, prep_cached, run_s_full, score_pool

C = "company_code"
CODE = "2059"
START = "2015-01-01"
TRADES = paths.OUT / "apex" / "n07_exit_census" / "trades.parquet"
OUT = paths.OUT / "apex" / "n15_kingslide"


def slices_vs_buyhold(px: pl.DataFrame, span_start: str) -> dict:
    """月中切片(11 日進、次月 5 日出)串接 vs 同錨買進抱住。還原價、收盤對收盤、未含成本。"""
    p = px.filter(pl.col("date") >= pl.lit(span_start).str.to_date())
    if p.height < 40:
        return {}
    d = p.with_columns([pl.col("date").dt.day().alias("dd"),
                        pl.col("date").dt.truncate("1mo").alias("mo")])
    ent = d.filter(pl.col("dd") >= 11).group_by("mo").agg(pl.col("date").min().alias("edate"))
    ext = d.filter(pl.col("dd") <= 5).group_by("mo").agg(pl.col("date").max().alias("xdate"))
    pairs = (ent.with_columns(pl.col("mo").dt.offset_by("1mo").alias("nmo"))
             .join(ext.rename({"mo": "nmo"}), on="nmo", how="inner")
             .join(p.rename({"date": "edate", "close": "ec"}).select(["edate", "ec"]), on="edate")
             .join(p.rename({"date": "xdate", "close": "xc"}).select(["xdate", "xc"]), on="xdate")
             .with_columns((pl.col("xc") / pl.col("ec") - 1).alias("r"))
             .sort("edate"))
    if pairs.is_empty():
        return {}
    sl = float((pairs["r"] + 1).product()) - 1
    e0 = pairs["edate"][0]
    c0 = p.filter(pl.col("date") == e0)["close"].item()
    bh = float(p.tail(1)["close"].item() / c0) - 1
    return {"起點": str(e0), "切片月數": pairs.height,
            "月中切片串接": round(sl, 4), "同錨買進抱住": round(bh, 4),
            "空手窗合計(隱含)": round((1 + bh) / (1 + sl) - 1, 4)}


def counterfactual_no_cfo() -> None:
    """反事實:拿掉 cfo 閘的 S。回答兩件事——會不會買到川湖?整體代價是什麼?

    cfo 閘擋掉川湖 95% 的資格日(739/775;n15 主診斷),且非資料缺漏而是真值:
    川湖高成長期應收/存貨隨營收暴增壓低 CFO,cfo/ni ≈ 0.9-1.2 低於池中位 1.1-1.5。
    B02(2026-07-09)已驗 gate 分位 p50 內部最優;本函式做的是**個案取捨的量化**:
    放閘換到川湖型 vs 放進來的營收灌水者整體傷多少。
    """
    from quantlib.apex import metrics
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    SPLIT = "2021-07-01"
    rows = []
    for label, q in (("A0 cfo 閘 p50(現行)", 0.5), ("C0 無 cfo 閘", 0.0)):
        for tag, start in (("全窗", START), ("後窗", SPLIT)):
            nav, tr = run_s_full(panel, feat, elig, start, _cfo_q=q)
            st = metrics.summarize(nav, tr)
            hit = tr.filter(pl.col(C) == CODE)
            rows.append({"臂": label, "窗": tag, "cagr": round(st["cagr"], 4),
                         "sharpe": round(st["sharpe"], 3), "mdd": round(st["mdd"], 4),
                         "川湖筆數": hit.height})
            if hit.height:
                with pl.Config(tbl_rows=-1, tbl_width_chars=160):
                    print(f"\n{label}/{tag} 買到川湖的交易:")
                    print(hit.select(["entry_date", "exit_date", "ret_net",
                                      "days_held", "exit_reason"]))
    with pl.Config(tbl_rows=-1, float_precision=4):
        print(pl.DataFrame(rows))


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    f59 = feat.filter(pl.col(C) == CODE)
    assert f59.height, "2059 不在特徵 panel 內"

    print("=" * 86)
    print("【0】S 全史官方交易中的 2059")
    print("=" * 86)
    tr = pl.read_parquet(TRADES) if TRADES.exists() else run_s_full(panel, feat, elig, START)[1]
    hit = tr.filter(pl.col(C) == CODE)
    if hit.height:
        with pl.Config(tbl_rows=-1, tbl_width_chars=160):
            print(hit)
    else:
        print(f"零筆(全史 {tr.height} 筆交易)—— S 從未進場過川湖")

    # ── gate 層:逐步歸因(鏡射 score_pool 過濾鏈) ──
    pre = (feat.filter(pl.col("rev_fresh_days") <= 7)
           .join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
           .drop_nulls(subset=list(WREL))
           .filter(pl.all_horizontal([pl.col(c).is_finite() for c in WREL])))
    pre59 = pre.filter(pl.col(C) == CODE)
    pool = (score_pool(feat, elig)
            .with_columns([((pl.col(c).rank() / pl.len()).over("date") * 100)
                           .round(0).alias(f"p_{c}") for c in WREL])
            .with_columns([
                pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"),
                pl.len().over("date").alias("pool_n"),
            ]))
    k = pool.filter(pl.col(C) == CODE).sort("date")
    k.write_parquet(OUT / "kingslide_pool_days.parquet")

    print("\n" + "=" * 86)
    print("【1】gate 層:川湖有沒有進池")
    print("=" * 86)
    n_fresh = f59.filter(pl.col("rev_fresh_days") <= 7).height
    print(f"全史交易日 {f59.height:,} | 月中窗(fresh≤7)天數 {n_fresh:,} | "
          f"過流動性+六因子完整 {pre59.height:,} | 再過 cfo 閘(最終在池) {k.height:,}")
    print(f"cfo 閘擋掉 {pre59.height - k.height:,} 天 | "
          f"在池期間 {k['date'].min()} ~ {k['date'].max()}")

    print("\n" + "=" * 86)
    print("【2】rank 層:在池內排第幾(進場門檻 = 每日前 5 名)")
    print("=" * 86)
    print(f"在池 {k.height} 天 | 最佳名次 {k['rk'].min()} | 中位名次 {k['rk'].median():.0f} | "
          f"名次≤5 天數 {k.filter(pl.col('rk') <= 5).height} | "
          f"≤10 {k.filter(pl.col('rk') <= 10).height} | ≤30 {k.filter(pl.col('rk') <= 30).height}")
    yearly = (k.group_by(pl.col("date").dt.year().alias("年"))
              .agg([pl.len().alias("在池天"), pl.col("rk").min().alias("最佳名次"),
                    pl.col("rk").median().alias("中位名次"),
                    pl.col("pool_n").median().alias("池大小")]).sort("年"))
    with pl.Config(tbl_rows=-1):
        print(yearly)

    pcols = [f"p_{c}" for c in WREL]
    show = ["date", "rk", "pool_n", "rev_yoy", "rev_yoy_accel"] + pcols
    print("\n--- 全史最接近進場的 10 天(名次最低;p_* = 該因子當日百分位,越高越好)---")
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=220, float_precision=3):
        print(k.sort("rk").head(10).select(show).sort("date"))

    print("\n--- 2024-01 以後逐月最佳名次(每月取名次最低那天)---")
    mb = (k.filter(pl.col("date") >= pl.date(2024, 1, 1))
          .group_by(pl.col("date").dt.truncate("1mo").alias("月"))
          .agg(pl.all().sort_by("rk").first()).sort("月"))
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=220, float_precision=3):
        print(mb.select(["月"] + show[1:] + [pl.col("rev_seq")]))

    print("\n--- 因子百分位中位數(在池日;100 = 全池最強)---")
    for tag, kk in (("全史", k), ("2024-01+", k.filter(pl.col("date") >= pl.date(2024, 1, 1)))):
        med = {c: round(float(kk[f"p_{c}"].median()), 0) for c in WREL}
        print(f"{tag:9s} {med}")

    print("\n" + "=" * 86)
    print("【3】結構層:就算每月完美選中,S 的月中切片能吃到多少(還原價,未含成本)")
    print("=" * 86)
    px = panel.filter(pl.col(C) == CODE).select(["date", "close"]).sort("date")
    for span in ("2023-01-01", "2024-01-01", "2025-01-01"):
        r = slices_vs_buyhold(px, span)
        print(f"{span} 起  {r}")
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
