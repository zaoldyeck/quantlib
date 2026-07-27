"""EV32:站位錨定的蒸餾樣本(取代 EV31)——把「暴漲起點」換成「實盤標記者站的那一天」。

## 為什麼要取代 EV31(兩個缺陷,都經自行實測確認)

EV31 已修好資料汙染、還原價、期間重疊三件事,但它自己帶進兩個新問題:

### 缺陷 A:對照組在前期動能上與正例系統性不同(混淆因子)
EV31 的對照組沿用舊定義「前 120 日漲 ≥25% 且前瞻最大 <20%」,但正例的選取
**完全沒有前期動能條件**。實測 EV31 樣本:

    暴漲組 prior120 中位 −7.3%,只有 18.6% 滿足 ≥+25%
    對照組 prior120 中位 +33.7%,100% 滿足 ≥+25%      ← 差 41 個百分點

蒸餾必然學到「前期已經漲很多 → 偽形」。**那 100% 是選樣條件造出來的,不是市場事實。**
修法:負例不再**要求**前期動能,改成與正例**配對**前期動能。

### 缺陷 B:用「暴漲起點日」當 t0 = 給了實盤永遠拿不到的視角
掃描器找的 t0 是「從這天起算前瞻報酬最大」的那一天——事後才知道。實盤的標記者
每月站在固定的站位日問「接下來會不會漲」。實測兩種錨定下正例的前期動能:

    起點日錨定:prior120 中位 −7.3%(系統性地挑在低點)
    站位日錨定:prior120 中位 +3.7%

差異證明起點日錨定會把樣本偏向「剛落底」那一型。修法:t0 一律改為**月度站位日**
(當月 10 日後首個交易日,與 live 標記日曆完全相同)。

## 站位錨定後的母體基準率(自行實測,非引用)
2008-01 ~ 2021-12、174 個站位、259,079 個「檔 × 站位」觀測:

    未來 120 交易日還原總報酬最高值 ≥ 50% → 13.96%
                                  ≥ 80% →  6.27%
                                  ≥100% →  4.01%

這個數字有實質用途:蒸餾與盲判的機率校準錨。給 30% 就是說「比隨機一檔高約 5 倍」。

## 設計(承襲 EV31 已驗證正確的部分)
還原價、120 交易日水平線、2008-01~2021-12 且前瞻窗須在 2022-07-01 前結束、
四碼普通股 + 已申報月營收、橫斷面流動性分位、年 × 層分層、limit_era 標籤。

## 新增
- **兩檔硬度的負例**:`near_miss`(前瞻最大 25~50%,看起來要動卻沒走完)與
  `quiet`(<15%,真的沒事)。只有 near_miss 的話蒸餾學不到「與沉寂股的差別」;
  只有 quiet 的話對比太容易、學不到細緻判別。各半。
- **配對鍵含前期動能十分位**,把缺陷 A 的混淆因子直接控制掉。

Run: uv run --project . python -m quantlib.evergreen.ev32_station_sample --explore
     uv run --project . python -m quantlib.evergreen.ev32_station_sample --per-cell 5
依賴 cache: 是。長任務,建議背景跑。
"""
from __future__ import annotations

import argparse
from datetime import date as Date

import polars as pl

from quantlib import paths, prices
from quantlib.apex import data

C = "company_code"
ERA_START, ERA_END = Date(2008, 1, 1), Date(2021, 12, 31)
FWD_MUST_END_BEFORE = Date(2022, 7, 1)
FWD_DAYS, PRIOR_DAYS = 120, 120
SURGE_MIN = 0.80
NEAR_MISS = (0.25, 0.50)      # 「看起來要動卻沒走完」
QUIET_MAX = 0.15              # 「真的沒事」
LIMIT_ERA_SPLIT = Date(2015, 6, 1)


def _panel(con) -> pl.DataFrame:
    fr = []
    for m in ("twse", "tpex"):
        f = prices.fetch_adjusted_panel(
            con, ERA_START.isoformat(), FWD_MUST_END_BEFORE.isoformat(),
            market=m, include_extra_history_days=PRIOR_DAYS + 60)
        if not f.is_empty():
            fr.append(f.select([C, "date", "close", "trade_value"])
                      .with_columns(pl.lit(m).alias("market")))
    return (pl.concat(fr).unique(subset=[C, "date"], keep="first")
            .filter(pl.col("close") > 0).sort([C, "date"]))


def _stations(panel: pl.DataFrame) -> list[Date]:
    """月度站位日 = 當月 10 日之後的第一個交易日(與 live 標記日曆同構)。"""
    d = panel.select("date").unique().sort("date")
    return (d.with_columns([pl.col("date").dt.strftime("%Y-%m").alias("ym"),
                            pl.col("date").dt.day().alias("dd")])
            .filter(pl.col("dd") > 10)
            .group_by("ym").agg(pl.col("date").min())
            .sort("date")["date"].to_list())


def _features(panel: pl.DataFrame) -> pl.DataFrame:
    return panel.with_columns([
        pl.col("close").shift(-1).reverse()
          .rolling_max(FWD_DAYS, min_samples=1).reverse().over(C).alias("fwd_max_px"),
        pl.col("date").shift(-FWD_DAYS).over(C).alias("fwd_end_date"),
        pl.col("close").shift(PRIOR_DAYS).over(C).alias("prior_px"),
        pl.col("trade_value").rolling_mean(20, min_samples=20).over(C).alias("adv20"),
    ]).with_columns([
        (pl.col("fwd_max_px") / pl.col("close") - 1.0).alias("fwd_max_ret"),
        (pl.col("close") / pl.col("prior_px") - 1.0).alias("prior_ret"),
    ])


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
                 & pl.col("fwd_max_ret").is_not_null()
                 & pl.col("prior_ret").is_not_null()
                 & pl.col("adv20").is_not_null()
                 & pl.col(C).str.contains(r"^[0-9]{4}$"))
         .join(first, on=C, how="inner")
         .filter(pl.col("date") >= pl.col("first_rev")).drop("first_rev"))
    u = u.with_columns([
        ((pl.col("adv20").rank("ordinal").over("date") * 10 - 1)
         // pl.len().over("date")).alias("adv_dec"),
        ((pl.col("prior_ret").rank("ordinal").over("date") * 10 - 1)
         // pl.len().over("date")).alias("mom_dec"),
    ])
    tax = con.sql("SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
                  "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
    return (u.sort("date")
            .join_asof(tax.sort("effective_date"), left_on="date",
                       right_on="effective_date", by=C, strategy="backward")
            .with_columns([
                pl.col("industry").fill_null("(未分類)"),
                pl.when(pl.col("date") >= LIMIT_ERA_SPLIT).then(pl.lit("10%"))
                  .otherwise(pl.lit("7%")).alias("limit_era"),
                pl.when(pl.col("adv_dec") >= 7).then(pl.lit("高"))
                  .when(pl.col("adv_dec") >= 3).then(pl.lit("中"))
                  .otherwise(pl.lit("低")).alias("tier"),
                pl.col("date").dt.year().alias("y"),
            ]))


def _match(pos: pl.DataFrame, negs: pl.DataFrame, seed: int, kind: str) -> pl.DataFrame:
    """同站位 × 同流動性十分位 × 同**前期動能**十分位 × 同產業(逐級放寬)。

    前期動能進配對鍵是本模組相對 EV31 的核心修正——EV31 的負例被**要求**前期漲
    ≥25%,正例卻沒有這個條件,兩組差 41 個百分點,蒸餾會把動能學成判別力。
    """
    p = negs.sample(fraction=1.0, shuffle=True, seed=seed).to_dicts()
    used: set[tuple] = set()
    out: list[dict] = []
    for t in pos.to_dicts():
        levels = (
            ("L1 站位+流動+動能+產業", lambda r: r["date"] == t["date"] and r["adv_dec"] == t["adv_dec"]
             and r["mom_dec"] == t["mom_dec"] and r["industry"] == t["industry"]),
            ("L2 站位+流動+動能", lambda r: r["date"] == t["date"] and r["adv_dec"] == t["adv_dec"]
             and r["mom_dec"] == t["mom_dec"]),
            ("L3 站位+流動±1+動能±1", lambda r: r["date"] == t["date"]
             and abs(r["adv_dec"] - t["adv_dec"]) <= 1 and abs(r["mom_dec"] - t["mom_dec"]) <= 1),
            ("L4 站位+層", lambda r: r["date"] == t["date"] and r["tier"] == t["tier"]),
        )
        for name, ok in levels:
            hit = next((r for r in p if (r[C], r["date"]) not in used
                        and r[C] != t[C] and ok(r)), None)
            if hit is not None:
                used.add((hit[C], hit["date"]))
                out.append({**hit, "match_level": name, "neg_kind": kind,
                            "matched_to": f"{t[C]}@{t['date']}"})
                break
    return pl.DataFrame(out) if out else pl.DataFrame()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--explore", action="store_true")
    ap.add_argument("--per-cell", type=int, default=5, help="每 (年 × 流動性層) 抽幾檔正例")
    ap.add_argument("--seed", type=int, default=20260727)
    a = ap.parse_args()

    con = data.connect()
    print("載入還原價面板…", flush=True)
    panel = _panel(con)
    st = _stations(panel)
    print(f"  {panel.height:,} 列;月度站位 {len(st)} 個({st[0]} ~ {st[-1]})", flush=True)
    u = _universe(con, _features(panel), st)
    print(f"  站位母體觀測 {u.height:,}", flush=True)

    print("\n=== 站位錨定的母體基準率(蒸餾與盲判的校準錨)===")
    for thr in (0.50, SURGE_MIN, 1.00):
        print(f"  未來 {FWD_DAYS} 交易日最大還原報酬 ≥ {thr:>4.0%} → {(u['fwd_max_ret'] >= thr).mean():.2%}")

    pos_all = u.filter(pl.col("fwd_max_ret") >= SURGE_MIN)
    print(f"\n正例母體 {pos_all.height:,};prior120 中位 {pos_all['prior_ret'].median():+.1%}")
    if a.explore:
        print("\n=== 正例逐年 × 層 ===")
        print(pos_all.group_by(["y", "tier"]).agg(pl.len().alias("n"))
              .pivot(on="tier", index="y", values="n").sort("y"))
        return

    pos = (pos_all.sample(fraction=1.0, shuffle=True, seed=a.seed)
           .group_by(["y", "tier"], maintain_order=True).head(a.per_cell).sort(["y", "tier", C]))
    half = pos.height // 2
    near = _match(pos.head(half),
                  u.filter((pl.col("fwd_max_ret") >= NEAR_MISS[0])
                           & (pl.col("fwd_max_ret") < NEAR_MISS[1])), a.seed, "near_miss")
    quiet = _match(pos.tail(pos.height - half),
                   u.filter(pl.col("fwd_max_ret") < QUIET_MAX), a.seed + 1, "quiet")
    neg = pl.concat([near, quiet], how="diagonal")

    print(f"\n正例 {pos.height} 檔;負例 {neg.height} 檔"
          f"(near_miss {near.height} / quiet {quiet.height})")
    print("  配對層級:", dict(neg.group_by("match_level").agg(pl.len().alias("n"))
                              .sort("n", descending=True).iter_rows()))
    print(f"\n=== 混淆因子檢核(EV31 的缺陷 A 是否修掉)===")
    print(f"  正例 prior120 中位 {pos['prior_ret'].median():+.1%}"
          f"  動能十分位中位 {pos['mom_dec'].median():.0f}")
    print(f"  負例 prior120 中位 {neg['prior_ret'].median():+.1%}"
          f"  動能十分位中位 {neg['mom_dec'].median():.0f}")

    cols = [C, "market", "date", "industry", "tier", "limit_era", "adv_dec", "mom_dec",
            "fwd_max_ret", "prior_ret"]
    paths.OUT.mkdir(parents=True, exist_ok=True)
    fp = paths.OUT / "evergreen_ev32_positives.csv"
    fn = paths.OUT / "evergreen_ev32_negatives.csv"
    pos.select(cols).write_csv(fp)
    neg.select(cols + ["neg_kind", "match_level", "matched_to"]).write_csv(fn)
    print(f"\n  正例 → {fp}\n  負例 → {fn}")


if __name__ == "__main__":
    main()
