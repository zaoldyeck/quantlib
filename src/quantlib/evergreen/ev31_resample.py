"""EV31:重新選定 Evergreen 蒸餾樣本——乾淨資料 × 還原價 × 與標記期不重疊。

## 為什麼要重選(EV30 診斷出三個問題)
1. **資料汙染**:舊 168 檔在乾淨資料上只剩 42% 成立(35% 漲幅根本不到門檻、
   23% 是除權息/減資的機械跳動被當成暴漲)。見 `ev30_sample_recheck.py`。
2. **選樣方法**:舊掃描器用**原始收盤價**,並把「窗內含除權息/減資」的樣本
   **整段丟掉**。正解是用還原價(總報酬)穿過公司行動正確量測,不必丟樣本。
3. **時期重疊(最嚴重)**:舊蒸餾期 2022-07~2025-06 與月度標記期 2022-07~2025-04、
   策略 refit 窗 2023-07~2026-07 **完全重疊**,而哲學逐條引用具名個股當證據
   (常珵/大綜/富野/8404…)。標記 Agent 被要求 PIT,但**它的判斷框架已經知道
   那些月份的答案**——PIT 紀律擋不住 meta 層的 look-ahead。

## 本模組的設計決定(每條都寫明理由,不留無出處的數字)

| 決定 | 值 | 理由 |
|---|---|---|
| 價格基準 | `prices.fetch_adjusted_panel` 還原價 | 專案鐵律:所有報酬計算走 canonical prices;能穿過除權息/減資,不必丟樣本 |
| 水平線 | 未來 **120 交易日**內的最大報酬 | 對齊標記任務問的「未來 2-6 個月」;舊版 60 日與下游不一致 |
| 門檻 | 最大前瞻報酬 ≥ **80%** | 沿用舊版的「大幅上漲」語義以維持可比;另輸出 50%/100% 的敏感度 |
| 蒸餾期 | t0 ∈ [2008-01-01, 2021-12-31] 且**前瞻窗須在 2022-07-01 前結束** | 與標記/回測期完全不重疊,消除 meta look-ahead;tpex 資料 2007-07 起,故從 2008 開始 |
| 去重 | 同檔 **120 交易日**冷卻 | 與前瞻窗同長,一段漲勢只取一個樣本(舊版冷卻 30 < 窗 60,會重複計同一波) |
| 流動性 | **當日橫斷面 ADV20 三分位,只記錄不過濾** | 絕對金額門檻在 2008 與 2021 意義不同,橫斷面分位自我正規化;哲學曾發現「純查無消息暴漲集中在低流通小型股」,濾掉就再也驗證不了那條,故留作維度 |
| 抽樣 | **按年分層** | 舊樣本只涵蓋約 3 年、1.5 個總經 regime,而哲學的第十道判別正是「宏觀 regime」;分層可涵蓋金融海嘯/歐債/2015 貶值/2018 貿易戰/2020 疫情/2021 狂熱 |
| 對照組 | **配對式 hard negative**:同年月 × 同產業 × 前 120 日漲幅 ≥25% × 前瞻最大 <20% | 舊版只有後兩個條件,沒有同期同業配對;配對後才能把「總經與族群」控制掉,對比才落在個股本身 |

Run:
  uv run --project . python -m quantlib.evergreen.ev31_resample --explore   # 只看分布,不抽樣
  uv run --project . python -m quantlib.evergreen.ev31_resample --per-year 20
依賴 cache: 是(要乾淨資料)。長任務,建議背景跑。
"""
from __future__ import annotations

import argparse
from datetime import date as Date

import polars as pl

from quantlib import paths, prices
from quantlib.apex import data

C = "company_code"
#: 蒸餾期:與標記/回測期(2022-07 起)完全不重疊
ERA_START, ERA_END = Date(2008, 1, 1), Date(2021, 12, 31)
#: 前瞻窗須整段落在標記期之前,否則哲學仍會知道標記期的結果
FWD_MUST_END_BEFORE = Date(2022, 7, 1)
FWD_DAYS = 120          # ≈ 6 個月,對齊標記任務的「未來 2-6 個月」
PRIOR_DAYS = 120        # 對照組的「前期動能」觀察窗(沿用舊版定義以可比)
SURGE_MIN = 0.80
CTRL_PRIOR_MIN, CTRL_FWD_MAX = 0.25, 0.20
COOLDOWN = 120          # 與前瞻窗同長:一段漲勢只取一個樣本


def _panel(con) -> pl.DataFrame:
    """雙市場還原價面板(含暖機),只留計算需要的欄。"""
    frames = []
    for m in ("twse", "tpex"):
        f = prices.fetch_adjusted_panel(
            con, ERA_START.isoformat(), FWD_MUST_END_BEFORE.isoformat(),
            market=m, include_extra_history_days=PRIOR_DAYS + 60)
        if not f.is_empty():
            frames.append(f.select([C, "date", "close", "trade_value"]).with_columns(
                pl.lit(m).alias("market")))
    return (pl.concat(frames)
            .unique(subset=[C, "date"], keep="first")
            .filter(pl.col("close") > 0)
            .sort([C, "date"]))


def _features(panel: pl.DataFrame) -> pl.DataFrame:
    """逐 (檔, 日) 算前瞻最大報酬、前期動能、流動性分位。"""
    df = panel.with_columns([
        # 未來 1..FWD_DAYS 根的最高還原收盤(不含當日)
        pl.col("close").shift(-1).reverse()
          .rolling_max(FWD_DAYS, min_samples=1).reverse().over(C).alias("fwd_max_px"),
        # 前瞻窗最後一根的日期:用來確保整段落在標記期之前
        pl.col("date").shift(-FWD_DAYS).over(C).alias("fwd_end_date"),
        pl.col("close").shift(PRIOR_DAYS).over(C).alias("prior_px"),
        pl.col("trade_value").rolling_mean(20, min_samples=20).over(C).alias("adv20"),
    ]).with_columns([
        (pl.col("fwd_max_px") / pl.col("close") - 1.0).alias("fwd_max_ret"),
        (pl.col("close") / pl.col("prior_px") - 1.0).alias("prior_ret"),
    ])
    # 流動性用「當日橫斷面分位」——絕對金額在 2008 與 2021 意義不同
    return df.with_columns(
        (pl.col("adv20").rank("average").over("date")
         / pl.len().over("date")).alias("adv_pctile"))


def _in_era(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(
        (pl.col("date") >= ERA_START) & (pl.col("date") <= ERA_END)
        & pl.col("fwd_end_date").is_not_null()
        & (pl.col("fwd_end_date") < FWD_MUST_END_BEFORE)
        & pl.col("fwd_max_ret").is_not_null())


def _cooldown(df: pl.DataFrame, days: int = COOLDOWN) -> pl.DataFrame:
    """同檔冷卻:一段漲勢只留最早的那個 t0(用交易日序號,不用日曆天)。"""
    if df.is_empty():
        return df
    keep: list[bool] = []
    last: dict[str, int] = {}
    for code, ix in zip(df[C].to_list(), df["_ix"].to_list()):
        prev = last.get(code)
        ok = prev is None or (ix - prev) >= days
        keep.append(ok)
        if ok:
            last[code] = ix
    return df.filter(pl.Series(keep))


def _operating_only(con, df: pl.DataFrame) -> pl.DataFrame:
    """只留**營運公司的普通股**——ETF / ETN / 權證 / TDR 不是消息面歸因的合法標的。

    2026-07-27 抽樣自查抓到的汙染:00633L・00655L(槓桿 ETF,漲跌是指數 2 倍的機械
    結果)、020010(ETN)、70391・71540(**權證**,槓桿極高)、912398(TDR)。
    對這些做「它為什麼暴漲」的消息面歸因毫無意義,還會把哲學帶偏。

    判準兩道,皆非拍板數字:①代碼形狀 `^[0-9]{4}$` = 台股普通股的慣例(ETF 為 00xxx、
    權證 5-6 碼、TDR 6 碼);②**在 t0 當下已有申報月營收**——營運公司的定義,且是
    PIT 正確的(上市前不算)。
    """
    first = con.sql("""
        SELECT company_code,
               make_date(min(year * 100 + month) // 100, min(year * 100 + month) % 100, 1) AS first_rev
        FROM operating_revenue GROUP BY company_code""").pl()
    return (df.filter(pl.col(C).str.contains(r"^[0-9]{4}$"))
            .join(first, on=C, how="inner")
            .filter(pl.col("date") >= pl.col("first_rev"))
            .drop("first_rev"))


def _industry(con, df: pl.DataFrame) -> pl.DataFrame:
    """接上 PIT 產業別(as-of t0)——配對對照組時要控制掉族群效應。"""
    tax = con.sql(
        "SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
        "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
    return (df.sort("date")
            .join_asof(tax.sort("effective_date"), left_on="date",
                       right_on="effective_date", by=C, strategy="backward")
            .with_columns(pl.col("industry").fill_null("(未分類)"))
            .sort([C, "_ix"]))


#: 台股漲跌幅 2015-06-01 由 7% 放寬到 10%(engine.LIMIT_CHANGE_DATE)。這是**市場微結構
#: 的斷點**:同樣的題材再定價,在 7% 限制下需要更多天才走完,漲勢的「形狀」不同。
#: 標記/回測期(2022+)全在 10% era,故 10% era 的樣本與下游同構、7% era 是外推。
#: 樣本一律標記所屬 era,讓蒸餾能分開檢視,也讓「老年代消息查不到」時可整層剔除
#: 而不必重抽。
LIMIT_ERA_SPLIT = Date(2015, 6, 1)


def _era_tag(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("date") >= LIMIT_ERA_SPLIT).then(pl.lit("10%"))
        .otherwise(pl.lit("7%")).alias("limit_era"))


def _tier(df: pl.DataFrame) -> pl.DataFrame:
    """流動性三分位標籤(橫斷面分位,自我正規化跨年代)。"""
    return df.with_columns(
        pl.when(pl.col("adv_pctile") >= 2 / 3).then(pl.lit("高"))
        .when(pl.col("adv_pctile") >= 1 / 3).then(pl.lit("中"))
        .otherwise(pl.lit("低")).alias("tier"))


def _match_controls(surge: pl.DataFrame, pool: pl.DataFrame, seed: int) -> pl.DataFrame:
    """為每個暴漲樣本配一個 hard negative:同期 × 同產業 × 同流動性層,但沒漲。

    逐級放寬(並記錄放寬到哪一級,讓蒸餾知道配對品質):
      L1 同月 + 同產業 + 同層 → L2 同月 + 同層 → L3 同季 + 同層 → L4 同季
    配對的意義:把總經 regime 與族群效應控制掉,對比才落在「個股本身有什麼不同」。
    舊版對照組沒有配對,拿的是全期任意偽形,對比裡混著年代與族群差異。
    """
    p = pool.with_columns([
        pl.col("date").dt.strftime("%Y-%m").alias("ym"),
        (pl.col("date").dt.year().cast(pl.Utf8) + "Q"
         + ((pl.col("date").dt.month() - 1) // 3 + 1).cast(pl.Utf8)).alias("yq"),
    ]).sample(fraction=1.0, shuffle=True, seed=seed)
    s = surge.with_columns([
        pl.col("date").dt.strftime("%Y-%m").alias("ym"),
        (pl.col("date").dt.year().cast(pl.Utf8) + "Q"
         + ((pl.col("date").dt.month() - 1) // 3 + 1).cast(pl.Utf8)).alias("yq"),
    ])
    used: set[tuple] = set()
    picked: list[dict] = []
    rows = p.to_dicts()
    for t in s.to_dicts():
        levels = (
            ("L1 同月+同業+同層", lambda r: r["ym"] == t["ym"] and r["industry"] == t["industry"] and r["tier"] == t["tier"]),
            ("L2 同月+同層", lambda r: r["ym"] == t["ym"] and r["tier"] == t["tier"]),
            ("L3 同季+同層", lambda r: r["yq"] == t["yq"] and r["tier"] == t["tier"]),
            ("L4 同季", lambda r: r["yq"] == t["yq"]),
        )
        for name, ok in levels:
            hit = next((r for r in rows
                        if (r[C], r["date"]) not in used and r[C] != t[C] and ok(r)), None)
            if hit is not None:
                used.add((hit[C], hit["date"]))
                picked.append({**hit, "match_level": name,
                               "matched_to": f"{t[C]}@{t['date']}"})
                break
    return pl.DataFrame(picked) if picked else pl.DataFrame()


def _explore(feat: pl.DataFrame) -> None:
    era = _in_era(feat)
    print(f"蒸餾期 {ERA_START} ~ {ERA_END}(前瞻窗須於 {FWD_MUST_END_BEFORE} 前結束)")
    print(f"可用 (檔×日) 觀測:{era.height:,}\n")
    print("=== 門檻敏感度:前瞻 120 交易日最大報酬 ≥ X 的**去重後**事件數 ===")
    print(f"  {'門檻':>8}{'事件數':>10}{'涵蓋檔數':>10}{'年均':>8}")
    for thr in (0.50, 0.80, 1.00, 1.50):
        ev = _cooldown(era.filter(pl.col("fwd_max_ret") >= thr).sort([C, "_ix"]))
        print(f"  {thr:>7.0%}{ev.height:>10,}{ev[C].n_unique():>10,}{ev.height / 14:>8.0f}")

    ev = _cooldown(era.filter(pl.col("fwd_max_ret") >= SURGE_MIN).sort([C, "_ix"]))
    print(f"\n=== 門檻 {SURGE_MIN:.0%} 的逐年分布(看分層抽樣要抽幾檔)===")
    yr = (ev.with_columns(pl.col("date").dt.year().alias("y"))
          .group_by("y").agg([pl.len().alias("n"),
                              pl.col("fwd_max_ret").median().alias("med"),
                              pl.col("adv_pctile").median().alias("adv_med")])
          .sort("y"))
    print(f"  {'年':>6}{'事件':>7}{'漲幅中位':>10}{'流動性分位中位':>14}")
    for r in yr.iter_rows(named=True):
        print(f"  {r['y']:>6}{r['n']:>7}{r['med']:>+10.0%}{r['adv_med']:>13.2f}")

    print(f"\n=== 流動性三分位分布(要不要濾掉小型股的判斷依據)===")
    t = ev.with_columns(
        pl.when(pl.col("adv_pctile") >= 2 / 3).then(pl.lit("高"))
        .when(pl.col("adv_pctile") >= 1 / 3).then(pl.lit("中"))
        .otherwise(pl.lit("低")).alias("tier"))
    for r in t.group_by("tier").agg(pl.len().alias("n")).sort("n", descending=True).iter_rows(named=True):
        print(f"  {r['tier']} 流動性:{r['n']:>5} 事件({r['n'] / t.height:.0%})")

    ctrl = _cooldown(era.filter((pl.col("prior_ret") >= CTRL_PRIOR_MIN)
                                & (pl.col("fwd_max_ret") < CTRL_FWD_MAX)).sort([C, "_ix"]))
    print(f"\n=== 對照組候選(前 {PRIOR_DAYS} 日漲 ≥{CTRL_PRIOR_MIN:.0%}、"
          f"前瞻最大 <{CTRL_FWD_MAX:.0%})===")
    print(f"  {ctrl.height:,} 個事件、{ctrl[C].n_unique():,} 檔 —— 配對池充足")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--explore", action="store_true", help="只看分布,不抽樣")
    ap.add_argument("--per-year", type=int, default=20, help="每年抽幾檔暴漲樣本")
    ap.add_argument("--seed", type=int, default=20260727)
    args = ap.parse_args()

    con = data.connect()
    print("載入雙市場還原價面板…", flush=True)
    panel = _panel(con)
    print(f"  {panel.height:,} 列、{panel[C].n_unique():,} 檔", flush=True)
    feat = _features(panel).with_columns(
        pl.int_range(pl.len()).over(C).alias("_ix"))   # 交易日序號(冷卻用)

    if args.explore:
        _explore(_operating_only(con, feat))
        return

    era = _era_tag(_tier(_industry(con, _operating_only(con, _in_era(feat)))))
    surge = _cooldown(era.filter(pl.col("fwd_max_ret") >= SURGE_MIN).sort([C, "_ix"]))
    pool = _cooldown(era.filter((pl.col("prior_ret") >= CTRL_PRIOR_MIN)
                                & (pl.col("fwd_max_ret") < CTRL_FWD_MAX)).sort([C, "_ix"]))

    # 年 × 流動性三分位分層:讓哲學看遍所有 regime 與所有市值層級。
    # 只按年分層會讓 2008/2020 這種崩盤修復年吃掉一半配額;只按層分層則失去 regime 覆蓋。
    per_cell = max(args.per_year // 3, 1)
    sampled = (surge.with_columns(pl.col("date").dt.year().alias("y"))
               .sample(fraction=1.0, shuffle=True, seed=args.seed)
               .group_by(["y", "tier"], maintain_order=True).head(per_cell)
               .sort(["y", "tier", C]))
    print(f"\n暴漲樣本:年 × 流動性層,每格 {per_cell} 檔 → 共 {sampled.height} 檔")

    ctrl = _match_controls(sampled, pool, seed=args.seed)
    print(f"對照樣本:配對 {ctrl.height} 檔")
    print("  配對層級分布:", dict(
        ctrl.group_by("match_level").agg(pl.len().alias("n")).sort("n", descending=True).iter_rows()))

    out = paths.OUT
    out.mkdir(parents=True, exist_ok=True)
    cols = [C, "market", "date", "industry", "tier", "limit_era",
            "fwd_max_ret", "prior_ret", "adv_pctile"]
    fs = out / "evergreen_ev31_surge_samples.csv"
    fc = out / "evergreen_ev31_control_samples.csv"
    sampled.select(cols).write_csv(fs)
    ctrl.select(cols + ["match_level", "matched_to"]).write_csv(fc)
    print(f"\n  暴漲樣本 → {fs}\n  對照樣本 → {fc}")

    print("\n=== 逐年 × 層 檢核 ===")
    chk = (sampled.group_by(["y", "tier"]).agg(pl.len().alias("n"))
           .pivot(on="tier", index="y", values="n").sort("y"))
    print(chk)


if __name__ == "__main__":
    main()
