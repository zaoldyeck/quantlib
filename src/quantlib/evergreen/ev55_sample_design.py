"""EV55:重新蒸餾的「選樣設計」量測——暴漲/偽形樣本該怎麼定義才站得住。

## 為什麼要這支程式
EV30 證明舊的 168 檔暴漲樣本只有 42% 在乾淨資料上成立(35% 價格汙染、23% 窗內
有公司行動)。重新蒸餾要換樣本,但「換成什麼」不能拍腦袋——本檔把選樣設計的
每一個旋鈕都量出來,讓提示詞與抽樣方案有出處(全域 §2.2)。

量測四件事:
  1. **還原價 vs 原始價**:同一門檻下,兩種價格各掃出多少事件、重疊多少。
     舊掃描器用原始價 + 「窗內有除權息就整段丟棄」,這裡驗證還原價能不能
     救回那些被丟掉的樣本(且不製造幽靈)。
  2. **門檻/水平線的基準率隨年份(regime)漂移多少**:固定 +80%/60 日在 2008
     與 2021 是完全不同難度的事件。量出每年事件數與市場截面分位,決定
     「固定門檻 + 分層抽樣」還是「每年取截面 top-k%」。
  3. **流動性/市值維度**:事件在成交額分位上的分佈——低流動性角落佔多少。
  4. **偽形對照組**:現行定義(前 120 日 +25%、後 60 日最高 <20%)的母體大小,
     以及 matched control(同月同產業同前期動能而未暴漲)的可行性與樣本量。

Run: uv run --project . python -m quantlib.evergreen.ev55_sample_design
依賴 cache: 是(需 2026-07-24 權威 rebuild 後的乾淨資料)。
輸出: var/out/ev55_*.csv(逐項明細,供提示詞與抽樣腳本引用)
"""
from __future__ import annotations

import polars as pl

from quantlib import paths, prices
from quantlib.db import connect

START, END = "2004-02-11", "2026-07-23"
PANEL_CACHE = paths.OUT / "ev55_adj_panel.parquet"


def load_panel() -> pl.DataFrame:
    if PANEL_CACHE.exists():
        return pl.read_parquet(PANEL_CACHE)
    con = connect()
    parts = [prices.fetch_adjusted_panel(con, START, END, market=m,
                                        include_extra_history_days=0)
             for m in ("twse", "tpex")]
    px = (pl.concat(parts)
            .filter(pl.col("company_code").str.contains(r"^[1-9][0-9]{3}$"))
            .sort(["company_code", "date"]))
    PANEL_CACHE.parent.mkdir(parents=True, exist_ok=True)
    px.write_parquet(PANEL_CACHE)
    return px


def with_windows(px: pl.DataFrame) -> pl.DataFrame:
    """逐檔加上前瞻/回顧視窗欄(還原價與原始價各一份)。"""
    g = "company_code"
    return px.with_columns([
        # 還原價(總報酬)
        (pl.col("close").shift(-60).over(g) / pl.col("close") - 1).alias("adj_f60"),
        (pl.col("close").shift(-120).over(g) / pl.col("close") - 1).alias("adj_f120"),
        (pl.col("close").reverse().rolling_max(120, min_periods=120).reverse().over(g)
         / pl.col("close") - 1).alias("adj_f120max_incl"),
        (pl.col("close") / pl.col("close").shift(120).over(g) - 1).alias("adj_p120"),
        # 原始價(舊掃描器語義)
        (pl.col("raw_close").shift(-60).over(g) / pl.col("raw_close") - 1).alias("raw_f60"),
        # 流動性:過去 60 日成交額中位數(NTD)
        (pl.col("trade_value").rolling_median(60, min_periods=40).over(g)).alias("adv60"),
    ])


def dedupe(ev: pl.DataFrame, cooldown: int = 120) -> pl.DataFrame:
    """同一檔在 cooldown 日內的重疊事件只留最早一筆(一段漲勢=一個樣本)。"""
    rows, last = [], {}
    for r in ev.sort(["company_code", "date"]).iter_rows(named=True):
        c, d = r["company_code"], r["date"]
        if c in last and (d - last[c]).days < cooldown:
            continue
        last[c] = d
        rows.append(r)
    return pl.DataFrame(rows)


def main() -> None:
    px = with_windows(load_panel())
    px = px.with_columns(pl.col("date").dt.year().alias("year"))
    out = paths.OUT
    out.mkdir(parents=True, exist_ok=True)

    # ── 1. 還原價 vs 原始價(同門檻 +80%/60 日)
    adj = dedupe(px.filter(pl.col("adj_f60") >= 0.80)
                   .select(["company_code", "date", "year", "adj_f60", "raw_f60", "adv60"]))
    raw = dedupe(px.filter(pl.col("raw_f60") >= 0.80)
                   .select(["company_code", "date", "year", "adj_f60", "raw_f60", "adv60"]))
    a = set(zip(adj["company_code"], adj["date"]))
    b = set(zip(raw["company_code"], raw["date"]))
    print("=== 1. 還原價 vs 原始價(+80%/60 日,cooldown 120 日)===")
    print(f"  還原價事件 {len(a):,} / 原始價事件 {len(b):,} / 交集 {len(a & b):,}")
    print(f"  只有原始價認定(疑似公司行動幽靈) {len(b - a):,}")
    print(f"  只有還原價認定(原始價低估的真暴漲) {len(a - b):,}")

    # ── 2. 年度基準率 × 門檻敏感度
    rows = []
    for thr, col, lbl in [(0.80, "adj_f60", "60日+80%"), (0.60, "adj_f60", "60日+60%"),
                          (1.00, "adj_f60", "60日+100%"),
                          (0.80, "adj_f120", "120日+80%"), (0.60, "adj_f120", "120日+60%"),
                          (0.80, "adj_f120max_incl", "120日內最高+80%")]:
        ev = dedupe(px.filter(pl.col(col) >= thr).select(["company_code", "date", "year"]))
        n_alive = (px.group_by("year").agg(pl.col("company_code").n_unique().alias("n"))
                     .sort("year"))
        cnt = ev.group_by("year").agg(pl.len().alias("n_ev")).sort("year")
        for r in cnt.join(n_alive, on="year", how="left").iter_rows(named=True):
            rows.append({"defn": lbl, "year": r["year"], "n_events": r["n_ev"],
                         "n_listed": r["n"], "rate_%": 100 * r["n_ev"] / r["n"]})
    yr = pl.DataFrame(rows)
    yr.write_csv(out / "ev55_yearly_baserate.csv")
    print("\n=== 2. 年度基準率(每年多少檔至少發生一次暴漲)===")
    piv = yr.pivot(values="rate_%", index="year", on="defn").sort("year")
    print(piv.to_pandas().round(1).to_string(index=False))

    # ── 3. 流動性維度
    ev80 = dedupe(px.filter(pl.col("adj_f60") >= 0.80)
                    .select(["company_code", "date", "year", "adv60", "adj_f60"]))
    ev80 = ev80.with_columns(
        pl.when(pl.col("adv60") < 2e7).then(pl.lit("A <2000萬"))
          .when(pl.col("adv60") < 1e8).then(pl.lit("B 2000萬-1億"))
          .when(pl.col("adv60") < 5e8).then(pl.lit("C 1-5億"))
          .otherwise(pl.lit("D >5億")).alias("liq"))
    base = px.with_columns(
        pl.when(pl.col("adv60") < 2e7).then(pl.lit("A <2000萬"))
          .when(pl.col("adv60") < 1e8).then(pl.lit("B 2000萬-1億"))
          .when(pl.col("adv60") < 5e8).then(pl.lit("C 1-5億"))
          .otherwise(pl.lit("D >5億")).alias("liq"))
    print("\n=== 3. 流動性分佈(事件 vs 全市場交易日)===")
    ec = ev80.group_by("liq").agg(pl.len().alias("n_ev"))
    bc = base.drop_nulls("adv60").group_by("liq").agg(pl.len().alias("n_days"))
    m = ec.join(bc, on="liq", how="full", coalesce=True).sort("liq")
    m = m.with_columns([(100 * pl.col("n_ev") / pl.col("n_ev").sum()).alias("ev_%"),
                        (100 * pl.col("n_days") / pl.col("n_days").sum()).alias("mkt_%")])
    print(m.to_pandas().round(1).to_string(index=False))
    ev80.write_csv(out / "ev55_surge_events_g80_w60.csv")

    # ── 4. 偽形對照組母體
    ctrl = px.filter((pl.col("adj_p120") >= 0.25) & (pl.col("adj_f120max_incl") < 0.20))
    ctrl_d = dedupe(ctrl.select(["company_code", "date", "year", "adj_p120",
                                 "adj_f120max_incl", "adv60"]))
    print("\n=== 4. 偽形母體(前120日+25%,後120日最高<20%)===")
    print(f"  去重後 {ctrl_d.height:,} 筆;年度分佈:")
    print(ctrl_d.group_by("year").agg(pl.len().alias("n")).sort("year")
          .to_pandas().to_string(index=False))
    ctrl_d.write_csv(out / "ev55_control_pool.csv")

    # ── 5. 硬對照:與暴漲事件同月、同前期動能帶、同流動性帶,但未暴漲
    print("\n=== 5. matched control 可行性(同年月 × 同前期動能十分位 × 同流動性帶)===")
    key = ["year", "month", "liq", "mom_dec"]
    allp = (base.drop_nulls(["adj_p120", "adj_f120max_incl", "adv60"])
                .with_columns([pl.col("date").dt.year().alias("year"),
                               pl.col("date").dt.month().alias("month")])
                .with_columns((pl.col("adj_p120").rank("ordinal")
                               .over(["year", "month"]) * 10
                               / pl.len().over(["year", "month"]))
                              .cast(pl.Int32).alias("mom_dec")))
    surge_keys = (allp.filter(pl.col("adj_f60") >= 0.80).select(key).unique())
    pool = (allp.filter(pl.col("adj_f120max_incl") < 0.20)
                .join(surge_keys, on=key, how="semi"))
    print(f"  暴漲事件所在的 (年月×流動性×動能十分位) 格子 {surge_keys.height:,} 個")
    print(f"  同格子內『未漲』候選(後120日最高<20%)共 {pool.height:,} 檔日 →"
          f" 每格平均 {pool.height / max(surge_keys.height, 1):.0f} 個可抽")

    # ── 6. 舊掃描器「窗內有公司行動就整段丟棄」丟掉了多少真樣本
    con = connect()
    ca = con.sql("""
        SELECT company_code, date FROM ex_right_dividend
        UNION ALL SELECT company_code, date FROM capital_reduction
    """).pl().rename({"date": "ca_date"})
    ev = (px.filter(pl.col("adj_f60") >= 0.80)
            .select(["company_code", "date"])
            .with_columns((pl.col("date") + pl.duration(days=95)).alias("d_end")))
    hit = (ev.join(ca, on="company_code", how="left")
             .filter((pl.col("ca_date") > pl.col("date"))
                     & (pl.col("ca_date") <= pl.col("d_end")))
             .select(["company_code", "date"]).unique())
    print("\n=== 6. 舊法『窗內有除權息/減資就丟棄』的代價 ===")
    print(f"  還原價認定的暴漲事件(未去重)中,窗內含公司行動者 {hit.height:,}"
          f" / {ev.height:,}({100 * hit.height / ev.height:.1f}%)——舊法整批丟掉,"
          f"還原價下它們是合格樣本")

    # ── 7. 站位錨定(與標記 agent 同一個時鐘):每月中站位 → 未來 2-6 個月最大漲幅
    fwd = px.with_columns([
        (pl.col("close").shift(-40).over("company_code")).alias("c40"),
    ])
    # 前瞻 40~126 交易日之間的最高收盤 / 站位日收盤
    fwd = fwd.with_columns(
        (pl.col("close").shift(-126).over("company_code")).alias("_dummy"))
    m = (px.select(["company_code", "date", "close", "adv60"])
           .with_columns([
               pl.col("close").shift(-40).over("company_code").alias("h_lo"),
           ]))
    # rolling max of close over the forward slice [t+40, t+126]
    rev = (px.select(["company_code", "date", "close", "adv60"])
             .sort(["company_code", "date"])
             .with_columns(
                 pl.col("close").reverse().rolling_max(87, min_samples=1)
                   .reverse().over("company_code").alias("fmax_87"))
             .with_columns(
                 pl.col("fmax_87").shift(-40).over("company_code").alias("fmax_40_126"))
             .with_columns(
                 (pl.col("fmax_40_126") / pl.col("close") - 1).alias("g_2_6m")))
    stations = (rev.select("date").unique().sort("date")
                  .with_columns([pl.col("date").dt.year().alias("y"),
                                 pl.col("date").dt.month().alias("mo"),
                                 pl.col("date").dt.day().alias("d")])
                  .filter(pl.col("d") >= 10)
                  .group_by(["y", "mo"]).agg(pl.col("date").min().alias("date"))
                  .select("date"))
    st = rev.join(stations, on="date", how="semi").drop_nulls("g_2_6m")
    print("\n=== 7. 站位錨定定義(月中站位,未來 2-6 個月最大漲幅)===")
    for thr in (0.40, 0.60, 0.80, 1.00):
        c = (st.filter(pl.col("g_2_6m") >= thr).group_by("date")
               .agg(pl.len().alias("n")).sort("date"))
        allst = st.group_by("date").agg(pl.len().alias("N")).sort("date")
        j = allst.join(c, on="date", how="left").with_columns(pl.col("n").fill_null(0))
        print(f"  門檻 +{thr:.0%}: 每站位合格檔數 中位 {j['n'].median():.0f}"
              f" / 10 分位 {j['n'].quantile(0.1):.0f} / 90 分位 {j['n'].quantile(0.9):.0f}"
              f" / 零檔站位 {int((j['n'] == 0).sum())} 個(共 {j.height})")
    st.write_csv(out / "ev55_station_anchored.csv")

    # ── 8. 流動性:改用當日截面分位(去除 22 年名目規模漂移)
    st2 = st.with_columns(
        (pl.col("adv60").rank("ordinal").over("date") * 10
         / pl.len().over("date")).cast(pl.Int32).clip(0, 9).alias("adv_dec"))
    tot = st2.group_by("adv_dec").agg(pl.len().alias("n_all"))
    win = (st2.filter(pl.col("g_2_6m") >= 0.80).group_by("adv_dec")
              .agg(pl.len().alias("n_win")))
    t = (tot.join(win, on="adv_dec", how="left").with_columns(pl.col("n_win").fill_null(0))
            .with_columns((100 * pl.col("n_win") / pl.col("n_all")).alias("hit_%"))
            .sort("adv_dec"))
    print("\n=== 8. 成交額截面十分位 × 2-6 個月 +80% 命中率 ===")
    print(t.to_pandas().round(2).to_string(index=False))
    # ── 9. 下游可執行母體條件化(h120 = 距 120 日高點比,引擎閘門 >0.7)
    h = (px.select(["company_code", "date", "close", "trade_value"])
           .sort(["company_code", "date"])
           .with_columns([
               (pl.col("close") / pl.col("close").rolling_max(120))
               .over("company_code").alias("h120"),
               pl.col("trade_value").rolling_median(20).over("company_code").alias("adv20"),
           ]).select(["company_code", "date", "h120", "adv20"]))
    stw = st.join(h, on=["company_code", "date"], how="left").drop_nulls("h120")
    for gate, lbl in [(0.0, "全部"), (0.7, "h120>0.7(引擎可執行)")]:
        s = stw.filter(pl.col("h120") > gate)
        for thr in (0.60, 0.80):
            w = (s["g_2_6m"] >= thr).sum()
            print(f"\n=== 9. {lbl} × 2-6 月 +{thr:.0%}:命中 {w:,}/{s.height:,}"
                  f"({100 * w / s.height:.1f}%)===")
        pos = s.filter(pl.col("g_2_6m") >= 0.80)
        per = pos.group_by("date").agg(pl.len().alias("n")).sort("date")
        print(f"   每站位合格檔數 中位 {per['n'].median():.0f} / 站位數 {per.height}")
        d10 = s.with_columns((pl.col("adv20").rank("ordinal").over("date") * 10
                              / pl.len().over("date")).cast(pl.Int32).clip(0, 9).alias("dec"))
        tt = (d10.group_by("dec").agg([pl.len().alias("n"),
                                       (pl.col("g_2_6m") >= 0.80).sum().alias("w")])
                 .with_columns((100 * pl.col("w") / pl.col("n")).alias("hit_%"))
                 .sort("dec"))
        print(tt.to_pandas().round(2).to_string(index=False))
    stw.write_csv(out / "ev55_station_actionable.csv")

    # ── 10. Regime 分層(市場 proxy = 截面中位日報酬累乘,不依賴 2009 才有的指數)
    ret = (px.sort(["company_code", "date"])
             .with_columns((pl.col("close") / pl.col("close").shift(1)
                            .over("company_code") - 1).alias("r"))
             .filter(pl.col("r").is_between(-0.2, 0.2)))
    mkt = (ret.group_by("date").agg(pl.col("r").mean().alias("mr")).sort("date")
              .with_columns((1 + pl.col("mr")).cum_prod().alias("idx")))
    mkt = mkt.with_columns([
        (pl.col("idx") / pl.col("idx").shift(250) - 1).alias("r12m"),
        (pl.col("idx") / pl.col("idx").rolling_max(750) - 1).alias("dd3y"),
    ]).with_columns(
        pl.when(pl.col("dd3y") < -0.25).then(pl.lit("1 崩跌/熊市"))
          .when(pl.col("r12m") > 0.30).then(pl.lit("4 狂熱"))
          .when(pl.col("r12m") > 0.0).then(pl.lit("3 多頭"))
          .otherwise(pl.lit("2 修正/盤整")).alias("regime"))
    sr = stw.join(mkt.select(["date", "regime"]), on="date", how="left").drop_nulls("regime")
    tab = (sr.group_by("regime").agg([
        pl.col("date").n_unique().alias("站位數"),
        pl.len().alias("檔站位"),
        (pl.col("g_2_6m") >= 0.80).sum().alias("暴漲數"),
    ]).with_columns((100 * pl.col("暴漲數") / pl.col("檔站位")).alias("命中%"))
       .sort("regime"))
    print("\n=== 10. Regime 分層(全史 2004-2026)===")
    print(tab.to_pandas().round(2).to_string(index=False))
    pre = sr.filter(pl.col("date") < pl.date(2022, 1, 1))
    tab2 = (pre.group_by("regime").agg([
        pl.col("date").n_unique().alias("站位數"),
        (pl.col("g_2_6m") >= 0.80).sum().alias("暴漲數")]).sort("regime"))
    print("\n  蒸餾期候選(站位 ≤ 2021-12,與標記期 2022-07+ 零重疊):")
    print(tab2.to_pandas().to_string(index=False))

    # ── 11. 「最高漲幅」vs「期末報酬」——尖刺 vs 真正走完一段
    end = (px.select(["company_code", "date", "close"]).sort(["company_code", "date"])
             .with_columns((pl.col("close").shift(-126).over("company_code")
                            / pl.col("close") - 1).alias("end126")))
    e = (st.join(end, on=["company_code", "date"], how="left").drop_nulls("end126")
           .filter(pl.col("g_2_6m") >= 0.80))
    print("\n=== 11. 站位錨定 +80%(2-6 月最高)樣本的期末(126 日)報酬分佈 ===")
    print(f"  n={e.height:,} 期末中位 {e['end126'].median():+.1%};"
          f" 期末 ≥+25% 佔 {100 * (e['end126'] >= 0.25).mean():.1f}%;"
          f" 期末 ≥0 佔 {100 * (e['end126'] >= 0).mean():.1f}%;"
          f" 期末 <-10%(尖刺後倒車)佔 {100 * (e['end126'] < -0.10).mean():.1f}%")

    # ── 12. 與現役出場規格對齊?(live: time_days=30、trail 25%、tp 40%)
    #     若暴漲樣本多半在站位後 30 個交易日內還沒動,引擎早已時間停損出場,
    #     那「2-6 個月會漲」這個標記問句就與可實現報酬脫節。
    fw = (px.select(["company_code", "date", "close"]).sort(["company_code", "date"])
            .with_columns([
                (pl.col("close").reverse().rolling_max(30, min_samples=30)
                   .reverse().over("company_code")).alias("m30"),
                (pl.col("close").reverse().rolling_max(63, min_samples=63)
                   .reverse().over("company_code")).alias("m63"),
            ])
            .with_columns([
                (pl.col("m30").shift(-1).over("company_code") / pl.col("close") - 1)
                .alias("g30max"),
                (pl.col("m63").shift(-1).over("company_code") / pl.col("close") - 1)
                .alias("g63max"),
                (pl.col("close").shift(-63).over("company_code") / pl.col("close") - 1)
                .alias("fwd63"),
            ]).select(["company_code", "date", "g30max", "g63max", "fwd63"]))
    z = st.join(fw, on=["company_code", "date"], how="left").drop_nulls("g30max")
    pos = z.filter(pl.col("g_2_6m") >= 0.80)
    print("\n=== 12. 2-6 月 +80% 樣本在『引擎持有得住的窗口』內的行為 ===")
    print(f"  n={pos.height:,};站位後 30 日內最高漲幅 中位 {pos['g30max'].median():+.1%}"
          f";其中 <+10%(引擎多半已 time-stop 出場)佔 {100 * (pos['g30max'] < 0.10).mean():.1f}%")
    print(f"  站位後 63 日內最高漲幅 中位 {pos['g63max'].median():+.1%}"
          f";fwd63 收盤報酬 中位 {pos['fwd63'].median():+.1%}")
    for lbl, expr in [("2-6月最高 ≥80%", pl.col("g_2_6m") >= 0.80),
                      ("fwd63 收盤 ≥30%", pl.col("fwd63") >= 0.30),
                      ("30日內最高 ≥25%", pl.col("g30max") >= 0.25)]:
        print(f"  母體命中率 {lbl}: {100 * z.filter(expr).height / z.height:.1f}%")
    ov = z.filter((pl.col("fwd63") >= 0.30))
    print(f"  「fwd63 ≥30%」與「2-6 月最高 ≥80%」重疊: "
          f"{100 * ov.filter(pl.col('g_2_6m') >= 0.80).height / max(ov.height, 1):.1f}%"
          f" 的前者也是後者")
    print(f"\n明細已寫入 {out}/ev55_*.csv")


if __name__ == "__main__":
    main()
