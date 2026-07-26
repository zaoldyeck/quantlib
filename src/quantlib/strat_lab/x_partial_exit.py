"""S 策略維度:**部分止盈 / 分批出場**(乾淨資料 campaign)。

問題:今晨測過的「全額止盈」(profit_take 40%/60%)全部劣化,解讀是**截斷右尾**
——贏家還在跑就被整筆賣掉。正確的追問不是放棄止盈,而是問「**只回收一部分**、
讓剩下的繼續跑」能否兩全:降低單筆風險(部位縮小 → 回吐變小)又保留右尾
(剩餘部位照樣參與後續上漲)。引擎原生支援 `ExitSpec.profit_recycle=(門檻, 比例)`
——浮盈 ≥ 門檻時**一次性**賣掉 fraction 的股數,其餘續抱(trail/time/signal 不變)。

四個家族(全部只用 run_s_full 的研究 hooks,不改 canonical 預設):
  G  grid_*    主網格:門檻 ∈ {30,50,80,100}% × 回收比例 ∈ {25,33,50}%(任務指定)
  P  hi_*      門檻高原延伸 {150,200}%:確認 thr 方向是單調還是有峰(高原驗證)
  R  trim_*    「回到目標權重」族:frac = thr/(1+thr) —— 部位漲 thr 後市值權重約為
                原目標的 (1+thr) 倍,賣掉 thr/(1+thr) 正好把它修回等權目標。
                這是唯一有經濟出處的 (thr, frac) 配對,不是網格湊數。
  U  uw_*      underwater_trail ∈ {15,20,25,30}%:水下(mark < 進場均價)時改用比
                canonical 35% 更緊的 trail。注意:從未站上進場價的倉位其 peak ≈ 進場
                價,故此參數對它們**近似等於 abs_stop**(而 abs_stop 10~25% 今晨已
                全數劣化)——本族真正的新資訊只在「先賺後賠」的倉位上。

判準(D2):Sortino / Calmar / MDD / bootstrap 下界必須**同時 ≥ canonical**;
再對日報酬差做配對 block-bootstrap(block=21, n_boot=4000),CI 跨 0 = 噪音級 = 證偽。

機制先驗(--mode diag,不看 NAV 路徑的獨立證據線):對每一筆已平倉交易,找出它
**第一次**觸及 +thr 的那天,量「從那天到實際出場」的剩餘報酬。部分止盈是把這段
剩餘報酬的 fraction 換成現金(0%),所以只要這段的平均顯著 > 0,部分止盈在數學上
就不可能贏——與 x_abs_gate_cash 的「對照組是現金」同一種先驗檢驗。

── 結論(2026-07-26,24 變體全跑完;**證偽**,負結果落地防重複試錯)────────
canonical:CAGR +82.3% / Sortino 3.28 / Calmar 2.40 / MDD −34.3% / boot_lo +51.3%。
24 個變體**無一**通過出廠標準。表現與門檻/比例**單調**:門檻愈低、回收比例愈大,
劣化愈深(P_15_50 −10.2%/yr、G_30_50 −5.0%/yr、G_30_25 −2.5%/yr),CI 全部整段在
0 以下。唯三「D2 全過」的 G_100_*(門檻 +100%)年化差僅 +0.1%、CI 跨 0、
**全史只觸發 2 次**(且同在 2021 年)——n=2 的路徑巧合,不是形態。
內建 null control N_null200(門檻 +200%,全史 0 次觸發)年化差恰為 0.0%、CI [0,0],
證明配對 bootstrap 機器本身無偏(該列 P(≤0)=1.000 是全 0 分佈的退化值,非劣化)。
U 家族(水下更緊 trail)同樣全劣化,且 uw15 連 **MDD 都變差**(−37.0% vs −34.3%)
——收緊水下停損沒換到任何風險降低,只換到更多換手與更少反彈參與。

為什麼失效(三層機制,層層獨立):
 1. **S 的獲利 100% 是右尾**:684 筆已平倉交易中,只有 95 筆(13.9%)曾漲逾 +30%,
    這 95 筆貢獻了 **99.1%** 的總獲利;曾漲逾 +50% 的 38 筆貢獻 58.6%。剩下 86% 的
    交易淨貢獻約 0。任何在漲勢中「減碼」的規則,減的都正好是那唯一在賺錢的族群。
 2. **觸線之後那一段還很賺**(--mode diag,不看 NAV 的獨立證據):首次觸及 +30% 之後
    到實際出場,平均**還有 +6.13%**(中位 +1.86%、勝率 57.9%、剩餘中位 9 個交易日,
    t = 3.65);+50% 之後平均還有 +5.57%(t = 2.37)。部分止盈就是把這段報酬的
    fraction 換成 0% 的現金,方向上必輸。
 3. **劣化幅度可用算術預測**(--mode arith):回收金額占當日 NAV 的比例 × 觸線後剩餘
    報酬 ÷ 年數 = 預測拖累。G_30_25 預測 −2.6% 對實測 −2.5%、G_30_50 預測 −5.1% 對
    實測 −5.0%。對得上到小數點,代表這不是雜訊或路徑運氣,是規則的機械後果。
另註:S 平均只佔用 4.15/5 個槽、71.7% 的日子滿倉,而候選只在月營收公布後那幾天出現
——回收來的現金多半沒有立即去處,實質就是躺著,連「換一檔更好的」都做不到
(全額止盈至少會釋放槽位換股,都還輸;部分止盈連這點都沒有)。

**反向啟示(給後續維度)**:同一份診斷指向相反方向——不是減碼贏家,而是**讓贏家跑更久**。
S 有 480 筆(70%)因「營收訊號陳舊」(rev_fresh_days ≥ 26)被賣掉,而觸線後仍有
+6.1%/9 天的動能。「強勢部位豁免 signal 出場」值得測,但 exit_flags 由 run_s_full
內部組裝、無法用現有 hook 條件化,需先給引擎加一個小接口。

Run:
  uv run --project . python -m quantlib.strat_lab.x_partial_exit --mode diag
  uv run --project . python -m quantlib.strat_lab.x_partial_exit --mode all
  uv run --project . python -m quantlib.strat_lab.x_partial_exit --mode arith
依賴 cache:是(prep_cached 讀 industry_taxonomy_pit / 價格 panel)。
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExitSpec
from quantlib.apex.metrics import perf_stats, trade_stats, yearly_table
from quantlib.apex.strategy_s import DS, prep_cached, run_s_full
from quantlib.apex.validate import block_bootstrap_cagr
from quantlib.strat_lab.s_accelrel_gate import paired_boot

C = "company_code"

#: canonical S 的出場規格(STRATEGY.md §5;本 harness 一律在此基礎上「只加不改」)
CANON_EXIT = {"trailing_stop": 0.35, "time_stop": 30, "loser_time_stop": 15}

#: 前後半段切點(樣本外精神:兩段各自表現,看是否只靠某一段)
SPLIT = "2020-07-01"

_G: dict = {}   # 每個工作程序的 panel/feat/elig(initializer 載入一次)


@dataclass(frozen=True)
class Variant:
    name: str
    thr: float = 0.0          # profit_recycle 門檻(浮盈)
    frac: float = 0.0         # profit_recycle 回收比例
    uw: float | None = None   # underwater_trail
    note: str = ""

    def exit_spec(self) -> ExitSpec:
        rc = (self.thr, self.frac) if self.frac > 0 else None
        return ExitSpec(**CANON_EXIT, profit_recycle=rc, underwater_trail=self.uw)


def _grid() -> list[Variant]:
    out: list[Variant] = []
    for thr in (0.3, 0.5, 0.8, 1.0):
        for frac in (0.25, 0.33, 0.5):
            out.append(Variant(f"G_{int(thr*100)}_{int(frac*100)}", thr, frac,
                               note=f"浮盈 ≥{thr:.0%} 回收 {frac:.0%}"))
    # P:門檻高原**往下**延伸。diag 量出持有期最大浮盈 P90 才 +36%、觸及 +150% 者 0 筆
    #    ——往上加門檻是無樣本的空跑,真正有樣本的方向在 15~20%。
    for thr in (0.15, 0.20):
        for frac in (0.33, 0.5):
            out.append(Variant(f"P_{int(thr*100)}_{int(frac*100)}", thr, frac,
                               note=f"高原(低端):浮盈 ≥{thr:.0%} 回收 {frac:.0%}"))
    # R:回到目標權重(frac = thr/(1+thr))——唯一有經濟出處的配對
    #    (thr 50/100 的 frac 恰為 0.333/0.5,已含在 G 網格,不重跑)
    for thr in (0.15, 0.20, 0.3):
        frac = thr / (1.0 + thr)
        out.append(Variant(f"R_trim{int(thr*100)}", thr, round(frac, 4),
                           note=f"漲 {thr:.0%} 後修回等權(賣 {frac:.0%})"))
    # N:null control——門檻 200% 全史 0 筆觸及,NAV 必須與 canonical 逐位相同。
    #    這是驗證「配對 bootstrap 機器本身沒問題」的內建錨(年化差應恰為 0)。
    out.append(Variant("N_null200", 2.0, 0.5, note="null control:門檻永不觸發"))
    # U:水下更緊的 trail
    for uw in (0.15, 0.20, 0.25, 0.30):
        out.append(Variant(f"U_uw{int(uw*100)}", uw=uw,
                           note=f"水下 trail {uw:.0%}(canonical 35%)"))
    return out


VARIANTS: list[Variant] = _grid()


# ── 執行 ────────────────────────────────────────────────────────────────

def _init_worker() -> None:
    con = data.connect()
    p, f, e = prep_cached(con)
    _G["panel"], _G["feat"], _G["elig"] = p, f, e


def run_variant(v: Variant) -> dict:
    panel, feat, elig = _G["panel"], _G["feat"], _G["elig"]
    nav, trades = run_s_full(panel, feat, elig, DS, _exit_spec=v.exit_spec())
    return {"name": v.name, "note": v.note,
            "nav": nav.to_dict(as_series=False), "trades": trades.to_dict(as_series=False)}


def _occupancy(trades: pl.DataFrame, nav: pl.DataFrame) -> dict:
    """從 trades 重建每日「佔用中的槽位數」(recycle 為部分出場、槽位仍佔用,故
    以 parent 交易的 entry→exit 區間計;recycle 列的區間被其 parent 覆蓋不重複計)。"""
    d = nav["date"].to_numpy()
    cnt = np.zeros(len(d), dtype=int)
    parent = trades.filter(pl.col("exit_reason") != "recycle")
    for a, b in zip(parent["entry_date"].to_numpy(), parent["exit_date"].to_numpy()):
        i, j = np.searchsorted(d, a), np.searchsorted(d, b)
        cnt[i:j + 1] += 1
    return {"avg_pos": float(cnt.mean()), "pct_days_full": float((cnt >= 5).mean())}


def _mdd_episode(nav: pl.DataFrame) -> str:
    v, d = nav["nav"].to_numpy(), nav["date"].to_numpy()
    rm = np.maximum.accumulate(v)
    dd = v / rm - 1.0
    j = int(dd.argmin())
    i = int(np.argmax(v[:j + 1]))
    return f"{str(d[i])[:10]} → {str(d[j])[:10]} ({dd[j]:.1%})"


def _subperiod(nav: pl.DataFrame) -> dict:
    out = {}
    for tag, f in (("H1", pl.col("date") < pl.lit(SPLIT).str.to_date()),
                   ("H2", pl.col("date") >= pl.lit(SPLIT).str.to_date())):
        s = nav.filter(f)
        if s.height < 50:
            continue
        s = s.with_columns(pl.col("nav") / pl.col("nav").first())
        st = perf_stats(s)
        out[tag] = (st["cagr"], st["sortino"], st["mdd"], st["calmar"])
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["diag", "all", "arith"], default="all")
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    con = data.connect()
    panel, feat, elig = prep_cached(con)

    if args.mode == "diag":
        diagnostics(panel, feat, elig)
        return
    if args.mode == "arith":
        arithmetic_check(panel, feat, elig)
        return

    nav_c, tr_c = run_s_full(panel, feat, elig, DS)
    st_c = perf_stats(nav_c)
    bs_c = block_bootstrap_cagr(nav_c, n_boot=4000)
    occ_c = _occupancy(tr_c, nav_c)
    print("=== canonical S(基準)===")
    print(f"  CAGR {st_c['cagr']:+.1%}  Sortino {st_c['sortino']:.2f}  "
          f"Calmar {st_c['calmar']:.2f}  MDD {st_c['mdd']:.1%}  boot_lo {bs_c['ci_lo']:+.1%}")
    print(f"  平均佔用 {occ_c['avg_pos']:.2f}/5  滿倉日 {occ_c['pct_days_full']:.1%}  "
          f"交易 {tr_c.height}")
    print(f"  最大回撤事件 {_mdd_episode(nav_c)}\n")

    with ProcessPoolExecutor(max_workers=args.workers, initializer=_init_worker) as ex:
        results = list(ex.map(run_variant, VARIANTS))

    print("=== 變體(全跨度;D2 判準:Sortino/Calmar/MDD/boot_lo 同時 ≥ canonical)===")
    print(f"{'變體':<14}{'CAGR':>8}{'Sortino':>9}{'Calmar':>8}{'MDD':>8}"
          f"{'boot_lo':>9}{'年化差':>9}{'CI下界':>9}{'CI上界':>9}{'P(≤0)':>8}"
          f"{'回收筆':>7}{'佔用':>7}  判定")
    rows = []
    for r in results:
        nav = pl.DataFrame(r["nav"]).with_columns(pl.col("date").cast(pl.Date))
        trades = pl.DataFrame(r["trades"]).with_columns(
            [pl.col("entry_date").cast(pl.Date), pl.col("exit_date").cast(pl.Date)])
        st = perf_stats(nav)
        bs = block_bootstrap_cagr(nav, n_boot=4000)
        pb = paired_boot(nav, nav_c)
        occ = _occupancy(trades, nav)
        n_rc = trades.filter(pl.col("exit_reason") == "recycle").height
        d2 = (st["sortino"] >= st_c["sortino"] and st["calmar"] >= st_c["calmar"]
              and st["mdd"] >= st_c["mdd"] and bs["ci_lo"] >= bs_c["ci_lo"])
        sig = pb["ci_lo"] > 0
        verdict = ("候選(D2過+CI>0)" if (d2 and sig) else
                   "D2過但CI跨0" if d2 else "劣化" if pb["ann_diff"] < 0 else "噪音")
        print(f"{r['name']:<14}{st['cagr']:>+8.1%}{st['sortino']:>9.2f}{st['calmar']:>8.2f}"
              f"{st['mdd']:>8.1%}{bs['ci_lo']:>+9.1%}{pb['ann_diff']:>+9.1%}"
              f"{pb['ci_lo']:>+9.1%}{pb['ci_hi']:>+9.1%}{pb['p_le0']:>8.3f}"
              f"{n_rc:>7}{occ['avg_pos']:>7.2f}  {verdict}")
        rows.append((r, nav, trades, st, bs, pb, d2, sig))

    print("\n=== D2 全過者的分段與逐年 ===")
    cands = [x for x in rows if x[6]]
    if not cands:
        print("  無:沒有任何變體在 Sortino/Calmar/MDD/boot_lo 四項同時 ≥ canonical。")
    spc = _subperiod(nav_c)
    for r, nav, trades, st, bs, pb, _, sig in cands:
        sp = _subperiod(nav)
        print(f"\n  {r['name']}({r['note']})  最大回撤事件 {_mdd_episode(nav)}")
        for tag in ("H1", "H2"):
            if tag in sp:
                a, b = sp[tag], spc[tag]
                print(f"    {tag}: CAGR {a[0]:+.1%}(canon {b[0]:+.1%}) "
                      f"Sortino {a[1]:.2f}({b[1]:.2f}) MDD {a[2]:.1%}({b[2]:.1%}) "
                      f"Calmar {a[3]:.2f}({b[3]:.2f})")
        yv = yearly_table(nav).join(yearly_table(nav_c), on="year", suffix="_c")
        print("    year " + " ".join(f"{y:>7}" for y in yv["year"]))
        print("    var% " + " ".join(f"{x*100:>+7.1f}" for x in yv["ret"]))
        print("    can% " + " ".join(f"{x*100:>+7.1f}" for x in yv["ret_c"]))
        print(f"    trades {trade_stats(trades)}")

    # 最好的三個(不論是否過 D2)——避免只報負面而漏掉「差一點」的形態
    print("\n=== 年化差前三(僅供機制觀察,非結論)===")
    for r, nav, trades, st, bs, pb, d2, sig in sorted(rows, key=lambda x: -x[5]["ann_diff"])[:3]:
        print(f"  {r['name']:<14}{r['note']:<26} 年化差 {pb['ann_diff']:+.1%} "
              f"CI [{pb['ci_lo']:+.1%}, {pb['ci_hi']:+.1%}] P(≤0)={pb['p_le0']:.3f} "
              f"Sortino {st['sortino']:.2f} Calmar {st['calmar']:.2f} MDD {st['mdd']:.1%}")


# ── 機制先驗 ─────────────────────────────────────────────────────────────

def diagnostics(panel: pl.DataFrame, feat: pl.DataFrame, elig: pl.DataFrame) -> None:
    """不看 NAV 路徑的獨立證據線:「觸及 +thr 之後那一段」到底值不值得留?

    部分止盈 = 把這一段的 fraction 換成現金(0%)。所以只要這段平均顯著 > 0,
    部分止盈在數學上就是穩定毀損;若顯著 < 0,才有機會贏。
    """
    nav, trades = run_s_full(panel, feat, elig, DS)
    st = perf_stats(nav)
    print(f"canonical: CAGR {st['cagr']:+.1%} Sortino {st['sortino']:.2f} "
          f"Calmar {st['calmar']:.2f} MDD {st['mdd']:.1%}")
    print(f"交易統計: {trade_stats(trades)}\n")

    closed = (trades.filter(pl.col("exit_reason") != "open")
              .with_row_index("tid"))
    # 只取涉及的股票,避免全 panel join 爆量(極速鐵律 §1)
    codes = closed[C].unique()
    px = (panel.select(["date", C, "close"])
          .join(codes.to_frame(C), on=C, how="semi"))
    seg = (closed.select(["tid", C, "entry_date", "exit_date", "entry_px", "exit_px",
                          "ret_net", "days_held"])
           .join(px, on=C, how="left")
           .filter((pl.col("date") > pl.col("entry_date"))
                   & (pl.col("date") <= pl.col("exit_date")))
           .with_columns((pl.col("close") / pl.col("entry_px") - 1.0).alias("ru"))
           .sort(["tid", "date"]))

    # 已平倉交易的最大 run-up 分佈:門檻能不能咬得到?
    mx = seg.group_by("tid").agg(pl.col("ru").max().alias("mru"))
    print("【觸發率】已平倉交易的持有期最大浮盈(相對進場成交價)分佈:")
    for q in (0.5, 0.75, 0.9, 0.95, 0.99):
        print(f"  P{int(q*100)}  {float(mx['mru'].quantile(q)):+.1%}")
    for thr in (0.15, 0.20, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0):
        n = int((mx["mru"] >= thr).sum())
        print(f"  觸及 +{thr:.0%}:{n} 筆 / {mx.height}({n/mx.height:.1%})")

    print("\n【機制檢驗】首次觸及 +thr 之後那一段的剩餘報酬(對照現金 0%):")
    print(f"{'thr':>6}{'n':>6}{'avg':>9}{'med':>9}{'win%':>8}{'剩餘天數':>9}"
          f"{'ann_eq':>9}{'t':>7}   說明")
    for thr in (0.15, 0.20, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0):
        hit = (seg.filter(pl.col("ru") >= thr)
               .group_by("tid").agg([pl.col("date").min().alias("hit_date"),
                                     pl.col("close").first().alias("hit_px")]))
        # group_by 後 first() 不保證順序 → 明確以 hit_date 取回該日收盤
        hit = (hit.join(seg.select(["tid", "date", "close"]),
                        left_on=["tid", "hit_date"], right_on=["tid", "date"], how="left")
               .drop("hit_px").rename({"close": "hit_px"}))
        j = (hit.join(closed.select(["tid", "exit_date", "exit_px", "days_held"]),
                      on="tid", how="left")
             .with_columns([(pl.col("exit_px") / pl.col("hit_px") - 1.0).alias("fwd"),
                            (pl.col("exit_date") - pl.col("hit_date")).dt.total_days()
                            .alias("cal_left")]))
        if j.height < 5:
            print(f"{thr:>6.1f}{j.height:>6}   樣本過少")
            continue
        f = j["fwd"].to_numpy()
        # 剩餘交易日 ≈ 日曆日 × 幾乎 5/7(近似;僅供 ann_eq 量級,不作判準)
        td = float(np.median(j["cal_left"].to_numpy())) * 5.0 / 7.0
        ann = (1.0 + float(f.mean())) ** (252.0 / max(td, 1.0)) - 1.0
        t = float(f.mean() / (f.std(ddof=1) / np.sqrt(len(f)))) if f.std(ddof=1) > 0 else 0.0
        tag = ("留著明顯划算(部分止盈必輸)" if t > 2 else
               "留著不划算(部分止盈有機會)" if t < -2 else "方向不明")
        print(f"{thr:>6.1f}{len(f):>6}{f.mean():>+9.2%}{float(np.median(f)):>+9.2%}"
              f"{float((f > 0).mean()):>8.1%}{td:>9.0f}{ann:>+9.0%}{t:>7.2f}   {tag}")

    # 右尾集中度:總獲利有多少來自「曾漲逾 thr」的少數交易?截斷右尾的代價量級
    tot = float(closed["ret_net"].sum())
    print(f"\n【右尾集中度】已平倉 {closed.height} 筆,ret_net 總和 {tot:+.1f}(等權和):")
    j2 = closed.join(mx, on="tid", how="left")
    for thr in (0.3, 0.5, 1.0):
        s = float(j2.filter(pl.col("mru") >= thr)["ret_net"].sum())
        n = j2.filter(pl.col("mru") >= thr).height
        print(f"  曾漲逾 +{thr:.0%} 的 {n} 筆({n/closed.height:.1%})貢獻 {s/tot:.1%} 的總獲利")

    # 水下族群:underwater_trail 到底影響誰?
    print("\n【水下族群】U 家族的作用對象:")
    ever_up = j2.filter(pl.col("mru") > 0.0)
    never_up = j2.filter(pl.col("mru") <= 0.0)
    print(f"  從未站上進場價 {never_up.height} 筆(此族 underwater_trail ≈ abs_stop),"
          f"平均 {float(never_up['ret_net'].mean()):+.2%}")
    lose_after_win = j2.filter((pl.col("mru") > 0.10) & (pl.col("ret_net") < 0))
    print(f"  曾漲逾 +10% 最後仍虧損 {lose_after_win.height} 筆,"
          f"平均 {float(lose_after_win['ret_net'].mean()):+.2%}"
          f"(最大浮盈中位 {float(lose_after_win['mru'].median()):+.1%})")
    print(f"  曾站上進場價 {ever_up.height} 筆,平均 {float(ever_up['ret_net'].mean()):+.2%}")


def arithmetic_check(panel: pl.DataFrame, feat: pl.DataFrame, elig: pl.DataFrame) -> None:
    """劣化幅度是**可預測的機械損失**還是路徑運氣?用交易明細直接算出預測值再對帳。

    每次回收 = 把「回收金額 / 當日 NAV」這麼多比例的資金,從一個未來 h 天還會賺
    fwd 的部位,換成 0% 的現金。預測年化拖累 ≈ Σ(回收占 NAV 比 × fwd) / 年數。
    若預測值對得上實測年化差,就證明這不是雜訊,是規則本身的算術後果。
    """
    nav_c, _ = run_s_full(panel, feat, elig, DS)
    base = float(3_000_000.0)   # PortSpec.capital;nav 已歸一化,乘回即實際 NAV
    years = perf_stats(nav_c)["years"]
    print("回收事件的資金占比與預測拖累(fwd 取 diag 量出的「觸線後剩餘報酬」平均):")
    print(f"{'變體':<12}{'回收筆':>7}{'占NAV中位':>11}{'占NAV合計':>11}"
          f"{'預測拖累':>10}{'實測年化差':>11}")
    for v, fwd in ((Variant("G_30_25", 0.3, 0.25), 0.0613),
                   (Variant("G_30_50", 0.3, 0.50), 0.0613),
                   (Variant("G_50_50", 0.5, 0.50), 0.0557)):
        nav, tr = run_s_full(panel, feat, elig, DS, _exit_spec=v.exit_spec())
        rc = (tr.filter(pl.col("exit_reason") == "recycle")
              .join(nav.select(["date", pl.col("nav").alias("n")]),
                    left_on="exit_date", right_on="date", how="left")
              # 回收金額 = 原始成本 × (1 + ret_net);NAV = 歸一化 nav × 起始資本
              .with_columns((pl.col("cost") * (1 + pl.col("ret_net"))
                             / (pl.col("n") * base)).alias("shr")))
        shr = rc["shr"].to_numpy()
        pred = float(shr.sum()) * fwd / years
        pb = paired_boot(nav, nav_c)
        print(f"{v.name:<12}{len(shr):>7}{float(np.median(shr)):>11.1%}"
              f"{float(shr.sum()):>11.2f}{-pred:>+10.1%}{pb['ann_diff']:>+11.1%}")


if __name__ == "__main__":
    main()
