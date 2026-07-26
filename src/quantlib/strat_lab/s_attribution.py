"""S 的報酬歸因 + 資金效率診斷:82% CAGR 從哪來?還有多少「本來吃得到卻沒吃到」?

不是又一個變體測試,是**畫出剩餘空間的地圖**——先知道錢在哪、漏在哪,才知道下一刀往哪切。

四張診斷:
1. **右尾集中度**:前 1/5/10% 交易貢獻多少總報酬?拿掉最好的 N 筆還剩多少?
   → 若極度集中 = 提升空間在「提高右尾捕獲率」(名單深度/持有期),不在參數微調。
2. **資金效率**:實際持倉檔數 vs slots 上限的時間分佈、現金閒置比例。
   → 若長期未滿倉 = 資金在空轉,提高利用率是免費的 alpha。
3. **未成交/遞補漏損**:每日候選名單(top-5)中因「已持有/漲停/資金不足」沒進場的比例,
   以及那些沒買到的候選後續 21 日報酬(= 漏掉的錢)。
4. **出場後的續漲**(exit regret):每筆出場後 21/63 日,該股又漲/跌多少?
   分 exit_reason 看——若某類出場後續漲很多 = 那個出場條件在砍活的。

Run: uv run --project . python -m quantlib.strat_lab.s_attribution
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.strategy_s import C, DS, prep_cached, run_s_full


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    nav, trades = run_s_full(panel, feat, elig, DS)
    nav = nav.sort("date")
    closed = trades.filter(pl.col("exit_reason") != "open")

    # ── 1. 右尾集中度 ────────────────────────────────────────────────
    r = closed["ret_net"].to_numpy()
    order = np.argsort(-r)
    tot_log = np.sum(np.log1p(r))          # 對數報酬可加總 = 複利貢獻
    print(f"=== 1. 右尾集中度(已平倉 {len(r)} 筆;總對數報酬 {tot_log:.2f})===")
    for pct in (0.01, 0.05, 0.10, 0.20):
        k = max(1, int(len(r) * pct))
        share = np.sum(np.log1p(r[order[:k]])) / tot_log
        print(f"  前 {pct:>4.0%}({k:>3} 筆)貢獻總複利的 {share:>6.1%};該組平均報酬 {r[order[:k]].mean():+.1%}")
    for k in (1, 3, 5, 10):
        rest = np.sum(np.log1p(np.delete(r, order[:k])))
        print(f"  拿掉最好的 {k:>2} 筆 → 剩餘複利 {rest/tot_log:>6.1%}(等於 exp {np.exp(rest):.1f}x vs 原 {np.exp(tot_log):.1f}x)")
    win_r = r[r > 0]; los_r = r[r <= 0]
    print(f"  勝率 {len(win_r)/len(r):.1%};平均賺 {win_r.mean():+.1%} / 平均賠 {los_r.mean():+.1%}"
          f";賺賠比 {abs(win_r.mean()/los_r.mean()):.2f}")

    # ── 2. 資金效率(同時持倉數的時間分佈)────────────────────────────
    days = nav["date"].to_list()
    held = []
    ed = closed.select(["entry_date", "exit_date"]).to_dicts() + \
        trades.filter(pl.col("exit_reason") == "open").select(["entry_date", "exit_date"]).to_dicts()
    for d in days:
        held.append(sum(1 for t in ed if t["entry_date"] <= d and (t["exit_date"] is None or d < t["exit_date"])))
    h = np.array(held)
    print(f"\n=== 2. 資金效率(slots=5;{len(h)} 個交易日)===")
    for k in range(6):
        print(f"  持 {k} 檔的日數佔比 {np.mean(h == k):>6.1%}" + ("  ← 滿倉" if k == 5 else ""))
    print(f"  平均持倉 {h.mean():.2f}/5 = 資金利用率 {h.mean()/5:.1%}(未計現金拖累的精確值,僅檔位口徑)")

    # ── 3. 未進場漏損:候選在榜但沒買到,後續漲多少 ────────────────────
    from quantlib.apex.assemble import entries_and_flags
    # 重算當日候選(與 run_s_full 同法,取 top5)
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    from quantlib.apex.strategy_s import WREL
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
          .drop_nulls(subset=list(WREL))
          .filter(pl.all_horizontal([pl.col(c).is_finite() for c in WREL]))
          .filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    expr = None
    for c_, wt in WREL.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = (df.with_columns(expr.alias("score")).select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(DS).str.to_date()))
    cands, _ = entries_and_flags(sc, 5, 10**9)
    got = trades.select([pl.col("entry_date").alias("date"), C]).unique()
    # 候選 vs 實際進場(進場日 = 候選日 +1,故比對用候選日)
    miss = cands.join(got.with_columns(pl.col("date")), on=["date", C], how="anti")
    px = panel.select([C, "date", "close"]).sort([C, "date"])
    fwd = (px.with_columns((pl.col("close").shift(-21).over(C) / pl.col("close") - 1).alias("f21"),
                           (pl.col("close").shift(-63).over(C) / pl.col("close") - 1).alias("f63")))
    m = miss.join(fwd, on=[C, "date"], how="left").drop_nulls(subset=["f21"])
    g = cands.join(got, on=["date", C], how="semi").join(fwd, on=[C, "date"], how="left").drop_nulls(subset=["f21"])
    print(f"\n=== 3. 候選漏損(top-5 名單中沒進場者的後續報酬)===")
    print(f"  候選總筆 {cands.height:,};實際進場 {got.height:,};未進場 {miss.height:,}({miss.height/max(cands.height,1):.0%})")
    if m.height and g.height:
        print(f"  未進場者 fwd21 中位 {m['f21'].median():+.2%} / fwd63 {m['f63'].median():+.2%}(n={m.height:,})")
        print(f"  有進場者 fwd21 中位 {g['f21'].median():+.2%} / fwd63 {g['f63'].median():+.2%}(n={g.height:,})")
        print("  判讀:未進場者若報酬與有進場者相當 → 漏掉的是真錢(提高利用率/名單深度有價值);"
              "明顯較差 → 引擎已優先挑到好的。")

    # ── 4. 出場後悔(exit regret,分 reason)──────────────────────────
    ex = (closed.select([C, "exit_date", "exit_reason", "ret_net"])
          .join(fwd.select([C, pl.col("date").alias("exit_date"), "f21", "f63"]),
                on=[C, "exit_date"], how="left"))
    print(f"\n=== 4. 出場後悔(出場後該股又走了多少;正 = 賣早了)===")
    print(f"  {'出場原因':<14}{'筆數':>6}{'本筆報酬中位':>13}{'出場後21日':>12}{'出場後63日':>12}")
    for reason, grp in sorted(ex.group_by("exit_reason"), key=lambda kv: -kv[1].height):
        rn = reason[0] if isinstance(reason, tuple) else reason
        f21 = grp["f21"].median(); f63 = grp["f63"].median()
        print(f"  {str(rn):<14}{grp.height:>6}{grp['ret_net'].median():>+12.1%}"
              f"{(f21 if f21 is not None else float('nan')):>+11.1%}{(f63 if f63 is not None else float('nan')):>+11.1%}")
    print("  判讀:某 reason 的『出場後續漲』顯著為正 = 該出場條件在砍活的(值得放寬);"
          "為負 = 出場正確(該跑就跑)。")


if __name__ == "__main__":
    main()
