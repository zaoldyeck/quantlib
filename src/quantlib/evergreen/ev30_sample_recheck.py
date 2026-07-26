"""EV30:用乾淨資料重驗 EV1 的 224 檔蒸餾樣本——哲學/提示詞的地基還在嗎?

## 問題(使用者 2026-07-27)
「歷史資料有誤的問題現在已經解決,過去 Evergreen 蒸餾出來的提示詞以及標記用來
回測的池子,難道全部都要重做嗎?」

## 為什麼要先驗這一層
Evergreen 的產物有明確的依賴鏈:

    224 樣本(價格掃描選出) → LLM 消息面歸因 → 蒸餾哲學/提示詞 → 月度標記 → 回測池

**下游的量化部分(回測池、閘門、KPI)早已在乾淨資料上重做**(commit e141c36 抓到
inst5 gate 是「汙染幻影」;baeda67 重錄 live_config)。**消息材料 ev27/ev28 完全不受
影響**——那是外部新聞存檔,與 cache.duckdb 無關。

真正沒被檢查的是**最上游**:那 224 檔樣本是用 `audits/01_find_spikes.py` 在**當時的
價格資料**上掃出來的。若其中有樣本是資料汙染造出的幽靈暴漲,蒸餾出來的哲學就是從
「不存在的暴漲」學規律——那才是要不要重做的關鍵。

## 判準(兩種失效機制要分開)
對每個舊樣本 (code, t0),在乾淨資料上重算 60 交易日報酬:
- **A 公司行動型**:窗內有除權息/減資 → 掃描器本就該排除。舊資料漏了該筆事件,
  把配股造成的價格跳動記成暴漲(例:2364 記 +328%,實際 +116% 且窗內有除權息)。
- **B 價格汙染型**:窗內無公司行動,但乾淨資料上漲幅**不到門檻** → 舊價格本身是錯的
  (錯日 / 幽靈日 / 截斷,已於 2026-07-24 權威 rebuild 清除)。
- **C 仍成立**:漲幅仍達門檻(日期可能因 dedupe cooldown 挪動)。

Run: uv run --project . python -m quantlib.evergreen.ev30_sample_recheck
依賴 cache: 是(要乾淨資料)。
"""
from __future__ import annotations

import glob
import json

import polars as pl

from quantlib.apex import data

MIN_GAIN, WINDOW = 0.80, 60


def _old_samples() -> list[dict]:
    out: list[dict] = []
    for f in sorted(glob.glob("src/quantlib/evergreen/data/ev18_packs/surge_*.json")):
        out += json.load(open(f))
    seen, uniq = set(), []
    for x in out:
        k = (x["code"], x["t0"])
        if k not in seen:
            seen.add(k)
            uniq.append(x)
    return uniq


def main() -> None:
    con = data.connect()
    old = _old_samples()
    print(f"EV1 暴漲樣本 {len(old)} 檔(t0 {min(x['t0'] for x in old)} ~ {max(x['t0'] for x in old)})")
    print(f"重驗門檻:{WINDOW} 交易日漲幅 ≥ {MIN_GAIN:.0%}(與當初掃描一致)\n")

    rows = []
    for x in old:
        code, t0 = x["code"], x["t0"]
        q = con.sql(f"""
          WITH px AS (
            SELECT date, closing_price,
                   LEAD(closing_price, {WINDOW}) OVER (ORDER BY date) AS fut,
                   LEAD(date, {WINDOW}) OVER (ORDER BY date) AS dfut
            FROM daily_quote WHERE company_code = '{code}' AND closing_price > 0)
          SELECT closing_price, fut, dfut FROM px WHERE date = '{t0}'
        """).pl()
        if q.height == 0 or q["fut"][0] is None:
            rows.append({"code": code, "t0": t0, "old": x.get("gain_60d"),
                         "new": None, "ca": 0, "verdict": "無報價"})
            continue
        r = q.row(0, named=True)
        gain = r["fut"] / r["closing_price"] - 1
        ca = con.sql(f"""
          SELECT (SELECT count(*) FROM ex_right_dividend WHERE company_code='{code}'
                    AND date > '{t0}' AND date <= '{r["dfut"]}')
               + (SELECT count(*) FROM capital_reduction WHERE company_code='{code}'
                    AND date > '{t0}' AND date <= '{r["dfut"]}')
        """).pl().item()
        if ca > 0:
            v = "A 公司行動(本就該排除)"
        elif gain >= MIN_GAIN:
            v = "C 仍成立"
        else:
            v = "B 價格汙染(乾淨資料不到門檻)"
        rows.append({"code": code, "t0": t0, "old": x.get("gain_60d"),
                     "new": gain, "ca": ca, "verdict": v})

    df = pl.DataFrame(rows)
    print("=== 重驗結果 ===")
    for v, n in df.group_by("verdict").agg(pl.len().alias("n")).sort("n", descending=True).iter_rows():
        print(f"  {v:<28} {n:>4} 檔  ({n / len(rows):.1%})")

    bad = df.filter(pl.col("verdict").str.starts_with("B"))
    if bad.height:
        print(f"\n=== B 類(價格汙染)樣本的漲幅落差 ===")
        b = bad.drop_nulls(["old", "new"]).with_columns((pl.col("old") - pl.col("new")).alias("gap"))
        print(f"  舊記錄中位 {b['old'].median():.1%} → 乾淨資料中位 {b['new'].median():.1%}"
              f"(中位落差 {b['gap'].median():.1%})")
        for r in b.sort("gap", descending=True).head(8).iter_rows(named=True):
            print(f"    {r['code']} {r['t0']}  舊 {r['old']:+.0%} → 新 {r['new']:+.0%}")

    ok = df.filter(pl.col("verdict") == "C 仍成立").height
    print(f"\n=== 白話結論 ===")
    print(f"  {ok}/{len(rows)} 檔({ok / len(rows):.0%})在乾淨資料上仍是貨真價實的暴漲。")
    print(f"  其餘是當初資料有誤才被選進來的——哲學就是從這批樣本蒸餾的。")

    from quantlib import paths
    fp = paths.OUT / "evergreen_ev30_sample_recheck.csv"
    fp.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(fp)
    print(f"\n  逐檔明細 → {fp}")


if __name__ == "__main__":
    main()
