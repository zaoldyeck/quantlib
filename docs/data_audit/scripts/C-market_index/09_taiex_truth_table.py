"""C-market_index #9: 用交易所第一手資料量化 TAIEX 線的失真幅度。

真值來源(第一手,非推測):TWSE「每日市場成交資訊 FMTQIK」月報,欄位含
『發行量加權股價指數』與『漲跌點數』。
  https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date=YYYYMM01&response=csv
本腳本把 8 個幽靈日的真值寫死(附抓取日期),與 cache 內容對照,算出
「用 cache 算日報酬」相對「用真值算日報酬」的誤差。

Run: PYTHONPATH=. uv run --project research python docs/data_audit/scripts/C-market_index/09_taiex_truth_table.py
"""
import duckdb
from research import paths

con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
TAIEX = "發行量加權股價指數"

# FMTQIK 實抓於 2026-07-22。None = 該日不在交易所的交易日清單裡(根本沒開市)。
TRUTH = {
    "2015-08-28": 8019.18, "2015-08-29": None,   "2015-08-31": 8174.92,
    "2016-05-25": 8396.20, "2016-05-26": 8394.12, "2016-05-27": 8463.61,
    "2017-08-01": 10437.29, "2017-08-02": 10519.27, "2017-08-03": 10469.88,
    "2018-08-03": 11012.43, "2018-08-04": None,   "2018-08-06": 11024.10,
    "2018-09-14": 10868.14, "2018-09-15": None,   "2018-09-17": 10828.61,
    "2018-10-02": 10919.63, "2018-10-03": 10863.94, "2018-10-04": 10718.91,
    "2019-07-04": 10775.90, "2019-07-05": 10785.73, "2019-07-08": 10751.22,
    "2019-09-24": 10918.01, "2019-09-25": 10873.69, "2019-09-26": 10871.99,
}
CASES = [
    ("2015-08-28", "2015-08-29", "2015-08-31"),
    ("2016-05-25", "2016-05-26", "2016-05-27"),
    ("2017-08-01", "2017-08-02", "2017-08-03"),
    ("2018-08-03", "2018-08-04", "2018-08-06"),
    ("2018-09-14", "2018-09-15", "2018-09-17"),
    ("2018-10-02", "2018-10-03", "2018-10-04"),
    ("2019-07-04", "2019-07-05", "2019-07-08"),
    ("2019-09-24", "2019-09-25", "2019-09-26"),
]


def cached(d: str):
    r = con.sql(f"SELECT close FROM market_index WHERE market='twse' "
                f"AND name='{TAIEX}' AND date=DATE '{d}'").fetchone()
    return r[0] if r else None


print(f"{'前一日':12} {'受汙染日':12} {'次一日':12} | "
      f"{'cache 當日ret':>13} {'真實當日ret':>12} | {'cache 次日ret':>13} {'真實次日ret':>12}")
print("-" * 108)
for prev, bad, nxt in CASES:
    cp, cb, cn = cached(prev), cached(bad), cached(nxt)
    tp, tb, tn = TRUTH[prev], TRUTH[bad], TRUTH[nxt]
    assert abs(cp - tp) < 0.01, f"{prev} cache {cp} != truth {tp}"
    assert abs(cn - tn) < 0.01, f"{nxt} cache {cn} != truth {tn}"
    c_bad = (cb / cp - 1) * 100
    c_nxt = (cn / cb - 1) * 100
    if tb is None:                       # 交易所無此交易日 → 該日不該存在
        t_bad, t_nxt = float("nan"), (tn / tp - 1) * 100
        tag = "(該日交易所無開市 → 整列都是幽靈)"
    else:
        t_bad, t_nxt = (tb / tp - 1) * 100, (tn / tb - 1) * 100
        tag = f"(cache close={cb} vs 真值={tb},差 {cb - tb:+.2f} 點)"
    print(f"{prev:12} {bad:12} {nxt:12} | {c_bad:>12.2f}% {t_bad:>11.2f}% | "
          f"{c_nxt:>12.2f}% {t_nxt:>11.2f}%   {tag}")

print("\n判讀:")
print("  · 2018-09-15 是週六、交易所根本沒開市,cache 卻有 114 列(整天是 2017-03-17 的複本);")
print("    用 cache 算出來的『2018-09-14→09-15 -8.83%』與『09-15→09-17 +9.28%』兩根都是憑空生出來的,")
print("    真實 09-14→09-17 只有 -0.36%。任何以大盤日報酬做 regime / 下跌市過濾的策略在此處會被誤導。")
print("  · 2019-07-05 存的是 2019-07-16 的收盤(10,886.05),那天在真實世界還沒發生 → 前視汙染。")
