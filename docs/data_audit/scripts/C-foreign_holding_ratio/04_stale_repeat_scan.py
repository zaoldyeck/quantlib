"""C-foreign_holding_ratio 稽核 04:「這一天的內容其實是別天的複製品」全史掃描。

方法:對每個 market 的相鄰兩個有資料日,以 company_code 內連接後比對
outstanding_shares / foreign_held_shares / foreign_held_ratio 三欄,
統計「完全相同的代號數 / 共同代號數」。比值 = 1.0 且共同代號 > 50 → 該日內容
與前一日逐檔完全一致 = 交易所回舊快照(或爬蟲存錯)。

真實邊界:外資持股是**存量快照**,單日不動的個股本來就多;但整市場 1,000+ 檔
「三欄全同」的機率極低,只有複製才會發生。因此同時輸出 identical_ratio 分佈
供判讀,而非只看 1.0。

Run: uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/04_stale_repeat_scan.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

SQL = """
WITH d AS (SELECT DISTINCT date FROM foreign_holding_ratio WHERE market=?),
     l AS (SELECT date, LAG(date) OVER (ORDER BY date) prev FROM d)
SELECT l.prev, l.date,
       COUNT(*) AS common_codes,
       SUM(CASE WHEN a.outstanding_shares = b.outstanding_shares
                 AND a.foreign_held_shares = b.foreign_held_shares
                 AND a.foreign_held_ratio  = b.foreign_held_ratio
                THEN 1 ELSE 0 END) AS identical
FROM l
JOIN foreign_holding_ratio a ON a.market=? AND a.date=l.date
JOIN foreign_holding_ratio b ON b.market=? AND b.date=l.prev
                             AND b.company_code=a.company_code
WHERE l.prev IS NOT NULL
GROUP BY 1,2
"""


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    for mkt in ("twse", "tpex"):
        df = con.execute(SQL, [mkt, mkt, mkt]).df()
        df["ratio"] = df["identical"] / df["common_codes"]
        df = df.sort_values("date")
        full = df[(df["ratio"] == 1.0) & (df["common_codes"] > 50)]
        print(f"\n===== [{mkt}] 相鄰日對 {len(df)} 組;逐檔完全相同的日 {len(full)} 組 =====")
        print("ratio 分位數:",
              {q: round(float(df["ratio"].quantile(q)), 4) for q in (0.01, 0.05, 0.5, 0.95, 0.99)})
        if len(full):
            print(f"  完全複製日期(前 60):")
            for _, r in full.head(60).iterrows():
                print(f"    {r['date']}  ← 與 {r['prev']} 逐檔相同 ({r['identical']}/{r['common_codes']})")
            print(f"  年度分佈:")
            print(full.assign(y=full["date"].map(lambda d: d.year)).groupby("y").size().to_string())
        # 次高:ratio > 0.99 但不到 1(可能是部分更新)
        near = df[(df["ratio"] >= 0.995) & (df["ratio"] < 1.0) & (df["common_codes"] > 50)]
        print(f"  ratio ∈ [0.995,1) 的日:{len(near)}")
        for _, r in near.head(20).iterrows():
            print(f"    {r['date']} ← {r['prev']} ({r['identical']}/{r['common_codes']} = {r['ratio']:.4f})")


if __name__ == "__main__":
    main()
