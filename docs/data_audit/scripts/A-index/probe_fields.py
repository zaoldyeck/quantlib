"""A-index:欄位語意探針 —— sign 取值、"--"/空值強制歸零、被丟棄的特殊處理註記、檔名 vs 內容日期。
run: uv run --project . python docs/data_audit/scripts/A-index/probe_fields.py
"""
import polars as pl, re, csv, io
from pathlib import Path
ROOT=Path("/Users/zaoldyeck/Documents/scala/quantlib")
ind = pl.read_parquet(ROOT/"docs/data_audit/scripts/A-index/indep_index.parquet")

print("== twse 漲跌(+/-) 欄取值分佈 (Scala match 只有 \"-\"/\"\"/\"+\" 三 case) ==")
print(ind.filter(pl.col("market")=="twse").group_by("sign").agg(pl.len().alias("n")).sort("n",descending=True))

t = ind.filter(pl.col("market")=="twse")
print("\n== twse: sign 為空字串時 mag_raw 的取值 ==")
print(t.filter(pl.col("sign")=="").group_by("mag_raw").agg(pl.len().alias("n")).sort("n",descending=True).head(10))

print("\n== twse: sign 非空(+/-) 但 mag_raw 無法解析 -> reader 落 0 ==")
bad = t.filter((pl.col("sign").is_in(["+","-"])) & (pl.col("change").is_null()))
print(bad.select(["date","name","sign","mag_raw","close","pct"]))

print("\n== 漲跌百分比欄原始為 '--'/空 -> reader 落 0 的筆數 ==")
pc = ind.filter(pl.col("pct").is_null())
print(pc.group_by("market").agg(pl.len().alias("n"), pl.col("date").min().alias("d0"), pl.col("date").max().alias("d1")))
print(pc.group_by(pl.col("date").str.slice(0,4).alias("yr")).agg(pl.len().alias("n")).sort("yr"))

print("\n== close 為 NULL(原始 '--')的筆數 ==")
cl = ind.filter(pl.col("close").is_null())
print(cl.group_by("market").agg(pl.len().alias("n"), pl.col("date").n_unique().alias("days")))

print("\n== twse 特殊處理註記(第 6 欄)取值分佈 —— schema 未接、資訊丟棄 ==")
print(t.group_by("note").agg(pl.len().alias("n"), pl.col("date").min().alias("d0"), pl.col("date").max().alias("d1")).sort("n",descending=True).head(10))

print("\n== 檔名日期 vs 內容日期(民國)核對 ==")
bad_date=[]
for mkt in ("twse","tpex"):
    for f in sorted((ROOT/"data/index"/mkt).rglob("*.csv")):
        y,m,d = (int(x) for x in re.match(r"(\d+)_(\d+)_(\d+)\.csv",f.name).groups())
        head = f.read_bytes()[:200].decode("big5hkscs", errors="replace")
        if mkt=="twse":
            mm = re.search(r"(\d{2,3})年(\d{2})月(\d{2})日", head)
            ok = mm and (int(mm.group(1))+1911==y and int(mm.group(2))==m and int(mm.group(3))==d)
        else:
            mm = re.search(r"Data Date:(\d{2,3})/(\d{2})/(\d{2})", head)
            ok = mm and (int(mm.group(1))+1911==y and int(mm.group(2))==m and int(mm.group(3))==d)
        if not ok:
            bad_date.append((mkt,f.name, head.splitlines()[0][:60] if head else ""))
print("mismatched files:", len(bad_date))
for b in bad_date[:20]: print("  ",b)

print("\n== 檔案大小 <=1024B(reader 的 filter(_.file.length > 1024) 會整檔略過) ==")
small=[(m,f.name,f.stat().st_size) for m in ("twse","tpex") for f in (ROOT/"data/index"/m).rglob("*.csv") if f.stat().st_size<=1024]
print(len(small), small[:20])

print("\n== 名稱中出現替換字元(Big5 解碼失敗)的筆數 ==")
print(ind.filter(pl.col("name").str.contains("�")).select(["market","date","name"]).head(10))
print("total:", ind.filter(pl.col("name").str.contains("�")).height)
