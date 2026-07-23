"""A-index 探針 2:sentinel 檔大小分佈、檔名/內容日期(只看非空檔)、pct 遺失但 close 正常的樣態、特殊處理註記實測。"""
import polars as pl, re
from pathlib import Path
ROOT=Path("/Users/zaoldyeck/Documents/scala/quantlib")
sizes={}
for mkt in ("twse","tpex"):
    for f in (ROOT/"data/index"/mkt).rglob("*.csv"):
        s=f.stat().st_size
        if s<=1024: sizes.setdefault(mkt,[]).append((f.name,s))
for mkt,v in sizes.items():
    from collections import Counter
    c=Counter(s for _,s in v)
    print(mkt,"<=1024B files:",len(v)," size histogram:",dict(sorted(c.items())))
    nz=[x for x in v if x[1]>0]
    print("  non-zero small files:",nz[:10], "count",len(nz))

print("\n== 檔名 vs 內容日期(僅非空檔) ==")
bad=[]
for mkt in ("twse","tpex"):
    for f in sorted((ROOT/"data/index"/mkt).rglob("*.csv")):
        if f.stat().st_size<=1024: continue
        y,m,d=(int(x) for x in re.match(r"(\d+)_(\d+)_(\d+)\.csv",f.name).groups())
        head=f.read_bytes()[:200].decode("big5hkscs",errors="replace")
        if mkt=="twse":
            mm=re.search(r"(\d{2,3})年(\d{2})月(\d{2})日",head)
        else:
            mm=re.search(r"Data Date:(\d{2,3})/(\d{2})/(\d{2})",head)
        ok = mm and (int(mm.group(1))+1911==y and int(mm.group(2))==m and int(mm.group(3))==d)
        if not ok: bad.append((mkt,f.name,head.splitlines()[0][:70] if head.strip() else "(empty)"))
print("mismatched:",len(bad)); [print("  ",b) for b in bad[:20]]

ind=pl.read_parquet(ROOT/"docs/data_audit/scripts/A-index/indep_index.parquet")
print("\n== pct 為 '--' 但 close/change 皆有值(reader 落 pct=0,看起來像 0% 平盤) ==")
x=ind.filter(pl.col("pct").is_null() & pl.col("close").is_not_null() & pl.col("change").is_not_null())
print("n =",x.height)
print(x.select(["market","date","name","close","sign","mag_raw","change"]).head(10))
print("\n== sign 空字串但 mag_raw 有值(change 被硬設 0) ==")
print(ind.filter((pl.col("sign")=="") & (pl.col("mag_raw")!="--")).select(["market","date","name","close","sign","mag_raw","change","pct"]))
