"""A-index 探針 6:tpex 報酬指數改名的破壞範圍、名稱含逗號/空白被吃掉的情形、DuckDB cache 是否有 index 表。"""
import polars as pl, csv, io, re
from pathlib import Path
ROOT=Path("/Users/zaoldyeck/Documents/scala/quantlib")
ind=pl.read_parquet(ROOT/"docs/data_audit/scripts/A-index/indep_index.parquet")

# 需要用未去空白/逗號的原始名 -> 重掃 tpex 報酬區原始名
raw_names=set(); mangled={}
for f in sorted((ROOT/"data/index/tpex").rglob("*.csv")):
    if f.stat().st_size<=1024: continue
    rows=[r for r in csv.reader(io.StringIO(f.read_bytes().decode("big5hkscs",errors="replace"))) if len(r)==4]
    seen=False
    for r in rows:
        h=r[0].strip()
        if h=="報酬指數": seen=True; continue
        if h=="指數": continue
        if seen:
            raw_names.add(h)
            nm=h.replace(" ","").replace(",","")
            exp=nm.replace("指數","")+"報酬指數"
            want=nm if nm.endswith("報酬指數") else nm.replace("指數","")+"報酬指數"
            if exp!=want: mangled[h]=exp
print("== tpex 報酬區原始名數 ==", len(raw_names))
print("== 被改名器多加一次「報酬」而失真的指數 ==", len(mangled))
for k,v in sorted(mangled.items()): print(f"   raw='{k}'  -> db='{v}'")
import subprocess
_q="SELECT market, date, name FROM index"
db=pl.read_csv(subprocess.run(["psql","-h","localhost","-p","5432","-d","quantlib","-c",
   f"COPY ({_q}) TO STDOUT WITH (FORMAT csv, HEADER true)"],capture_output=True,text=True).stdout.encode(),
   schema_overrides={"date":pl.Utf8})
bad=db.filter(pl.col("name").str.contains("報酬報酬"))
print("\n受影響列數:", bad.height, " 佔 tpex 全表:", round(bad.height/db.filter(pl.col("market")=='tpex').height*100,2), "%")

print("\n== 原始指數名稱含逗號(會被 reader 的 replace(\",\",\"\") 吃掉) ==")
cm=[n for n in raw_names if "," in n]
print(cm[:10], len(cm))
print("== 原始名稱含空白(被吃掉,如 'Quality 50指數'->'Quality50指數') ==")
sp=sorted(n for n in raw_names if " " in n)
print(sp[:15], len(sp))
