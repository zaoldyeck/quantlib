"""A-index 探針 5:以 daily_quote 交易日為日曆,盤點 index 表缺漏的交易日與缺檔。"""
import polars as pl, subprocess
from pathlib import Path
ROOT=Path("/Users/zaoldyeck/Documents/scala/quantlib")
def psql(q):
    out=subprocess.run(["psql","-h","localhost","-p","5432","-d","quantlib","-c",
        f"COPY ({q}) TO STDOUT WITH (FORMAT csv, HEADER true)"],capture_output=True,text=True)
    return pl.read_csv(out.stdout.encode(), schema_overrides={"date":pl.Utf8})
dq=psql("SELECT market, date, count(*) n FROM daily_quote GROUP BY 1,2")
ix=psql("SELECT market, date, count(*) n FROM index GROUP BY 1,2").rename({"n":"n_idx"})
for mkt,start in (("twse","2009-01-05"),("tpex","2016-01-04")):
    cal=dq.filter((pl.col("market")==mkt)&(pl.col("date")>=start)&(pl.col("date")<="2026-07-17"))
    j=cal.join(ix.filter(pl.col("market")==mkt), on=["market","date"], how="left")
    miss=j.filter(pl.col("n_idx").is_null()).sort("date")
    print(f"== {mkt}: 交易日 {cal.height} 天,index 缺 {miss.height} 天 ==")
    if miss.height:
        rows=[]
        for d in miss["date"]:
            y,m,dd=d.split("-")
            f=ROOT/f"data/index/{mkt}/{y}/{int(y)}_{int(m)}_{int(dd)}.csv"
            rows.append((d, f.stat().st_size if f.exists() else "NO_FILE"))
        print(rows[:60])
        print("  缺檔(NO_FILE)天數:", sum(1 for _,s in rows if s=="NO_FILE"),
              " 有檔但被略過(<=1024B):", sum(1 for _,s in rows if s!="NO_FILE"))
