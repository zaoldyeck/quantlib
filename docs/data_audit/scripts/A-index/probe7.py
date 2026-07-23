"""A-index 探針 7:嚴格版內部一致性 —— 僅在「前一個 index 日期就是相鄰交易日且 close 有值」時檢查
   close_t - close_{t-1} == change_t。剔除因前日 close 為 NULL 造成的假警報。"""
import polars as pl, subprocess
def psql(q):
    out=subprocess.run(["psql","-h","localhost","-p","5432","-d","quantlib","-c",
        f"COPY ({q}) TO STDOUT WITH (FORMAT csv, HEADER true)"],capture_output=True,text=True)
    return pl.read_csv(out.stdout.encode(), schema_overrides={"date":pl.Utf8})
db=psql('SELECT market,date,name,close,change,"change(%)" AS pct FROM index')
for mkt,nm in (("twse","發行量加權股價指數"),("twse","電子類指數"),("tpex","櫃買指數")):
    s=(db.filter((pl.col("market")==mkt)&(pl.col("name")==nm)).sort("date")
        .with_columns(prev=pl.col("close").shift(1), prevd=pl.col("date").shift(1)))
    s=s.filter(pl.col("prev").is_not_null() & pl.col("close").is_not_null())
    s=s.with_columns(resid=(pl.col("close")-pl.col("prev")-pl.col("change")).abs())
    bad=s.filter(pl.col("resid")>0.02)
    print(f"== {mkt}/{nm}: n={s.height} 不一致={bad.height} ==")
    with pl.Config(tbl_rows=30):
        print(bad.select(["date","prevd","prev","close","change","resid"]).sort("date"))
