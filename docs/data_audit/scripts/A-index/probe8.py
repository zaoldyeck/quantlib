"""A-index 探針 8:全表內部一致性掃描 —— 逐日統計「close_t - close_{t-1} != change_t」的指數比例,
   高比例即代表該日檔案內容與宣稱日期不符(TWSE/TPEx stale publish)。"""
import polars as pl, subprocess
def psql(q):
    out=subprocess.run(["psql","-h","localhost","-p","5432","-d","quantlib","-c",
        f"COPY ({q}) TO STDOUT WITH (FORMAT csv, HEADER true)"],capture_output=True,text=True)
    return pl.read_csv(out.stdout.encode(), schema_overrides={"date":pl.Utf8})
db=psql('SELECT market,date,name,close,change FROM index WHERE close IS NOT NULL')
s=(db.sort(["market","name","date"])
     .with_columns(prev=pl.col("close").shift(1).over(["market","name"]),
                   prevd=pl.col("date").shift(1).over(["market","name"])))
# 只在「前一筆就是該市場的前一個 index 日」時檢查
days=db.select(["market","date"]).unique().sort(["market","date"])
days=days.with_columns(pd_=pl.col("date").shift(1).over("market"))
s=s.join(days, on=["market","date"]).filter(pl.col("prevd")==pl.col("pd_"))
s=s.with_columns(ok=((pl.col("close")-pl.col("prev")-pl.col("change")).abs()<=0.02))
agg=s.group_by(["market","date"]).agg(pl.len().alias("n"), (~pl.col("ok")).sum().alias("bad"))
agg=agg.with_columns(frac=pl.col("bad")/pl.col("n")).sort("frac",descending=True)
print("== 全表:可檢查列數 ==", s.height)
print("== 該日 >50% 指數對不上(檔案內容與日期不符)==")
with pl.Config(tbl_rows=40):
    print(agg.filter(pl.col("frac")>0.5).sort(["market","date"]))
print("\n== 該日 5%~50% 對不上 ==", agg.filter((pl.col("frac")>0.05)&(pl.col("frac")<=0.5)).height)
print(agg.filter((pl.col("frac")>0.05)&(pl.col("frac")<=0.5)).sort(["market","date"]).head(20))
print("\n== 全表不一致列數/總可檢查列數 ==", int(agg["bad"].sum()), "/", int(agg["n"].sum()),
      "=", round(100*agg["bad"].sum()/agg["n"].sum(),4), "%")
