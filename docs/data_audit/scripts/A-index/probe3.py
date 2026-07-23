"""A-index 探針 3:pct 歸零的實質影響、tpex 漲跌欄可解析性、change 與相鄰收盤差的語意交叉驗證。"""
import polars as pl
from pathlib import Path
ROOT=Path("/Users/zaoldyeck/Documents/scala/quantlib")
ind=pl.read_parquet(ROOT/"docs/data_audit/scripts/A-index/indep_index.parquet")

print("== pct 原始 '--' 但實際漲跌幅可由 close/change 推得,且 |推得值| >= 0.005% ==")
x=(ind.filter(pl.col("pct").is_null() & pl.col("close").is_not_null() & pl.col("change").is_not_null())
      .with_columns(implied=(pl.col("change")/(pl.col("close")-pl.col("change"))*100)))
big=x.filter(pl.col("implied").abs()>=0.005)
print("total pct-null with数 :",x.height," of which |implied|>=0.005%:",big.height)
print(big.select(["market","date","name","close","change","implied"]).sort(pl.col("implied").abs(),descending=True).head(10))

print("\n== close 為 NULL(TWSE 印 '--')的列,DB 仍存 change=0 / pct=0 ==")
print(ind.filter(pl.col("close").is_null()).group_by(pl.col("date").str.slice(0,4).alias("yr")).agg(pl.len().alias("n")).sort("yr"))

print("\n== tpex 漲跌欄(reader 用 .toDouble,非 Option)無法解析的筆數 ==")
tp=ind.filter(pl.col("market")=="tpex")
print(tp.filter(pl.col("change").is_null()).select(["date","name","close","mag_raw"]).head(10), tp.filter(pl.col("change").is_null()).height)
print("tpex close 無法解析:",tp.filter(pl.col("close").is_null()).height)

print("\n== 語意交叉驗證:change 是否 = 當日 close - 前一有效交易日 close(主要指數) ==")
for mkt,nm in (("twse","發行量加權股價指數"),("tpex","櫃買指數"),("twse","電子工業類指數")):
    s=(ind.filter((pl.col("market")==mkt)&(pl.col("name")==nm)&pl.col("close").is_not_null())
         .sort("date").with_columns(prev=pl.col("close").shift(1))
         .with_columns(diff=(pl.col("close")-pl.col("prev"))))
    s=s.filter(pl.col("prev").is_not_null())
    bad=s.filter((pl.col("diff")-pl.col("change")).abs()>0.05)
    print(f"{mkt}/{nm}: n={s.height} 不符={bad.height}")
    if bad.height: print(bad.select(["date","close","prev","diff","change"]).head(5))
