"""A-index 探針 4:落地後的可信度交叉檢查
 (a) index 有資料但 daily_quote 當日無交易 -> 疑似 TWSE 假回應 / daily_quote 缺料
 (b) 旗艦指數 close 與「前日 close + 當日 change」不符 -> 檔案內容與宣稱日期不符(stale publish)
run: uv run --project . python docs/data_audit/scripts/A-index/probe4.py
"""
import polars as pl, subprocess

def psql(q):
    out = subprocess.run(["psql","-h","localhost","-p","5432","-d","quantlib","-c",
                          f"COPY ({q}) TO STDOUT WITH (FORMAT csv, HEADER true)"],
                         capture_output=True, text=True).stdout
    return pl.read_csv(out.encode(), schema_overrides={"date": pl.Utf8})

db = psql('SELECT market, date, name, close, change, "change(%)" AS pct FROM index')
dq = psql("SELECT market, date, count(*) n FROM daily_quote GROUP BY 1,2")

j = db.select(["market","date"]).unique().join(dq, on=["market","date"], how="left")
orphan = j.filter(pl.col("n").is_null()).sort(["market","date"])
print("== index 有資料但 daily_quote 該日無任何成交 ==", orphan.height)
print(orphan)

for mkt, nm in (("twse","發行量加權股價指數"), ("tpex","櫃買指數")):
    s = (db.filter((pl.col("market")==mkt) & (pl.col("name")==nm) & pl.col("close").is_not_null())
           .sort("date").with_columns(prev=pl.col("close").shift(1), prevd=pl.col("date").shift(1)))
    s = s.filter(pl.col("prev").is_not_null()).with_columns(
        resid=(pl.col("close")-pl.col("prev")-pl.col("change")).abs())
    bad = s.filter(pl.col("resid") > 1.0)
    print(f"\n== {mkt}/{nm}: |close - prev_close - change| > 1 的日數 = {bad.height} / {s.height} ==")
    with pl.Config(tbl_rows=40):
        print(bad.select(["date","prevd","prev","close","change","resid"]).sort("date"))
