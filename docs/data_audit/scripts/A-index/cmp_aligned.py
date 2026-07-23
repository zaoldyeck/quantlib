"""A-index:套用 reader 宣稱的欄位語意後,與 PG `index` 全表逐列比對。

expected name:
  twse            -> 原始名(去空白/逗號)
  tpex 價格區     -> 原始名
  tpex 報酬區     -> name.replace("指數","") + "報酬指數"   (受測轉換,故意照抄以對齊)
expected change:
  twse            -> sign(values2) * |values3|;sign=="" -> 0
  tpex            -> values2 直接數值
run: uv run --project research python docs/data_audit/scripts/A-index/cmp_aligned.py
"""
import polars as pl
import subprocess
def load_db():
    """直接從 PG 讀 index 全表,免除外部匯出檔依賴(第三者可原樣重跑)。"""
    q = 'SELECT market, date, name, close, change, "change(%)" AS pct FROM index'
    out = subprocess.run(["psql","-h","localhost","-p","5432","-d","quantlib","-c",
                          f"COPY ({q}) TO STDOUT WITH (FORMAT csv, HEADER true)"],
                         capture_output=True, text=True).stdout
    return pl.read_csv(out.encode(), schema_overrides={"date": pl.Utf8})

ROOT="/Users/zaoldyeck/Documents/scala/quantlib"
ind = pl.read_parquet(f"{ROOT}/docs/data_audit/scripts/A-index/indep_index.parquet")
ind = ind.with_columns(
    exp_name=pl.when((pl.col("market")=="tpex") & (pl.col("section")=="return"))
              .then(pl.col("name").str.replace_all("指數","") + pl.lit("報酬指數"))
              .otherwise(pl.col("name"))
)
print("== raw rows named literally 'null' (source-side broken name) ==")
print(ind.filter(pl.col("name")=="null").group_by("market").agg(pl.len(), pl.col("date").min().alias("d0"), pl.col("date").max().alias("d1")))

# reader drops name=="null"
exp = ind.filter(pl.col("exp_name")!="null")
# duplicate (market,date,exp_name) -> unique index collision candidates
dups = exp.group_by(["market","date","exp_name"]).agg(pl.len().alias("k")).filter(pl.col("k")>1)
print(f"\n== expected-name collisions inside one file: {dups.height} ==")
print(dups.group_by(["market","exp_name"]).agg(pl.len().alias("days"), pl.col("date").min().alias("d0"), pl.col("date").max().alias("d1")).sort("days", descending=True).head(20))

db = load_db().rename({"name": "exp_name"})
e = exp.select(["market","date","exp_name","close","change","pct","sign","mag_raw","section"]).unique(["market","date","exp_name"], keep="first")
j = e.join(db, on=["market","date","exp_name"], how="full", coalesce=True, suffix="_db")
only_raw = j.filter(pl.col("close").is_null() & pl.col("change").is_null() & pl.col("pct").is_null() & pl.col("close_db").is_not_null())
missing_in_db = j.filter(pl.col("change_db").is_null())
missing_in_raw = j.filter(pl.col("change").is_null() & pl.col("sign").is_null())
print(f"\n== rows in raw but NOT in db: {missing_in_db.height} ==")
print(missing_in_db.group_by(["market"]).agg(pl.len()))
print(missing_in_db.group_by(["market","exp_name"]).agg(pl.len().alias("n"), pl.col("date").min().alias("d0"), pl.col("date").max().alias("d1")).sort("n",descending=True).head(25))
print(f"\n== rows in db but NOT in raw: {missing_in_raw.height} ==")
print(missing_in_raw.group_by(["market","exp_name"]).agg(pl.len().alias("n"), pl.col("date").min().alias("d0"), pl.col("date").max().alias("d1")).sort("n",descending=True).head(25))

m = j.filter(pl.col("change").is_not_null() | pl.col("sign").is_not_null()).filter(pl.col("change_db").is_not_null())
def bad(col):
    a,b = pl.col(col), pl.col(col+"_db")
    return m.filter(((a.is_null() != b.is_null()) | ((a-b).abs()>5e-7).fill_null(False)))
for c in ("close","change","pct"):
    x = bad(c)
    print(f"\n== {c} mismatch: {x.height} / {m.height} ==")
    if x.height:
        with pl.Config(tbl_rows=15, fmt_str_lengths=30):
            print(x.select(["market","date","exp_name","section","sign","mag_raw",c,c+"_db"]).head(15))
