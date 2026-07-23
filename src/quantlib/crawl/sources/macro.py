"""台灣總經資料爬蟲 —— 官方源 → 全史 parquet(PIT 語義內建)。

為台股量化系統提供「總經 regime 特徵」的單一事實來源。每個 series 落一個 parquet,
欄位一律 `date`(資料期)、`value`、`avail_date`(保守可用日),外加該 series 的輔助欄。
可重跑冪等:每次全量重抓、整檔覆寫(不做增量 append —— 增量會把官方事後修正藏成
無聲汙染,全量重抓才是唯一事實)。網路一律走 requests;任一源失敗只記錄、不硬湊,
其餘源照樣落檔。

──────────────────────────────────────────────────────────────────────────
PIT(Point-In-Time)語義 —— 為什麼需要 avail_date
──────────────────────────────────────────────────────────────────────────
總經資料的「資料期」≠「可用日」。M1B 的 5 月值要到 6 月下旬才由央行公布;若回測
在 5/31 就用到 5 月 M1B,即是前視(look-ahead)偏誤。`avail_date` 是「這個值最早
可以被策略看到」的保守日,策略端 merge 一律以 `avail_date <= 決策日` 過濾:

  - 月頻(M1B/M2、景氣信號):次月底(資料月 M → M+1 月最後一天)。實際公布更早
    (M1B/M2 約次月 25 日、景氣信號約次月 27 日,見各源註記),次月底是安全上界。
  - 政策利率(重貼現率,事件):調整日當天 —— 央行理監事會決議即公告、即生效、
    即公開;逐月序列取「月初已生效值」,月中調整要下月才反映(不前視)。
  - 日頻(USD/TWD):當日 —— 收盤匯率 16:00–17:00 公布,當日盤後即可得,對「收盤
    或次日」執行不前視。若你在 17:00 前盤中執行,請自行 +1 日。

──────────────────────────────────────────────────────────────────────────
資料源(全部官方、免金鑰、requests 直取)
──────────────────────────────────────────────────────────────────────────
1. M1B / M2 年增率(月頻,日平均數)
   來源:中央銀行 · data.gov.tw dataset 6024「貨幣總計數」
   CSV :https://www.cbc.gov.tw/public/data/OpenData/經研處/EF15M01.csv  (UTF-8-BOM)
   欄位:期間(1987M05…)、…、貨幣總計數-Ｍ１Ｂ-年增率、貨幣總計數-Ｍ２-年增率
   說明:採「日平均數」序列(景氣對策信號的 M1B 構成項目即用此),非月底數。
   → m1b_yoy.parquet, m2_yoy.parquet(value=年增率 %,附 level=原始值 百萬元)

2. 重貼現率(央行政策利率,不定期)
   來源:中央銀行 · data.gov.tw dataset 6022「央行貼放利率」
   CSV :https://www.cbc.gov.tw/Public/Data/opendata/webF1.csv  (Big5)
   欄位:調整日期、重貼現率、擔保放款融通利率、短期融通利率
   說明:檔案是「變動事件表」(只在調整時新增一列),起 2000-12-29。
   → rediscount_rate.parquet(月頻 forward-fill,月初已生效值)
     + rediscount_rate_changes.parquet(原始變動事件,審計用)

3. 景氣對策信號(綜合判斷分數 + 燈號,月頻)
   來源:國家發展委員會 · data.gov.tw dataset 6099「景氣指標及燈號」
   ZIP :動態解析 https://data.gov.tw/api/v2/rest/dataset/6099 的 distribution[0]
        resourceDownloadUrl(該 base64 的 ws.ndc.gov.tw/Download.ashx 連結會隨改版
        變動,故每次動態解析,不寫死)。ZIP 內 `景氣指標與燈號.csv`(UTF-8-BOM)。
   欄位:Date(198201…)、領先/同時/落後指標綜合指數、景氣對策信號綜合分數、景氣對策信號
   → business_climate_signal.parquet(value=綜合分數 9–45,附燈號 signal_light +
     領先/同時/落後綜合指數)

4. USD/TWD 銀行間收盤匯率(日頻)
   來源:中央銀行 · data.gov.tw dataset 7232(台北外匯經紀 提供)
   JSON:https://cpx.cbc.gov.tw/api/OpenData/FTDOpenData_Day
   欄位:日期(YYYYMMDD)、NTD_USD;起 2008-01-02,每營業日更新
   → usdtwd.parquet(value=每 1 美元兌新台幣)

未整合(誠實記錄,見 NOT_INTEGRATED)—— 加值項在時限內無「全國 · 全史 · 機器可讀」
官方端點,不硬湊:
  - 台指選擇權波動率指數(TAIWAN VIX,期交所):免費端點(cht/7/vixDaily3MNew)只回
    近 3 個月且是 HTML 頁,全史為 edatashop 付費商品 → 無法全史回填。
  - CPI 年增率(主計總處)/ 外銷訂單年增率(經濟部):data.gov.tw 命中的多為縣市層級
    (如 dataset 124459 = 新北市物價指數)或彙總頁,未找到全國時序 CSV/JSON。

Run(從 repo root;依賴 requests+polars,與 DuckDB cache 無關,不需 cache_tables.py):
  uv run --project . python -m quantlib.data_macro.fetch_macro
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import date, datetime
from pathlib import Path

import polars as pl
import requests

OUT_DIR = Path(__file__).resolve().parent
HEADERS = {"User-Agent": "Mozilla/5.0 (quantlib-macro-fetcher)"}
TIMEOUT = 90

URL_MONEY = (
    "https://www.cbc.gov.tw/public/data/OpenData/"
    "%E7%B6%93%E7%A0%94%E8%99%95/EF15M01.csv"  # 經研處/EF15M01.csv
)
URL_REDISCOUNT = "https://www.cbc.gov.tw/Public/Data/opendata/webF1.csv"
URL_USDTWD = "https://cpx.cbc.gov.tw/api/OpenData/FTDOpenData_Day"
NDC_DATASET_API = "https://data.gov.tw/api/v2/rest/dataset/6099"

NOT_INTEGRATED = {
    "taifex_vix": "期交所免費端點僅近 3 個月且為 HTML;全史為 edatashop 付費商品",
    "cpi_yoy": "data.gov.tw 未見全國全史機器可讀 CSV/JSON(命中多為縣市層級)",
    "export_orders_yoy": "經濟部未見全國全史機器可讀 CSV/JSON 端點",
}


# ── IO boundary ──────────────────────────────────────────────────────────
def http_get(url: str, *, tries: int = 3) -> bytes:
    last: Exception | None = None
    for _ in range(tries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.content
        except Exception as e:  # noqa: BLE001 — retry then re-raise honestly
            last = e
    raise RuntimeError(f"GET failed after {tries} tries: {url} :: {last}")


def decode_bytes(raw: bytes) -> str:
    """CBC/NDC 檔案編碼不一(UTF-8-BOM / Big5),依序嘗試。"""
    for enc in ("utf-8-sig", "cp950", "big5", "utf-8"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


# ── pure parse helpers ───────────────────────────────────────────────────
def parse_period_m(s: str) -> date:
    """'1987M05' → date(1987, 5, 1)。"""
    y, m = s.strip().split("M")
    return date(int(y), int(m), 1)


def parse_yyyymm(s: str) -> date:
    """'198201' → date(1982, 1, 1)。"""
    s = s.strip()
    return date(int(s[:4]), int(s[4:6]), 1)


def _num(x: str) -> float | None:
    x = x.strip()
    return None if x in ("", "-", "－", "…", "N/A") else float(x)


def _month_end_next(col: str) -> pl.Expr:
    """月頻資料期 → 次月最後一天(保守可用日)。date 為當月 1 日。"""
    return pl.col(col).dt.offset_by("2mo").dt.offset_by("-1d").alias("avail_date")


def _norm_header(h: str) -> str:
    return h.replace(" ", "").replace("　", "")


def _find_col(header: list[str], variants: list[list[str]]) -> int:
    """回傳第一個「正規化後含 variants 任一組全部 token」的欄位索引。"""
    norm = [_norm_header(h) for h in header]
    for i, hn in enumerate(norm):
        if any(all(t in hn for t in toks) for toks in variants):
            return i
    raise KeyError(f"column not found; variants={variants}; header={header}")


# ── per-series fetch (IO) + parse (pure) ─────────────────────────────────
def fetch_money_supply() -> dict[str, pl.DataFrame]:
    rows = list(csv.reader(io.StringIO(decode_bytes(http_get(URL_MONEY)))))
    header = rows[0]
    i_m1b_yoy = _find_col(header, [["Ｍ１Ｂ", "年增率"], ["M1B", "年增率"]])
    i_m1b_lvl = _find_col(header, [["Ｍ１Ｂ", "原始值"], ["M1B", "原始值"]])
    i_m2_yoy = _find_col(header, [["Ｍ２", "年增率"], ["M2", "年增率"]])
    i_m2_lvl = _find_col(header, [["Ｍ２", "原始值"], ["M2", "原始值"]])

    recs = []
    for r in rows[1:]:
        if not r or not r[0].strip():
            continue
        recs.append(
            (parse_period_m(r[0]), _num(r[i_m1b_yoy]), _num(r[i_m1b_lvl]),
             _num(r[i_m2_yoy]), _num(r[i_m2_lvl]))
        )
    df = pl.DataFrame(
        recs,
        schema={"date": pl.Date, "m1b_yoy": pl.Float64, "m1b_level": pl.Float64,
                "m2_yoy": pl.Float64, "m2_level": pl.Float64},
        orient="row",
    ).with_columns(_month_end_next("date"))

    m1b = (df.select(pl.col("date"), pl.col("m1b_yoy").alias("value"),
                     pl.col("m1b_level").alias("level"), pl.col("avail_date"))
             .drop_nulls("value").sort("date"))
    m2 = (df.select(pl.col("date"), pl.col("m2_yoy").alias("value"),
                    pl.col("m2_level").alias("level"), pl.col("avail_date"))
            .drop_nulls("value").sort("date"))
    return {"m1b_yoy": m1b, "m2_yoy": m2}


def fetch_rediscount() -> dict[str, pl.DataFrame]:
    rows = list(csv.reader(io.StringIO(decode_bytes(http_get(URL_REDISCOUNT)))))
    recs = []
    for r in rows[1:]:
        if not r or not r[0].strip():
            continue
        d = datetime.strptime(r[0].strip(), "%Y/%m/%d").date()
        recs.append((d, float(r[1])))
    changes = (pl.DataFrame(recs, schema={"date": pl.Date, "value": pl.Float64},
                            orient="row")
               .sort("date")
               .with_columns(pl.col("date").alias("avail_date")))

    # 月頻 forward-fill:每月 1 日的「已生效」重貼現率(月中調整下月才反映 → 不前視)。
    first = changes["date"].min()
    grid = pl.DataFrame(
        {"date": pl.date_range(first.replace(day=1), date.today().replace(day=1),
                               interval="1mo", eager=True)}
    )
    monthly = (grid.join_asof(changes.select("date", "value"), on="date",
                              strategy="backward")
               .drop_nulls("value")
               .with_columns(pl.col("date").alias("avail_date"))
               .sort("date"))
    return {"rediscount_rate": monthly, "rediscount_rate_changes": changes}


def fetch_business_climate() -> dict[str, pl.DataFrame]:
    meta = json.loads(http_get(NDC_DATASET_API))
    zip_url = meta["result"]["distribution"][0]["resourceDownloadUrl"]
    zf = zipfile.ZipFile(io.BytesIO(http_get(zip_url)))

    text = None
    for info in zf.infolist():
        if info.file_size < 5000:  # 跳過 manifest / schema 小檔
            continue
        candidate = decode_bytes(zf.read(info))
        if "景氣對策信號綜合分數" in candidate.splitlines()[0]:
            text = candidate
            break
    if text is None:
        raise RuntimeError("NDC zip 內找不到含『景氣對策信號綜合分數』的 CSV")

    rows = list(csv.reader(io.StringIO(text)))
    header = rows[0]
    i_score = header.index("景氣對策信號綜合分數")
    i_light = header.index("景氣對策信號")
    i_lead = header.index("領先指標綜合指數")
    i_coin = header.index("同時指標綜合指數")
    i_lag = header.index("落後指標綜合指數")

    recs = []
    for r in rows[1:]:
        if not r or not r[0].strip():
            continue
        score = _num(r[i_score])
        if score is None:  # 早年(信號 1984 才起編)綜合分數為 '-'
            continue
        recs.append((parse_yyyymm(r[0]), int(score), r[i_light].strip(),
                     _num(r[i_lead]), _num(r[i_coin]), _num(r[i_lag])))
    df = (pl.DataFrame(
            recs,
            schema={"date": pl.Date, "value": pl.Int64, "signal_light": pl.Utf8,
                    "leading_index": pl.Float64, "coincident_index": pl.Float64,
                    "lagging_index": pl.Float64},
            orient="row")
          .with_columns(_month_end_next("date"))
          .sort("date"))
    return {"business_climate_signal": df}


def fetch_usdtwd() -> dict[str, pl.DataFrame]:
    data = json.loads(http_get(URL_USDTWD))
    recs = []
    for row in data:
        v = _num(row["NTD_USD"])
        if v is None:
            continue
        recs.append((datetime.strptime(row["日期"].strip(), "%Y%m%d").date(), v))
    # avail_date = 當日(收盤 16:00–17:00 公布,對收盤/次日執行不前視)
    df = (pl.DataFrame(recs, schema={"date": pl.Date, "value": pl.Float64},
                       orient="row")
          .unique("date", keep="last").sort("date")
          .with_columns(pl.col("date").alias("avail_date")))
    return {"usdtwd": df}


SOURCES = [
    ("M1B/M2 年增率", fetch_money_supply),
    ("重貼現率", fetch_rediscount),
    ("景氣對策信號", fetch_business_climate),
    ("USD/TWD 匯率", fetch_usdtwd),
]


# ── validation ───────────────────────────────────────────────────────────
def _spot_checks(out: dict[str, pl.DataFrame]) -> list[tuple[str, bool, str]]:
    """與固定歷史錨(不被事後修正)或已知公開值抽查;軟性回報,不中斷落檔。"""
    checks: list[tuple[str, bool, str]] = []

    def at(df: pl.DataFrame, d: date):
        s = df.filter(pl.col("date") == d)["value"]
        return s.item() if s.len() else None

    if "rediscount_rate" in out:  # 政策利率不被修正
        # 直接驗 PIT forward-fill 不前視:2009-02-19 降息(1.5→1.25),
        # 故月初值 2009-02 仍是 1.5(還沒發生),2009-03 才反映 1.25。
        seq = [at(out["rediscount_rate"], d) for d in
               (date(2009, 1, 1), date(2009, 2, 1), date(2009, 3, 1))]
        checks.append(("重貼現率月初值 2009 1/2/3 == 2.0/1.5/1.25（PIT 不前視:2/19 降息下月才反映）",
                       seq == [2.0, 1.5, 1.25], f"got {seq}"))
        latest = out["rediscount_rate"].sort("date")["value"][-1]
        checks.append(("重貼現率 現值 == 2.0%（2024-03-22 起）", latest == 2.0,
                       f"got {latest}"))
    if "usdtwd" in out:  # 匯率不被修正
        v = at(out["usdtwd"], date(2008, 1, 2))
        checks.append(("USD/TWD 2008-01-02 == 32.443（首日）",
                       v is not None and abs(v - 32.443) < 1e-6, f"got {v}"))
    if "m1b_yoy" in out:  # 央行公開值,微幅修正容忍 ±0.5pp
        v = at(out["m1b_yoy"], date(2026, 5, 1))
        checks.append(("M1B 年增率 2026-05 ≈ 9.56%（CBC 公布）",
                       v is not None and abs(v - 9.56) < 0.5, f"got {v}"))
    if "business_climate_signal" in out:
        row = out["business_climate_signal"].filter(pl.col("date") == date(2026, 5, 1))
        if row.height:
            sc, lt = row["value"].item(), row["signal_light"].item()
            checks.append(("景氣信號 2026-05 == 39 分 / 紅燈",
                           sc == 39 and lt == "紅", f"got {sc} / {lt}"))
    return checks


def _summarize(name: str, df: pl.DataFrame) -> None:
    print(f"\n=== {name}.parquet ===")
    print(f"  rows={df.height}  cols={df.columns}")
    print(f"  date range: {df['date'].min()} → {df['date'].max()}")
    print(f"  avail_date range: {df['avail_date'].min()} → {df['avail_date'].max()}")
    with pl.Config(tbl_rows=3, tbl_cols=20, fmt_str_lengths=40):
        print(df.sort("date").tail(3))


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    written: dict[str, pl.DataFrame] = {}
    status: dict[str, str] = {}

    for label, fn in SOURCES:
        try:
            for name, df in fn().items():
                df.write_parquet(OUT_DIR / f"{name}.parquet")
                written[name] = df
            status[label] = "ok"
        except Exception as e:  # noqa: BLE001 — 單源失敗不拖垮其餘
            status[label] = f"FAILED: {e}"
            print(f"[FAIL] {label}: {e}")

    for name, df in written.items():
        _summarize(name, df)

    print("\n=== spot checks (與固定歷史錨/公開已知值) ===")
    for desc, ok, detail in _spot_checks(written):
        print(f"  [{'PASS' if ok else 'CHECK'}] {desc} — {detail}")

    print("\n=== source status ===")
    for label, st in status.items():
        print(f"  {label}: {st}")
    print("\n=== 未整合(誠實記錄) ===")
    for k, why in NOT_INTEGRATED.items():
        print(f"  {k}: {why}")


if __name__ == "__main__":
    main()
