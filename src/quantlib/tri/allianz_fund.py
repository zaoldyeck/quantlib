"""安聯台灣科技基金(鉅亨網代碼 A36004)逐日淨值 — pnl_dashboard 額外基準線.

2026-07-19 資料源查證(依 CLAUDE.md §2.3 事實來源鐵律,實測而非憑記憶):
- SITCA 官方 open data(https://www.sitca.org.tw/MemberK0000/F/03/nav.csv)
  雖是權威來源,但實測只保留最近 2 個交易日快照,無歷史回溯能力,故不可用。
- 改用鉅亨網基金 API(公開、免登入、免 key,經瀏覽器 devtools 網路請求實測抓出):
      https://fund.api.cnyes.com/fund/api/v1/funds/A36004/nav?by=d&startAt=1
  `by=d` 為日頻(`by=w` 為週頻,圖表在長區間會自動降頻,故顯式帶 d 取全日頻)、
  `startAt=1` 取全部歷史。回應 `items.nav` / `items.tradeDate` 為平行陣列
  (`items.data` 是恆空的裝飾欄位,勿誤用)。實測回溯至 2011-10-31,涵蓋
  pnl_dashboard 同窗起點 2022-07-11 無虞。
- 基金代碼互相對照(同一檔基金):鉅亨網 A36004 = ISIN TW000T3604Y3 =
  SITCA 基金統編 18480065 / 受益憑證代號 T3604Y。
- `tradeDate` 為 UTC epoch 秒,對應台北時間當日 00:00(即 UTC+8),換算日期
  需先 +8 小時再取 date,否則會早一天。
- 基金為累積型:API 回傳 `dividendDistributionFrequency`/`distributionStatus`
  皆為空字串,即不配息。淨值本身已是總報酬,不需另做股息再投資調整
  (對照 0050/2330 走 `prices.total_return_series` 手動含息調整的必要性,
  這支基金不需要,因為配息從未發生)。

Run: uv run --project . python -m quantlib.tri.allianz_fund
依賴 cache: 否(獨立外部 API,與 var/cache/cache.duckdb 無關)。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import requests

FUND_CODE = "A36004"
FUND_NAME = "安聯台灣科技基金"
API_URL = f"https://fund.api.cnyes.com/fund/api/v1/funds/{FUND_CODE}/nav"
REQUEST_HEADERS = {"Origin": "https://fund.cnyes.com", "Referer": "https://fund.cnyes.com/"}
CACHE_CSV = Path(__file__).resolve().parent / "data" / "allianz_tech_fund_nav.csv"


def _fetch_from_api() -> pd.DataFrame:
    resp = requests.get(API_URL, params={"by": "d", "startAt": 1},
                        headers=REQUEST_HEADERS, timeout=30)
    resp.raise_for_status()
    items = resp.json()["items"]
    ts = pd.to_datetime(items["tradeDate"], unit="s", utc=True) + pd.Timedelta(hours=8)
    df = pd.DataFrame({"date": ts.date, "nav": items["nav"]})
    return df.drop_duplicates("date").sort_values("date").reset_index(drop=True)


def refresh_cache() -> pd.DataFrame:
    """全量重抓並覆蓋本地快取(資料量小 <200KB,增量合併非必要)。"""
    df = _fetch_from_api()
    CACHE_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CACHE_CSV, index=False)
    return df


def load_nav(end: date) -> pd.Series:
    """回傳 date-indexed 淨值序列。本地快取缺 end 當日資料時自動重抓。"""
    if CACHE_CSV.exists():
        df = pd.read_csv(CACHE_CSV, parse_dates=["date"])
        if df.empty or df["date"].max().date() < end:
            df = refresh_cache()
            df["date"] = pd.to_datetime(df["date"])
    else:
        df = refresh_cache()
        df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")["nav"]


def main() -> None:
    df = refresh_cache()
    print(f"{FUND_NAME}({FUND_CODE}): {len(df)} 筆 {df['date'].min()} ~ {df['date'].max()} -> {CACHE_CSV}")


if __name__ == "__main__":
    main()
