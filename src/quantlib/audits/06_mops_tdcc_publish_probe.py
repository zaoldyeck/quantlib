"""MOPS / TDCC 公布時刻探針 — 用第一手回應判斷「當日資料是否已產製」。

用途
----
爬蟲每天只跑一次完整更新,必須排在「全表齊備」之後。MOPS 官方**沒有**
任何頁面明文寫出各表的公布時刻(2026-07-15 查證:MOPS 首頁、t56sb12、
t21sc03、TWSE OpenAPI swagger、data.gov.tw metadata 皆無 更新時間 欄位;
data.gov.tw 的「更新頻率」欄甚至是錯的 —— 週頻的 TDCC 標「每1月」、
月頻的營收標「每1年」)。因此正確做法不是硬編時刻,而是**讀官方回應的
狀態哨兵**。

本探針把「有沒有產製」變成可觀測、零猜測的判斷:

MOPS t56sb12(內部人持股轉讓事前申報日報)有三種**互斥**終局狀態:
  1. NOT_READY   「當日申報資料，尚未產製，請稍後再查詢!」  (~2568 bytes)
  2. READY_EMPTY 「無持股轉讓之情形」                        (~2537 bytes)
  3. READY_DATA   真實資料表                                 (~10-40 KB)

⚠️ 1 與 2 只差 ~31 bytes —— **任何 size threshold 都無法區分**。
Crawler.getInsiderHolding 目前用 `outFile.length() < 2000` 當哨兵判準,
兩者皆 > 2000 → 兩個分支都不觸發 → NOT_READY 頁會被當成正常檔案存下,
getDatesOfExistFiles 之後永遠跳過該日 → 該日內部人資料靜靜變成 0 筆。
(2026-07-15 掃描 150 個既存檔:0 個中毒 —— 因歷史上都是 D+1 才抓;
「當日收盤後即可抓」政策一上路這個 bug 就會引爆。)
→ 唯一正解:比對**內文**而非檔案大小。

TDCC(集保股權分散表)endpoint 無日期參數,永遠回傳「最新」快照,
資料日期 = 每週最後一個營業日(遇假日不必然是週五;2026-07-09 即為週四,
因 07-10 休市)。**必須從 CSV 內文讀 資料日期,不可用下載日當資料日。**

執行
----
    uv run --project . python -m quantlib.audits.06_mops_tdcc_publish_probe
    # 持續觀測直到今日 t56sb12 產製(用來實測公布時刻):
    uv run --project . python -m quantlib.audits.06_mops_tdcc_publish_probe --watch

cache 依賴:無(純線上探針,不讀 DuckDB cache)。
"""

from __future__ import annotations

import argparse
import datetime as dt
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from zoneinfo import ZoneInfo

TAIPEI = ZoneInfo("Asia/Taipei")

MOPS_T56_DATA_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t56sb12"
TDCC_URL = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
TWSE_OPENAPI_INSIDER = "https://openapi.twse.com.tw/v1/opendata/t187ap12_L"

_UA = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Content-Type": "application/x-www-form-urlencoded",
}

# 官方回應哨兵字串 —— 這是判準的唯一事實來源,勿改用 size threshold。
SENTINEL_NOT_READY = "尚未產製"
SENTINEL_NO_TRANSFER = "無持股轉讓之情形"

NOT_READY = "NOT_READY"
READY_EMPTY = "READY_EMPTY"
READY_DATA = "READY_DATA"

# MOPS 對連續請求會回 502 Bad Gateway(實測 2026-07-15:twse→tpex 無間隔即 502)。
# Scala Crawler.getInsiderHolding 同樣以 20s sleep 間隔各 market,此處對齊。
MOPS_RATE_LIMIT_SEC = 20


@dataclass(frozen=True)
class ProbeResult:
    state: str
    n_bytes: int

    @property
    def ready(self) -> bool:
        return self.state != NOT_READY


def probe_t56sb12(date: dt.date, market: str = "twse", timeout: int = 30) -> ProbeResult:
    """回傳指定日的 t56sb12 產製狀態。market: twse -> report=SY / tpex -> OY。"""
    report = "SY" if market == "twse" else "OY"
    body = urllib.parse.urlencode(
        {
            "encodeURIComponent": "1",
            "run": "",
            "step": "2",
            "year": str(date.year - 1911),  # 民國年
            "month": f"{date.month:02d}",
            "day": f"{date.day:02d}",
            "report": report,
            "firstin": "true",
        }
    ).encode()
    req = urllib.request.Request(MOPS_T56_DATA_URL, data=body, headers=_UA)
    raw = urllib.request.urlopen(req, timeout=timeout).read()
    html = raw.decode("utf-8", "ignore")

    if SENTINEL_NOT_READY in html:
        state = NOT_READY
    elif SENTINEL_NO_TRANSFER in html:
        state = READY_EMPTY
    else:
        state = READY_DATA
    return ProbeResult(state=state, n_bytes=len(raw))


def probe_twse_openapi_insider(timeout: int = 30) -> str | None:
    """TWSE OpenAPI 的同源資料,`出表日期`(民國 yyymmdd)= 已產製的最新日。

    比 MOPS 2-step ajax 乾淨:單一 GET、JSON、無 rate-limit sleep。
    可作為 t56sb12 是否已產製的交叉驗證(或直接取代 scrape)。
    """
    import json

    req = urllib.request.Request(TWSE_OPENAPI_INSIDER, headers={"User-Agent": _UA["User-Agent"]})
    rows = json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    if not rows:
        return None
    return max(r["出表日期"] for r in rows)


def probe_tdcc_data_date(timeout: int = 60) -> dt.date:
    """TDCC endpoint 無日期參數 —— 資料日期只能從 CSV 內文第一列讀。"""
    req = urllib.request.Request(TDCC_URL, headers={"User-Agent": _UA["User-Agent"]})
    raw = urllib.request.urlopen(req, timeout=timeout).read()
    text = raw.decode("utf-8-sig", "ignore")
    first_data_line = text.split("\n", 2)[1]
    return dt.datetime.strptime(first_data_line.split(",")[0], "%Y%m%d").date()


def _snapshot() -> None:
    now = dt.datetime.now(TAIPEI)
    today = now.date()
    print(f"[{now:%Y-%m-%d %H:%M:%S} Taipei]")

    for i, market in enumerate(("twse", "tpex")):
        if i:
            time.sleep(MOPS_RATE_LIMIT_SEC)
        r = probe_t56sb12(today, market)
        print(f"  t56sb12 {market:5s} {today}: {r.state:11s} ({r.n_bytes} bytes)")

    roc = probe_twse_openapi_insider()
    print(f"  TWSE OpenAPI t187ap12_L 最新出表日期: {roc}  (民國 yyymmdd)")

    d = probe_tdcc_data_date()
    print(f"  TDCC 資料日期(內文): {d} ({d:%A}) — 該週最後一個營業日")


def _watch(interval: int = 120) -> None:
    """持續觀測直到今日 t56sb12 產製 —— 用來**實測**公布時刻(官方無明文)。"""
    today = dt.datetime.now(TAIPEI).date()
    prev: str | None = None
    while True:
        ts = dt.datetime.now(TAIPEI).strftime("%H:%M:%S")
        try:
            r = probe_t56sb12(today, "twse")
        except Exception as exc:  # 單次網路失敗不該中斷長時觀測
            print(f"{ts} ERR {exc}")
            time.sleep(interval)
            continue

        if r.state != prev:
            print(f"{ts} STATE -> {r.state} ({r.n_bytes} bytes)", flush=True)
            prev = r.state
            if r.ready:
                print(f"{ts} 今日 t56sb12 已產製 → 公布時刻 <= {ts}")
                return
        time.sleep(interval)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--watch", action="store_true", help="持續觀測直到今日產製,實測公布時刻")
    ap.add_argument("--interval", type=int, default=120, help="--watch 輪詢間隔秒數")
    args = ap.parse_args()
    _watch(args.interval) if args.watch else _snapshot()


if __name__ == "__main__":
    main()
