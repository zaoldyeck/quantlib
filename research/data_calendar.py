"""交易日曆與「資料齊備」判定——Python 側的單一事實來源。

政策(依據 `docs/data_ops/twse_publish_times.md` 的第一手蒐證):

> **D 日的資料自 D+1 00:30 起視為齊備;一天只跑一次完整更新。**

為什麼不是「D 日收盤後」:
- `MI_MARGN` 融資融券的**官方保證只到「次一營業日開市前公告」**(操作辦法 §69),
  D 日晚間抓得到是實務、不是承諾。
- `TWT93U`/TPEx `sbl` 借券官方明文「每日晚間二次更新(約 20:30、22:30)」且時間會隨
  日結作業浮動——20:30~22:30 之間抓到的是**部分更新**,檔案看起來完整卻會被改寫(無聲汙染)。
- 實證:margin/sbl/foreign/insider 四表全史**零次**同日成功;唯一「全表齊備」的紀錄是
  D+1 凌晨(2026-05-21 00:33 抓齊 5/20 全部 8 表)。

結果:D 日盤中/傍晚**不應期待 D 的資料**——期望日永遠是「最近一個已齊備的交易日」。
這根除了表間日期錯位(2026-07-15 事故:報價 7/14、法人 7/13 → 策略閘門靜靜零候選)。
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_DB = paths.CACHE_DB
QUOTE_DIR = REPO_ROOT / "data" / "daily_quote" / "twse"

#: D 的資料自 D+1 的此時刻起視為齊備(見模組 docstring 的依據)
DATA_COMPLETE_AFTER = time(0, 30)

#: research.crawl.update 每日會抓的日頻表(cache 表名)。齊備 = 這些表都有該交易日的資料。
DAILY_TABLES = (
    "daily_quote",
    "market_index",
    "daily_trading_details",
    "stock_per_pbr",
    "margin_transactions",
    "sbl_borrowing",
    "foreign_holding_ratio",
)


def is_trading_day(d: date) -> bool:
    """該日有沒有開市。

    週末直接排除;平日則看爬蟲的 0-byte sentinel——那是交易所親口回的「無資料」,
    也是我們唯一的休市日曆(國定假日、**颱風假**都靠它;平日 ≠ 交易日,
    2026-07-10 颱風休市即是教訓)。沒有檔案 = 還沒抓過 → 當作交易日(樂觀),
    抓完若真的無資料就會留下 sentinel,下次自動修正。
    """
    if d.weekday() >= 5:
        return False
    f = QUOTE_DIR / str(d.year) / f"{d.year}_{d.month}_{d.day}.csv"
    if f.exists() and f.stat().st_size == 0:
        return False
    return True


def latest_complete_trading_day(now: datetime | None = None) -> date:
    """現在能保證「全表齊備」的最近交易日。

    D 的資料自 D+1 00:30 起齊備 → 今天過了 00:30 就能要求昨天(含)以前的交易日。
    """
    now = now or datetime.now()
    d = now.date() - timedelta(days=1)
    if now.time() < DATA_COMPLETE_AFTER:
        d -= timedelta(days=1)
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d


def cache_max_date(table: str) -> date | None:
    import duckdb

    con = duckdb.connect(str(CACHE_DB), read_only=True)
    try:
        return con.execute(f"SELECT max(date) FROM {table}").fetchone()[0]
    except Exception:  # noqa: BLE001 - 表不存在 = 視為缺
        return None
    finally:
        con.close()


def stale_tables(as_of: date | None = None) -> dict[str, date | None]:
    """回傳「未涵蓋齊備日」的表 → 其現有最新日。空 dict = 資料完整。"""
    want = as_of or latest_complete_trading_day()
    out: dict[str, date | None] = {}
    for t in DAILY_TABLES:
        got = cache_max_date(t)
        if got is None or got < want:
            out[t] = got
    return out
