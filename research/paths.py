"""檔案位置的唯一真源。**任何新程式一律從這裡取路徑,禁止再寫字面值。**

## 三種生命週期,三個根,規則各不相同

    RAW  `data/`   **原始封存**——爬回來的 CSV 與 1 分 K。不可重生(重爬要數週,
                   部分歷史端點已不再供應舊資料),不進 git(51 GB),但比程式碼
                   還珍貴:它是整套系統的事實地基。
    VAR  `var/`    **可重生產物**——cache、回測輸出、報告、log、runtime state。
                   整包 gitignore(一行 `/var/`),刪掉只是重算,不會失去知識。
    SRC  repo      **源碼與策展紀錄**——進 git。

把這三者混在一起是本專案長年的結構病:程式碼旁邊躺著自己的輸出,於是
`.gitignore` 得靠人工維護一長串規則,規則一旦跟不上目錄演進,952 個檔就會卡在
`git status` 裡讓人視而不見——而視而不見正是每日重生的 5 MB HTML 一直被塞進
版控的原因(2026-07-22 事故)。

## 為什麼要有這支模組(而不是「搬一搬就好」)

同一次稽核量到:`cache.duckdb` 硬編在 **75 個檔**、`strat_lab/results` 在 **104 個檔**。
路徑寫成字面值時,「搬一次目錄」等於「改 180 個地方」,於是沒人敢搬,於是結構
永遠爛在原地。**位置收斂成常數之後,搬家就是改這一支檔案。**

## 一律以 repo 根為錨

相對路徑會隨行程的工作目錄漂移。那在本專案出過災難級事故:2026-07-22,
`advisors` 用相對路徑讀成交紀錄,由別的 cwd 啟動時靜默讀到空 → 全部持股被判為
外人 → 差點整批賣光。**任何路徑都不得依賴 cwd。**
"""
from __future__ import annotations

from pathlib import Path

#: repo 根(本檔位於 <repo>/research/paths.py)
REPO = Path(__file__).resolve().parents[1]

# ── RAW:原始封存(不可重生)────────────────────────────────────────────
RAW = REPO / "data"
#: 台股 1 分 K 歷史(永豐 Shioaji;每日 2 GB 上限,全補約 30 天)
RAW_INTRADAY = RAW / "intraday" / "kbars_1m"

# ── VAR:可重生產物(整包 gitignore)──────────────────────────────────
VAR = REPO / "var"

CACHE_DIR = VAR / "cache"
#: 全表 DuckDB 快照(唯一結構化真源;由 research/crawl/rebuild.py 從 raw 封存重建、
#: research.crawl.update 每日增量更新。PostgreSQL 已退役 2026-07-23)
CACHE_DB = CACHE_DIR / "cache.duckdb"
#: S 策略用的瘦身快照(雲端 VM 用;內容為全表快照的子集)
CACHE_SLIM_DB = CACHE_DIR / "cache_s_slim.duckdb"
#: 財務品質因子面板(Piotroski F-Score / cfo_ni;由 crawl.rebuild_financials 從 is/bs/cf
#: 重生,可重生產物 → 放 var/,不進源碼樹)。db.connect() 自動註冊為 raw_quarterly view。
RAW_QUARTERLY = CACHE_DIR / "raw_quarterly.parquet"

#: 回測與研究的輸出根
OUT = VAR / "out"
OUT_STRAT_LAB = OUT / "strat_lab"
OUT_EXPERIMENTS = OUT / "experiments"
OUT_TRADING = OUT / "trading"
#: 執行器逐腿成交紀錄(TCA jsonl)。**永久保存**:富邦此帳戶查不到歷史成交,
#: 跨日成本只能靠成交當天自己記帳,這批檔案是唯一的成本真相。
OUT_EXECUTIONS = OUT_TRADING / "executions"

#: 給人看的報告(HTML/Markdown;每日重生)
REPORTS = VAR / "reports"

#: 執行期 log(券商 SDK、爬蟲)
LOG = VAR / "log"

#: runtime state。**只存事實**(部位首見日、成本、已掛的條件單 guid),
#: 不存判斷——判斷一律每次由市場資料現算(見 tri/advisors.py 的進場錨)。
STATE = VAR / "state"
#: 三策略的持股帳本(first_seen / cost)
STATE_POSITIONS = STATE / "positions"
#: S live 營運狀態(每日計劃、券商端安全網帳本)
STATE_LIVE = STATE / "live"
STATE_PLANS = STATE_LIVE / "plans"
#: 交易急停旗標(touch 此檔即中止當日一切下單)
HALT_FLAG = STATE / "HALT"

# ── SRC:進 git 的策展紀錄 ───────────────────────────────────────────
#: **不可重生的觀測紀錄**——「我們哪一天第一次看到這筆營收揭露」無法從資料庫
#: 回推,它是觀測不是計算,而且正是 S 事件驅動進場的時間真相。故進版控。
RECORDS = REPO / "research" / "records"
REVENUE_FIRST_SEEN = RECORDS / "revenue_first_seen.parquet"
CONFCALL_EVENTS = RECORDS / "confcall_events.parquet"
NEWS_ALIASES = RECORDS / "tw_stock_news_aliases.csv"


def ensure_dirs() -> None:
    """建立所有可重生根目錄(idempotent)。新機器/新 VM 開機時呼叫一次即可。"""
    for d in (CACHE_DIR, OUT, OUT_STRAT_LAB, OUT_EXPERIMENTS, OUT_EXECUTIONS,
              REPORTS, LOG, STATE, STATE_POSITIONS, STATE_PLANS):
        d.mkdir(parents=True, exist_ok=True)
