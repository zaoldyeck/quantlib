"""台股 1 分 K 歷史回補(永豐 Shioaji;由近而遠 + 額度自適應 + 隨時可中斷續傳)。

**工作順序:由近而遠,逐月往回抓**(使用者定調 2026-07-22)

先前用「階段」把時間切段,結果階段順序本身變成一個要維護、而且很容易排錯的參數
——舊排法就把四年研究窗裡最新的 8 個月,排在 33 個月史前資料的**後面**。
改成一律從最新的月份往回抓之後,**分階段這件事本身就沒必要了**:

  任何時刻停下來,手上都是一段「從今天往回連續」的資料。
  要幾年的研究窗,就是等到抓到那個月份為止——不必事先宣告,也不會排錯。

月內順序照流動性(成交值高者先);指數與 ETF 併入同一序列,不另外分段。

**額度紀律(使用者質疑後修正)**:不自設保守停損線。以 API 自己的
`usage().remaining_bytes` 為準,剩餘不足一個工作單位(RESERVE)或 API 回報額度
錯誤才停;額度於交易日 08:00 重置,launchd 每日 08:30 自動續跑。

**兩道官方限制,綁住的是不同東西(2026-07-21 實測 + 官方文件)**
  流量:每日 2 GB —— **這才是總工期的瓶頸**。實測 6,225 格用盡當日額度
        (每格約 321 KB);全市場 2,395 檔 × 77 個月 ≈ 184,415 格 → 約 30 天補完。
  頻率:行情類 50 次/5 秒 —— 綁的是「每天那一輪跑多久」。序列實測 1.76 格/秒
        (單次 kbars 往返約 546 ms),一輪約 1 小時;開平行後由限流器頂到
        8 次/秒(官方上限的 80%,見 ratelimit.py),一輪約十幾分鐘。
  ⚠ 超頻的罰則是停用 1 分鐘、累犯封 IP 與帳號,故一律留 20% 安全邊際。

**平行安全性不靠猜**:官方沒說 client 是否執行緒安全、SDK 核心又是編譯過的 .so,
所以開平行前程式會**自己證明**一次(同批 chunk 序列與平行結果逐位比對),
通過才用平行;結論以 shioaji 版本為 key 快取,升版自動重驗。

**完整度 = 逐日核對,不是猜的(刻意無狀態檔)**
「這一檔這個月該有哪幾天」的權威來源是 **`daily_quote`**——它早就逐日記著每檔
在哪天有交易。缺口 = 應有集合 − 磁碟上實有的日子,是**減法**。
所以:額度中途用盡留下的洞找得到也補得回來(`--gaps` 可離線盤點);
而且**絕不會把「抓不到」記成「沒有資料」**——舊碼在 API 因超額回 null 時會寫下
0-byte 哨兵,把暫時性故障永久固化成假事實。哨兵機制已整個廢除。
寫入一律 tmp → os.replace(同分割區原子換名),故 kill -9 / 斷網 / 當機最多
重做「當下那一格」,絕不留半檔汙染。
**只補缺的那幾天**:當月每天多一個交易日,若每次重抓「月初→今天」整段,月中平均
要重載 11 天份 = 每天燒掉當日額度的 19%、一個月累積 8.1 GB(整整 4 天的額度)。

**資料聲明(回測必讀)**:(1) Shioaji 合約表僅含現存上市股 → 本資料集含存活者
偏差;(2) 價格為原始價(未還原權息),研究時以 daily adj_factor 對齊;
(3) 官方歷史下限:股票/指數 2020-03-02、期貨 2020-03-22。

用法:
  uv run --project research python -m research.intraday.pull_kbars             # 續傳回補
  uv run --project research python -m research.intraday.pull_kbars --status    # 進度(不連線)
  uv run --project research python -m research.intraday.pull_kbars --gaps      # 缺哪幾天(不連線)
  uv run --project research python -m research.intraday.pull_kbars --workers 1 # 強制序列
  uv run --project research python -m research.intraday.pull_kbars --since 2022-07-01  # 只補近四年
  uv run --project research python -m research.intraday.pull_kbars --selftest  # 登入+單檔驗證
金鑰:research/.env 的 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY(資料查詢免 CA 憑證)。
依賴 cache:是(流動性排序,缺 cache 則退回代碼序)。資料不進 git。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date as Date
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from research.intraday.ratelimit import RateLimiter
from research import paths

REPO = Path(__file__).resolve().parents[2]
OUT = paths.RAW_INTRADAY
HIST_FLOOR = Date(2020, 3, 2)      # 官方股票/指數歷史下限
RESERVE_BYTES = 2 * 1024 ** 2      # 一個工作單位的量級;剩餘低於此註定失敗,提早停
MAX_RETRY = 3
#: 預設平行度。真正的瓶頸是**每日 2 GB 流量**(實測 2026-07-21:6,225 格用盡當日
#: 額度 → 每格約 321 KB),平行不會讓總工期變短,只讓「每天那一輪」快一些。
#:
#: **實測(2026-07-22,workers=6)**:3,600 格 / 1,523 秒 = **2.36 格/秒**,
#: 相對序列的 1.76 格/秒只有 **1.34×**。限流器設在 8 次/秒卻只跑到 2.36——
#: 代表**瓶頸不在頻率限制,而在 client 內部**(核心是編譯過的 .so,請求疑似
#: 被序列化)。再加 worker 收益有限,故維持 6:再高只是多開執行緒等同一把鎖。
WORKERS_MEASURED = "2026-07-22: 2.36 格/秒 @ workers=6(序列 1.76;加速 1.34×)"
WORKERS = 6
#: 額度查詢每 N 格一次(實測 usage() 往返僅 20 ms,但沒必要每格都問;
#: 真的超額時 API 會回空/報錯,下方錯誤路徑會接住)
USAGE_EVERY = 25
#: 平行自證的結果(以 shioaji 版本為 key;升版即重驗)
PARITY_FILE = paths.RAW / "intraday" / "parallel_parity.json"

#: 工作順序:**由近而遠,逐月往回抓**(使用者定調 2026-07-22)。
#:
#: 先前用「階段」把時間切段,結果是階段順序本身變成一個要維護、而且很容易排錯的
#: 參數——舊排法就把四年窗裡最新的 8 個月排在 33 個月史前資料的後面。
#: 改成「一律從最新的月份往回抓」之後,**分階段這件事本身就沒必要了**:
#:
#:   任何時刻停下來,手上都是一段「從今天往回連續」的資料。
#:   要幾年的研究窗,就是等到抓到那個月份為止 —— 不必事先宣告,也不會排錯。
#:
#: 月內順序仍照流動性(成交值高者先),因為研究幾乎都從流動性夠的名字開始。
#: 指數與 ETF 併入同一序列,不另外分段:量小,而且 regime 研究要跟個股同窗。


class QuotaExhausted(RuntimeError):
    """API 額度用盡(由 usage 或錯誤訊息判定)。

    **帶著已完成的計數**:額度用盡是正常收尾路徑(每天都會走到),若讓計數
    留在被中斷的迴圈裡,收尾訊息會永遠印「本輪 0 格」——實際抓了 3,600 格卻
    回報 0,是不誠實的回報。
    """

    def __init__(self, msg: str, done: int = 0, empty: int = 0):
        super().__init__(msg)
        self.done, self.empty = done, empty


# ── 環境 / 連線 ──────────────────────────────────────────────────────────
def _env() -> tuple[str, str]:
    envp = REPO / "research" / ".env"
    if envp.exists():
        for line in envp.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    key, sec = os.environ.get("SHIOAJI_API_KEY"), os.environ.get("SHIOAJI_SECRET_KEY")
    if not key or not sec:
        sys.exit("✗ 缺 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY(research/.env)")
    return key, sec


#: SDK 在登入握手時把連線細節印到 stdout,對使用者是純噪音(不是錯誤、不需處理)。
#: 在**程式內**吞掉,而不是要使用者在外面接 grep——輸出乾不乾淨是程式的責任。
#: 非預期的行照樣印出來,不做無差別靜音。
_SDK_NOISE = ("Response Code", "Event Code", "Session up", "Reconnect", "api = ")


def _login():
    import contextlib
    import io
    import warnings

    import shioaji as sj
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="shioaji")
    api = sj.Shioaji()
    key, sec = _env()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        api.login(api_key=key, secret_key=sec, subscribe_trade=False)
    for ln in buf.getvalue().splitlines():
        if ln.strip() and not any(k in ln for k in _SDK_NOISE):
            print(ln)
    for _ in range(60):                      # 合約表非同步下載,等就緒才動手
        try:
            if len(list(api.Contracts.Stocks.TSE)) > 0:
                break
        except Exception:
            pass
        time.sleep(1.0)
    else:
        sys.exit("✗ 合約表 60 秒未就緒")
    return api


def _remaining(api) -> int | None:
    """API 自報剩餘額度(bytes);查詢失敗回 None = 不阻擋(讓真錯誤來說話)。"""
    try:
        u = api.usage()
        return int(getattr(u, "remaining_bytes", 0))
    except Exception:
        return None


def _is_quota_error(exc: Exception) -> bool:
    s = f"{type(exc).__name__} {exc}".lower()
    return any(k in s for k in ("quota", "usage limit", "exceed", "limit_bytes",
                                "流量", "額度", "超過上限"))


# ── 宇宙 ────────────────────────────────────────────────────────────────
def _universe(api, kind: str) -> list[tuple[str, object]]:
    """回 [(code, contract)];stock = 4 碼非 0 開頭(含 91xx KY),index_etf = 指數 + 00 開頭 ETF。"""
    out: list[tuple[str, object]] = []
    if kind == "stock":
        for mkt in ("TSE", "OTC"):
            for c in getattr(api.Contracts.Stocks, mkt):
                if len(c.code) == 4 and c.code.isdigit() and not c.code.startswith("0"):
                    out.append((c.code, c))
    else:
        for mkt in ("TSE", "OTC"):
            for c in getattr(api.Contracts.Stocks, mkt):
                if c.code.startswith("00") and c.code[:4].isdigit():
                    out.append((c.code, c))
        try:                                   # 大盤指數(regime 研究用)
            for ex in api.Contracts.Indexs:
                for c in ex:
                    out.append((c.code, c))
        except Exception as exc:               # 指數合約結構異動不擋 ETF
            print(f"  ! 指數合約略過:{type(exc).__name__} {exc}", file=sys.stderr)
    return sorted({c: (c, k) for c, k in out}.values())


def _adv_rank() -> dict[str, int]:
    """近 90 日均成交值排名(cache);缺 cache 或缺該檔 → 殿後。"""
    try:
        import duckdb
        con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
        try:
            adv = con.execute(
                "SELECT company_code, avg(trade_value) AS adv FROM daily_quote "
                "WHERE date >= current_date - INTERVAL 90 DAY GROUP BY company_code"
            ).pl()
        finally:
            con.close()
        return dict(adv.sort("adv", descending=True)
                    .with_row_index("i").select(["company_code", "i"]).iter_rows())
    except Exception as exc:
        print(f"  ! 流動性排序不可用({type(exc).__name__}),改用代碼序", file=sys.stderr)
        return {}


# ── 工作單位:(code, 月) ──────────────────────────────────────────────────
def _months(start: Date, end: Date) -> list[tuple[str, Date, Date]]:
    """回 [(YYYY-MM, 該月起, 該月訖)];已裁切到 [start, end] 與歷史下限、今日。"""
    lo, hi = max(start, HIST_FLOOR), min(end, Date.today())
    out, cur = [], Date(lo.year, lo.month, 1)
    while cur <= hi:
        nxt = Date(cur.year + cur.month // 12, cur.month % 12 + 1, 1)
        s, e = max(cur, lo), min(nxt - timedelta(days=1), hi)
        if s <= e:
            out.append((f"{cur.year:04d}-{cur.month:02d}", s, e))
        cur = nxt
    return out


def _expected(con, s: Date, e: Date) -> tuple[dict[str, set], set]:
    """該區間**應該要有**的交易日 → ({code: {日期}}, 全市場交易日集合)。

    **權威來源是 `daily_quote`**:它已經逐日記錄了每一檔在哪幾天有交易。
    先前用「檔案存在」或 mtime 猜完整度,兩者都只是代理指標,而且各出一種無聲錯誤:
      ① 額度耗盡時 API 回 null(官方文件明載),舊碼會據此寫下 `.empty` 哨兵
         → **「抓不到」被永久記成「這個月沒有資料」**,資料集被汙染且無人知曉;
      ② 月中抓的檔在日曆翻頁後被判為完成,中間缺的幾天永遠補不回來。
    改成逐日核對後,「缺哪幾天」是**算得出來的事實**,不是猜的——也才補得回來。

    指數不在 daily_quote(它在 market_index),故以「全市場交易日」為其應有集合。
    """
    rows = con.execute(
        "SELECT company_code, date FROM daily_quote WHERE date BETWEEN ? AND ?",
        [s, e]).fetchall()
    per: dict[str, set] = {}
    alld: set = set()
    for code, d in rows:
        per.setdefault(str(code), set()).add(d)
        alld.add(d)
    return per, alld


def _have(tag: str, code: str) -> set:
    """檔案裡實際涵蓋了哪些日子(只讀 dt 這一欄)。"""
    f = OUT / tag / f"{code}.parquet"
    if not f.exists():
        return set()
    try:
        return set(pl.scan_parquet(f).select(pl.col("dt").dt.date().unique())
                   .collect().to_series().to_list())
    except Exception:                      # noqa: BLE001 - 壞檔當作沒有,重抓
        return set()


def _ranges(days: list) -> list[tuple[Date, Date]]:
    """把缺的日子併成連續區間,減少呼叫數(一次呼叫本來就能帶回一整段)。
    間隔 ≤3 天視為連續,以免週末/連假把區間切碎。"""
    out: list[tuple[Date, Date]] = []
    for d in sorted(days):
        if out and (d - out[-1][1]).days <= 3:
            out[-1] = (out[-1][0], d)
        else:
            out.append((d, d))
    return out


def _write_atomic(df: pl.DataFrame | None, tag: str, code: str) -> None:
    """tmp → os.replace(同分割區原子換名)。**空資料一律不落盤**:
    那正是額度耗盡時 API 的回應,寫下去等於把暫時故障固化成永久事實。"""
    if df is None or df.is_empty():
        return
    d = OUT / tag
    d.mkdir(parents=True, exist_ok=True)
    tmp = d / f"{code}.parquet.tmp"
    df.write_parquet(tmp, compression="zstd")
    os.replace(tmp, d / f"{code}.parquet")
    stale = d / f"{code}.empty"            # 舊哨兵已被逐日核對取代
    if stale.exists():
        stale.unlink()


def _month_todo(universe, tag: str, per: dict[str, set], alld: set
                ) -> list[tuple[str, str, object, list]]:
    """該月待補清單 [(月tag, code, contract, 缺的日期)];已齊備者不列入。"""
    todo = []
    for code, contract in universe:
        exp = per.get(code)
        if exp is None:
            exp = alld if not code[:1].isdigit() else None   # 指數 → 全市場交易日
        if not exp:
            continue                       # daily_quote 說該月沒交易 → 本來就不該有
        miss = sorted(exp - _have(tag, code))
        if miss:
            todo.append((tag, code, contract, miss))
    return todo


def _pull(api, contract, code: str, tag: str, days: list) -> int:
    """補齊 `days` 這幾天;分成連續區間逐段抓,與既有資料合併去重後原子換名。

    **絕不因為「回傳空的」就記下任何完成標記**——那正是額度耗盡時 API 的行為。
    抓不到就是抓不到,下次再補;完整度一律由 `daily_quote` 逐日核對決定。
    """
    got = []
    for a, b in _ranges(days):
        df = _to_frame(api.kbars(contract=contract,
                                 start=a.isoformat(), end=b.isoformat()))
        if df is not None and not df.is_empty():
            got.append(df)
    if not got:
        return 0
    new = pl.concat(got, how="vertical_relaxed") if len(got) > 1 else got[0]
    f = OUT / tag / f"{code}.parquet"
    if f.exists():
        new = pl.concat([pl.read_parquet(f), new], how="vertical_relaxed")
    _write_atomic(new.unique(subset=["ts"], keep="last").sort("dt"), tag, code)
    return len(new)


def _to_frame(kb) -> pl.DataFrame | None:
    """Kbars → polars(**純函式**:無 IO、無副作用,故可單測、可在平行 worker 內安全呼叫)。
    無資料回 None(= 該檔該月沒開過盤,與「抓失敗」語義不同)。"""
    ts = list(kb.ts)
    if not ts:
        return None
    return (pl.DataFrame({
        "ts": pl.Series(ts, dtype=pl.Int64),
        "open": list(kb.Open), "high": list(kb.High), "low": list(kb.Low),
        "close": list(kb.Close), "volume": list(kb.Volume), "amount": list(kb.Amount),
    }).with_columns(pl.from_epoch("ts", time_unit="ns").alias("dt"))
      .sort("dt"))


# ── 平行自證 ────────────────────────────────────────────────────────────
# 官方文件沒有說 client 是否執行緒安全,SDK 核心又是編譯過的 .so(無原始碼可查),
# 而 1.3.1 的 release note 才剛修過「race condition in contracts」。沒有證據就
# 不准開平行——所以**讓程式自己證明**:同一批 chunk 序列抓一次、平行抓一次,
# 兩邊資料指紋逐位相同才放行。證明結果以 shioaji 版本為 key 快取(升版即重驗)。
# 成本:4 格 × 2 輪 ≈ 1.3 MB,佔每日 2 GB 的 0.07%,而且一個版本只付一次。


def _fingerprint(df: pl.DataFrame | None) -> str:
    if df is None or df.is_empty():
        return "empty"
    return f"{df.height}:{hashlib.sha256(df.sort('dt').write_ipc(None).getvalue()).hexdigest()[:16]}"


def _sj_version() -> str:
    import shioaji
    return str(getattr(shioaji, "__version__", "unknown"))


def _load_parity() -> bool | None:
    try:
        rec = json.loads(PARITY_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return rec.get("ok") if rec.get("shioaji") == _sj_version() else None


def _prove_parallel_safe(api, workers: int) -> bool:
    """→ 平行是否安全。已有同版本結論就直接用,否則實測一次並記錄。"""
    cached = _load_parity()
    if cached is not None:
        print(f"[pull] 平行自證:沿用 shioaji {_sj_version()} 的既有結論 "
              f"({'安全' if cached else '不安全'})")
        return cached

    probe = ["2330", "2317", "2454", "2308"]
    m0 = Date.today().replace(day=1)
    prev = Date(m0.year - (m0.month == 1), m0.month - 1 or 12, 1)
    tag, s, e = _months(prev, m0 - timedelta(days=1))[0]
    jobs = [(api.Contracts.Stocks[c], c) for c in probe]
    print(f"[pull] 平行自證中({tag},{len(probe)} 格 × 2 輪 ≈ 1.3 MB)…")

    def fetch(job):
        contract, _code = job
        return _fingerprint(_to_frame(
            api.kbars(contract=contract, start=s.isoformat(), end=e.isoformat())))

    lim = RateLimiter()
    seq = []
    for j in jobs:
        lim.acquire(); seq.append(fetch(j))
    if all(f == "empty" for f in seq):
        print("[pull] 平行自證:序列抓不到資料(多半是當日額度已用盡)→ 本輪維持序列")
        return False
    with ThreadPoolExecutor(max_workers=workers) as ex:
        par = list(ex.map(lambda j: (lim.acquire(), fetch(j))[1], jobs))

    ok = seq == par
    PARITY_FILE.parent.mkdir(parents=True, exist_ok=True)
    PARITY_FILE.write_text(json.dumps(
        {"shioaji": _sj_version(), "ok": ok, "workers": workers,
         "probe": probe, "month": tag, "seq": seq, "par": par},
        ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"[pull] 平行自證:{'✓ 逐位一致,開平行' if ok else '✗ 不一致,退回序列'}"
          f"(紀錄 → {PARITY_FILE.name})")
    return ok


# ── 進度 ────────────────────────────────────────────────────────────────
def _status() -> None:
    """離線進度:逐月已下載的檔數與磁碟量(不連線、不吃額度)。
    要看**缺哪幾天**請用 `--gaps`(對照 daily_quote 逐日核對)。"""
    if not OUT.exists():
        print("(尚無資料)")
        return
    rows, size = [], 0
    for d in sorted(OUT.iterdir()):
        if not d.is_dir():
            continue
        fs = list(d.glob("*.parquet"))
        size += sum(f.stat().st_size for f in fs)
        rows.append((d.name, len(fs)))
    print(f"{'月份':<10}{'檔數':>8}")
    for tag, n in rows:
        print(f"{tag:<10}{n:>8,}")
    print(f"{'合計':<10}{sum(n for _, n in rows):>8,}   磁碟 {size/1e9:.2f} GB / "
          f"{len(rows)} 個月")


def _gaps(since: Date | None = None) -> None:
    """離線盤點:對照 daily_quote,列出每個月**缺哪幾個交易日**(不連線、不吃額度)。

    這是「額度中途不足會不會缺日期、你知不知道要補哪幾天」的直接答案:
    知道,而且是算出來的——`daily_quote` 逐日記著每檔哪天有交易,把它跟磁碟上
    實際涵蓋的日子相減就是缺口。下次執行會自動從最近的月份開始補這些缺口。
    """
    import duckdb
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    months = _months(HIST_FLOOR, Date.today())
    if since:
        months = [m for m in months if m[2] >= since]
    months.reverse()
    codes = sorted({p.stem for d in OUT.iterdir() if d.is_dir()
                    for p in d.glob("*.parquet")})
    print(f"缺口盤點(對照 daily_quote;已下載過的 {len(codes):,} 檔)")
    tot_miss = tot_exp = 0
    for tag, s, e in months:
        per, alld = _expected(con, s, e)
        miss = exp = 0
        for code in codes:
            want = per.get(code)
            if not want:
                continue
            exp += len(want)
            miss += len(want - _have(tag, code))
        if exp:
            tot_miss += miss; tot_exp += exp
            if miss:
                print(f"  {tag}: 缺 {miss:,} 檔·日 / 應有 {exp:,}({miss/exp:.0%})")
    print(f"合計:缺 {tot_miss:,} / 應有 {tot_exp:,}"
          + (f"({tot_miss/tot_exp:.1%})" if tot_exp else ""))
    con.close()


def _run_phase(api, todo, lim: RateLimiter, workers: int, t0: float,
               n_done: int, n_empty: int) -> tuple[int, int]:
    """跑完一個階段的待抓清單。workers=1 即序列;>1 走執行緒池(共用限流器)。

    額度檢查刻意**不是每格一次**:usage() 往返 20 ms、且平行時會與抓取搶頻率額度。
    每 `USAGE_EVERY` 格查一次已足夠——真的超額時 API 會回空或報錯,由錯誤路徑接住。
    """
    lock = threading.Lock()
    stop = threading.Event()
    state = {"done": n_done, "empty": n_empty, "quota": None}

    def work(job) -> None:
        if stop.is_set():
            return
        tag, code, contract, days = job
        for attempt in range(1, MAX_RETRY + 1):
            lim.acquire()
            try:
                n = _pull(api, contract, code, tag, days)
                break
            except Exception as exc:  # noqa: BLE001
                if _is_quota_error(exc):
                    state["quota"] = str(exc); stop.set(); return
                if attempt == MAX_RETRY:
                    print(f"  ! {code} {tag} 放棄:{type(exc).__name__} {exc}",
                          file=sys.stderr)
                    return
                time.sleep(1.5 * attempt)
        with lock:
            state["done"] += 1
            state["empty"] += (n == 0)
            d = state["done"]
        if d % USAGE_EVERY == 0:
            rem = _remaining(api)
            if rem is not None and rem < RESERVE_BYTES:
                state["quota"] = f"剩餘 {rem/1e6:.1f} MB < 一個工作單位"; stop.set()
            elif d % 200 == 0:
                print(f"  … {d:,} 格(空 {state['empty']:,});剩餘 "
                      f"{'?' if rem is None else f'{rem/1e6:.0f} MB'};"
                      f"{time.time()-t0:.0f}s")

    if workers <= 1:
        for job in todo:
            work(job)
            if stop.is_set():
                break
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(work, todo))
    if state["quota"]:
        raise QuotaExhausted(state["quota"], state["done"], state["empty"])
    return state["done"], state["empty"]


def main() -> None:
    ap = argparse.ArgumentParser(description="1 分 K 歷史回補(分階段/額度自適應/可中斷)")
    ap.add_argument("--selftest", action="store_true", help="登入 + 抓 2330 當月驗證")
    ap.add_argument("--status", action="store_true", help="只印進度(離線)")
    ap.add_argument("--workers", type=int, default=WORKERS,
                    help=f"平行度(預設 {WORKERS};1 = 序列)")
    ap.add_argument("--gaps", action="store_true",
                    help="離線盤點缺哪幾天(對照 daily_quote,不連線、不吃額度)")
    ap.add_argument("--since", default=None,
                    help="只抓此日之後的月份(YYYY-MM-DD);預設抓到官方歷史下限")
    args = ap.parse_args()

    if args.status:
        _status()
        return

    if args.gaps:
        _gaps(Date.fromisoformat(args.since) if args.since else None)
        return

    api = _login()
    rem0 = _remaining(api)
    print(f"[pull] 登入 OK;剩餘額度 "
          f"{'未知' if rem0 is None else f'{rem0/1e6:.0f} MB'}")

    if args.selftest:
        c = api.Contracts.Stocks["2330"]
        tag = f"{Date.today().year:04d}-{Date.today().month:02d}"
        n = _pull(api, c, "2330", tag, Date.today().replace(day=1), Date.today())
        d = pl.read_parquet(OUT / tag / "2330.parquet")
        print(f"[selftest] 2330 {tag}:{n} 列,{d['dt'].min()} → {d['dt'].max()}")
        print(f"[selftest] 剩餘 {(_remaining(api) or 0)/1e6:.0f} MB;✓ 管線可用")
        api.logout()
        return

    workers = max(1, args.workers)
    if workers > 1 and not _prove_parallel_safe(api, workers):
        workers = 1
    lim = RateLimiter()
    print(f"[pull] 平行度 {workers};限流 {lim.per_second:.1f} 次/秒"
          f"(官方行情上限 10 次/秒,留 20% 安全邊際)")

    rank = _adv_rank()
    # 完整度的權威來源:daily_quote 逐日記錄了每檔在哪幾天有交易(見 _expected)
    import duckdb
    qcon = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    # 一份宇宙(個股 + 指數 + ETF),月內依流動性排序;逐月重用,不重建
    universe = _universe(api, "stock") + _universe(api, "index_etf")
    universe = sorted({c: (c, k) for c, k in universe}.values(),
                      key=lambda ck: rank.get(ck[0], 10 ** 9))
    months = _months(HIST_FLOOR, Date.today())
    if args.since:
        lo = Date.fromisoformat(args.since)
        months = [m for m in months if m[2] >= lo]
    months.reverse()                       # **由近而遠**
    print(f"[pull] 宇宙 {len(universe):,} 檔 × {len(months)} 個月;"
          f"由近而遠({months[0][0]} → {months[-1][0]})")

    n_done = n_empty = 0
    t0 = time.time()
    try:
        for tag, ms, me in months:
            per, alld = _expected(qcon, ms, me)
            todo = _month_todo(universe, tag, per, alld)
            if not todo:
                continue
            print(f"\n[{tag}] 待抓 {len(todo):,} 格")
            d, em = _run_phase(api, todo, lim, workers, t0, n_done, n_empty)
            n_done, n_empty = d, em
        print(f"\n[pull] 全部月份完成 ✅ 本輪 {n_done:,} 格")
    except QuotaExhausted as exc:
        n_done, n_empty = max(n_done, exc.done), max(n_empty, exc.empty)
        print(f"\n[pull] 額度用盡而停({exc});進度已在磁碟,"
              f"明日 08:30 launchd 自動續傳。本輪 {n_done:,} 格")
    except KeyboardInterrupt:
        print(f"\n[pull] 手動中斷;進度已在磁碟,重跑即續傳。本輪 {n_done:,} 格")
    finally:
        try:
            api.logout()
        except Exception:
            pass


if __name__ == "__main__":
    main()
