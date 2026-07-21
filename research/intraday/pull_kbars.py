"""台股 1 分 K 歷史回補(永豐 Shioaji;分階段 + 額度自適應 + 隨時可中斷續傳)。

**分階段(使用者指定優先序,2026-07-21)**
  P1 優化參數窗  2022-12-01 → 2025-12-01(去年 12/1 往前 3 年;S 年度 refit 窗)
  P2 因子研究窗  2020-03-02 → 2022-12-01(再往前;受官方歷史下限 2020-03-02 截斷,
                 故「前四年」實得 2.75 年——誠實聲明,非偷工)
  P3 最新段補齊  2025-12-01 → 今日(至此個股 2020-03→今全齊)
  P4 指數與 ETF  全期(大盤 regime + 0050 對照;量小價值高)

**額度紀律(使用者質疑後修正)**:不自設保守停損線。以 API 自己的
`usage().remaining_bytes` 為準,剩餘不足一個工作單位(RESERVE)或 API 回報額度
錯誤才停;額度於交易日 08:00 重置,launchd 每日 08:30 自動續跑。

**隨時中斷即可續傳(刻意無狀態檔)**:完成單位 = 磁碟上的檔案本身——
  kbars_1m/{YYYY-MM}/{code}.parquet  有資料
  kbars_1m/{YYYY-MM}/{code}.empty    確認無資料(未上市/停牌整月)
寫入一律 tmp → os.replace(同分割區原子換名),故 kill -9 / 斷網 / 當機最多
重做「當下那一格」,絕不留半檔汙染,也不需要任何 state 檔案同步。
當月檔案(仍在長大)以 mtime 判定:非今日抓的則重抓增量。

**資料聲明(回測必讀)**:(1) Shioaji 合約表僅含現存上市股 → 本資料集含存活者
偏差;(2) 價格為原始價(未還原權息),研究時以 daily adj_factor 對齊;
(3) 官方歷史下限:股票/指數 2020-03-02、期貨 2020-03-22。

用法:
  uv run --project research python -m research.intraday.pull_kbars             # 續傳回補
  uv run --project research python -m research.intraday.pull_kbars --selftest  # 登入+單檔驗證
  uv run --project research python -m research.intraday.pull_kbars --status    # 只看進度(不連線)
金鑰:research/.env 的 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY(資料查詢免 CA 憑證)。
依賴 cache:是(流動性排序,缺 cache 則退回代碼序)。資料不進 git。
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date as Date
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

REPO = Path(__file__).resolve().parents[2]
OUT = REPO / "research" / "data" / "intraday" / "kbars_1m"
HIST_FLOOR = Date(2020, 3, 2)      # 官方股票/指數歷史下限
RESERVE_BYTES = 2 * 1024 ** 2      # 一個工作單位的量級;剩餘低於此註定失敗,提早停
PACE_SEC = 0.2
MAX_RETRY = 3

PHASES: list[tuple[str, Date, Date, str]] = [
    ("P0 S實際持倉", HIST_FLOOR, Date.today(), "s_trades"),
    ("P1 優化參數窗", Date(2022, 12, 1), Date(2025, 12, 1), "stock"),
    ("P2 因子研究窗", HIST_FLOOR, Date(2022, 12, 1), "stock"),
    ("P3 最新段補齊", Date(2025, 12, 1), Date.today(), "stock"),
    ("P4 指數與 ETF", HIST_FLOOR, Date.today(), "index_etf"),
]


class QuotaExhausted(RuntimeError):
    """API 額度用盡(由 usage 或錯誤訊息判定)。"""


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


def _login():
    import shioaji as sj
    api = sj.Shioaji()
    key, sec = _env()
    api.login(api_key=key, secret_key=sec, subscribe_trade=False)
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
        con = duckdb.connect(str(REPO / "research" / "cache.duckdb"), read_only=True)
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
def _s_trade_spans(lo: Date, hi: Date) -> list[tuple[str, Date, Date]]:
    """S 實際成交的 (code, 進場日, 出場日),裁切到 [lo, hi]。

    用途(使用者 2026-07-21 提案,已升級為「只拉持有期間」):優先取得 S 真正
    持有過的價格路徑,即可校準出場/執行面參數(trail%、停損價位、盤中 vs 收盤
    觸發)。**限制**:此子集由 S 現行參數決定,不可用於重新優化「進場因子」
    ——換因子會選到別的股票,那些股票無資料 → 選擇偏誤。全市場照樣續拉(P1-P4)。
    """
    from research.apex import data as _d
    from research.apex.strategy_s import prep as _prep, run_s_full
    con = _d.connect()
    try:
        panel, feat, elig = _prep(con)
        _nav, trades = run_s_full(panel, feat, elig, lo.isoformat())
    finally:
        con.close()
    out = []
    for r in trades.select(["company_code", "entry_date", "exit_date"]).iter_rows():
        code, ed, xd = r[0], r[1], r[2]
        s, e = max(ed, lo), min(xd, hi)
        if s <= e:
            out.append((code, s, e))
    return out


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


def _done(tag: str, code: str) -> bool:
    """完成判定 = 檔案存在;當月檔案若非今日抓的則需重抓(資料仍在長大)。"""
    d = OUT / tag
    pq, empty = d / f"{code}.parquet", d / f"{code}.empty"
    cur_tag = f"{Date.today().year:04d}-{Date.today().month:02d}"
    for f in (pq, empty):
        if f.exists():
            if tag == cur_tag:
                mt = datetime.fromtimestamp(f.stat().st_mtime).date()
                return mt >= Date.today()
            return True
    return False


def _write_atomic(df: pl.DataFrame | None, tag: str, code: str) -> None:
    d = OUT / tag
    d.mkdir(parents=True, exist_ok=True)
    if df is None:                              # 確認無資料 → 0-byte 哨兵
        tmp = d / f"{code}.empty.tmp"
        tmp.write_bytes(b"")
        os.replace(tmp, d / f"{code}.empty")
        return
    tmp = d / f"{code}.parquet.tmp"
    df.write_parquet(tmp)
    os.replace(tmp, d / f"{code}.parquet")       # 同分割區 → 原子換名


def _phase_todo(api, kind: str, ps: Date, pe: Date, rank: dict[str, int]
                ) -> list[tuple[str, Date, Date, str, object]]:
    """該階段待抓清單 [(月tag, 起, 訖, code, contract)];已完成者已濾除。

    s_trades:僅 S 實際持倉期間的月份(去重);其餘:宇宙 × 月份,流動性高者優先。
    """
    todo: list[tuple[str, Date, Date, str, object]] = []
    if kind == "s_trades":
        seen: set[tuple[str, str]] = set()
        for code, s0, e0 in _s_trade_spans(ps, pe):
            try:
                contract = api.Contracts.Stocks[code]
            except Exception:
                contract = None
            if contract is None:                 # 已下市 → 拉不到,跳過(存活者偏差來源)
                continue
            for tag, s, e in _months(s0, e0):
                if (tag, code) in seen:
                    continue
                seen.add((tag, code))
                if not _done(tag, code):
                    todo.append((tag, s, e, code, contract))
        todo.sort(key=lambda t: (t[0], rank.get(t[3], 10 ** 9)))
        return todo
    codes = _universe(api, kind)
    codes.sort(key=lambda ck: rank.get(ck[0], 10 ** 9))
    for tag, s, e in _months(ps, pe):
        for code, contract in codes:
            if not _done(tag, code):
                todo.append((tag, s, e, code, contract))
    return todo


def _pull(api, contract, code: str, tag: str, s: Date, e: Date) -> int:
    kb = api.kbars(contract=contract, start=s.isoformat(), end=e.isoformat())
    ts = list(kb.ts)
    if not ts:
        _write_atomic(None, tag, code)
        return 0
    df = (pl.DataFrame({
        "ts": pl.Series(ts, dtype=pl.Int64),
        "open": list(kb.Open), "high": list(kb.High), "low": list(kb.Low),
        "close": list(kb.Close), "volume": list(kb.Volume), "amount": list(kb.Amount),
    }).with_columns(pl.from_epoch("ts", time_unit="ns").alias("dt"))
      .sort("dt"))
    _write_atomic(df, tag, code)
    return len(df)


# ── 進度 ────────────────────────────────────────────────────────────────
def _status() -> None:
    """離線進度:逐月已存檔格數 + 總量(不連線、不吃額度)。"""
    if not OUT.exists():
        print("(尚無資料)")
        return
    rows = []
    for d in sorted(OUT.iterdir()):
        if not d.is_dir():
            continue
        pq = len(list(d.glob("*.parquet")))
        em = len(list(d.glob("*.empty")))
        rows.append((d.name, pq, em))
    tot_pq = sum(r[1] for r in rows)
    tot_em = sum(r[2] for r in rows)
    size = sum(f.stat().st_size for f in OUT.rglob("*.parquet"))
    print(f"{'月份':<10s}{'有資料':>8s}{'空檔':>8s}")
    for tag, pq, em in rows:
        print(f"{tag:<10s}{pq:>8,}{em:>8,}")
    print(f"{'合計':<10s}{tot_pq:>8,}{tot_em:>8,}   磁碟 {size/1e9:.2f} GB / "
          f"{len(rows)} 個月")


def main() -> None:
    ap = argparse.ArgumentParser(description="1 分 K 歷史回補(分階段/額度自適應/可中斷)")
    ap.add_argument("--selftest", action="store_true", help="登入 + 抓 2330 當月驗證")
    ap.add_argument("--status", action="store_true", help="只印進度(離線)")
    ap.add_argument("--phase", type=int, default=None,
                    help="只跑指定階段(0=S實際持倉, 1-3=個股分段, 4=指數ETF)")
    args = ap.parse_args()

    if args.status:
        _status()
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

    rank = _adv_rank()
    n_done = n_empty = 0
    t0 = time.time()
    try:
        for i, (name, ps, pe, kind) in enumerate(PHASES):
            if args.phase is not None and args.phase != i:
                continue
            todo = _phase_todo(api, kind, ps, pe, rank)
            print(f"\n[{name}] {ps}→{pe};待抓 {len(todo):,} 格")
            for tag, s, e, code, contract in todo:
                rem = _remaining(api)
                if rem is not None and rem < RESERVE_BYTES:
                    raise QuotaExhausted(f"剩餘 {rem/1e6:.1f} MB < 一個工作單位")
                for attempt in range(1, MAX_RETRY + 1):
                    try:
                        n = _pull(api, contract, code, tag, s, e)
                        n_done += 1
                        n_empty += (n == 0)
                        break
                    except Exception as exc:  # noqa: BLE001
                        if _is_quota_error(exc):
                            raise QuotaExhausted(str(exc)) from exc
                        if attempt == MAX_RETRY:
                            print(f"  ! {code} {tag} 放棄:{type(exc).__name__} {exc}",
                                  file=sys.stderr)
                        else:
                            time.sleep(1.5 * attempt)
                if n_done % 100 == 0 and n_done:
                    rem = _remaining(api)
                    print(f"  … {n_done:,} 格(空 {n_empty:,});剩餘 "
                          f"{'?' if rem is None else f'{rem/1e6:.0f} MB'};"
                          f"{time.time()-t0:.0f}s")
                time.sleep(PACE_SEC)
        print(f"\n[pull] 全部階段完成 ✅ 本輪 {n_done:,} 格")
    except QuotaExhausted as exc:
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
