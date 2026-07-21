"""台股全個股 1 分 K 歷史回補(永豐 Shioaji;每日 2GB 額度,狀態機續傳)。

範圍:TSE + OTC 全部 4 碼數字個股(含 91xx KY;不含 ETF/權證/興櫃)。
深度:官方歷史下限 2020-03-02 → 今日(sinotrade.github.io/tutor/market_data/historical)。
順序:年份新→舊為外圈、流動性(近 60 日均成交值,cache)高→低為內圈——
最近年份最先齊,研究可儘早在「近 3 年窗」(M01 最優)起步。

額度紀律:每檔抓取前查 `api.usage()`,用量 > QUOTA_STOP(1.85GB,留 headroom)
即停;state(research/data/intraday/state.json)記錄每 (code, year) 完成狀態,
重跑自動續傳;空回應(該年未上市/停牌)記 done-empty 不重抓。節流 0.25s/請求。

誠實聲明(回測時必讀):Shioaji 合約表僅含**現存上市股**,2020 年後下市者拉不到
→ 本資料集含存活者偏差;價格為**原始價**(未還原),研究時以 daily adj_factor 對齊。

用法:
  uv run --project research python -m research.intraday.pull_kbars            # 續傳回補
  uv run --project research python -m research.intraday.pull_kbars --selftest # 登入+單檔驗證
金鑰:research/.env 的 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY(資料查詢免 CA 憑證)。
依賴 cache:是(流動性排序)。資料不進 git(.gitignore:research/data/intraday/)。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date as Date
from pathlib import Path

import polars as pl

REPO = Path(__file__).resolve().parents[2]
OUT = REPO / "research" / "data" / "intraday" / "kbars_1m"
STATE = REPO / "research" / "data" / "intraday" / "state.json"
HIST_FLOOR = 2020          # 官方股票歷史下限 2020-03-02
QUOTA_STOP = 1.85 * 1024 ** 3   # bytes;官方每日 2GB、交易日 08:00 重置
PACE_SEC = 0.25


def _env() -> tuple[str, str]:
    envp = REPO / "research" / ".env"
    if envp.exists():
        for line in envp.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    key, sec = os.environ.get("SHIOAJI_API_KEY"), os.environ.get("SHIOAJI_SECRET_KEY")
    if not key or not sec:
        sys.exit("✗ 缺 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY(research/.env)")
    return key, sec


def _login():
    import shioaji as sj
    api = sj.Shioaji()
    key, sec = _env()
    api.login(api_key=key, secret_key=sec, contracts_timeout=30_000)
    return api


def _usage_bytes(api) -> int:
    try:
        return int(api.usage().bytes)
    except Exception:
        return 0  # usage 查詢偶發失敗不擋回補(額度超限時 API 自會擋)


def _stock_codes(api) -> list[str]:
    codes = []
    for mkt in ("TSE", "OTC"):
        for c in getattr(api.Contracts.Stocks, mkt):
            code = c.code
            if len(code) == 4 and code.isdigit():
                codes.append(code)
    return sorted(set(codes))


def _adv_order(codes: list[str]) -> list[str]:
    """近 60 日均成交值 DESC(cache);cache 缺者殿後(新上市)。"""
    import duckdb
    con = duckdb.connect(str(REPO / "research" / "cache.duckdb"), read_only=True)
    try:
        adv = con.execute(
            "SELECT company_code, avg(trade_value) AS adv FROM daily_quote "
            "WHERE date >= current_date - INTERVAL 90 DAY GROUP BY company_code"
        ).pl()
    finally:
        con.close()
    rank = dict(adv.sort("adv", descending=True)
                .with_row_index("i").select(["company_code", "i"]).iter_rows())
    return sorted(codes, key=lambda c: rank.get(c, 10 ** 9))


def _load_state() -> dict:
    return json.loads(STATE.read_text()) if STATE.exists() else {}


def _save_state(st: dict) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(st))
    tmp.replace(STATE)


def _pull_one(api, code: str, year: int) -> int:
    """抓一檔一年 → parquet;回列數。當年抓到今日、state 記錄截止日供日後增量。"""
    import shioaji as sj
    contract = api.Contracts.Stocks[code]
    if contract is None:
        return -1
    start = f"{year}-01-01" if year > HIST_FLOOR else "2020-03-02"
    end = min(Date(year, 12, 31), Date.today()).isoformat()
    kb = api.kbars(contract=contract, start=start, end=end)
    df = pl.DataFrame({
        "ts": pl.Series(kb.ts, dtype=pl.Int64),
        "open": kb.Open, "high": kb.High, "low": kb.Low, "close": kb.Close,
        "volume": kb.Volume, "amount": kb.Amount,
    })
    if df.is_empty():
        return 0
    df = (df.with_columns(pl.from_epoch("ts", time_unit="ns").alias("dt"))
          .sort("dt"))
    out = OUT / str(year) / f"{code}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out)
    return len(df)


def main() -> None:
    ap = argparse.ArgumentParser(description="全個股 1 分 K 歷史回補(額度狀態機)")
    ap.add_argument("--selftest", action="store_true", help="登入 + 抓 2330 近 3 日驗證")
    args = ap.parse_args()
    api = _login()
    used0 = _usage_bytes(api)
    print(f"[pull] 登入 OK;今日已用 {used0/1e6:.0f} MB / 2000 MB")

    if args.selftest:
        n = _pull_one(api, "2330", Date.today().year)
        f = OUT / str(Date.today().year) / "2330.parquet"
        d = pl.read_parquet(f)
        print(f"[selftest] 2330 {Date.today().year} 年:{n} 列,"
              f"{d['dt'].min()} → {d['dt'].max()}")
        print(f"[selftest] 用量 {_usage_bytes(api)/1e6:.0f} MB;✓ 管線可用")
        api.logout()
        return

    codes = _adv_order(_stock_codes(api))
    years = list(range(Date.today().year, HIST_FLOOR - 1, -1))
    st = _load_state()
    print(f"[pull] 目標 {len(codes)} 檔 × {years} 年;已完成 {len(st)} 格")
    n_done = 0
    try:
        for year in years:
            for code in codes:
                key = f"{code}:{year}"
                if key in st and (year < Date.today().year or st[key].get("final")):
                    continue  # 歷史年已完成;當年若非今日抓的可重抓增量
                if key in st and st[key].get("asof") == Date.today().isoformat():
                    continue
                used = _usage_bytes(api)
                if used > QUOTA_STOP:
                    print(f"[pull] 額度達 {used/1e6:.0f} MB,今日停(明日 08:00 重置續傳)")
                    raise SystemExit(0)
                try:
                    n = _pull_one(api, code, year)
                except Exception as exc:  # noqa: BLE001 - 單格失敗不毀整批,下輪重試
                    print(f"  ! {key}: {type(exc).__name__} {exc}", file=sys.stderr)
                    time.sleep(2.0)
                    continue
                st[key] = {"rows": n, "asof": Date.today().isoformat(),
                           "final": year < Date.today().year}
                n_done += 1
                if n_done % 50 == 0:
                    _save_state(st)
                    print(f"  … {n_done} 格(額度 {_usage_bytes(api)/1e6:.0f} MB;"
                          f"最新 {key}={n} 列)")
                time.sleep(PACE_SEC)
        print("[pull] 全部完成 ✅")
    finally:
        _save_state(st)
        try:
            api.logout()
        except Exception:
            pass


if __name__ == "__main__":
    main()
