"""日線結構層(執行器 v3):跨日結構位 + 昨日 1 分 K 價值區。

「盡可能買在低點/賣在高點」不能只看今日盤中——開盤初期 TPO/SMC 樣本太少。
本模組把兩種跨日結構餵給 MicrostructureDetector 當掛單錨:

1. **日線結構位**(`var/cache/cache.duckdb` 近 30 個交易日 OHLC):
   前日低/高、前日收盤、5 根分形 swing 低/高、日線 FVG 中線、20 日極值。
2. **昨日價值區 prior**(`var/out/trading/candles/<date>_<code>.json`,
   執行器每次收盤自動 dump 的 1 分 K 自建歷史):昨日 VAL/POC/VAH。
   台股沒有免費的歷史 1 分 K 端點,這份存檔從 2026-07-09 起自我累積。

全部 fail-open:cache 缺席/被 cache_tables 重建鎖住/檔案不存在 → 回空,
執行器退回純盤中結構,絕不因此中斷。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
CACHE_DB = paths.CACHE_DB
CANDLES_DIR = paths.OUT / "trading" / "candles"


def _load_daily_bars(code: str, before: date, limit: int = 30) -> list[dict]:
    try:
        import duckdb

        con = duckdb.connect(str(CACHE_DB), read_only=True)
        try:
            rows = con.execute(
                "SELECT date, opening_price, highest_price, lowest_price, closing_price "
                "FROM daily_quote WHERE company_code = ? AND date < ? "
                "ORDER BY date DESC LIMIT ?",
                [code, before, limit],
            ).fetchall()
        finally:
            con.close()
    except Exception:
        return []
    bars = [
        {"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]}
        for r in reversed(rows)
        if r[3] and r[3] > 0
    ]
    return bars


def _swings(bars: list[dict], key: str, is_low: bool, wing: int = 2) -> list[float]:
    """5 根分形 swing(中間根低/高於左右各 wing 根)。"""
    out = []
    for i in range(wing, len(bars) - wing):
        v = bars[i][key]
        neigh = [bars[j][key] for j in range(i - wing, i + wing + 1) if j != i]
        if (is_low and v < min(neigh)) or (not is_low and v > max(neigh)):
            out.append(v)
    return out


def _fvg_mids(bars: list[dict], bullish: bool) -> list[float]:
    """日線 FVG 中線。多方 FVG(支撐)= bar[i].low > bar[i-2].high 的缺口。"""
    out = []
    for i in range(2, len(bars)):
        if bullish and bars[i]["low"] > bars[i - 2]["high"]:
            out.append((bars[i - 2]["high"] + bars[i]["low"]) / 2)
        elif not bullish and bars[i]["high"] < bars[i - 2]["low"]:
            out.append((bars[i]["high"] + bars[i - 2]["low"]) / 2)
    return out


def load_daily_levels(code: str, side: str, today: date | None = None) -> list[tuple[float, str]]:
    """回傳 (價位, 依據) 清單;買方=支撐(低於現價的候選),賣方=阻力。
    過濾「離現價太遠」交給 anchor_level(它只取 ref 同側最近者)+ 護欄。"""
    bars = _load_daily_bars(code, today or date.today())
    if not bars:
        return []
    prev = bars[-1]
    levels: list[tuple[float, str]] = []
    if side == "Buy":
        levels.append((prev["low"], "昨日低"))
        levels.append((prev["close"], "昨收"))
        levels += [(v, "日線swing低") for v in _swings(bars, "low", is_low=True)[-3:]]
        levels += [(v, "日線FVG") for v in _fvg_mids(bars, bullish=True)[-2:]]
        levels.append((min(b["low"] for b in bars), "20日低"))
    else:
        levels.append((prev["high"], "昨日高"))
        levels.append((prev["close"], "昨收"))
        levels += [(v, "日線swing高") for v in _swings(bars, "high", is_low=False)[-3:]]
        levels += [(v, "日線FVG") for v in _fvg_mids(bars, bullish=False)[-2:]]
        levels.append((max(b["high"] for b in bars), "20日高"))
    return [(p, lab) for p, lab in levels if p and p > 0]


def load_prior_value_area(code: str, today: date | None = None) -> tuple[float, float, float] | None:
    """最近一個交易日的 (VAL, POC, VAH),取自 1 分 K 自建歷史;無檔回 None。"""
    from .microstructure import tpo_value_area

    today = today or date.today()
    try:
        cands = sorted(
            f for f in CANDLES_DIR.glob(f"*_{code}.json")
            if f.name.split("_")[0] < today.isoformat()
        )
    except OSError:
        return None
    if not cands:
        return None
    try:
        bars = json.loads(cands[-1].read_text())
    except (OSError, ValueError):
        return None
    if not bars:
        return None
    val, poc, vah = tpo_value_area(bars)
    return (val, poc, vah) if poc > 0 else None


def lookup_names(codes: list[str]) -> dict[str, str]:
    """公司名(operating_revenue 最新一筆)。fail-open:cache 缺席/鎖住回空
    ——名字只是核對輔助,絕不因此擋執行。ETF 無月營收查不到,顯示代碼。"""
    if not codes:
        return {}
    try:
        import duckdb

        con = duckdb.connect(str(CACHE_DB), read_only=True)
        try:
            rows = con.execute(
                "SELECT company_code, last(company_name ORDER BY year * 100 + month) "
                "FROM operating_revenue WHERE company_code IN ({}) "
                "GROUP BY company_code".format(",".join("?" * len(codes))),
                list(codes),
            ).fetchall()
        finally:
            con.close()
        return {str(c).zfill(4): str(n) for c, n in rows if n}
    except Exception:
        return {}


def dump_candles(code: str, bars: list[dict], today: date | None = None) -> Path | None:
    """收盤存檔當日 1 分 K(冪等覆寫)——自建歷史的累積入口。"""
    if not bars:
        return None
    try:
        CANDLES_DIR.mkdir(parents=True, exist_ok=True)
        path = CANDLES_DIR / f"{(today or date.today()).isoformat()}_{code}.json"
        path.write_text(json.dumps(bars, ensure_ascii=False))
        return path
    except OSError:
        return None
