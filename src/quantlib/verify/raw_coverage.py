"""raw-vs-cache 涵蓋對照稽核 —— 「cache 是否忠實反映既有 raw」的單一真源。

**動機(2026-07-24)**:`pipeline_health` 只看 cache 一層,把「cache 落後/稀疏」誤讀成
「資料缺、要去下載」。但本專案鐵律是 raw(`data/`)才是事實地基、cache 只是其衍生、
隨時可從 raw 重建。**任何「疑似缺資料」必須先對照 raw**:raw 有而 cache 沒有 = rebuild
沒吃全(重解析即可,絕不重抓);raw 真的沒有、且過齊備日 = 才是前向增量下載的範圍。

本工具逐源掃 raw 檔(依命名慣例抽日期,區分資料檔 vs 0-byte 休市 sentinel)對照 cache
的日期集合,給每源判詞:
  ✓ cache ⊇ raw 資料日           —— 一致,無需動作
  ⚠ cache 缺 N 個 raw 資料日      —— rebuild 沒吃全 → `rebuild` 從 raw 重解析(不重抓)
  ✗ raw 有日期但 cache 表不存在   —— 未接線 → 補 rebuild 函式

**只涵蓋可量化的結構化源**(使用者定調:需 LLM/語意分析的源〔MOPS 重大訊息 free-text、
法說會逐字稿〕不進管線,故不在此稽核)。

Run: uv run --project . python -m quantlib.verify.raw_coverage
依賴 cache:是(唯讀對照);不改任何資料。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date as Date

from quantlib import paths
from quantlib.db import connect

_YMD = re.compile(r"^(\d{4})_(\d{1,2})_(\d{1,2})$")   # 日頻 stem
_YM = re.compile(r"^(\d{4})_(\d{1,2})(?:_.*)?$")       # 月頻 stem(可帶 _i/_c/_type 後綴)
_Y = re.compile(r"^(\d{4})$")                          # 年 stem
_YQ = re.compile(r"^(\d{4})_(\d)(?:_.*)?$")            # 季 stem(Y_Q_...)


@dataclass
class Source:
    """一個結構化源的 raw↔cache 對照定義。"""
    table: str                       # cache 表名(None 語意由 layout 決定)
    raw_glob: str                    # 相對 data/ 的 glob(如 "insider_holding/*/*/*.html")
    layout: str                      # daily | monthly | annual | quarterly
    ext: str = "csv"
    cache_date_col: str = "date"     # cache 表的日期欄(insider=report_date)
    note: str = ""


#: 結構化源清單(可量化;需 LLM 的源不列)。raw_glob 以 data/ 為根。
SOURCES = [
    # ── 日頻(股票)──────────────────────────────────────────────────
    Source("daily_quote", "daily_quote/*/*/*.csv", "daily"),
    Source("daily_trading_details", "daily_trading_details/*/*/*.csv", "daily"),
    Source("margin_transactions", "margin_transactions/*/*/*.csv", "daily"),
    Source("foreign_holding_ratio", "foreign_holding_ratio/*/*/*.csv", "daily"),
    Source("sbl_borrowing", "sbl_borrowing/*/*/*.csv", "daily"),
    Source("stock_per_pbr", "stock_per_pbr/*/*/*.csv", "daily"),
    Source("market_index", "market_index/*/*/*.csv", "daily"),
    Source("insider_holding", "insider_holding/*/*/*.html", "daily",
           ext="html", cache_date_col="report_date"),
    Source("ex_right_dividend", "ex_right_dividend/*/*/*.csv", "dump",
           note="raw 檔=全史 dump(檔名 Y_M_D 為查詢標記,內容涵蓋 2003+);cache==raw parse 已驗"),
    Source("capital_reduction", "capital_reduction/*/*/*.csv", "dump",
           note="raw 檔=全史 dump(檔名為查詢範圍標記,內容涵蓋 2011+);cache==raw parse 已驗"),
    # ── 月頻 ────────────────────────────────────────────────────────
    Source("operating_revenue", "operating_revenue/*/*/*", "monthly",
           note="raw 檔 Y_M_{i,c}.{html,csv};cache 以 year/month 對照"),
    Source("treasury_stock_buyback", "treasury_stock_buyback/*/*/*.html", "monthly",
           ext="html", cache_date_col="announce_date", note="快照式;raw 僅近月、cache 含全史"),
    # ── 期貨(futures 子系統)──────────────────────────────────────
    Source("taifex_futures_institutional", "taifex/futures_institutional/*/*.csv", "monthly",
           note="raw 僅 2023+(TAIFEX 期貨法人揭露起點)"),
    Source("taifex_futures_final_settlement", "taifex/futures_final_settlement/*/*.html", "annual",
           ext="html", note="raw 年檔 1998+"),
    # ── 季頻(財報)──────────────────────────────────────────────
    Source("bs_concise_raw", "balance_sheet/*/*/*.csv", "quarterly",
           note="raw Y_Q_..._idx.csv"),
    Source("is_progressive_raw", "income_statement/*/*/*.csv", "quarterly",
           note="raw Y_Q_..._idx.csv"),
]


@dataclass
class Coverage:
    source: str
    layout: str
    raw_keys: set = field(default_factory=set)   # 資料檔的日期/期別鍵
    sentinels: int = 0                           # 0-byte 休市 sentinel 數
    raw_min: object = None
    raw_max: object = None
    cache_keys: set = field(default_factory=set)
    cache_min: object = None
    cache_max: object = None
    table_exists: bool = True
    real_gap: set = field(default_factory=set)   # 交叉真交易日過濾後的實缺(日頻);其餘=raw_only
    raw_files: int = 0                            # raw 資料檔數(dump 源以此表示涵蓋)


def _scan_raw(src: Source) -> tuple[set, int]:
    """掃 raw 檔 → (資料檔鍵集合, sentinel 數, 資料檔數)。鍵依 layout:日=date、月=(y,m)、
    年=y、季=(y,q);dump=無鍵(檔名非事件日,以檔數表示)。"""
    keys: set = set()
    sentinels = nfiles = 0
    for f in paths.RAW.glob(src.raw_glob):
        if not f.is_file():
            continue
        # 0-byte = 休市 sentinel(僅日頻有;交易所親口「無資料」)
        if f.stat().st_size == 0:
            sentinels += 1
            continue
        nfiles += 1
        key = _key_from_stem(f.stem, src.layout)
        if key is not None:
            keys.add(key)
    return keys, sentinels, nfiles


def _key_from_stem(stem: str, layout: str):
    if layout == "daily":
        m = _YMD.match(stem)
        if m:
            y, mo, d = (int(x) for x in m.groups())
            try:
                return Date(y, mo, d)
            except ValueError:
                return None
    elif layout == "monthly":
        m = _YM.match(stem)
        if m:
            return (int(m.group(1)), int(m.group(2)))
    elif layout == "annual":
        m = _Y.match(stem)
        if m:
            return int(m.group(1))
    elif layout == "quarterly":
        m = _YQ.match(stem)
        if m:
            return (int(m.group(1)), int(m.group(2)))
    return None


def _cache_keys(con, src: Source) -> tuple[set, bool]:
    """cache 表的鍵集合(對齊 layout);表不存在回 (空, False)。"""
    exists = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [src.table]).fetchone()[0] > 0
    if not exists:
        return set(), False
    cols = {c[0] for c in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [src.table]).fetchall()}
    # 防禦性偵測日期欄:宣告欄 → date → 任一含 'date' 欄
    dc = src.cache_date_col
    if dc not in cols and src.layout in ("daily", "annual", "dump"):
        dc = "date" if "date" in cols else next((c for c in cols if "date" in c.lower()), dc)
    if src.layout in ("daily", "dump"):
        rows = con.execute(f"SELECT DISTINCT {dc} FROM {src.table}").fetchall()
        return {r[0] for r in rows if r[0] is not None}, True
    if src.layout == "monthly":
        # 優先用 year/month 欄;無則從日期欄抽
        if {"year", "month"} <= cols:
            rows = con.execute(f"SELECT DISTINCT year, month FROM {src.table}").fetchall()
            return {(r[0], r[1]) for r in rows if r[0] is not None}, True
        rows = con.execute(f"SELECT DISTINCT {dc} FROM {src.table}").fetchall()
        return {(r[0].year, r[0].month) for r in rows if r[0] is not None}, True
    if src.layout == "annual":
        rows = con.execute(f"SELECT DISTINCT {dc} FROM {src.table}").fetchall()
        return {r[0].year for r in rows if r[0] is not None}, True
    if src.layout == "quarterly":
        rows = con.execute(f"SELECT DISTINCT year, quarter FROM {src.table}").fetchall()
        return {(r[0], r[1]) for r in rows if r[0] is not None}, True
    return set(), True


def _trading_days(con) -> set:
    """交易日曆真源 = daily_quote cache 有資料的日子(有成交=開市;是唯一權威日曆)。
    用來把其他源的「raw-only 日」交叉過濾:只有落在真交易日的 raw-only 才是真缺口,
    落在假日/週末(raw 是非空『共 0 筆』回應或探針檔)的一律不算(parse 出 0 列本就正確)。"""
    return {r[0] for r in con.execute("SELECT DISTINCT date FROM daily_quote").fetchall()}


def audit() -> list[Coverage]:
    con = connect()
    tdays = _trading_days(con)
    out: list[Coverage] = []
    for src in SOURCES:
        raw_keys, sentinels, nfiles = _scan_raw(src)
        cache_keys, exists = _cache_keys(con, src)
        raw_only = raw_keys - cache_keys if exists else set()
        # 日頻:只有落在真交易日的 raw-only 才是實缺(排假日/週末的非空無資料檔)。
        # daily_quote 自身即日曆,無可交叉的外部真源 → 實缺=落在其他源都確認的交易日,
        #   故對 daily_quote 用「該 raw-only 日是否 ≥1 其他日頻源在 cache 有」近似不可行,
        #   保守以 weekday 過濾(週末 raw 檔=補班存疑,列資訊不列實缺)。
        if src.layout == "dump":
            real_gap = set()  # 全史 dump:檔名≠事件日,無 filename gap(cache==raw parse 另 rebuild 驗)
        elif src.layout == "daily":
            if src.table == "daily_quote":
                real_gap = {d for d in raw_only if d.weekday() < 5} & tdays  # 幾乎恆空(自身即真源)
            else:
                real_gap = raw_only & tdays
        else:
            real_gap = raw_only  # 月/年/季:period-keyed,無假日雜訊
        cov = Coverage(
            source=src.table, layout=src.layout, raw_keys=raw_keys, sentinels=sentinels,
            raw_min=min(raw_keys) if raw_keys else None,
            raw_max=max(raw_keys) if raw_keys else None,
            cache_keys=cache_keys, table_exists=exists,
            cache_min=min(cache_keys) if cache_keys else None,
            cache_max=max(cache_keys) if cache_keys else None,
            real_gap=real_gap, raw_files=nfiles,
        )
        out.append(cov)
    return out


def _fmt(k, layout: str = "daily") -> str:
    if isinstance(k, tuple):
        return f"{k[0]}Q{k[1]}" if layout == "quarterly" else f"{k[0]}-{k[1]:02d}"
    return str(k)


def main() -> None:
    covs = audit()
    print("=== raw ↔ cache 涵蓋對照(結構化源;raw=data/ 事實地基)===")
    print("  實缺 = raw 有資料、cache 沒有、且落在真交易日(已濾掉假日的非空無資料檔)\n")
    hdr = f"  {'源':30} {'raw 範圍':21} {'cache 範圍':21} {'實缺':>5}  判詞"
    print(hdr)
    print("  " + "-" * 90)
    need_rebuild, missing_tbl = [], []
    for c in covs:
        lay = c.layout
        raw_rng = (f"{c.raw_files} 檔 dump" if lay == "dump"
                   else f"{_fmt(c.raw_min, lay)}~{_fmt(c.raw_max, lay)}" if c.raw_keys else "(無 raw)")
        if not c.table_exists:
            verdict = "✗ cache 表不存在 → 補 rebuild 接線"
            missing_tbl.append(c.source)
            cache_rng = "—"
        else:
            cache_rng = f"{_fmt(c.cache_min, lay)}~{_fmt(c.cache_max, lay)}" if c.cache_keys else "(空)"
            if lay == "dump":
                verdict = "· 全史 dump(檔名≠事件日;cache==raw parse 已驗)"
            elif not c.raw_keys:
                verdict = "· 無 raw(純線上增量源)"
            elif not c.real_gap:
                extra = len(c.raw_keys - c.cache_keys)
                verdict = "✓ cache ⊇ raw 資料日" + (f"(+{extra} 假日檔已濾)" if extra else "")
            else:
                verdict = f"⚠ 缺 {len(c.real_gap)} 交易日 → 從 raw rebuild"
                need_rebuild.append((c.source, len(c.real_gap), sorted(c.real_gap)[:4], lay))
        print(f"  {c.source:30} {raw_rng:21} {cache_rng:21} {len(c.real_gap):>5}  {verdict}"
              + (f"  [假日 sentinel {c.sentinels}]" if c.sentinels else ""))
    print("\n=== 需動作(從既有 raw rebuild,不重抓)===")
    if not need_rebuild and not missing_tbl:
        print("  ✓ 全部結構化源 cache ⊇ raw 資料日,無缺口。")
    for s, n, sample, lay in need_rebuild:
        print(f"  ⚠ {s}: 缺 {n} 交易日(樣本 {[_fmt(k, lay) for k in sample]})")
    for s in missing_tbl:
        print(f"  ✗ {s}: raw 有但 cache 無表 → rebuild.py 補接線")


if __name__ == "__main__":
    main()
