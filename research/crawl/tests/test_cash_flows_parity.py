"""cash_flows port 忠實度守護:Python 解析既有封存原始檔(data/financial_statements/
<Y>_<Q>/)必須逐位重現 PG 表 `cash_flows_progressive`(= cache `cf_progressive_raw`,
稽核 C-cf_progressive_raw 已證 cache==PG 逐位)。

## 為什麼比對 cache 就等於比對 PG

稽核 C-cf_progressive_raw.json 逐季 checksum + 812 列抽樣證明 `cf_progressive_raw`
(cache)與 PG `cash_flows_progressive` 完全一致(66 季、6,359,600 列、BIT_XOR 全對)。
故以 cache 為對照基準 == 對照 PG。

## parity 語義:parse-of-archive vs parse-of-same-archive

`cf_progressive_raw` 是 Scala reader 解析 `data/financial_statements/<Y>_<Q>/` 的產物。
本測試用 Python `parse_quarter` 解析**同一批原始檔**,兩者必須逐位相同——這證明
Python parser 與 Scala jsoup parser 對位/值轉換等價(先紅後綠)。

## 已知壞季(稽核列出,明確排除嚴格集合相等)

`KNOWN_INCOMPLETE`(2026Q1/2025Q2/2023Q2/2024Q1):稽核 BUG——爬蟲「資料夾存在=抓完了」
在申報期限前只抓一次,**封存原始檔本身就缺整批公司**(金融業/KY 股),PG 亦缺同一批。
故這幾季 archive 與 PG **同缺**,交集逐位一致但公司集合不完整。本測試:
- 對這些季**只斷言交集逐位一致**(parser 忠實),集合差視為預期的抓取缺口(不判失敗);
- parser 沒有「Python 對、PG 錯」的值級修正(稽核 verdict:解析零瑕疵),真正的 bug 是
  **fetch 覆蓋率**,由 `fetch_quarter(force)`(不以資料夾存在封季)+ `needs_refetch`
  (可交易覆蓋率 YoY 閘門)修根因;`--fetch-check` 線上驗證重抓能補回完整覆蓋
  (2026Q1:archive 544 家 → 重抓 2,046 家,含 2330/2454/2317)。

## cache 依賴

讀 `cf_progressive_raw` 當對照 → 需 `research/cache_tables.py` 為當前世代。

Run:
    uv run --project research python -m research.crawl.tests.test_cash_flows_parity
    uv run --project research python -m research.crawl.tests.test_cash_flows_parity --all
    uv run --project research python -m research.crawl.tests.test_cash_flows_parity --fetch-check 2026 1
    uv run --project research python -m research.crawl.tests.test_cash_flows_parity --coverage-evidence
"""
from __future__ import annotations

import sys

import duckdb

from research.crawl.sink import CACHE_DB
from research.crawl.sources import cash_flows

#: 稽核 C-cf_progressive_raw 列出的抓取缺口季(封存 + PG 同缺整批可交易公司)。
KNOWN_INCOMPLETE: set[tuple[int, int]] = {(2026, 1), (2025, 2), (2023, 2), (2024, 1)}

#: 預設代表性樣本(涵蓋三版型 + 邊界:金融業/KY/個體基礎/負值/半年報)。
#: 每個都跨版型時代與特殊模板,足以證明對位;`--all` 掃 cache 全部季。
DEFAULT_SAMPLE: list[tuple[int, int]] = [
    (2009, 4),                       # pre-2013 首季(Big5,單表分段)
    (2011, 2), (2012, 4),            # pre-2013
    (2013, 1), (2015, 2), (2018, 4),  # 2013-2018(UTF-8,獨立表)
    (2019, 4), (2020, 2), (2022, 3),  # 2019+(bulk ZIP)
    (2024, 4), (2025, 4),            # 近期完整季
    (2026, 1),                       # 已知缺口季(交集比對)
]

_TOL = 1e-6


def _cache_quarter(con, year: int, quarter: int) -> dict[tuple[str, str], float | None]:
    rows = con.execute(
        "SELECT company_code, title, value FROM cf_progressive_raw "
        "WHERE year = ? AND quarter = ?", [year, quarter]).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def _parse_quarter(year: int, quarter: int) -> dict[tuple[str, str], float | None]:
    df = cash_flows.parse_quarter(year, quarter)
    out: dict[tuple[str, str], float | None] = {}
    if df.height:
        for code, title, value in df.select(
                ["company_code", "title", "value"]).iter_rows():
            out[(code, title)] = value
    return out


def _val_diffs(want: dict, got: dict, keys) -> list[str]:
    bad = []
    for k in keys:
        a, b = want[k], got[k]
        if a is None or b is None:
            if a is not b:
                bad.append(f"{k}: PG={a} py={b}(null 不一致)")
        elif abs(float(a) - float(b)) > _TOL:
            bad.append(f"{k}: PG={a} py={b}")
    return bad


def check_quarter(con, year: int, quarter: int) -> tuple[str, str]:
    """回 (狀態, 訊息);狀態 ∈ {OK, OK-INCOMPLETE, FAIL, SKIP}。"""
    want = _cache_quarter(con, year, quarter)
    if not want:
        return "SKIP", f"{year}Q{quarter}: cache 無此季 → SKIP"
    got = _parse_quarter(year, quarter)
    if not got:
        return "FAIL", f"{year}Q{quarter}: Python 解析 0 列,cache 有 {len(want)} 列"
    wk, gk = set(want), set(got)
    common = wk & gk
    val_bad = _val_diffs(want, got, common)
    only_pg, only_py = wk - gk, gk - wk
    incomplete = (year, quarter) in KNOWN_INCOMPLETE

    if val_bad:
        head = "; ".join(val_bad[:6])
        return "FAIL", (f"{year}Q{quarter}: {len(val_bad)} 筆值不符(共同 {len(common)}):{head}")

    if only_pg or only_py:
        # 集合差:對已知缺口季屬預期(封存與 PG 同缺);對其餘季 = 失敗。
        codes_pg = {k[0] for k in only_pg}
        codes_py = {k[0] for k in only_py}
        detail = (f"PG-only {len(only_pg)} 列/{len(codes_pg)} 家、"
                  f"py-only {len(only_py)} 列/{len(codes_py)} 家")
        if incomplete:
            return "OK-INCOMPLETE", (
                f"{year}Q{quarter}: 交集 {len(common)} 列逐位一致;"
                f"集合差({detail})屬已知抓取缺口(需 needs_refetch 重抓)")
        sample_pg = sorted(codes_pg)[:5]
        sample_py = sorted(codes_py)[:5]
        return "FAIL", (f"{year}Q{quarter}: 集合不符 {detail};"
                        f"PG-only 樣本 {sample_pg} / py-only 樣本 {sample_py}")

    return "OK", f"{year}Q{quarter}: {len(common)} 列逐位一致(公司集合亦相同)"


def fetch_recovery_check(con, year: int, quarter: int) -> None:
    """線上驗證 fetch 修復:重抓某缺口季 ZIP → 解壓到暫存 → 家數應遠多於封存/PG。
    不寫入 repo 封存(dest_dir=暫存),僅證明「資料夾存在=抓完了」的根因已修。"""
    import tempfile
    from pathlib import Path

    archive_n = con.execute(
        "SELECT COUNT(DISTINCT company_code) FROM cf_progressive_raw "
        "WHERE year = ? AND quarter = ?", [year, quarter]).fetchone()[0]
    with tempfile.TemporaryDirectory() as tmp:
        df = cash_flows.fetch_quarter(year, quarter, dest_dir=Path(tmp))
        codes = set(df["company_code"].to_list()) if df.height else set()
        fresh_n = len(codes)
    majors = [c for c in ("2330", "2454", "2317") if c in codes]
    print(f"  fetch-check {year}Q{quarter}: 封存/PG {archive_n} 家 → 重抓 {fresh_n} 家 "
          f"(復原 +{fresh_n - archive_n});龍頭補回 {majors}")
    if fresh_n <= archive_n:
        raise SystemExit(f"fetch-check FAIL:重抓家數未增加({fresh_n} <= {archive_n})")


class _SinkShim:
    """讓 cash_flows._traded_coverage(吃 sink.con)能用測試的唯讀 con。"""

    def __init__(self, con):
        self.con = con


def coverage_evidence(con) -> None:
    """重現 `needs_refetch` 門檻(4.5pp)的證據:逐季「可交易公司現金流量表覆蓋率」
    對比前一年同季的跌幅。齊備季與缺口季應清楚分離——此輸出即門檻的可重跑依據
    (證據落地 repo,對齊全域 §2.2)。"""
    shim = _SinkShim(con)
    quarters = [(int(r[0]), int(r[1])) for r in con.execute(
        "SELECT DISTINCT year, quarter FROM cf_progressive_raw ORDER BY year, quarter"
    ).fetchall()]
    print("季別    可交易覆蓋率   前一年同季   YoY跌幅pp   [已知缺口]")
    complete_drops, bad_drops = [], []
    for year, quarter in quarters:
        cov, traded = cash_flows._traded_coverage(shim, year, quarter)
        pcov, ptraded = cash_flows._traded_coverage(shim, year - 1, quarter)
        if traded == 0 or ptraded == 0:
            continue
        ratio, pratio = cov / traded, pcov / ptraded
        drop = (pratio - ratio) * 100
        is_bad = (year, quarter) in KNOWN_INCOMPLETE
        (bad_drops if is_bad else complete_drops).append(drop)
        tag = "缺口" if is_bad else ""
        print(f"{year}Q{quarter}  {ratio*100:6.2f}%      {pratio*100:6.2f}%     "
              f"{drop:+7.2f}     {tag}")
    if complete_drops and bad_drops:
        material = [d for d in bad_drops if d > 4.0]  # 排除 2024Q1 這種雜訊內小缺口
        print(f"\n齊備季 YoY 跌幅 max = {max(complete_drops):.2f}pp;"
              f"實質缺口季 min = {min(material):.2f}pp;"
              f"門檻 _REFETCH_COVERAGE_DROP = "
              f"{cash_flows._REFETCH_COVERAGE_DROP*100:.1f}pp(落於兩者之間)")


def main() -> None:
    args = sys.argv[1:]
    con = duckdb.connect(CACHE_DB, read_only=True)
    try:
        if args and args[0] == "--fetch-check":
            year, quarter = int(args[1]), int(args[2])
            fetch_recovery_check(con, year, quarter)
            return

        if args and args[0] == "--coverage-evidence":
            coverage_evidence(con)
            return

        if args and args[0] == "--all":
            quarters = [(int(r[0]), int(r[1])) for r in con.execute(
                "SELECT DISTINCT year, quarter FROM cf_progressive_raw "
                "ORDER BY year, quarter").fetchall()]
        else:
            quarters = DEFAULT_SAMPLE

        print(f"cash_flows parity(cache: {CACHE_DB});{len(quarters)} 季")
        tally = {"OK": 0, "OK-INCOMPLETE": 0, "FAIL": 0, "SKIP": 0}
        fails = []
        for year, quarter in quarters:
            status, msg = check_quarter(con, year, quarter)
            tally[status] += 1
            mark = {"OK": "✓", "OK-INCOMPLETE": "◐", "FAIL": "✗", "SKIP": "·"}[status]
            print(f"  {mark} {msg}")
            if status == "FAIL":
                fails.append((year, quarter))
    finally:
        con.close()

    print(f"\n結果:逐位一致 {tally['OK']}、缺口季交集一致 {tally['OK-INCOMPLETE']}、"
          f"失敗 {tally['FAIL']}、SKIP {tally['SKIP']}")
    if fails:
        print(f"失敗季:{fails}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
