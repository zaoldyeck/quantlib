"""treasury_stock_buyback port 忠實度守護:Python 解析既有封存原始檔
(data/treasury_stock_buyback/<market>/2026/2026_*.html)必須逐位重現 PG 表
`treasury_stock_buyback` 的**正確欄位**——並對稽核 C 列出的 4 個上游 bug 反向斷言
**Python 對、PG 錯**(先紅後綠)。

## 為什麼比對 PG(且 == 比對 cache)

直接 ATTACH PostgreSQL READ_ONLY 逐欄比對(task 明訂「與 PG 該表逐欄比對」)。稽核
C-treasury_stock_buyback 已證 cache 與 PG 全 2933 列、11 欄雙向 EXCEPT 零差異,故結論
同時適用 cache(`--cache` 對照同 11 欄)。PG 不可達 → SKIP。

## 快照端點語義:多檔 first-wins 重建 PG 的增量入庫

t35sc09 是**全史快照**端點(每個 2026_M.html 都含 2000~今全部宣告)。PG 由 Scala
`Main read buyback` 逐月 insert-only 累積:某鍵第一次被匯入的快照值入庫、之後不覆蓋。
故本測試以「各市場封存快照依 mtime 排序、逐檔 parse、first-wins 去重」重建 PG 的入庫
語義(`_first_wins`),與 PG 比對。實測:PG 2933 鍵**全部**被重建覆蓋(gk-pk=∅)。

## parity 語義:parse-of-archive vs PG(先紅後綠)

PG 是 Scala reader 解析**同一批**封存快照的產物。port 對每列「正確欄位」(市場、公告日、
代號、預定股數、價格上下限、執行起訖)須與 PG 逐位相同——證明對位/值轉換/市場分流/去重
與 Scala 等價。**先紅後綠**:若 port 沒修 4 個 bug,對應斷言會紅:
- 沒修 BUG#1(民國年 `yyy` 3 位)→ 2000-2010 恆缺 → 覆蓋斷言紅。
- 沒修 BUG#3(executed=cols[12])→ port 也全 0 → 與 PG 無分岔 → 分岔斷言紅。
- 沒修 BUG#2(pct=cols[16])→ port 也存金額 → pct>10 → 合法性斷言紅。
- 沒修 BUG#4(編碼)→ port 也亂碼 → U+FFFD 斷言紅。

## 稽核 C 的 4 個 bug:本 port 一次寫對,測試反向鎖 Python 對 / PG 錯

1. **BUG#1 2000-2010 整 11 年被丟(民國 2 位年 `yyy` 解析失敗)**。port 用
   `parse_minguo_slash`(int(y)+1911)吃任意位數 → 覆蓋 2000-2010。斷言:port 有
   >2700 列 announce_date<2011,PG 該區間 0 列(端到端:台泥 2008-11-12 port 有、PG 無)。
2. **BUG#2 pct_of_capital 存錯欄(cols[16]=已買回總金額,真值在 cols[18])**。斷言:
   port 每列 pct∈[0,100](法定 ≤10%);PG 2743 列 pct>10(億元級金額)。台泥 2019-05-10:
   port=0.15,PG=348,959,120。
3. **BUG#3 executed_shares 整欄全 0(cols[12] 空欄,真值在 cols[13])**。斷言:PG 全表
   executed=0;port >2000 列 executed>0。台泥 2019-05-10:port=8,000,000,PG=0。
4. **BUG#4 company_name 近九成亂碼(編碼)**。斷言:port 全表 0 個 U+FFFD;PG >2000 列
   含 U+FFFD。台泥:port='台泥',PG='�唳野'。

## 已排除的 9 列(PG 修正案凍結,非 port bug;task 要求「明確排除並註明」)

9 檔 2026 年買回宣告在 PG insert-only 首匯後被公司**變更**(展延執行期 ±1 日 / 調整價格
區間);PG 凍結首匯(某中繼快照)值,而該中繼快照已被月更覆蓋 → **現存任何封存快照都
重現不出 PG 的舊值**。逐檔實證(見 `_AMENDED`):9 鍵在 2026_3/4 皆 absent,2026_5+ 全部
一致為「變更後」值、與 PG 分岔。這是資料本身的更正,非解析差異,故 static 欄比對排除這 9
鍵;測試對每鍵**自我驗證**:PG 的舊值不被任何封存快照重現(證明是凍結非 port bug)。
其餘 2924 列 static 欄必須逐位一致。

Run:
    uv run --project research python -m research.crawl.tests.test_treasury_stock_buyback_parity
    uv run --project research python -m research.crawl.tests.test_treasury_stock_buyback_parity --cache
    uv run --project research python -m pytest research/crawl/tests/test_treasury_stock_buyback_parity.py -q
"""
from __future__ import annotations

import os
import sys
from datetime import date as Date
from pathlib import Path

import duckdb
import polars as pl

from research import paths
from research.crawl.parse import SchemaDrift
from research.crawl.sink import CACHE_DB
from research.crawl.sources import treasury_stock_buyback as tsb

#: static 欄:Scala 與 port 皆正確、應逐位一致(去 3 鍵 + 3 bug 欄 + name)。
_STATIC = ["planned_shares", "price_low", "price_high", "period_start", "period_end"]

#: 9 列 PG 凍結變更前值(非 port bug;逐檔實證見 module docstring)。allow-list:
#: 唯有這 9 鍵准許 static 分岔,其餘任何 static 不一致 → 硬失敗(擋 port 解析 bug)。
_AMENDED: set[tuple[str, Date, str]] = {
    ("tpex", Date(2026, 5, 12), "2718"),
    ("tpex", Date(2026, 5, 15), "6712"),
    ("twse", Date(2026, 5, 12), "3056"),
    ("twse", Date(2026, 5, 12), "6272"),
    ("twse", Date(2026, 5, 12), "8028"),
    ("twse", Date(2026, 5, 13), "6472"),
    ("twse", Date(2026, 5, 14), "6589"),
    ("twse", Date(2026, 5, 19), "2543"),
    ("twse", Date(2026, 7, 8), "2388"),
}
#: 端到端定樁:台泥 1101 2019-05-10(稽核 C 的逐欄鐵證行)。
_REF = ("twse", Date(2019, 5, 10), "1101")
_REF_PORT = {  # port 正確值(20 格資料列實測)
    "company_name": "台泥", "planned_shares": 10_000_000,
    "price_low": 29.75, "price_high": 63.62,
    "period_start": Date(2019, 5, 13), "period_end": Date(2019, 7, 12),
    "executed_shares": 8_000_000, "pct_of_capital": 0.15,
}
_REF_PG = {"executed_shares": 0, "pct_of_capital": 348_959_120.0}  # PG 錯值


# ---- 封存快照(依 mtime 排序,重建逐月 insert-only 入庫序)--------------------

def _snapshots() -> dict[str, list[tuple[str, dict[tuple, dict]]]]:
    """{market: [(fname, {key: rec}), ...]}(mtime 序)。每檔 parse 一次,供 first-wins
    重建 + 變更列自我驗證共用。"""
    out: dict[str, list[tuple[str, dict[tuple, dict]]]] = {}
    for market in tsb.MARKETS:
        root = paths.RAW / tsb.TABLE / market
        files = sorted((p for yd in root.glob("*") if yd.is_dir()
                        for p in yd.glob("*.html")),
                       key=lambda p: p.stat().st_mtime)
        snaps: list[tuple[str, dict[tuple, dict]]] = []
        for f in files:
            df = tsb.parse_raw(market, f.read_bytes())
            recs = {(r["market"], r["announce_date"], r["company_code"]): r
                    for r in df.to_dicts()}
            snaps.append((f.name, recs))
        out[market] = snaps
    return out


def _first_wins(snaps: dict[str, list[tuple[str, dict[tuple, dict]]]]) -> dict[tuple, dict]:
    """逐檔 first-wins 去重(對齊 Scala insert-only:首匯快照值入庫、之後不覆蓋)。"""
    port: dict[tuple, dict] = {}
    for market in tsb.MARKETS:
        for _, recs in snaps[market]:
            for k, r in recs.items():
                port.setdefault(k, r)
    return port


def _pg_value_in_any_snapshot(snaps, market: str, k: tuple, pg_rec: dict) -> bool:
    """PG 的 static 值是否被該市場任一封存快照重現(用於證明變更列 = 凍結非 port bug)。"""
    for _, recs in snaps[market]:
        r = recs.get(k)
        if r is not None and all(r[c] == pg_rec[c] for c in _STATIC):
            return True
    return False


# ---- 對照來源:PG(預設)或 cache(--cache)----------------------------------

def _load_pg(use_cache: bool) -> dict[tuple, dict]:
    con = duckdb.connect()
    if use_cache:
        con.execute(f"ATTACH '{CACHE_DB}' AS src_db (READ_ONLY)")
        table = "src_db.treasury_stock_buyback"
    else:
        user = os.environ.get("USER", "zaoldyeck")
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH 'host=localhost port=5432 dbname=quantlib user={user}' "
                    "AS src_db (TYPE postgres, READ_ONLY)")
        table = "src_db.public.treasury_stock_buyback"
    try:
        rows = con.execute(f"SELECT {','.join(tsb.CACHE_COLS)} FROM {table}").fetchall()
    finally:
        con.close()
    out: dict[tuple, dict] = {}
    for r in rows:
        rec = dict(zip(tsb.CACHE_COLS, r))
        out[(rec["market"], rec["announce_date"], rec["company_code"])] = rec
    return out


# ---- parity 主體 -----------------------------------------------------------

def run(mode: str) -> tuple[list[str], dict]:
    """回 (失敗訊息, 統計)。逐位 static parity + 4 bug 反向斷言 + 9 變更列排除自驗。"""
    snaps = _snapshots()
    port = _first_wins(snaps)
    pg = _load_pg(mode == "--cache")
    pk, gk = set(port), set(pg)
    fails: list[str] = []

    # ── 1. 鍵集覆蓋 + BUG#1(2000-2010 找回)──────────────────────────────
    missing = gk - pk
    if missing:
        fails.append(f"PG {len(missing)} 鍵未被封存快照重建(first-wins 漏?):"
                     f"{sorted(missing)[:5]}")
    pre_port = sum(1 for k in pk if k[1].year < 2011)
    pre_pg = sum(1 for k in gk if k[1].year < 2011)
    if pre_port < 2700:
        fails.append(f"BUG#1 未修:port 僅 {pre_port} 列 announce<2011(應 >2700,2000-2010)")
    if pre_pg != 0:
        fails.append(f"BUG#1 前提破:PG 有 {pre_pg} 列 announce<2011(應 0,證明 Scala 丟光)")
    if _REF[:1] and ("twse", Date(2008, 11, 12), "1101") not in pk:
        fails.append("BUG#1 端到端:port 缺台泥 2008-11-12(民國 97 應被找回)")

    # ── 2. static 逐位 parity(排除 9 變更列,並自我驗證排除正當)──────────
    inter = pk & gk
    static_ok = 0
    for k in sorted(inter):
        p, g = port[k], pg[k]
        diffs = [c for c in _STATIC if p[c] != g[c]]
        if not diffs:
            static_ok += 1
            continue
        if k in _AMENDED:
            # 自我驗證:PG 舊值不被任何封存快照重現 → 凍結變更、非 port bug。
            if _pg_value_in_any_snapshot(snaps, k[0], k, g):
                fails.append(f"{k} 列在 _AMENDED 但 PG 值被封存快照重現 → 應是 port 去重/解析 "
                             f"bug 非變更(diffs={diffs})")
        else:
            fails.append(f"{k} static 分岔(非變更列):"
                         + ";".join(f"{c} PG={g[c]!r} port={p[c]!r}" for c in diffs))
    # _AMENDED 內每鍵確須存在於交集(否則 stale 清單)
    for k in _AMENDED:
        if k not in inter:
            fails.append(f"_AMENDED 列 {k} 不在 PG∩port 交集(清單過時?)")

    # ── 3. BUG#3 executed_shares:PG 全 0 / port 有值(Python 對、PG 錯)────
    pg_exec_nz = sum(1 for g in pg.values() if g["executed_shares"] != 0)
    port_exec_nz = sum(1 for p in port.values() if p["executed_shares"] != 0)
    if pg_exec_nz != 0:
        fails.append(f"BUG#3 前提破:PG executed_shares 非全 0(nz={pg_exec_nz})")
    if port_exec_nz < 2000:
        fails.append(f"BUG#3 未修:port executed>0 僅 {port_exec_nz} 列(應 >2000,讀 cols[13])")

    # ── 4. BUG#2 pct_of_capital:port 合法比例 / PG 億元金額 ───────────────
    port_pct_bad = [k for k in pk if not (0.0 <= port[k]["pct_of_capital"] <= 100.0)]
    if port_pct_bad:
        fails.append(f"BUG#2 未修:port {len(port_pct_bad)} 列 pct∉[0,100]"
                     f"(仍讀到金額?){port_pct_bad[:3]}")
    pg_pct_gt10 = sum(1 for g in pg.values() if g["pct_of_capital"] > 10.0)
    if pg_pct_gt10 < 2000:
        fails.append(f"BUG#2 前提破:PG pct>10 僅 {pg_pct_gt10} 列(應 >2000=裝金額)")

    # ── 5. BUG#4 company_name:port 全乾淨 / PG 亂碼 ──────────────────────
    port_fffd = sum(1 for p in port.values() if "�" in p["company_name"])
    pg_fffd = sum(1 for g in pg.values() if "�" in g["company_name"])
    if port_fffd != 0:
        fails.append(f"BUG#4 未修:port {port_fffd} 列 company_name 含 U+FFFD(編碼未修對)")
    if pg_fffd < 2000:
        fails.append(f"BUG#4 前提破:PG U+FFFD 僅 {pg_fffd} 列(應 >2000)")

    # ── 6. 端到端定樁:台泥 2019-05-10(static 全同 / 3 bug 欄分岔)────────
    if _REF in inter:
        p, g = port[_REF], pg[_REF]
        for c, v in _REF_PORT.items():
            if p[c] != v:
                fails.append(f"REF 台泥2019 port.{c}={p[c]!r} 應={v!r}")
        for c in _STATIC:  # static 欄 PG 應與 port 同(這列非變更)
            if g[c] != _REF_PORT[c]:
                fails.append(f"REF 台泥2019 PG.{c}={g[c]!r} 應={_REF_PORT[c]!r}(static 應同)")
        for c, v in _REF_PG.items():  # 3 bug 欄 PG 錯值
            if g[c] != v:
                fails.append(f"REF 台泥2019 PG.{c}={g[c]!r} 應={v!r}(PG 錯值定樁)")
        if "�" not in g["company_name"]:
            fails.append(f"REF 台泥2019 PG.company_name={g['company_name']!r} 應含 U+FFFD")
    else:
        fails.append(f"REF {_REF} 不在交集(封存快照缺?)")

    stats = dict(port=len(pk), pg=len(gk), inter=len(inter), static_ok=static_ok,
                 amended=len(_AMENDED), pre_port=pre_port, pg_exec_nz=pg_exec_nz,
                 port_exec_nz=port_exec_nz, pg_pct_gt10=pg_pct_gt10,
                 port_fffd=port_fffd, pg_fffd=pg_fffd)
    return fails, stats


# ---- 定樁:型別 / 表頭守衛(合成表負向測試)--------------------------------

def _check_dtypes() -> tuple[bool, str]:
    """兩股數欄 Int64(大型股 planned/executed 可 > 2^31);_long 純 Python int 無溢位。"""
    f = paths.RAW / tsb.TABLE / "twse" / "2026" / "2026_7.html"
    if not f.exists():
        return True, "型別定樁:找不到 twse/2026/2026_7.html → SKIP"
    df = tsb.parse_raw("twse", f.read_bytes())
    for col in ("planned_shares", "executed_shares"):
        if df.schema[col] != pl.Int64:
            return False, f"型別定樁:{col} dtype={df.schema[col]}(應 Int64)"
    if tsb._long("3,000,000,000") != 3_000_000_000:
        return False, "型別定樁:_long 3,000,000,000 溢位"
    return True, "型別定樁:planned/executed = Int64、3e9 無溢位"


def _synthetic(*, drop_top: bool = False, shift_kw: tuple[str, str] | None = None,
               bad_cells: bool = False) -> bytes:
    """合成一張 t35sc09 表(2 列表頭 18+4、1 資料列 20 格,台泥 2019 值)供守衛負向測試。"""
    top_cells = ["序號", "公司代號", "公司名稱", "董事會決議日期", "買回目的",
                 "買回股份總金額上限", "預定買回股數", "買回價格區間", "預定買回期間",
                 "是否執行完畢", "買回達一定標準資料", "本次已買回股數",
                 "本次執行完畢已註銷或轉讓股數", "本次已買回股數佔預定買回股數比例",
                 "本次已買回總金額", "本次平均每股買回價格",
                 "本次買回股數佔公司已發行股份總數比例", "本次未執行完畢之原因"]
    if drop_top:
        top_cells[1] = "XXX"  # 抹掉『公司代號』→ 頂表頭找不到
    if shift_kw:
        top_cells = [c.replace(*shift_kw) for c in top_cells]
    top = "".join(f"<th>{c}</th>" for c in top_cells)
    sub = "".join(f"<th>{c}</th>" for c in ("最低", "最高", "起", "迄"))
    data_cells = ["1", "1101", "台泥", "108/05/10", "1", "78,440,337,000", "10,000,000",
                  "29.75", "63.62", "108/05/13", "108/07/12", "Y", "", "8,000,000",
                  "8,000,000", "80.00", "348,959,120", "43.62", "0.15", "原因"]
    if bad_cells:
        data_cells = data_cells[:-1]  # 19 格 → 版型漂移
    data = "".join(f"<td>{c}</td>" for c in data_cells)
    pad = "<!-- " + "x" * 1200 + " -->"  # 破 parse_raw 的 <1024 bytes 門檻
    html = (f"<html><body>{pad}<table class='hasBorder'>"
            f"<tr>{top}</tr><tr>{sub}</tr><tr>{data}</tr></table></body></html>")
    return html.encode("utf-8")


def _check_guards() -> tuple[bool, str]:
    # 正向:合成表 → 1 列,executed=cols[13]=8,000,000、pct=cols[18]=0.15。
    df = tsb.parse_raw("twse", _synthetic())
    if df.height != 1:
        return False, f"守衛正向:合成表 height={df.height}"
    r = df.to_dicts()[0]
    if r["executed_shares"] != 8_000_000 or r["pct_of_capital"] != 0.15:
        return False, (f"守衛正向:executed={r['executed_shares']}(應 8,000,000)"
                       f" pct={r['pct_of_capital']}(應 0.15)—— cols 對位錯?")
    if r["announce_date"] != Date(2019, 5, 10) or r["planned_shares"] != 10_000_000:
        return False, f"守衛正向:announce/planned 錯 {r['announce_date']}/{r['planned_shares']}"
    # 負向:缺頂表頭 / 頂表頭關鍵欄位移 / 資料列非 20 格 → SchemaDrift fail-loud。
    for kind, raw in (
        ("缺頂表頭", _synthetic(drop_top=True)),
        ("關鍵欄位移(已買回總金額)", _synthetic(shift_kw=("本次已買回總金額", "XXX"))),
        ("佔已發行比例位移", _synthetic(shift_kw=("本次買回股數佔公司已發行股份總數比例", "XXX"))),
        ("資料列非20格", _synthetic(bad_cells=True)),
    ):
        try:
            tsb.parse_raw("twse", raw)
            return False, f"守衛:{kind} 未 fail-loud"
        except SchemaDrift:
            pass
    return True, ("守衛:合成表 executed=cols[13]/pct=cols[18] 對位正確;缺表頭/關鍵欄位移/"
                  "資料列非20格 → SchemaDrift fail-loud")


# ---- CLI 入口 --------------------------------------------------------------

def main() -> None:
    mode = next((a for a in sys.argv[1:] if a == "--cache"), "")
    print(f"treasury_stock_buyback parity 對照 {'cache' if mode else 'PG'}(11 欄);"
          f"排除 {len(_AMENDED)} 變更列(PG 凍結首匯值、封存快照不重現;逐鍵自驗)")
    fails, stats = run(mode)
    print(f"  port {stats['port']} 列 / PG {stats['pg']} 列 / 交集 {stats['inter']};"
          f"static 逐位一致 {stats['static_ok']} 列(+{stats['amended']} 變更列排除)")
    print(f"  BUG#1 找回 announce<2011: port {stats['pre_port']} 列 / PG 0 列")
    print(f"  BUG#3 executed>0: port {stats['port_exec_nz']} 列 / PG 非零 {stats['pg_exec_nz']} 列")
    print(f"  BUG#2 pct>10: PG {stats['pg_pct_gt10']} 列(裝金額)/ port 皆合法")
    print(f"  BUG#4 U+FFFD: PG {stats['pg_fffd']} 列 / port {stats['port_fffd']} 列")
    ok_t, msg_t = _check_dtypes()
    ok_g, msg_g = _check_guards()
    print(f"  {'✓' if ok_t else '✗'} {msg_t}")
    print(f"  {'✓' if ok_g else '✗'} {msg_g}")
    if fails or not ok_t or not ok_g:
        for f in fails[:20]:
            print("  ✗", f)
        raise SystemExit(1)
    print("  ✓ 全數逐位一致(4 bug 反向斷言 Python 對 / PG 錯)")


# ---- pytest 入口(離線:讀本機 PG + 封存;PG 不可達或無封存 → skip)----------

def test_treasury_stock_buyback_parity() -> None:
    import pytest

    if not (paths.RAW / tsb.TABLE).exists():
        pytest.skip("無 treasury_stock_buyback 封存原始檔")
    try:
        fails, stats = run("")
    except duckdb.Error as exc:  # PG 不可達
        pytest.skip(f"PG 不可達,parity 需對照 PG:{exc}")
    ok_t, msg_t = _check_dtypes()
    ok_g, msg_g = _check_guards()
    assert not fails, f"{len(fails)} 項 parity 失敗:{fails[:8]}"
    assert stats["static_ok"] >= 2900, f"static 逐位一致僅 {stats['static_ok']} 列(應 ~2924)"
    assert stats["pre_port"] > 2700, f"BUG#1 找回僅 {stats['pre_port']} 列"
    assert ok_t, msg_t
    assert ok_g, msg_g


def test_treasury_stock_buyback_guards_offline() -> None:
    """守衛/型別定樁不需 PG(純合成表 + 讀封存),PG 不可達也能跑。"""
    ok_g, msg_g = _check_guards()
    assert ok_g, msg_g


if __name__ == "__main__":
    main()
