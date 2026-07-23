"""index port 忠實度守護:Python 獨立解析既有封存原始檔
(data/index/<market>/<year>/<y>_<m>_<d>.csv)必須逐位重現 PG `index`
(Scala `TradingReader.readIndex` 的產物)——**乾淨日六欄逐格一致**,稽核列出的
**壞日期明確排除並註明**,port 修好而 PG 錯的 row 斷言「Python 對、PG 錯」。

## 對照基準 = PG(非 cache)

cache `market_index` 與 PG `index` 已由稽核 C-market_index 證實逐位相同(769,751 列
三重指紋零差異),但唯一鍵語義最完整的是 PG。用 DuckDB `ATTACH … (TYPE postgres,
READ_ONLY)`(與稽核 A/C 腳本、margin port 測試同法),零寫入。

## parity 方法:三方位置對齊(PG ORDER BY id = 檔案解析順序)

Scala `++= data` 依解析順序批次插入、`id` 自增,故 PG `ORDER BY id` **精確重現原始檔
由上而下的解析順序**(已驗:tpex 2022-01-13 id 序 = 價格區→報酬區,與 raw 一致)。
TPEx 報酬區指數名 port 修好後與 PG 不同(不能以 name join),故一律**位置對齊**,並以
close 作對齊校驗(close 不受任何修復影響、逐格恆等)。

本測試從 raw **獨立**重算兩份(不呼叫 Scala):
  · `scala_expected`:忠實重演 Scala readIndex(含 bug:change/pct 歸零、報酬報酬、name 去空白)
  · `port_expected`:本 port 規格(修好上述 bug)
再驗:
  (1) scala_expected == PG（逐位、位置對齊)          → 證「PG = Scala 之產物」且我對 Scala 的理解正確
  (2) index.parse(...) == port_expected（逐位、位置對齊）→ 證 port 模組實作 == 規格
  (3) 修復不變式:change_port≠change_scala ⟹ change_scala==0;pct 同理(證修的正是「歸零」bug)
  (4) close 三方恆等(port==scala==PG)                → 證數值主線未被任何修復動到

## 稽核壞日期的處理(docs/data_audit/_done/{A,C}-index.json)

- **8 個 twse body 汙染日(947 列,整片是別天資料)**:原始檔檔頭日期是對的、body 卻是別天
  (TWSE 靜默 fallback body 汙染;3 天是週六幽靈、2019-07-05 前視)。單檔解析抓不到,Scala 與
  port **都忠實搬運同一份壞 raw → port==PG**。列為 CONTAM:確認 port==PG(parser 忠實),但
  此日**資料本身不可信**,已排除於乾淨 tally 並註明需整合層跨日守護(close−change ?= 前一交易日
  close)。
- **舊名冊半殘日 twse 2026-02-26 / 03-11(135 列)**:值正確、只是名冊是舊的(電子類指數 vs
  電子工業類指數)。port 忠實重現 raw==PG → 正常通過(缺 160 檔現行指數是缺料、非解析錯)。
- **缺日**:tpex 2024-06-27~08-12(連檔案都沒有)、twse 0-byte 2009-12-12 / 2026-03-12——
  原始檔不存在或空,port/PG 皆 0 列 → SKIP。屬整合層補抓職責。

Run:
    uv run --project research python -m research.crawl.tests.test_index_parity
    uv run --project research python -m research.crawl.tests.test_index_parity --full
    uv run --project research python -m research.crawl.tests.test_index_parity 2022-01-13
"""
from __future__ import annotations

import csv
import io
import sys
from dataclasses import dataclass
from datetime import date as Date

import duckdb

from research.crawl import archive
from research.crawl.sources import index as ix
from research.db import DEFAULT_DSN

TABLE = "index"          # PG 表名(cache 表名為 market_index)


# --------------------------------------------------------------------------- #
# 稽核壞日期集合(A/C-index.json)                                               #
# --------------------------------------------------------------------------- #
#: 8 個 twse body 汙染日(整片別天資料;port==PG 但資料不可信 → 排除乾淨 tally)。
_CONTAM_TWSE = {Date(2015, 8, 29), Date(2016, 5, 26), Date(2017, 8, 2),
                Date(2018, 8, 4), Date(2018, 9, 15), Date(2018, 10, 3),
                Date(2019, 7, 5), Date(2019, 9, 25)}


# --------------------------------------------------------------------------- #
# 值轉換(獨立於 port 模組的複本;供 scala 與 port 兩份期望共用)                 #
# --------------------------------------------------------------------------- #
def _num(cell: str) -> float | None:
    c = cell.replace(",", "").replace(" ", "").strip()
    if c in ("", "--", "---", "----"):
        return None
    try:
        return float(c)
    except ValueError:
        return None


def _scala_pct(cell: str) -> float:
    """Scala `values(N).toDoubleOption.getOrElse(0)`。"""
    v = _num(cell)
    return 0.0 if v is None else v


def _scala_twse_change(sign: str, mag: str) -> float:
    """Scala:`case "-" => Try(-mag).getOrElse(0); case "" => 0; case "+" => Try(mag).getOrElse(0)`。"""
    m = _num(mag)
    s = sign.strip()
    if s == "-":
        return 0.0 if m is None else -m
    if s == "+":
        return 0.0 if m is None else m
    if s == "":
        return 0.0
    raise AssertionError(f"scala replica:未預期方向 {sign!r}(Scala 會 MatchError)")


def _scala_tpex_change(cell: str) -> float:
    """Scala `values(2).toDouble`(無 getOrElse;全史零失敗)。"""
    v = _num(cell)
    if v is None:
        raise AssertionError(f"scala replica:tpex change 無法解析 {cell!r}(Scala 會 throw)")
    return v


# --------------------------------------------------------------------------- #
# 獨立重解析:每列同時算 scala(含 bug)與 port(修好)兩份                        #
# --------------------------------------------------------------------------- #
@dataclass
class Row:
    name_scala: str
    name_port: str
    close: float | None
    change_scala: float
    change_port: float | None
    pct_scala: float
    pct_port: float | None


def _extract_twse(text: str, day: Date) -> list[Row]:
    rows = list(csv.reader(io.StringIO("\n".join(ix._twse_data_lines(text)))))
    out: list[Row] = []
    for r in rows:
        if len(r) not in (6, 7):
            continue
        name = r[0].strip()
        if name in ("指數", "報酬指數"):
            continue
        name_scala = name.replace(" ", "").replace(",", "")   # Scala 對所有欄去空白/逗號
        if name_scala == "null":                              # Scala filterNot _._3=="null"
            continue
        sign, mag, pct = r[2], r[3], r[4]
        out.append(Row(
            name_scala=name_scala, name_port=name, close=_num(r[1]),
            change_scala=_scala_twse_change(sign, mag),
            change_port=ix._twse_change(sign, mag),
            pct_scala=_scala_pct(pct), pct_port=_num(pct),
        ))
    return out


def _extract_tpex(text: str, day: Date) -> list[Row]:
    four = [r for r in csv.reader(io.StringIO(text)) if len(r) == 4]
    if not four:
        return []
    ridx = next((i for i, r in enumerate(four) if r[0].strip() == "報酬指數"), None)
    if ridx is None:
        price, ret = four[1:], []
    else:
        price, ret = four[1:ridx], four[ridx + 1:]

    out: list[Row] = []

    def add(r: list[str], name_scala: str, name_port: str) -> None:
        out.append(Row(
            name_scala=name_scala, name_port=name_port, close=_num(r[1]),
            change_scala=_scala_tpex_change(r[2]), change_port=_num(r[2]),
            pct_scala=_scala_pct(r[3]), pct_port=_num(r[3]),
        ))

    for r in price:
        raw = r[0].strip()
        add(r, raw.replace(" ", "").replace(",", ""), raw)
    for r in ret:
        raw = r[0].strip()
        cleaned = raw.replace(" ", "").replace(",", "")
        add(r, cleaned.replace("指數", "") + "報酬指數", ix._return_name(raw))
    # 改名後才判 'null'(順序同 Scala);scala 與 port 的 'null' 判定在此 corpus 一致。
    return [x for x in out if x.name_scala != "null"]


def _extract(market: str, text: str, day: Date) -> list[Row]:
    return _extract_twse(text, day) if market == "twse" else _extract_tpex(text, day)


# --------------------------------------------------------------------------- #
def _feq(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= 1e-9


def _connect():
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{DEFAULT_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    return con


def _pg_ordered(con, market: str, day: Date) -> list[dict]:
    """PG 該日全列,ORDER BY id(= 原始檔解析順序)。"""
    return con.execute(
        f'SELECT name, close, change, "change(%)" AS change_pct '
        f"FROM pg.public.{TABLE} WHERE market = ? AND date = ? ORDER BY id",
        [market, day]).pl().to_dicts()


def _port_ordered(market: str, day: Date) -> list[dict] | None:
    p = archive.raw_path("index", market, day)
    if not p.exists() or p.stat().st_size <= 1024:   # 比照 readIndex 的 >1024 濾網
        return None
    text = p.read_bytes().decode("Big5-HKSCS", errors="replace")
    df = ix.parse(market, text, day)
    return [] if df is None else df.to_dicts()


@dataclass
class Stats:
    change_fix: int = 0
    pct_fix: int = 0
    name_space_fix: int = 0
    name_return_fix: int = 0
    rows: int = 0


def _eval(market: str, day: Date, exp: list[Row], pg: list[dict],
          port: list[dict], st: Stats) -> tuple[str, str]:
    """name-key 對齊 + 修復不變式(純比對,pg/port 已就位)。回 (status, msg)。

    以 name 為 join key(非位置):PG (market,date,name) 唯一,且 PG 存的正是 Scala 產出的
    (含 bug)名稱 = 本測試的 `name_scala`;port 模組產出的是修好的 `name_port`。故:
      scala_expected ↔ PG 以 name_scala join;  index.parse ↔ port_expected 以 name_port join。
    (不用 PG `id` 位置:實測 tpex 2016-01-04 的 id 序與解析序不符——PG 插入序非解析序的
    歷史遺留;name-join 對此免疫,且才是 (market,date,name) 唯一鍵的正確 parity 語義。)
    """
    if not exp and not pg:
        return "SKIP", f"{market} {day}: 兩邊皆 0 列 → SKIP"

    exp_by_scala = {e.name_scala: e for e in exp}
    exp_by_port = {e.name_port: e for e in exp}
    if len(exp_by_scala) != len(exp) or len(exp_by_port) != len(exp):
        return "FAIL", f"{market} {day}: 重解析內部 name 重複(scala/port 唯一性破裂)"

    # (1) scala_expected == PG(name join、逐格)
    pg_by_name = {g["name"]: g for g in pg}
    if len(pg_by_name) != len(pg):
        return "FAIL", f"{market} {day}: PG 內 name 重複 {len(pg)} vs {len(pg_by_name)}"
    if set(exp_by_scala) != set(pg_by_name):
        only_s = sorted(set(exp_by_scala) - set(pg_by_name))[:5]
        only_p = sorted(set(pg_by_name) - set(exp_by_scala))[:5]
        return "FAIL", (f"{market} {day}: 名稱集合不符 scala-only {only_s} / PG-only {only_p} "
                        f"(scala {len(exp)} / PG {len(pg)})")
    for name, e in exp_by_scala.items():
        g = pg_by_name[name]
        if not _feq(e.close, g["close"]):
            return "FAIL", f"{market} {day} {name}: close scala={e.close} ≠ PG={g['close']}"
        if not _feq(e.change_scala, g["change"]):
            return "FAIL", f"{market} {day} {name}: change scala={e.change_scala} ≠ PG={g['change']}"
        if not _feq(e.pct_scala, g["change_pct"]):
            return "FAIL", f"{market} {day} {name}: pct scala={e.pct_scala} ≠ PG={g['change_pct']}"

    # (2) index.parse(...) == port_expected(name join、逐格)
    port_by_name = {pr["name"]: pr for pr in port}
    if len(port_by_name) != len(port):
        return "FAIL", f"{market} {day}: port 模組內 name 重複"
    if set(exp_by_port) != set(port_by_name):
        only_e = sorted(set(exp_by_port) - set(port_by_name))[:5]
        only_m = sorted(set(port_by_name) - set(exp_by_port))[:5]
        return "FAIL", f"{market} {day}: 模組 vs 規格 名稱集合不符 規格-only {only_e} / 模組-only {only_m}"
    for name, e in exp_by_port.items():
        pr = port_by_name[name]
        if not (_feq(pr["close"], e.close) and _feq(pr["change"], e.change_port)
                and _feq(pr["change_pct"], e.pct_port)):
            return "FAIL", (f"{market} {day} {name}: 模組=({pr['close']},{pr['change']},{pr['change_pct']}) "
                            f"≠ 規格=({e.close},{e.change_port},{e.pct_port})")

    # (3)+(4) 修復不變式 + close 恆等,並累計「port 對、PG 錯」統計
    for e in exp:
        st.rows += 1
        if not _feq(e.change_port, e.change_scala):
            if e.change_scala != 0.0:
                return "FAIL", (f"{market} {day} {e.name_scala}: change 修復但 PG≠0 "
                                f"(scala={e.change_scala} port={e.change_port})——非歸零 bug")
            st.change_fix += 1
        if not _feq(e.pct_port, e.pct_scala):
            if e.pct_scala != 0.0:
                return "FAIL", (f"{market} {day} {e.name_scala}: pct 修復但 PG≠0 "
                                f"(scala={e.pct_scala} port={e.pct_port})——非歸零 bug")
            st.pct_fix += 1
        if e.name_port != e.name_scala:
            if " " in e.name_port and " " not in e.name_scala:
                st.name_space_fix += 1
            else:
                st.name_return_fix += 1        # 報酬區改名(去『報酬報酬』/單次加報酬)

    return "OK", (f"{market} {day}: {len(pg)} 列 name-key 逐格一致 "
                  f"(scala==PG;模組==規格;close 三方恆等)")


def _check_day(con, market: str, day: Date, st: Stats) -> tuple[str, str]:
    """單日 parity:讀封存 raw → 重解析 + 查 PG → `_eval`。"""
    p = archive.raw_path("index", market, day)
    if not p.exists() or p.stat().st_size <= 1024:
        return "SKIP", f"{market} {day}: 無原始檔 / ≤1024B(header-only/sentinel) → SKIP"
    text = p.read_bytes().decode("Big5-HKSCS", errors="replace")
    exp = _extract(market, text, day)
    pg = _pg_ordered(con, market, day)
    port = _port_ordered(market, day) or []
    return _eval(market, day, exp, pg, port, st)


def _check_contam(con, day: Date, st: Stats) -> tuple[str, str]:
    """CONTAM:確認 port==PG(parser 忠實搬運同一份壞 raw),但資料不可信 → 排除乾淨 tally。"""
    status, msg = _check_day(con, "twse", day, st)
    if status == "SKIP":
        return "SKIP", f"twse {day}: CONTAM 但無檔 → SKIP"
    if status != "OK":
        return "FAIL", f"twse {day}: CONTAM 期望 port 忠實==PG 卻不符 → {msg}"
    return "CONTAM", (f"twse {day}: port 與 PG 逐位一致(都忠實搬運同一份被汙染的 raw);"
                      f"此日資料本身不可信(整片別天),已列冊排除,需整合層跨日守護")


# --------------------------------------------------------------------------- #
# 代表性樣本(kind:CLEAN=乾淨逐位、CONTAM=汙染排除)                             #
# --------------------------------------------------------------------------- #
SAMPLE: list[tuple[str, str, str]] = [
    ("twse", "2009-01-05", "CLEAN"),   # 最舊(上漲日 +)
    ("twse", "2013-06-14", "CLEAN"),   # 稽核抽樣日
    ("twse", "2026-07-09", "CLEAN"),   # 近期(含 報酬指數『--』未公布 → change/pct 歸零修復)
    ("twse", "2026-07-17", "CLEAN"),   # 最新齊備日
    ("twse", "2026-02-26", "CLEAN"),   # 舊名冊半殘日(135 列;值正確、忠實重現)
    ("tpex", "2016-01-04", "CLEAN"),   # 最舊 tpex
    ("tpex", "2019-11-07", "CLEAN"),   # 稽核抽樣日
    ("tpex", "2022-01-13", "CLEAN"),   # 報酬報酬 + Quality 50 空白 + null 名(三修一次到位)
    ("tpex", "2026-07-09", "CLEAN"),   # 近期
    ("tpex", "2026-07-17", "CLEAN"),   # 最新
    ("twse", "2016-05-26", "CONTAM"),  # body 汙染(= 2016-01-18);raw 檔頭日期卻是對的
    ("twse", "2019-07-05", "CONTAM"),  # body 汙染 + 前視(= 2019-07-16)
]

#: 具體 先紅後綠 錨(port 對、PG 錯):(market, iso, name_port, 期望 close/change/pct)。
FIX_ANCHORS = [
    # twse 未公布 報酬指數:PG change/pct=0(假平盤),port=None(未公布)。
    ("twse", "2026-07-09", "臺灣50報酬指數", None, None, None),
    # tpex 報酬區已自帶『報酬指數』:PG 疊成『報酬報酬』,port 保留單次 + 值不動。
    ("tpex", "2022-01-13", "櫃買半導體領航報酬指數", 10570.30, -12.95, -0.12),
    # tpex 名稱含空白:PG 去空白,port 保留『Quality 50報酬指數』。
    ("tpex", "2022-01-13", "Quality 50報酬指數", 8873.74, -26.39, -0.30),
]


def _check_anchor(market: str, iso: str, name: str,
                  close, change, pct) -> tuple[str, str]:
    day = Date.fromisoformat(iso)
    port = _port_ordered(market, day) or []
    hit = [r for r in port if r["name"] == name]
    if not hit:
        return "FAIL", f"{market} {iso}: port 查無指數名 {name!r}(改名修復失效?)"
    r = hit[0]
    if not (_feq(r["close"], close) and _feq(r["change"], change) and _feq(r["change_pct"], pct)):
        return "FAIL", (f"{market} {iso} {name}: port=({r['close']},{r['change']},{r['change_pct']}) "
                        f"≠ 期望 ({close},{change},{pct})")
    return "OK", f"{market} {iso} {name}: port=({close},{change},{pct}) ✓(PG 為 bug 值)"


# --------------------------------------------------------------------------- #
def _market_years(market: str):
    root = archive.raw_path("index", market, Date(2000, 1, 1)).parent.parent
    years = sorted(int(p.name) for p in root.iterdir()
                   if p.is_dir() and p.name.isdigit())
    return root, years


def _year_days(root, year: int):
    for f in sorted((root / f"{year:04d}").glob("*.csv")):
        parts = f.stem.split("_")
        if len(parts) != 3:
            continue
        try:
            yield Date(int(parts[0]), int(parts[1]), int(parts[2]))
        except ValueError:
            continue


def _pg_year(con, market: str, year: int) -> dict[Date, list[dict]]:
    """一次拉整年 PG 列,按 date 分組(避免逐檔往返 PG)。"""
    df = con.execute(
        f'SELECT date, name, close, change, "change(%)" AS change_pct '
        f"FROM pg.public.{TABLE} WHERE market = ? AND date >= ? AND date < ?",
        [market, Date(year, 1, 1), Date(year + 1, 1, 1)]).pl()
    out: dict[Date, list[dict]] = {}
    for r in df.to_dicts():
        out.setdefault(r["date"], []).append(r)
    return out


def check_full(con) -> tuple[int, int, int, int, Stats, list[str]]:
    ok = skip = fail = contam = 0
    st = Stats()
    fails: list[str] = []
    for market in ix.MARKETS:
        root, years = _market_years(market)
        n = 0
        for year in years:
            pg_year = _pg_year(con, market, year)
            for day in _year_days(root, year):
                if market == "twse" and day in _CONTAM_TWSE:
                    status, _ = _check_contam(con, day, st)
                    contam += (status == "CONTAM")
                    skip += (status == "SKIP")
                    if status == "FAIL":
                        fail += 1
                        fails.append(f"CONTAM 非忠實:{market} {day}")
                    n += 1
                    continue
                p = archive.raw_path("index", market, day)
                if not p.exists() or p.stat().st_size <= 1024:
                    skip += 1
                    n += 1
                    continue
                text = p.read_bytes().decode("Big5-HKSCS", errors="replace")
                exp = _extract(market, text, day)
                pdf = ix.parse(market, text, day)
                port = [] if pdf is None else pdf.to_dicts()
                status, msg = _eval(market, day, exp, pg_year.get(day, []), port, st)
                if status == "OK":
                    ok += 1
                elif status == "SKIP":
                    skip += 1
                else:
                    fail += 1
                    fails.append(msg)
                n += 1
        print(f"  {market}: 掃 {n} 檔")
    return ok, skip, fail, contam, st, fails


def _print_stats(st: Stats) -> None:
    print(f"\n[port 對、PG 錯 統計] 比對 {st.rows} 列:")
    print(f"  · change 歸零修復(PG=0→port=None):{st.change_fix} 列")
    print(f"  · change_pct 歸零修復(PG=0→port=None):{st.pct_fix} 列")
    print(f"  · 名稱去空白修復(PG 去空白→port 保留):{st.name_space_fix} 列")
    print(f"  · 報酬區改名修復(PG 報酬報酬→port 單次):{st.name_return_fix} 列")


def main() -> None:
    args = sys.argv[1:]
    con = _connect()
    st = Stats()
    try:
        if args and args[0] == "--full":
            print(f"index FULL parity(PG: {DEFAULT_DSN})")
            ok, skip, fail, contam, st, fails = check_full(con)
            print(f"\n結果:逐位一致 {ok}、CONTAM 排除 {contam}、SKIP {skip}、失敗 {fail}")
            _print_stats(st)
            for m in fails[:40]:
                print(f"  ✗ {m}")
            raise SystemExit(1 if fail else 0)

        if args:  # 單日:兩市場都試
            day = Date.fromisoformat(args[-1])
            print(f"index parity 單日 {day}")
            rc = 0
            for market in ix.MARKETS:
                status, msg = _check_day(con, market, day, st)
                mark = {"OK": "✓", "FAIL": "✗", "SKIP": "·", "CONTAM": "◐"}[status]
                print(f"  {mark} {msg}")
                rc |= (status == "FAIL")
            _print_stats(st)
            raise SystemExit(1 if rc else 0)

        print(f"index parity 樣本(PG: {DEFAULT_DSN});{len(SAMPLE)} 組 + {len(FIX_ANCHORS)} 先紅後綠錨")
        tally = {"OK": 0, "FAIL": 0, "SKIP": 0, "CONTAM": 0}
        fails = []
        for market, iso, kind in SAMPLE:
            day = Date.fromisoformat(iso)
            if kind == "CONTAM":
                status, msg = _check_contam(con, day, st)
            else:
                status, msg = _check_day(con, market, day, st)
            tally[status] = tally.get(status, 0) + 1
            mark = {"OK": "✓", "FAIL": "✗", "SKIP": "·", "CONTAM": "◐"}[status]
            print(f"  {mark} [{kind}] {msg}")
            if status == "FAIL":
                fails.append((market, iso))
        print("  --- 先紅後綠錨(port 對、PG 錯)---")
        for market, iso, name, cl, ch, pc in FIX_ANCHORS:
            status, msg = _check_anchor(market, iso, name, cl, ch, pc)
            tally[status] = tally.get(status, 0) + 1
            print(f"  {'✓' if status == 'OK' else '✗'} {msg}")
            if status == "FAIL":
                fails.append((market, iso, name))
        _print_stats(st)
    finally:
        con.close()

    print(f"\n結果:逐位一致 {tally['OK']}、CONTAM 排除 {tally['CONTAM']}、"
          f"失敗 {tally['FAIL']}、SKIP {tally['SKIP']}")
    if fails:
        print(f"失敗:{fails}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
