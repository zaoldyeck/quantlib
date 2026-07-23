"""raw 完整性總閘:一個指令跑齊「cache-vs-PG 比對抓不到」的三類 raw 汙染檢查。

當年 remediation 的驗證是 cache-vs-PG 逐表比對全綠,但 PG 與 cache 同源同錯(共用 Scala
爬蟲),**兩邊都錯的汙染**(錯日/幽靈/截斷)綠燈也照過。這三類只能對 raw 本身、對現實檢驗:

1. **錯日**(content_dates):raw 檔頭日期 ≠ 檔名日期(下載游標錯位)。
2. **幽靈日**(ghost_days):兩日期整日內容指紋相同(非交易日請求回鄰日資料、標頭印請求日)。
3. **截斷**(本檔):TPEx 日報缺「共N筆」結尾(下載中斷),parser 完整性守衛會拒,但 raw 仍髒。

全綠 = raw 這三類汙染已清,可安心「從 raw 全量 rebuild = cache」。

Run: uv run --project . python -m quantlib.verify.raw_integrity
唯讀,不改資料;發現的修法見 refetch_wrongday(錯日)、ghost_days --fix(幽靈)。
"""
from __future__ import annotations

from quantlib import paths
from quantlib.db import connect
from quantlib.verify import content_dates, ghost_days

#: 有「共N筆」結尾標記可驗截斷的源。**只有 daily_quote/tpex 有此標記**(實測:TWSE daily_quote、
#: dtd、margin 結尾為資料列或備註註記,無筆數結尾)。其餘源的截斷完整性由各 parser 的列數/
#: 標頭守衛把關(截斷檔 parse 出的列數與自報筆數不符即拒),不靠此檔。
_TRUNC_GLOBS = {
    "daily_quote/tpex": ("共", "筆"),
}


def scan_truncation() -> list[str]:
    """回缺結尾標記(截斷)的 raw 檔相對路徑。"""
    bad: list[str] = []
    for sub, markers in _TRUNC_GLOBS.items():
        base = paths.RAW / sub
        if not base.exists():
            continue
        for f in base.rglob("*.csv"):
            if not f.is_file() or f.stat().st_size == 0:
                continue
            tail = f.read_bytes()[-200:].decode("big5", errors="replace")
            if not any(m in tail for m in markers):
                bad.append(str(f.relative_to(paths.RAW)))
    return bad


def main() -> None:
    print("=== raw 完整性總閘(三類 cache-vs-PG 抓不到的汙染)===\n")
    issues = 0

    # 1. 錯日
    wrongday = sum(len(content_dates.scan_source(s)["mismatches"]) for s in content_dates._DAILY_SOURCES)
    print(f"① 錯日(檔頭日≠檔名日):{wrongday} 檔" + ("  ✓" if wrongday == 0 else "  ❌ → refetch_wrongday"))
    issues += wrongday

    # 2. 幽靈日
    con = connect()
    ghosts = ghost_days.detect(con)
    print(f"② 幽靈日(整日內容指紋碰撞):{len(ghosts)} 對" + ("  ✓" if not ghosts else "  ❌ → ghost_days --fix"))
    for g in ghosts[:6]:
        print(f"      {g['table']}/{g['market']} {g['ghost']} = 複製 {g['keep']}")
    issues += len(ghosts)

    # 3. 截斷
    trunc = scan_truncation()
    print(f"③ 截斷(缺「共N筆」結尾):{len(trunc)} 檔" + ("  ✓" if not trunc else "  ❌ → 重爬該檔"))
    for t in trunc[:6]:
        print(f"      {t}")
    issues += len(trunc)

    print(f"\n{'✓ raw 三類汙染全清,可從 raw 全量 rebuild = 正確 cache' if issues == 0 else f'❌ 共 {issues} 項待修'}")


if __name__ == "__main__":
    main()
