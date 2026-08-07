"""EV58 期別語境卡:證交所法規「當年生效版本」取條器(TWSE regulation time machine)。

為什麼要這支
------------
重建某個年代的交易制度時,最容易犯的錯是**引用今天的條文當年當年的事實**。幾乎每
一條規則都改過:整戶擔保維持率今天 130%、2008 年是 120%;現股當沖今天是常識、
2008 年根本還沒這個制度。拿現行條文寫年代語境卡,等於帶著答案回到過去。

證交所「法規分享知識庫」(twse-regulation.twse.com.tw)其實把每一版都留著,只是
入口藏在三層 aspx 後面。本模組把它變成一行指令:給法規代碼與觀察日,回傳**那天
實際生效的那一版條文**,外加一個第三者可以自己點開覆核的網址。

站台的四個關鍵頁(逆向而得,官方無 API 文件)
--------------------------------------------
- ``/TW/LawLst.aspx``                              全部法規清單(名稱 → FLCODE)
- ``/TW/law/DAT0201.aspx?FLCODE=<code>``            現行全文
- ``/TW/law/DAT01.aspx?FLCODE=<code>``              歷史沿革(列出每一版的 FLDATE)
- ``/TW/law/DAT06.aspx?FLCODE=<code>&FLDATE=<ymd>&LSER=001``  **某一版的歷史全文**

「觀察日生效版本」的判定
------------------------
沿革頁給的 FLDATE 是**修正日**。某日 D 生效的版本 = 所有 FLDATE ≤ D 之中最大的那個。
注意修正日與實施日可能不同(例:當沖辦法 102-10-29 訂定、103-01-06 才實施),故
``asof`` 只做機械挑版,實施日以人工讀沿革頁原文為準——這一步不自動化是刻意的,
把「條文寫的實施日」交給人看,比讓程式猜錯而無聲通過安全。

不需要 cache.duckdb(純網路 + 本地快取)。

用法
----
    # 找法規代碼(關鍵字比對法規名稱)
    uv run --project . python -m quantlib.ev58.twse_reg find --term 融資融券業務操作辦法

    # 列出所有版本(修正日),標出某日生效的那一版
    uv run --project . python -m quantlib.ev58.twse_reg versions --code FL007225 --asof 2008-06-30

    # 取某日生效版本的全文(附可覆核網址)
    uv run --project . python -m quantlib.ev58.twse_reg text --code FL007225 --asof 2008-06-30

    # 在某日生效版本裡找關鍵字(前後給脈絡),語境卡逐條找證據就靠這個
    uv run --project . python -m quantlib.ev58.twse_reg grep --code FL007121 --asof 2008-06-30 --term 維持率

快取於 ``var/out/ev58_news/_era_brief/_probe_cache/twse_reg/``,重跑零請求。
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
import urllib.request
from pathlib import Path

from quantlib import paths

CACHE_DIR = paths.OUT / "ev58_news" / "_era_brief" / "_probe_cache" / "twse_reg"
BASE = "https://twse-regulation.twse.com.tw"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)


def _get(url: str, timeout: int = 60, tries: int = 4) -> str:
    """站方偶發 500「系統忙碌中」,單次失敗就放棄會讓整批枚舉變得不可重現,故退避重試。"""
    cache = CACHE_DIR / (hashlib.sha1(url.encode()).hexdigest() + ".html")
    if cache.exists():
        return cache.read_text(encoding="utf-8")
    last: Exception | None = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as fh:
                html = fh.read().decode("utf-8", errors="replace")
            if "系統忙碌中" in html:
                raise RuntimeError("server busy")
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(html, encoding="utf-8")
            return html
        except Exception as exc:  # noqa: BLE001 - 網路層任何錯都退避重試
            last = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"fetch failed: {url} ({last})")


def _plain(html: str) -> str:
    """條文頁的可讀純文字。條文用全形空白排版,壓掉會讓「第 7 條」變「第7條」而搜不到。"""
    body = re.sub(r"<script.*?</script>", " ", html, flags=re.S)
    body = re.sub(r"<style.*?</style>", " ", body, flags=re.S)
    body = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
    body = re.sub(r"</(p|div|tr|td|th|li)>", "\n", body, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", body)
    txt = txt.replace("&nbsp;", " ").replace("&#12288;", "　")
    txt = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), txt)
    txt = re.sub(r"&amp;", "&", txt)
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n\s*\n+", "\n", txt)
    return txt.strip()


# ---------------------------------------------------------------- 法規清單


def law_list() -> list[tuple[str, str]]:
    """[(FLCODE, 法規名稱)];站方清單頁一次給全(約 300 部),故不分頁。"""
    html = _get(f"{BASE}/TW/LawLst.aspx")
    out: dict[str, str] = {}
    for url, label in re.findall(
        r'<a[^>]*href="([^"]*FLCODE=FL\d+[^"]*)"[^>]*>(.*?)</a>', html, flags=re.S
    ):
        code = re.search(r"FLCODE=(FL\d+)", url).group(1)
        name = re.sub(r"<[^>]+>", "", label).strip()
        if name and code not in out:
            out[code] = name
    return sorted(out.items())


# ---------------------------------------------------------------- 版本


def versions(code: str) -> list[str]:
    """該法規所有版本的修正日(YYYYMMDD,遞增)。"""
    html = _get(f"{BASE}/TW/law/DAT01.aspx?FLCODE={code}")
    return sorted(set(re.findall(r"FLDATE=(\d{8})", html)))


def asof_version(code: str, day: str) -> str | None:
    """觀察日 day(YYYY-MM-DD 或 YYYYMMDD)當天生效的版本修正日。

    機械規則:FLDATE ≤ day 之中最大者。**修正日 ≠ 實施日**,跨版邊界請人工讀沿革。
    """
    ymd = day.replace("-", "")
    prior = [v for v in versions(code) if v <= ymd]
    return prior[-1] if prior else None


def history_url(code: str, fldate: str) -> str:
    return f"{BASE}/TW/law/DAT06.aspx?FLCODE={code}&FLDATE={fldate}&LSER=001"


def history_text(code: str, fldate: str) -> str:
    return _plain(_get(history_url(code, fldate)))


def changelog(code: str) -> str:
    """沿革頁純文字:每一版的發文字號、實施日、主管機關核定文號都在這裡。"""
    return _plain(_get(f"{BASE}/TW/law/DAT01.aspx?FLCODE={code}"))


# ---------------------------------------------------------------- CLI


def _resolve(args) -> tuple[str, str]:
    fldate = args.fldate or asof_version(args.code, args.asof)
    if not fldate:
        sys.exit(f"[no version] {args.code} 在 {args.asof} 之前沒有任何版本")
    return args.code, fldate


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("find", help="用關鍵字找法規代碼")
    p.add_argument("--term", required=True)

    for name, helptext in (
        ("versions", "列出所有版本修正日"),
        ("changelog", "印出歷史沿革全文(含實施日)"),
        ("text", "印出某日生效版本全文"),
        ("grep", "在某日生效版本裡找關鍵字"),
    ):
        p = sub.add_parser(name, help=helptext)
        p.add_argument("--code", required=True)
        p.add_argument("--asof", default="", help="觀察日 YYYY-MM-DD")
        p.add_argument("--fldate", default="", help="直接指定版本修正日 YYYYMMDD")
        if name == "grep":
            p.add_argument("--term", required=True, help="關鍵字(| 分隔多個)")
            p.add_argument("--ctx", type=int, default=260, help="前後脈絡字數")

    args = ap.parse_args()

    if args.cmd == "find":
        for code, name in law_list():
            if args.term in name:
                print(f"{code}\t{name}")
        return

    if args.cmd == "versions":
        vs = versions(args.code)
        hit = asof_version(args.code, args.asof) if args.asof else None
        for v in vs:
            print(f"{'>>' if v == hit else '  '} {v}  {history_url(args.code, v)}")
        if args.asof:
            print(f"\n[asof {args.asof}] 生效版本 = {hit}")
        return

    if args.cmd == "changelog":
        print(changelog(args.code))
        return

    code, fldate = _resolve(args)
    txt = history_text(code, fldate)
    print(f"# {code} version {fldate}\n# {history_url(code, fldate)}\n")
    if args.cmd == "text":
        print(txt)
        return

    for term in args.term.split("|"):
        print(f"===== {term} =====")
        for m in re.finditer(re.escape(term), txt):
            lo = max(0, m.start() - args.ctx)
            print(txt[lo : m.end() + args.ctx].replace("\n", " "), "\n---")


if __name__ == "__main__":
    main()
