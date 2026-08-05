"""EV59:量 G1(MOPS 重大訊息)在各年代的實際覆蓋——把唯一還開著的假設變成數字。

## 為什麼是這一格
歸因提示詞的九格矩陣裡,G1 被寫成「後續能否宣告『真的沒有消息』的決定性依據」。
其餘八格是新聞、社群、時光機——那些在舊年代注定稀薄,而**只要 G1 在,「查無外部
報導」就仍然是一個有內容的結論**(公司當時確實沒公告);G1 若不在,舊年代的樣本
連基準線都沒有,`no_news_verdict` 永遠只能填 `NOT_RETRIEVABLE`,那批卡片就沒有
判別價值。所以「老年代能不能做」這個問題,主要就是「MOPS 回得出多久以前」。

這件事桌面上推不出來(端點的歷史深度沒有文件承諾),但**不需要派 agent 也量得到**:
直接打端點,數 d0 前 12 個月有沒有回得出公告即可。

## 量法
對每張試跑卡,打 MOPS `ajax_t05st01`(歷史重大訊息)查 `d0` 前 12 個月,逐月記錄
「有資料 / 查無 / 端點失敗」。**逐月而非整段**:MOPS 一次只吃一個月,而「某月查無」
與「該年查不到」是兩件事——前者是公司那個月沒公告(有內容的事實),後者才是覆蓋
斷層。兩者混為一談,正是提示詞裡 `miss` 與 `blocked` 必須分開的同一個道理。

併發打完 36 張 × 12 個月 = 432 次查詢;端點無官方速率限制文件,故取保守併發。

Run: uv run --project . python -m quantlib.evergreen.ev59_retrievability_probe
依賴 cache: 否(只讀 ev58 卡片 + 連外)。長任務,建議背景跑。
"""
from __future__ import annotations

import argparse
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date as Date

from quantlib import paths
from quantlib.evergreen.ev58_build_cards import OUT as CARDS

URL = "https://mopsov.twse.com.tw/mops/web/ajax_t05st01"
OUT = paths.OUT / "ev59_g1_coverage.json"
#: **序列查詢 + 間隔**。第一版用 6 併發,結果:最早跑的 E1 拿到 83% 覆蓋,其餘期別
#: 全部 100% 失敗——那不是覆蓋斷層,是端點在限流我。而報表把兩者都印成「覆蓋率 0%」,
#: 差點讓「2020 年的 MOPS 查不到」這種顯然荒謬的結論寫進決策。
#: 教訓正是這支自己的 docstring 講的那條:端點沒答 ≠ 該月無資料。現在失敗要重試、
#: 要記下**為什麼**失敗,而不是折疊成一個「error」計數。
WORKERS = 1
#: 每次查詢之間的間隔秒數。無官方文件,取實測不再觸發限流的值。
GAP_SEC = 1.0
_HAS = re.compile(r"發言日期")
_NONE = re.compile(r"查無需求資料")


def _month_has_filings(code: str, y: int, m: int, timeout: int = 40,
                       tries: int = 4) -> tuple[str, str]:
    """回傳 (`hit`/`miss`/`error`, 失敗原因)。

    三態而非布林:`miss`(端點答了、該月無公告)與 `error`(端點沒答)在語義上
    完全不同,合併會讓「覆蓋斷層」與「公司沒公告」互相偽裝——這正是提示詞裡
    `miss` 與 `blocked` 必須分開的同一個理由。

    失敗要**重試並記下原因**:第一版只回一個 `error`,於是「被限流」長得跟
    「那幾年查不到」一模一樣,報表照樣印得出漂亮的表格。
    """
    body = urllib.parse.urlencode({
        "step": "1", "firstin": "1", "off": "1", "TYPEK": "all",
        "co_id": code, "year": str(y - 1911), "month": f"{m:02d}"}).encode()
    req = urllib.request.Request(URL, data=body, headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (quantlib EV59 coverage probe)"})
    why = ""
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                t = r.read().decode("utf-8", "replace")
            if _HAS.search(t):
                return "hit", ""
            if _NONE.search(t):
                return "miss", ""
            why = f"unexpected_body:{t[:60].strip()!r}"
        except urllib.error.HTTPError as e:
            why = f"http_{e.code}"
        except Exception as e:                             # noqa: BLE001
            why = type(e).__name__
        time.sleep(GAP_SEC * (2 ** i))                     # 指數退避,讓限流有機會鬆手
    return "error", why


def _prior_months(d0: Date, n: int = 12) -> list[tuple[int, int]]:
    y, m = d0.year, d0.month
    out = []
    for _ in range(n):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        out.append((y, m))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=12, help="往前查幾個月(對齊 G1 的窗)")
    a = ap.parse_args()

    cards = [c for b in sorted(CARDS.glob("batch_*"))
             for c in json.loads((b / "cards.json").read_text(encoding="utf-8"))]
    if not cards:
        raise SystemExit("尚未產生卡片:uv run -m quantlib.evergreen.ev58_build_cards --pilot")
    era_map = json.loads((CARDS / "_era_map.json").read_text(encoding="utf-8"))

    jobs = [(c, ym) for c in cards
            for ym in _prior_months(Date.fromisoformat(c["d0"]), a.months)]
    print(f"{len(cards)} 張卡 × {a.months} 個月 = {len(jobs)} 次查詢,併發 {WORKERS}…",
          flush=True)
    if WORKERS > 1:
        with ThreadPoolExecutor(WORKERS) as ex:
            res = list(ex.map(lambda j: _month_has_filings(j[0]["code"], *j[1]), jobs))
    else:
        res = []
        for i, j in enumerate(jobs):
            res.append(_month_has_filings(j[0]["code"], *j[1]))
            time.sleep(GAP_SEC)
            if (i + 1) % 50 == 0:
                print(f"  {i + 1}/{len(jobs)}", flush=True)

    per_card: dict[str, dict] = {}
    why_count: dict[str, int] = {}
    for (c, _), (r, why) in zip(jobs, res):
        d = per_card.setdefault(c["code"] + "@" + c["d0"], {
            "era_code": c["era_code"], "limit_era": c["limit_era"],
            "hit": 0, "miss": 0, "error": 0})
        d[r] += 1
        if why:
            why_count[why] = why_count.get(why, 0) + 1

    print("\n=== G1(MOPS 重大訊息)覆蓋:d0 前 12 個月 ===")
    print(f"{'期別':<5}{'區間':<24}{'卡數':>5}{'有公告的卡':>11}{'月覆蓋率':>10}{'端點失敗':>9}")
    by_era: dict[str, list[dict]] = {}
    for v in per_card.values():
        by_era.setdefault(v["era_code"], []).append(v)
    for era in sorted(by_era):
        rows = by_era[era]
        answered = sum(r["hit"] + r["miss"] for r in rows)
        # 「月覆蓋率」= 端點答得出來的月份比例(hit + miss);它量的是**端點的深度**,
        # 不是公司有沒有公告。分母刻意含 miss——把 miss 排除會讓覆蓋率永遠是 100%。
        cov = answered / max(sum(r["hit"] + r["miss"] + r["error"] for r in rows), 1)
        with_any = sum(1 for r in rows if r["hit"] > 0)
        print(f"{era:<5}{era_map[era]:<24}{len(rows):>5}{with_any:>11}"
              f"{cov:>9.0%}{sum(r['error'] for r in rows):>9}")

    if why_count:
        # 失敗原因必須印出來:它決定結論是「那個年代沒有資料」還是「我們被擋了」,
        # 而這兩個結論一個叫我們放棄樣本、一個叫我們換打法。
        print("\n端點失敗原因分布:", dict(sorted(why_count.items(), key=lambda x: -x[1])))
    OUT.write_text(json.dumps({"per_card": per_card, "errors": why_count},
                              ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\n→ {OUT}")


if __name__ == "__main__":
    main()
