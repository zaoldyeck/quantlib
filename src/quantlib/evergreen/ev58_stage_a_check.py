"""階段 A 出廠檢查:燒下一輪之前,先確認已產出的卡片合格。

## 為什麼要有這支

試跑的六批只有一批接近做完,而**續跑會把同樣的問題複製到另外五批**。所以先驗卡片,
再決定要不要續跑——這比「跑完再看」便宜一個數量級。

檢查的都是「壞掉時完全看不出來」的東西:

1. **thin 卡的 schema** —— 實測 48 張裡有 46 張套用了 deep 的完整 schema,而不是我指定的
   扁平表單。後果:`ev58_tables` 讀不到 `p_up80`(它在 `bet` 底下),T5b 會**靜默**回報
   零配對,而 T5b 正是本次設計的承重點。
2. **thin → deep 的修正方向** —— 若加深查證只會讓人更樂觀、從不往下修,那多出來的
   判別力就不是資訊,是**投入本身造成的偏誤**。這一項用符號檢定量,不靠印象。
3. **投入對稱性** —— 查詢次數若能預測臂別,「有消息的比較會漲」就有一部分是研究員
   自己的搜尋行為造出來的。
4. **PIT** —— 卡片引用的材料發布日不得晚於該站位。
5. **九格留痕** —— `queries` 必須含未命中的查詢,否則「查無」沒有憑據。

Run: uv run --project . python -m quantlib.evergreen.ev58_stage_a_check
依賴 cache: 否。
"""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter

from quantlib.evergreen.ev58_build_cards import OUT as CARDS
from quantlib.evergreen.ev58_build_cards import TRUTH
from quantlib.evergreen.ev58_prompt import NEWS

#: thin 卡我指定的扁平欄位。它們刻意與下游標記 Agent 的輸出表單同名同義——
#: 那一戳就是在模擬那張表單。缺了它們,thin 就退化成「只是查得少一點的 deep」。
THIN_FORM = ("theme", "signal_type", "event", "evidence", "invalidation", "conviction")


def _p(card: dict) -> int | None:
    """取 `p_up80`,容忍兩種擺放位置。

    容忍是為了**救回已產出的資料**,不是接受 schema 漂移:漂移本身照樣回報。
    只讀扁平的話,已經燒掉的 46 張卡會被當成不存在,而它們其實有判斷值。
    """
    # 欄位在門檻改為 20% 時由 `p_up80` 更名為 `p_hit`——舊名寫著 80 而門檻是 20,
    # 留著就是一個永久的謊。兩個名字都讀,是為了救回改名前已產出的卡片。
    for k in ("p_hit", "p_up80"):
        if card.get(k) is not None:
            return card[k]
    bet = card.get("bet")
    if not isinstance(bet, dict):
        return None
    return bet.get("p_hit", bet.get("p_up80"))


def _sign_test(deltas: list[float]) -> tuple[int, int, int, float]:
    """回傳 (上修數, 下修數, 持平數, 雙尾 p)。

    用符號檢定而非 t 檢定:我們問的是**方向**有沒有系統性偏斜,不是幅度多大;
    而幅度分布長尾(實測單案 +18),平均數會被少數幾案主導。
    """
    up = sum(1 for d in deltas if d > 0)
    dn = sum(1 for d in deltas if d < 0)
    n = up + dn
    if n == 0:
        return (up, dn, len(deltas) - n, 1.0)
    k = min(up, dn)
    tail = sum(math.comb(n, i) for i in range(k + 1)) / (2 ** n)
    return (up, dn, len(deltas) - n, min(1.0, 2 * tail))


#: PIT 掃描要跳過的子樹。**這兩個欄位裡出現站位後的日期是設計要求的正確行為**,
#: 不是違規——第一版把它們算成違規,12 張卡全是誤報:
#:   `leakage_log`      agent 誠實記錄「我撞見了站位之後的材料」。要它記,又因為它記了
#:                      而判它違規,等於獎勵隱瞞——這是最不該建立的誘因。
#:   `upcoming_trigger` 提示詞明文要的是「站位當天就已知日期的未來事件」(法說會、
#:                      月營收公告日),那些日期本來就在站位之後。
_PIT_EXEMPT = ("leakage_log", "upcoming_trigger")


def _dates(obj, out: list[str], key: str = "") -> None:
    """遞迴撈出所有看起來像日期的字串——PIT 檢查用,跳過豁免子樹。"""
    if key in _PIT_EXEMPT:
        return
    if isinstance(obj, str):
        if len(obj) >= 10 and obj[4] == "-" and obj[7] == "-" and obj[:4].isdigit():
            out.append(obj[:10])
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _dates(v, out, k)
    elif isinstance(obj, list):
        for v in obj:
            _dates(v, out, key)


def check() -> dict:
    truth = {}
    for f in sorted(TRUTH.glob("batch_*.json")):
        for t in json.loads(f.read_text(encoding="utf-8")):
            truth[(t["code"], t["d0"])] = t
    stations = {}
    for b in sorted(CARDS.glob("batch_*")):
        for c in json.loads((b / "cards.json").read_text(encoding="utf-8")):
            stations[(c["code"], c["d0"])] = {"d_prev": c["d_prev"], "d0": c["d0"],
                                              "batch": b.name}

    rows, schema_bad, pit_bad, no_miss_q = [], [], [], []
    leak_seen: list[tuple] = []
    prior_mem: list[str] = []
    for (code, d0), t in sorted(truth.items()):
        cdir = NEWS / f"{code}_{d0}"
        for station in ("d_prev", "d0"):
            thin_f = cdir / f"ex_ante_thin_{station}.json"
            deep_f = cdir / f"ex_ante_{station}.json"
            if not thin_f.exists():
                continue
            thin = json.loads(thin_f.read_text(encoding="utf-8"))
            missing_form = [k for k in THIN_FORM if thin.get(k) is None]
            if missing_form:
                schema_bad.append((f"{code}@{d0}", station, missing_form))
            pt = _p(thin)
            pd_ = _p(json.loads(deep_f.read_text(encoding="utf-8"))) if deep_f.exists() else None

            # PIT:卡片內任何日期都不得晚於該站位
            sdate = stations.get((code, d0), {}).get(station)
            if sdate:
                ds: list[str] = []
                _dates(thin, ds)
                late = sorted({x for x in ds if x > sdate and x.startswith(("19", "20"))})
                if late:
                    pit_bad.append((f"{code}@{d0}", station, late[:4]))
            rows.append({"case": f"{code}@{d0}", "station": station,
                         "arm": t["arm"], "era": t["era_code"],
                         "p_thin": pt, "p_deep": pd_,
                         "delta": None if (pt is None or pd_ is None) else pd_ - pt})

            lk = thin.get("leakage_log") or {}
            enc = lk.get("encountered") or []
            if enc:
                leak_seen.append((f"{code}@{d0}", station, len(enc),
                                  sum(1 for e in enc if e.get("changed_my_view"))))
            if lk.get("prior_memory"):
                prior_mem.append(f"{code}@{d0} {station}")

        log = cdir / "retrieval_log.jsonl"
        if log.exists():
            gates = [json.loads(x) for x in log.read_text(encoding="utf-8").splitlines() if x.strip()]
            misses = [g for g in gates if g.get("status") == "miss"]
            if misses and not any(g.get("queries") for g in misses):
                no_miss_q.append(f"{code}@{d0}")

    paired = [r for r in rows if r["delta"] is not None]
    up, dn, tie, p = _sign_test([r["delta"] for r in paired])
    by_arm = {a: [r["delta"] for r in paired if r["arm"] == a]
              for a in {r["arm"] for r in paired}}
    return {"rows": rows, "paired": paired, "schema_bad": schema_bad,
            "pit_bad": pit_bad, "no_miss_q": no_miss_q,
            "leak_seen": leak_seen, "prior_mem": prior_mem,
            "sign": {"up": up, "down": dn, "tie": tie, "p_two_sided": p},
            "by_arm": {a: {"n": len(v), "median": sorted(v)[len(v) // 2] if v else None}
                       for a, v in by_arm.items()}}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", type=int, default=8)
    ap.parse_args()
    r = check()

    print(f"=== 階段 A 出廠檢查 ===  卡片 {len(r['rows'])} 張、thin/deep 配對 {len(r['paired'])} 對\n")

    print(f"[1] thin schema:{len(r['schema_bad'])}/{len(r['rows'])} 張缺標記表單欄位")
    if r["schema_bad"]:
        miss = Counter(k for _, _, ks in r["schema_bad"] for k in ks)
        print(f"    最常缺:{dict(miss.most_common(6))}")
        print("    ⇒ thin 退化成「只是查得少一點的 deep」,失去『模擬標記表單』的設計意圖;"
              "\n      且 p_up80 落在 bet 底下,ev58_tables 會靜默讀不到,T5b 回報零配對。")

    s = r["sign"]
    print(f"\n[2] thin → deep 的修正方向:上修 {s['up']}、下修 {s['down']}、持平 {s['tie']}"
          f"(符號檢定雙尾 p = {s['p_two_sided']:.4f})")
    if s["down"] == 0 and s["up"] >= 5:
        print("    ⚠ **一次也沒有往下修**。若加深查證是資訊,應該有些標的看得更清楚後變差;"
              "\n      單向上修比較像『投入本身讓人更樂觀』——那是偏誤,不是判別力。")
    print(f"    分臂中位:{r['by_arm']}")
    if len(r["by_arm"]) == 2:
        vals = [v["median"] for v in r["by_arm"].values()]
        if None not in vals and abs(vals[0] - vals[1]) <= 1:
            print("    ⇒ 兩臂的上修幅度幾乎一樣 ⇒ 深度加的不是判別力,是整體樂觀度平移。")

    print(f"\n[3] PIT:{len(r['pit_bad'])} 張卡片含晚於站位的日期")
    for c, st, ds in r["pit_bad"][:8]:
        print(f"    ✗ {c} {st}: {ds}")

    changed = [x for x in r["leak_seen"] if x[3]]
    print(f"\n[3b] 洩漏誠實記錄:{len(r['leak_seen'])} 張卡主動記下撞見站位後的材料"
          f"(其中 {len(changed)} 張自承改變了判斷);自承靠記憶 {len(r['prior_mem'])} 張")
    if changed:
        print("    ⚠ `changed_my_view=true` 的案子,其 ex_ante 判斷已被後見之明污染,應作廢:")
        for c, st, n, ch in changed[:8]:
            print(f"      ✗ {c} {st}(撞見 {n} 筆,{ch} 筆改變判斷)")
    else:
        print("    ⇒ 撞見但未改變判斷,是 PIT 紀律正常運作的樣子。")

    print(f"\n[4] 九格留痕:{len(r['no_miss_q'])} 案的 miss 格沒有記下未命中查詢")
    for c in r["no_miss_q"][:8]:
        print(f"    ✗ {c}")

    print("\n=== 逐案 ===")
    for x in r["paired"][:40]:
        print(f"  {x['case']:<18}{x['station']:<8}{x['era']:<4}"
              f"thin {x['p_thin']:>3} → deep {x['p_deep']:>3}  Δ{x['delta']:+}")


if __name__ == "__main__":
    main()
