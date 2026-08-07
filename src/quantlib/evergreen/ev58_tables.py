"""EV58 統計表 T1-T15 + T5b——蒸餾提示詞承諾「程式預先算好」的那些數字。

## 為什麼必須有這支
蒸餾提示詞的鐵律第 2 條是「**禁止自己算數字**。所有比率、佔比、命中率、樣本數一律
引自統計表並註明表號」。那條鐵律存在的理由很具體:上一版哲學寫了「6-6.5 成」與
「12.5-37.5%」這種沒有出處的數字,而後者事後被證明只是批次之間的極差、不是分層估計。

**但那些統計表沒有任何程式在算。** 沒有它們,鐵律第 2 條就是一句空話——蒸餾器要嘛
自己估(正是要禁止的),要嘛整份產出卡住。這與「提示詞沒有渲染器」「去識別掃描器
不存在」是同一類缺口:散文承諾了一個機制,而機制不存在。

## 設計上的兩個要點

**一、雙邊計數是入場券。** 每張列聯表都同時報正例與負例兩邊的出現率與 Wilson 95%
信賴區間。上一版的核心缺陷就是條件全從贏家身上讀出來、從沒問過輸家——所以這裡
不提供「只看正例」的表。

**二、AUC 分 thin 與 deep 兩個深度(T5b)。** 下游標記 Agent 每月掃全市場、挑不到
十五檔,拿得到的是標記日深度;只報考掘深度的 AUC 等於報一個生產端永遠達不到的
天花板。兩者的差就是「加深查證的邊際價值」。

Run: uv run --project . python -m quantlib.evergreen.ev58_tables
依賴 cache: 是(days_to_peak 需還原價面板)。
"""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import date as Date
from typing import Any, Callable

from quantlib import paths
from quantlib.evergreen.ev58_build_cards import OUT as CARDS
from quantlib.evergreen.ev58_build_cards import TRUTH
from quantlib.evergreen.ev58_prompt import NEWS

OUT = paths.OUT / "ev58_tables"

#: 封閉詞彙欄位 → 取值函式。路徑寫死而非萬用遍歷:遍歷會把自由文字欄位一起收進來,
#: 而自由文字的「取值」是無限多個,列聯表會退化成每格 n=1 的雜訊。
FIELDS: dict[str, Callable[[dict], Any]] = {
    "V1_theme_archetype": lambda c: c.get("context", {}).get("theme_archetype"),
    "V2_value_chain": lambda c: c.get("context", {}).get("value_chain_position"),
    "V3_theme_stage": lambda c: c.get("context", {}).get("theme_stage"),
    "V5_theme_rev_share": lambda c: c.get("context", {}).get("theme_revenue_share"),
    "V9_own_numbers": lambda c: c.get("context", {}).get("own_numbers_direction"),
    "V10_market_state": lambda c: c.get("context", {}).get("market_state"),
    "V11_stock_vs_market": lambda c: c.get("context", {}).get("stock_vs_market"),
    "substitutability": lambda c: c.get("context", {}).get("substitutability"),
    "theme_leader": lambda c: c.get("context", {}).get("theme_leader"),
    "V6_diffusion": lambda c: c.get("information_diffusion"),
    "V7_horizon": lambda c: c.get("monetization", {}).get("horizon"),
    "V8_durability": lambda c: c.get("monetization", {}).get("durability"),
    "V4_verifiability": lambda c: c.get("positioning", {}).get("verifiability"),
    "bottleneck": lambda c: c.get("positioning", {}).get("bottleneck"),
    "V13_upcoming": lambda c: c.get("upcoming_trigger", {}).get("level"),
    "contradiction": lambda c: c.get("adverse", {}).get("contradiction"),
    "story_also_fits": lambda c: c.get("story_also_fits"),
    "mark_or_not": lambda c: c.get("bet", {}).get("mark_or_not"),
    "news_count_90d": lambda c: c.get("news_structure", {}).get("news_count_90d"),
    "retail_attention_30d": lambda c: c.get("news_structure", {}).get("retail_attention_30d"),
    "sell_side_coverage": lambda c: c.get("news_structure", {}).get("sell_side_coverage"),
    "no_pre_station_news": lambda c: c.get("news_structure", {}).get("no_pre_station_news"),
    "no_news_verdict": lambda c: c.get("retrieval", {}).get("no_news_verdict"),
}


# ------------------------------------------------------------------ 統計原語

def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson 95% 信賴區間。

    不用常態近似:小樣本(這裡每格常常只有個位數)下常態近似會給出超出 [0,1] 的
    區間,而那種區間看起來像有結論、實際上沒有。Wilson 在 n 小時仍然守在界內。
    """
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - h) / d, (c + h) / d)


def auc(scores: list[float], labels: list[int]) -> tuple[float, int]:
    """Mann-Whitney AUC 與有效樣本數。

    用 AUC 而非 Brier 當主指標:本樣本是 50/50 富集設計、母體基準率 6.38%,
    Brier 在富集樣本上算出來的數字與生產端無關;AUC 是排序統計,不受類別比例影響。
    """
    xy = [(s, y) for s, y in zip(scores, labels) if s is not None]
    if not xy or len({y for _, y in xy}) < 2:
        return (float("nan"), len(xy))
    xy.sort(key=lambda t: t[0])
    ranks: dict[int, float] = {}
    i = 0
    while i < len(xy):                       # 平手取平均秩,否則 AUC 會被排序順序左右
        j = i
        while j + 1 < len(xy) and xy[j + 1][0] == xy[i][0]:
            j += 1
        r = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[k] = r
        i = j + 1
    n1 = sum(y for _, y in xy)
    n0 = len(xy) - n1
    s1 = sum(ranks[k] for k, (_, y) in enumerate(xy) if y == 1)
    return ((s1 - n1 * (n1 + 1) / 2) / (n1 * n0), len(xy))


def contingency(rows: list[tuple[Any, int]]) -> list[dict]:
    """單一欄位 × 結果的列聯表。**雙邊計數是入場券**——只報正例出現率的表不生產。"""
    n1 = sum(y for _, y in rows)
    n0 = len(rows) - n1
    out = []
    for v, cnt in Counter(v for v, _ in rows).most_common():
        k1 = sum(1 for vv, y in rows if vv == v and y == 1)
        k0 = cnt - k1
        r1, r0 = (k1 / n1 if n1 else 0.0), (k0 / n0 if n0 else 0.0)
        lo, hi = wilson(k1, cnt)
        out.append({"value": v, "n": cnt, "n_pos": k1, "n_neg": k0,
                    "rate_in_pos": round(r1, 4), "rate_in_neg": round(r0, 4),
                    "lift": round(r1 / r0, 3) if r0 else None,
                    "p_pos_given_value": round(k1 / cnt, 4),
                    "wilson_lo": round(lo, 4), "wilson_hi": round(hi, 4),
                    # 基準率是 50%(本樣本 50/50 富集),故 CI 跨 0.5 即無判別力
                    "ci_excludes_base": not (lo <= 0.5 <= hi)})
    return out


# ------------------------------------------------------------------ 資料載入

def _p_up80(card: dict) -> int | None:
    """取 `p_up80`,容忍扁平與 `bet` 巢狀兩種擺放。"""
    # 欄位在門檻改為 20% 時由 `p_up80` 更名為 `p_hit`——舊名寫著 80 而門檻是 20,
    # 留著就是一個永久的謊。兩個名字都讀,是為了救回改名前已產出的卡片。
    for k in ("p_hit", "p_up80"):
        if card.get(k) is not None:
            return card[k]
    bet = card.get("bet")
    if not isinstance(bet, dict):
        return None
    return bet.get("p_hit", bet.get("p_up80"))


def _load() -> list[dict]:
    """每個「案 × 站位」一列,把真相、卡片特徵、thin/deep 判斷併在一起。"""
    truth: dict[tuple[str, str], dict] = {}
    for f in sorted(TRUTH.glob("batch_*.json")):
        for t in json.loads(f.read_text(encoding="utf-8")):
            truth[(t["code"], t["d0"])] = t
    feat: dict[tuple[str, str], dict] = {}
    for b in sorted(CARDS.glob("batch_*")):
        for c in json.loads((b / "cards.json").read_text(encoding="utf-8")):
            feat[(c["code"], c["d0"])] = c

    rows = []
    for (code, d0), t in sorted(truth.items()):
        cdir = NEWS / f"{code}_{d0}"
        if (cdir / "voided.json").exists() or not cdir.exists():
            continue
        y = 1 if t["arm"] == "positive" else 0
        for station in ("d_prev", "d0"):
            deep = cdir / f"ex_ante_{station}.json"
            thin = cdir / f"ex_ante_thin_{station}.json"
            if not deep.exists():
                continue
            dc = json.loads(deep.read_text(encoding="utf-8"))
            tc = json.loads(thin.read_text(encoding="utf-8")) if thin.exists() else {}
            rows.append({
                "code": code, "d0": d0, "station": station, "y": y,
                "era_code": t["era_code"], "regime": t.get("regime"),
                "realized_ret": t.get("realized_ret"),
                "propensity_gap": t.get("propensity_gap"),
                "adv_decile": feat.get((code, d0), {}).get("adv_decile"),
                "card": dc,
                # thin 卡實測會被寫成站位資訊卡的形狀(`p_up80` 掉進 `bet`)。
                # 兩處都讀是為了**救回已產出的資料**,不是接受漂移——漂移由
                # `ev58_stage_a_check` 單獨回報。只讀一處會讓 T5b 靜默回報零配對。
                "p_deep": _p_up80(dc),
                "p_thin": _p_up80(tc),
                "conv_deep": dc.get("bet", {}).get("conviction"),
                "conv_thin": tc.get("conviction"),
                "retriev": dc.get("retrieval", {}).get("retrievability_score"),
            })
    return rows


def _effort(code: str, d0: str) -> dict:
    """搜尋投入——T11「努力洩漏稽核」的原料。

    要稽核的事:研究員有沒有對「看起來有戲」的股票多查幾輪。若查詢次數本身就能預測
    結果,那麼統計出來的「有消息的比較會漲」有一部分是搜尋行為造出來的,不是市場的性質。
    """
    log = NEWS / f"{code}_{d0}" / "retrieval_log.jsonl"
    mat = NEWS / f"{code}_{d0}" / "materials.jsonl"
    q = gates = hits = blocked = 0
    if log.exists():
        for line in log.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            gates += 1
            q += len(r.get("queries", []))
            hits += r.get("status") == "hit"
            blocked += r.get("status") == "blocked"
    n_mat = sum(1 for x in mat.read_text(encoding="utf-8").splitlines()
                if x.strip()) if mat.exists() else 0
    return {"queries": q, "gates_run": gates, "gates_hit": hits,
            "gates_blocked": blocked, "materials": n_mat}


def _days_to_peak(rows: list[dict]) -> dict[tuple[str, str], int]:
    """站位次日起 120 交易日內,還原總報酬最高點落在第幾個交易日。

    這是**現象本身**的時序,不涉及任何持有或出場規格——那些屬於下游量化引擎,
    蒸餾器不該知道也不需要知道。
    """
    import polars as pl

    from quantlib import prices
    from quantlib.apex import data
    con = data.connect()
    keys = {(r["code"], r["d0"]) for r in rows if r["y"] == 1}
    if not keys:
        return {}
    lo = min(d for _, d in keys)
    hi = max(d for _, d in keys)
    frames = []
    for m in ("twse", "tpex"):
        f = prices.fetch_adjusted_panel(con, lo, str(Date.fromisoformat(hi).replace(
            year=Date.fromisoformat(hi).year + 1)), market=m)
        if not f.is_empty():
            frames.append(f.select(["company_code", "date", "close"]))
    panel = pl.concat(frames).sort(["company_code", "date"])
    by_code = {c: g for c, g in panel.group_by("company_code")}
    out = {}
    for code, d0 in keys:
        g = by_code.get(code)
        if g is None:
            continue
        g = g.sort("date")
        fut = g.filter(pl.col("date") > Date.fromisoformat(d0)).head(120)
        if fut.height < 2:
            continue
        base = g.filter(pl.col("date") <= Date.fromisoformat(d0))["close"]
        if base.is_empty() or base[-1] <= 0:
            continue
        rets = (fut["close"] / base[-1] - 1).to_list()
        out[(code, d0)] = int(max(range(len(rets)), key=lambda i: rets[i]) + 1)
    return out


# ------------------------------------------------------------------ 表

def build(rows: list[dict]) -> dict[str, Any]:
    T: dict[str, Any] = {}
    d0_rows = [r for r in rows if r["station"] == "d0"]

    def table(rs: list[dict]) -> dict:
        out = {}
        for name, get in FIELDS.items():
            pairs = [(get(r["card"]), r["y"]) for r in rs if get(r["card"]) is not None]
            if pairs:
                out[name] = contingency(pairs)
        return out

    T["T1"] = {"note": "全樣本(d0 站位)每個封閉詞彙取值 × 結果",
               "n": len(d0_rows), "fields": table(d0_rows)}
    T["T2"] = {"note": "分 retrievability_score(≥2 / <2)。**「查無消息」只有在 ≥2 的"
                       "子集上才可詮釋為「當時沒有消息」**,否則那是網頁存活率的假象",
               "strata": {k: {"n": len(v), "fields": table(v)} for k, v in
                          (("cov_ge2", [r for r in d0_rows if (r["retriev"] or 0) >= 2]),
                           ("cov_lt2", [r for r in d0_rows if (r["retriev"] or 0) < 2]))}}
    by_era = defaultdict(list)
    for r in d0_rows:
        by_era[r["era_code"]].append(r)
    T["T3"] = {"note": "分密封期別碼(複製門檻:跨 ≥2 個不重疊期別才算規律)",
               "strata": {e: {"n": len(v), "fields": table(v)} for e, v in sorted(by_era.items())}}
    T["T4"] = {"note": "分成交值截面十分位帶(底部十分位 vs 其餘)",
               "strata": {k: {"n": len(v), "fields": table(v)} for k, v in
                          (("bottom_decile", [r for r in d0_rows if r["adv_decile"] == 0]),
                           ("rest", [r for r in d0_rows if (r["adv_decile"] or 0) > 0]))}}

    def calib(rs: list[dict], p: str) -> dict:
        bins = defaultdict(lambda: [0, 0])
        for r in rs:
            if r[p] is None:
                continue
            b = min(int(r[p] // 10) * 10, 90)
            bins[b][0] += 1
            bins[b][1] += r["y"]
        a, n = auc([r[p] for r in rs], [r["y"] for r in rs])
        return {"auc": None if math.isnan(a) else round(a, 4), "n": n,
                "reliability": {str(k): {"n": v[0], "hit_rate": round(v[1] / v[0], 4)}
                                for k, v in sorted(bins.items())}}

    T["T5"] = {"note": "盲判校準。**不報 Brier**——本樣本 50/50 富集、母體 6.38%,"
                       "Brier 在富集樣本上與生產端無關;AUC 是排序統計,不受類別比例影響",
               "base_rate_population": 0.0638, "sample_is_enriched_50_50": True,
               "deep": {"overall": calib(d0_rows, "p_deep"),
                        "by_era": {e: calib(v, "p_deep") for e, v in sorted(by_era.items())},
                        "by_coverage": {k: calib(v, "p_deep") for k, v in
                                        (("ge2", [r for r in d0_rows if (r["retriev"] or 0) >= 2]),
                                         ("lt2", [r for r in d0_rows if (r["retriev"] or 0) < 2]))}},
               "thin": {"overall": calib(d0_rows, "p_thin")}}

    paired = [r for r in d0_rows if r["p_thin"] is not None and r["p_deep"] is not None]
    T["T5b"] = {
        "note": "**標記日深度 vs 考掘深度**。下游標記 Agent 拿得到的是 thin 那個深度"
                "(實測舊管線每檔 3.5 筆材料),不是九格考掘。**thin 的 AUC 才是這套"
                "質化系統實際能做到的數字;deep 是上限,生產端到不了。** 兩者的差就是"
                "加深查證的邊際價值,直接決定要不要改生產設計。",
        "n_paired": len(paired),
        "thin": calib(paired, "p_thin"), "deep": calib(paired, "p_deep"),
        "delta_per_case": sorted(r["p_deep"] - r["p_thin"] for r in paired),
        "delta_mean": round(sum(r["p_deep"] - r["p_thin"] for r in paired) / len(paired), 2)
        if paired else None,
        "flipped_decision": sum(
            1 for r in paired
            if (r["card"].get("bet", {}).get("mark_or_not")
                != json.loads((NEWS / f"{r['code']}_{r['d0']}" /
                               "ex_ante_thin_d0.json").read_text(encoding="utf-8")).get("mark_or_not")
                if (NEWS / f"{r['code']}_{r['d0']}" / "ex_ante_thin_d0.json").exists() else False)),
    }

    mk = [(r["card"].get("bet", {}).get("mark_or_not"), r["y"], r["realized_ret"])
          for r in d0_rows]
    T["T6"] = {"note": "mark_or_not 混淆矩陣 + 標記組與未標記組的前瞻報酬分布",
               "confusion": {f"{m}|y={y}": c for (m, y), c in
                             Counter((m, y) for m, y, _ in mk).items()},
               "fwd_by_decision": {m: sorted(round(f, 3) for _, _, f in
                                             [x for x in mk if x[0] == m] if f is not None)
                                   for m in {m for m, _, _ in mk if m}}}

    prev = {(r["code"], r["d0"]): r for r in rows if r["station"] == "d_prev"}
    both = [(prev[(r["code"], r["d0"])], r) for r in d0_rows if (r["code"], r["d0"]) in prev]
    T["T7"] = {"note": "兩站對比:**等一個月的資訊價值**",
               "n_both": len(both),
               "auc_d_prev": calib([p for p, _ in both], "p_deep"),
               "auc_d0": calib([c for _, c in both], "p_deep"),
               "mark_window": Counter(
                   ("both" if p["card"].get("bet", {}).get("mark_or_not") == "標記"
                    and c["card"].get("bet", {}).get("mark_or_not") == "標記"
                    else "d0_only" if c["card"].get("bet", {}).get("mark_or_not") == "標記"
                    else "d_prev_only" if p["card"].get("bet", {}).get("mark_or_not") == "標記"
                    else "neither") for p, c in both)}

    T["T8"] = {"note": "within_theme_rank × 結果:**題材方向對但選錯股**的比率",
               "rows": contingency([(r["card"].get("within_theme_rank"), r["y"])
                                    for r in d0_rows if r["card"].get("within_theme_rank")])}
    T["T9"] = {"note": "story_also_fits 三組 × 結果——**過度解釋檢驗**。若『找得到好故事』"
                       "在三組間沒有分離,故事品質本身零價值,判別必須建立在同儕比較上",
               "rows": contingency([(r["card"].get("story_also_fits"), r["y"])
                                    for r in d0_rows if r["card"].get("story_also_fits")])}

    # 「最強」= 該欄位任一取值離基準率(50%,本樣本 50/50 富集)最遠者。
    # 只取前 8 是為了讓兩兩交叉表不爆炸;交叉表本來就只在單變量有訊號時才有意義。
    strong = [f for f, _ in sorted(
        ((f, max((abs(x["p_pos_given_value"] - 0.5) for x in v), default=0.0))
         for f, v in T["T1"]["fields"].items()), key=lambda x: -x[1])[:8]]
    T["T10"] = {"note": "單變量前 8 強特徵的兩兩交叉表", "features": strong,
                "pairs": {f"{a}×{b}": contingency(
                    [((FIELDS[a](r["card"]), FIELDS[b](r["card"])), r["y"]) for r in d0_rows
                     if FIELDS[a](r["card"]) is not None and FIELDS[b](r["card"]) is not None])
                    for i, a in enumerate(strong) for b in strong[i + 1:]}}

    eff = {(r["code"], r["d0"]): _effort(r["code"], r["d0"]) for r in d0_rows}
    T["T11"] = {"note": "**努力洩漏稽核**:查詢次數若本身就能預測結果,統計出來的"
                        "「有消息的比較會漲」就有一部分是搜尋行為造出來的,不是市場的性質",
                "auc_by_effort": {k: (lambda a: {"auc": None if math.isnan(a[0]) else round(a[0], 4),
                                                 "n": a[1]})(
                    auc([eff[(r["code"], r["d0"])][k] for r in d0_rows],
                        [r["y"] for r in d0_rows]))
                    for k in ("queries", "gates_run", "gates_hit", "materials")}}

    def freq(get: Callable[[dict], list]) -> dict:
        c = Counter()
        for r in d0_rows:
            for x in get(r["card"]) or []:
                c[str(x)[:60]] += 1
        return dict(c.most_common(40))

    T["T12"] = {"note": "自由文字欄位的**字面**頻次(未正規化)——聚類由蒸餾器由下而上做,"
                        "這裡不預設任何分類",
                "novel_features": freq(lambda c: [x.get("name") for x in
                                                  c.get("novel_features", []) or []]),
                "kill_criteria": freq(lambda c: c.get("bet", {}).get("kill_criteria")),
                "news_types": freq(lambda c: [i.get("type") for i in
                                              c.get("news_structure", {}).get("items", []) or []])}

    dtp = _days_to_peak(d0_rows)
    T["T13"] = {"note": "days_to_peak(站位到區間高點的交易日數)。**這是現象本身的時序**,"
                        "不涉及任何持有或出場規格——那屬於下游量化引擎",
                "values": sorted(dtp.values()),
                "by_case": {f"{k[0]}@{k[1]}": v for k, v in sorted(dtp.items())}}

    T["T14"] = {"note": "證據等級組成 × 結果",
                "rows": contingency([
                    (max(r["card"].get("retrieval", {}).get("coverage_tier_mix", {}) or {"?": 0},
                         key=lambda k: (r["card"].get("retrieval", {})
                                        .get("coverage_tier_mix", {}) or {"?": 0})[k]), r["y"])
                    for r in d0_rows])}

    def driver(c: dict) -> str | None:
        v = c.get("driver_found")
        return v if v is not None else None

    T["T15"] = {"note": "站位前是否存在可事前得知的驅動 × 結果,分覆蓋與期別。"
                        "**類型由蒸餾器從 driver_shape 自由文字由下而上聚類**,此處只報有無",
                "overall": contingency([(driver(r["card"]), r["y"]) for r in d0_rows
                                        if driver(r["card"])]),
                "by_coverage": {k: contingency([(driver(r["card"]), r["y"]) for r in v
                                                if driver(r["card"])]) for k, v in
                                (("ge2", [r for r in d0_rows if (r["retriev"] or 0) >= 2]),
                                 ("lt2", [r for r in d0_rows if (r["retriev"] or 0) < 2]))},
                "by_era": {e: contingency([(driver(r["card"]), r["y"]) for r in v
                                           if driver(r["card"])]) for e, v in sorted(by_era.items())}}
    return T


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-prices", action="store_true", help="跳過 T13(不連 cache)")
    a = ap.parse_args()
    rows = _load()
    if not rows:
        raise SystemExit(f"找不到任何階段 A 卡片({NEWS});先跑歸因批次")
    print(f"載入 {len(rows)} 列(案 × 站位);"
          f"正例 {sum(r['y'] for r in rows)} / 負例 {sum(1 - r['y'] for r in rows)}")
    if a.skip_prices:
        globals()["_days_to_peak"] = lambda _: {}
    T = build(rows)
    OUT.mkdir(parents=True, exist_ok=True)
    for name, tbl in T.items():
        (OUT / f"{name}.json").write_text(
            json.dumps(tbl, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"{len(T)} 張表 → {OUT}")
    if (t5b := T.get("T5b", {})).get("n_paired"):
        print(f"  T5b 標記日深度 AUC {t5b['thin']['auc']} vs 考掘深度 {t5b['deep']['auc']}"
              f"(配對 {t5b['n_paired']} 例,平均差 {t5b['delta_mean']})")


if __name__ == "__main__":
    main()
