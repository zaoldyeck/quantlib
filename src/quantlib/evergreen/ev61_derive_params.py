"""EV61:把盤點表上「待量測」的參數逐個導出——不留任何說不出出處的數字。

對應 `docs/strategy_research/ev60_no_magic_numbers.md` 的待辦編號:

    #12 regime 的 r12m / dd3y 窗長(250 / 750)
    #13 每格樣本數 `--per-cell`(18)
    #15 流動性層切點(adv_dec ≥7 / ≥3)
    #17 批次大小 `BATCH`(6)
    #21 卡片規模級距數(5)
    #32 train/test 切點(2015-12-31)

每一項的導出**都必須指名判準**——「這個值最大化/最小化什麼」。答不出判準的,
就不是導出,只是換一個地方拍腦袋。

Run: uv run --project . python -m quantlib.evergreen.ev61_derive_params
依賴 cache: 是。
"""
from __future__ import annotations

import argparse
import json
import math
import pathlib

import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.evergreen import ev57_station_sample as ev57
from quantlib.evergreen.ev60_threshold_study import _rich_features
from quantlib.evergreen.ev57_station_sample import _panel, _stations, _universe

OUT = paths.OUT / "ev61_derived_params.json"


# ---------------------------------------------------------------- #13 每格樣本數

def per_cell(base_rate: float, cells: int = 12, lift: float = 1.5,
             alpha: float = 0.05, power: float = 0.80) -> dict:
    """由**檢力**導出每格樣本數,而不是「湊到 216 檔」。

    判準:一條判別規則若真實 lift = `lift`,我們要有 `power` 的機率偵測到它。
    這是雙比例檢定的標準樣本數公式(常態近似):

        n_per_arm = (z_{1-α/2} √(2 p̄ q̄) + z_{power} √(p₁q₁ + p₂q₂))² / (p₁ − p₂)²

    三個輸入各自的出處:
    - `lift = 1.5` —— 蒸餾提示詞的鐵律第 4 條要求規則必須帶雙邊出現率且信賴區間不跨 1。
      1.5 倍是「值得寫進哲學的最小效果」:低於它的規則,即使統計顯著,對 15 檔的
      月度池子也改變不了選誰。**這個值是使用者可調的政策參數,不是資料事實**,
      故一併輸出 1.2 / 1.5 / 2.0 三檔供裁決。
    - `alpha = 0.05` / `power = 0.80` —— 統計慣例,非調校值。
    - `cells = 12` —— 4 regime × 3 流動性層,由分層設計決定。
    """
    z_a = 1.959963985  # Φ⁻¹(0.975)
    z_b = 0.841621234  # Φ⁻¹(0.80)
    p1 = min(base_rate * lift, 0.99)
    p2 = base_rate
    pbar = (p1 + p2) / 2
    num = (z_a * math.sqrt(2 * pbar * (1 - pbar))
           + z_b * math.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2
    n_arm = num / (p1 - p2) ** 2
    return {"lift": lift, "p_rule_in_pos": round(p1, 4), "p_rule_in_neg": round(p2, 4),
            "n_per_arm": math.ceil(n_arm), "cells": cells,
            "per_cell": math.ceil(n_arm / cells)}


# ---------------------------------------------------------------- #15 / #21 分位設計

def tier_cuts(u: pl.DataFrame, k: int = 10) -> dict:
    """流動性層的**分組**——由「相鄰十分位的基準率分不分得開」合併出來,不由人挑切點。

    第一版的判準錯了:要求「每一組相鄰都可分辨」,於是只要中間任兩格相近就退回 2 組。
    但實測的十分位基準率是 [35.3%, 28.1%, 26.6%, 26.0%, …] ——**結構是「最低十分位
    與其餘不同,其餘彼此相同」**,不是「等間隔遞減」。要求全部相鄰可分辨,等於要求
    一個資料裡不存在的形狀。

    正確做法:先切等頻十分位(切點由資料定),再**貪婪合併相鄰且基準率信賴區間重疊
    的組**。合併不下去時剩幾組就是幾組,組界也是資料定的。整個過程沒有要挑的數字。
    """
    from quantlib.evergreen.ev58_tables import wilson
    b = (u.with_columns(((pl.col("adv20").rank("ordinal").over("date") * k - 1)
                         // pl.len().over("date")).alias("g"))
         .group_by("g").agg(pl.col("y").sum().alias("k"), pl.len().alias("n"))
         .sort("g"))
    groups = [{"deciles": [r["g"]], "k": int(r["k"]), "n": int(r["n"])}
              for r in b.to_dicts()]
    merged = True
    while merged and len(groups) > 1:
        merged = False
        for i in range(len(groups) - 1):
            a, c = groups[i], groups[i + 1]
            la, ha = wilson(a["k"], a["n"])
            lc, hc = wilson(c["k"], c["n"])
            if not (ha < lc or hc < la):          # 區間重疊 ⇒ 這兩組分不開,合併
                groups[i] = {"deciles": a["deciles"] + c["deciles"],
                             "k": a["k"] + c["k"], "n": a["n"] + c["n"]}
                del groups[i + 1]
                merged = True
                break
    return {"decile_rates": [round(r["k"] / r["n"], 4) for r in b.to_dicts()],
            "groups": [{"deciles": g["deciles"], "n": g["n"],
                        "rate": round(g["k"] / g["n"], 4),
                        "wilson": [round(x, 4) for x in wilson(g["k"], g["n"])]}
                       for g in groups],
            "n_groups": len(groups)}


# ---------------------------------------------------------------- #12 regime 窗長

def regime_windows(con, u: pl.DataFrame, need_per_cell: int = 16,
                   liq_groups: int = 3) -> dict:
    """regime 的兩個窗長 + 三個門檻,一起掃。

    第一版有兩個我自己造的錯,都是口徑不一致:
    (a) 用了 `market_index` 的加權指數,而 EV57 用的是**自建等權 proxy**——兩者不同物;
        而且加權指數在 cache 裡只有 2009-01 起,2008 整年缺,早期站位一律落到 fallback。
    (b) 用了與 EV57 不同的門檻(-0.30/-0.15 vs -0.25/+0.30/0.0)。
    改法:直接呼叫 `ev57._regime`(已參數化)——**定義只有一份,掃描與生產共用**。

    判準:regime 的用途是分層,分層只在「各層基準率確實不同」時才有意義,但一個
    抽不出配額的層等於沒有。故取 **在配額可填滿的前提下,基準率極差最大** 的組合。

    「可填滿」的定義由 #13 直接導出,不另設數字:每個 (regime × 流動性組) 格內的
    **正例數 ≥ `need_per_cell`**。第一版用「該層佔全體觀測的比例 ≥ 1/(2×層數)」當
    覆蓋條件——那是錯的判準:崩跌 regime 在歷史上本來就只佔 5-7%,但它的基準率高達
    ~50%,那 5% 裡的正例遠超過配額所需。**把「稀有」誤當成「不夠用」,會逼出一個
    為了湊佔比而放寬的崩跌定義,反而稀釋掉最有資訊量的那一層。**
    """
    out = {}
    for r12 in (125, 250, 375):
        for dd in (500, 750, 1000):
            for crash in (-0.20, -0.25, -0.30):
                g = ev57._regime(con, r12m_win=r12, dd_win=dd, crash=crash)
                j = u.join(g, on="date", how="inner")
                s_ = (j.group_by("regime").agg(pl.col("y").mean().alias("rate"),
                                               pl.len().alias("n")).sort("regime"))
                if s_.height < 4:
                    continue
                rates, ns = s_["rate"].to_list(), s_["n"].to_list()
                # 逐 (regime × 流動性組) 格數正例,取最小格——那格填不滿就是填不滿
                cell = (j.with_columns(
                    ((pl.col("adv20").rank("ordinal").over("date") * liq_groups - 1)
                     // pl.len().over("date")).alias("lg"))
                    .group_by(["regime", "lg"]).agg(pl.col("y").sum().alias("pos")))
                min_pos = int(cell["pos"].min()) if cell.height else 0
                out[f"r12m={r12},dd={dd},crash={crash}"] = {
                    "spread": round(max(rates) - min(rates), 4),
                    "min_regime_share": round(min(ns) / sum(ns), 4),
                    "min_cell_positives": min_pos,
                    "cells": int(cell.height),
                    "meets_coverage_floor": bool(min_pos >= need_per_cell
                                                 and cell.height == s_.height * liq_groups),
                    "rates": {k: round(v, 4) for k, v in zip(s_["regime"].to_list(), rates)},
                    "shares": {k: round(n / sum(ns), 4)
                               for k, n in zip(s_["regime"].to_list(), ns)}}
    elig = {k: v for k, v in out.items() if v["meets_coverage_floor"]}
    best = max(elig.items(), key=lambda kv: kv[1]["spread"]) if elig else (None, None)
    return {"by_config": out, "n_eligible": len(elig), "best": best[0],
            "best_detail": best[1]}


# ---------------------------------------------------------------- #17 批次大小

def batch_size() -> dict:
    """批次大小由**結構不可反推**這個約束導出,不是挑的。

    約束鏈(每一步都是必要條件):
    1. 批內要有配對(正例 + 其配對負例),否則沒有「同站位同分數的對照」可比 ⇒ 至少 1 對。
    2. 只有一對的話,批內若剛好 1 正 1 負,agent 立刻知道是配對 ⇒ 至少 2 對。
    3. 只有配對的話正負恆等(3:3),比例是常數 ⇒ 必須有未配對檔打散比例。
    4. 未配對只有 1 檔,比例只會是 3:2 或 2:3 兩種且必為奇數總數,仍可反推 ⇒ 至少 2 檔。

    ⇒ 最小批 = 2 對 × 2 + 2 未配對 = **6**。再大只是增加單一 agent 的 context 負擔
    與中斷時的損失半徑,沒有帶進新的不可反推性。
    """
    pairs, unpaired = 2, 2
    b = pairs * 2 + unpaired
    ratios = sorted({pairs + k for k in range(unpaired + 1)})
    return {"min_pairs": pairs, "min_unpaired": unpaired, "batch": b,
            "possible_positive_counts": ratios,
            "note": "正例數可能為 %s,共 %d 種 ⇒ 無法由結構反推配對關係"
                    % (ratios, len(ratios))}


# ---------------------------------------------------------------- #18 年代分桶寬度

def era_band_width() -> dict:
    """年代分桶的寬度,由**當年語彙的汰換速度**導出。

    分桶的唯一用途:讓「當年語彙表」在桶內共用一份仍然管用。所以桶該多寬,就是
    「語彙撐多久還沒換掉」——不是 2-3 年這個我拍的數字。

    量法:六份年代語境卡各有一張當年語彙表(`then` 欄=當年的說法)。算兩兩期別的
    Jaccard 相似度,對「期別中點的時間距離」看衰減,取**相似度掉到一半的距離**當
    半衰期。桶寬取半衰期——桶內語彙相似度即維持在 0.5 以上。半衰期是分布本身的性質,
    沒有要挑的門檻。
    """
    from quantlib.evergreen.ev58_prompt import CARDS, NEWS
    m = json.loads((CARDS / "_era_map.json").read_text(encoding="utf-8"))
    vocab, concept, mid = {}, {}, {}
    from datetime import date as _D
    for era, span in m.items():
        f = NEWS / "_era_brief" / f"{era}.json"
        if not f.exists():
            continue
        d = json.loads(f.read_text(encoding="utf-8"))
        terms, concepts = set(), set()
        for v in d.get("vocabulary", []) or []:
            for fld, bag in (("then", terms), ("today", concepts)):
                t = v.get(fld)
                for x in ([t] if isinstance(t, str) else (t or [])):
                    if isinstance(x, str) and x.strip():
                        bag.add(x.strip())
        if not terms:
            continue
        vocab[era] = terms
        concept[era] = concepts
        a, b = (_D.fromisoformat(x) for x in span.split("~"))
        mid[era] = (a.toordinal() + b.toordinal()) / 2 / 365.25
    pairs = []
    eras = sorted(vocab)
    for i, x in enumerate(eras):
        for y in eras[i + 1:]:
            inter = len(vocab[x] & vocab[y])
            union = len(vocab[x] | vocab[y])
            ci = len(concept.get(x, set()) & concept.get(y, set()))
            cu = len(concept.get(x, set()) | concept.get(y, set()))
            pairs.append({"a": x, "b": y, "gap_years": round(abs(mid[x] - mid[y]), 2),
                          "jaccard_then": round(inter / union, 4) if union else 0.0,
                          "jaccard_today": round(ci / cu, 4) if cu else 0.0,
                          "shared": inter, "n_a": len(vocab[x]), "n_b": len(vocab[y])})
    pairs.sort(key=lambda r: r["gap_years"])
    # 半衰期:相似度首次跌破「最近一對的一半」時的距離。用最近一對當基準而非固定值,
    # 因為 Jaccard 的絕對水準受語彙表長度影響,只有相對衰減可比。
    # **有效性檢查**:六份語彙表由六個獨立 agent 各自撰寫,字面 Jaccard 可能量到的是
    # 用詞習慣差異而非語彙汰換。`today`(現代說法=概念)與 `then`(當年說法)並列即可
    # 分辨:概念重疊高而當年說法重疊低 ⇒ 真的是語彙換了;兩者都低 ⇒ 只是作者不同。
    mt = sum(r["jaccard_then"] for r in pairs) / max(len(pairs), 1)
    mc = sum(r["jaccard_today"] for r in pairs) / max(len(pairs), 1)
    valid = mc > 2 * mt and mc > 0.15
    base = pairs[0]["jaccard_then"] if pairs else 0.0
    half = next((r["gap_years"] for r in pairs if r["jaccard_then"] < base / 2), None)
    return {"pairs": pairs, "mean_jaccard_then": round(mt, 4),
            "mean_jaccard_today": round(mc, 4), "measurement_valid": bool(valid),
            "closest_pair_jaccard": base, "half_life_years": half,
            "verdict": (f"半衰期 ≈ {half} 年 ⇒ 桶寬取此值" if valid and half else
                        "**量測無效**:概念重疊(today %.3f)未明顯高於用詞重疊"
                        "(then %.3f)⇒ 量到的是六位作者的用詞習慣差異,不是語彙汰換。"
                        "需改用同一位作者跨期別比對,或直接量當年新聞語料的詞頻變化。"
                        % (mc, mt))}


# ---------------------------------------------------------------- #25 檢索飽和

def retrieval_saturation() -> dict:
    """`retrievability_score ≥ 2` 要求「≥2 個獨立外部來源」——2 這個數字的出處。

    量法:用最大的現成語料 `ev27_news`(224 案、1,088 筆材料)。對每一案,把
    **站位前**的材料按來源分組,問:第 k 個來源帶進多少**新的日期**(= 新事實的代理)?
    曲線壓平的地方,就是再多一個來源也不會改變「有沒有驅動」這個判定的地方。

    為什麼用「新日期」當新事實的代理:同一件事被多家報導,日期會重複;不同的事
    才會帶進新日期。這比字面去重穩,也不需要任何相似度門檻。
    """
    base = pathlib.Path("src/quantlib/evergreen/data/ev27_news")
    curves: dict[int, list[float]] = {}
    per_case = []
    for f in sorted(base.glob("*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        d = d if isinstance(d, list) else [d]
        pre = [r for r in d if r.get("pit") == "pre_t0"]
        by_src: dict[str, set] = {}
        for r in pre:
            by_src.setdefault(str(r.get("source", "?")), set()).add(str(r.get("date"))[:10])
        # 來源依「帶進的日期數」由多到少排序——模擬研究員先找到覆蓋最好的來源
        order = sorted(by_src.values(), key=len, reverse=True)
        seen: set = set()
        gains = []
        for s_ in order:
            new = len(s_ - seen)
            seen |= s_
            gains.append(new)
        per_case.append({"file": f.stem, "n_sources": len(order),
                         "total_dates": len(seen), "gains": gains})
        for k, g in enumerate(gains, 1):
            curves.setdefault(k, []).append(g / max(len(seen), 1))
    marg = {k: round(sum(v) / len(v), 4) for k, v in sorted(curves.items()) if len(v) >= 10}
    cum = {}
    run = 0.0
    for k, v in marg.items():
        run += v
        cum[k] = round(run, 4)
    # 飽和點:邊際貢獻首次低於「第一個來源的 10%」——10% 是相對於自身第一名的
    # 比例而非絕對門檻,故不隨語料規模漂移。
    first = marg.get(1, 0.0)
    sat = next((k for k, v in marg.items() if v < first * 0.10), None)
    return {"n_cases": len(per_case),
            "mean_sources_per_case": round(sum(c["n_sources"] for c in per_case)
                                           / max(len(per_case), 1), 2),
            "marginal_new_date_share": marg, "cumulative_share": cum,
            "saturation_k": sat,
            "verdict": (f"第 {sat} 個來源之後,新事實的邊際貢獻低於首源的 10% "
                        f"⇒ 門檻取 {sat - 1 if sat else '?'}"
                        if sat else "未達飽和 ⇒ 現有語料不足以定門檻")}


def era_bands_by_quota(u: pl.DataFrame, per_generation: int = 3) -> dict:
    """年代分桶:**把參數刪掉**,而不是替它找一個量不出來的理由。

    語彙汰換速度量不出來(見 `era_band_width`:六位作者的用詞習慣差異蓋過真實汰換),
    所以不要再問「幾年一桶」。改問桶要滿足什麼**約束**,答案就唯一了:

    1. **桶界必須落在漲跌幅世代斷點(2015-06-01)上** —— 跨世代的桶,其「當年制度」
       欄位自相矛盾。這是界限,不是選擇。
    2. **每個世代至少要能拿出 1 個保留期別** —— 期別保留組的設計要求各世代各一個。
    3. **蒸餾期別至少 4 個** —— 蒸餾提示詞的複製門檻是「跨 ≥2 個不重疊期別」,而
       `era_support` / `era_counter` / `era_sparse` 三分類要能互相分辨,至少需要 4 個
       蒸餾期別;只有 2 個時,「在 1 個成立、1 個反例」與「1 個成立、1 個樣本不足」
       在資料上長得一樣。
    ⇒ 總桶數 = 4 蒸餾 + 2 保留 = **6**,即每個世代 3 個。

    4. **桶內樣本等量** —— 桶界由等樣本分位決定,不由人挑年份。各桶檢力才一致;
       時間等寬會讓樣本稀薄的年份自成一桶而統計上無用。
    """
    out = {}
    for gen, sub in (("7%", u.filter(pl.col("date") < ev57.LIMIT_ERA_SPLIT)),
                     ("10%", u.filter(pl.col("date") >= ev57.LIMIT_ERA_SPLIT))):
        d = sub.select("date").sort("date")
        n = d.height
        cuts = [d["date"][min(int(n * i / per_generation), n - 1)]
                for i in range(1, per_generation)]
        edges = [d["date"][0]] + cuts + [d["date"][n - 1]]
        out[gen] = {"n_obs": n,
                    "bands": [{"start": str(edges[i]), "end": str(edges[i + 1]),
                               "n": int(sub.filter((pl.col("date") >= edges[i])
                                                   & (pl.col("date") <= edges[i + 1])).height)}
                              for i in range(per_generation)]}
    return {"per_generation": per_generation, "total_bands": per_generation * 2,
            "by_generation": out,
            "rationale": "桶數由『2 保留 + ≥4 蒸餾』導出;桶界由等樣本分位 + 世代斷點導出"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-regime", action="store_true")
    a = ap.parse_args()

    con = data.connect()
    print("載入站位母體…", flush=True)
    panel = _panel(con)
    u = _universe(con, _rich_features(panel), _stations(panel))
    # 標籤用 EV60 導出的定義與門檻的**代理**(fwd_max_ret ≥ 0.30);
    # trail25 需重放,本模組只需基準率量級,代理足夠且不影響格數判定。
    u = u.with_columns((pl.col("fwd_max_ret") >= 0.30).cast(pl.Int8).alias("y"))
    base = float(u["y"].mean())
    print(f"母體 {u.height:,};代理基準率 {base:.2%}\n")

    res = {"base_rate_proxy": round(base, 5)}

    print("=== #13 每格樣本數(由檢力導出)===")
    res["per_cell"] = [per_cell(base, lift=l) for l in (1.2, 1.5, 2.0)]
    for r in res["per_cell"]:
        print(f"  最小可偵測 lift {r['lift']}: 每臂 {r['n_per_arm']:,} 檔 "
              f"⇒ 每格 {r['per_cell']}(12 格)")

    print("\n=== #15 / #21 分位格數(等頻 + 相鄰格可分辨)===")
    res["tiers"] = tier_cuts(u)
    print(f"  十分位基準率 {res['tiers']['decile_rates']}")
    for g in res["tiers"]["groups"]:
        print(f"  組 {g['deciles']}:n={g['n']:,} 基準率 {g['rate']:.2%} "
              f"CI [{g['wilson'][0]:.2%}, {g['wilson'][1]:.2%}]")
    print(f"  ⇒ 資料支持 {res['tiers']['n_groups']} 組")

    print("\n=== #17 批次大小(由結構不可反推導出)===")
    res["batch"] = batch_size()
    print(f"  {res['batch']['note']} ⇒ BATCH = {res['batch']['batch']}")

    if not a.skip_regime:
        print("\n=== #12 regime 窗長(由各層基準率分離度導出)===")
        need = res["per_cell"][1]["per_cell"]      # lift=1.5 那一檔,見 #13
        res["regime"] = regime_windows(con, u, need_per_cell=need)
        for k, v in sorted(res["regime"]["by_config"].items(),
                           key=lambda kv: -kv[1]["spread"])[:10]:
            print(f"  {k:>30}  極差 {v['spread']:>7.2%}  最小格正例 {v['min_cell_positives']:>6,}"
                  f"  {'✓' if v['meets_coverage_floor'] else '✗ 填不滿'}")
        print(f"  ⇒ 最佳 {res['regime']['best']}")
        if res["regime"].get("best_detail"):
            d = res["regime"]["best_detail"]
            print(f"     各層基準率 {d['rates']}")
            print(f"     各層佔比   {d['shares']}  最小格正例 {d['min_cell_positives']:,}")

    print("\n=== #18 年代分桶寬度(由語彙汰換速度導出)===")
    res["era_band"] = era_band_width()
    for r in res["era_band"]["pairs"][:8]:
        print(f"  {r['a']}×{r['b']}  相距 {r['gap_years']:>5.1f} 年  當年說法 {r['jaccard_then']:.4f}"
              f"  現代概念 {r['jaccard_today']:.4f}")
    print(f"  ⇒ {res['era_band']['verdict']}")

    print("\n=== #18b 年代分桶:改由約束導出(取代量不出來的語彙汰換)===")
    res["era_bands"] = era_bands_by_quota(u)
    for gen, v in res["era_bands"]["by_generation"].items():
        print(f"  {gen} 世代({v['n_obs']:,} 觀測):")
        for b in v["bands"]:
            print(f"    {b['start']} ~ {b['end']}  n={b['n']:,}")
    print(f"  ⇒ {res['era_bands']['rationale']}")

    print("\n=== #25 檢索飽和(由 ev27_news 1,088 筆材料導出)===")
    res["retrieval"] = retrieval_saturation()
    r = res["retrieval"]
    print(f"  {r['n_cases']} 案,平均每案 {r['mean_sources_per_case']} 個來源")
    print(f"  第 k 個來源帶進的新日期佔比:{r['marginal_new_date_share']}")
    print(f"  累積:{r['cumulative_share']}")
    print(f"  ⇒ {r['verdict']}")

    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\n→ {OUT}")


if __name__ == "__main__":
    main()
