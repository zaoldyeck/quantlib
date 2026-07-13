"""三策略每日顧問核心:S / Evergreen / Serenity 的 live 評判(永不下單)。

每個 advisor 把整個帳戶視為自己的:對每檔現有持股給 KEEP / SELL(附機械
原因),並給出今日買入清單。出場評判一律 **lot 錨定、對使用者的真實持倉**
——絕不用任何回測模擬簿的成員資格當判準(2026-07-13 教訓)。

規格來源:S = research/apex/STRATEGY.md(逐條移植;per-position 狀態存
research/tri/state/,cost=收養日收盤);Evergreen v3.3 = LEDGER EV30-33
定版(h52-only × pm3 × h120>0.6 × trail40 × lts45 × 5 席 mn2 等權);
Serenity = live ledger 錨 × `research/serenity/exit_rules.py`(與執行系統
同一份六道門規則源)。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date as Date

import polars as pl

C = "company_code"
STATE_DIR = "research/tri/state"


# ── per-position 出場狀態(首見日 + 持有期收盤峰值)────────────────


def load_state(name: str) -> dict:
    p = f"{STATE_DIR}/{name}_positions.json"
    return json.load(open(p)) if os.path.exists(p) else {}


def save_state(name: str, st: dict) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    json.dump(st, open(f"{STATE_DIR}/{name}_positions.json", "w"),
              ensure_ascii=False, indent=1)


def update_state(st: dict, holdings: dict[str, float], today: Date,
                 closes: dict[str, float], tdays_index: dict) -> dict:
    """收養新持股、更新峰值、移除已出清者。

    cost = 收養日收盤(輸家時間止損的水線;2026-07-13 修正:先前從未寫入,
    導致 S/Evergreen 的輸家止損是永不觸發的死代碼)。舊 state 缺 cost 者
    以當日收盤回填——語義同「今日才收養」,時鐘以 first_seen 為準不重啟。"""
    out = {}
    for code in holdings:
        prev = st.get(code)
        px = closes.get(code)
        if prev is None:
            out[code] = {"first_seen": today.isoformat(), "peak_close": px,
                         "cost": px, "peak_missing": px is None}
        else:
            peak = prev.get("peak_close")
            if px is not None:
                peak = px if peak is None else max(peak, px)
            entry = {**prev, "peak_close": peak}
            if entry.get("cost") is None and px is not None:
                entry["cost"] = px
            out[code] = entry
    return out


def held_tdays(first_seen: str, today: Date, all_dates: list[Date]) -> int:
    fs = Date.fromisoformat(first_seen)
    return sum(1 for d in all_dates if fs < d <= today)


@dataclass
class Advice:
    strategy: str
    keeps: list = field(default_factory=list)     # (code, reason)
    sells: list = field(default_factory=list)     # (code, reason)
    buys: list = field(default_factory=list)      # (code, weight, reason)
    notes: list = field(default_factory=list)
    # 理想持倉完全體:照本策略規則收斂後的最終組合(code, 身分標記)。
    # renderer 會帶公司名輸出——使用者要求「要能一眼看懂完全體長什麼樣」。
    ideal_title: str = ""
    ideal: list = field(default_factory=list)


# ── S 策略顧問(apex_revcycle_S,STRATEGY.md 逐條)─────────────────

S_WTS = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0,
         "mom_126_5": 0.5, "rev_seq": 0.5, "accel_rel": 0.5}


def shares_for(weight: float, nav: float, price: float | None) -> str:
    if not price or nav <= 0:
        return "股數待現價確認"
    sh = int(nav * weight / price)
    lots = sh / 1000
    return f"約 {sh:,} 股(≈{lots:.1f} 張,現價 {price:.1f})"


def _sizing_hint(nav: float, weight: float, px: float | None, held: float) -> str:
    """KEEP 的收斂指示:照著做,持倉會逼近策略的目標權重。"""
    if not px or nav <= 0:
        return ""
    tgt = int(nav * weight / px)
    m = int(held)
    if tgt < 1:
        return f"|目標 {weight:.0%} 不足 1 股(現持 {m},維持)"
    if tgt == m:
        return f"|目標 {weight:.0%} ≈ {tgt} 股 ✓已達"
    verb = "加碼" if tgt > m else "減碼"
    return f"|目標 {weight:.0%} ≈ {tgt} 股(現持 {m} → {verb} {abs(tgt - m)} 股)"


def s_advisor(con, holdings: dict[str, float], today: Date,
              nav: float = 0.0) -> Advice:
    import os

    from research.apex import data
    from research.apex.assemble import apply_avail_override, build_features

    # 月營收事件驅動生效:資料庫首見即納入(不等法定 10 日)。
    # 歷史回測維持保守 10 日語義;此 override 僅 live 決策使用。
    fs_path = "research/data/revenue_first_seen.parquet"
    ov = None
    if os.path.exists(fs_path):
        ov = (pl.read_parquet(fs_path)
              .with_columns(pl.col("first_seen").str.to_date().alias("avail_date"))
              .select([C, "year", "month", "avail_date"]))

    ws = (today.replace(year=today.year - 2)).isoformat()
    panel, feat, elig = build_features(con, ws, today.isoformat(),
                                       avail_override=ov)
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    d0 = max(d for d in dates_all if d <= today)

    # rev_seq / accel_rel(STRATEGY.md 兩軸,照 apex 原式補算)
    rev = (data.load_monthly_revenue(con, today.isoformat())
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue").rolling_sum(3)
                / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
               .over(C).alias("rev_seq"),
           ]))
    rev = (apply_avail_override(rev, ov)
           .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance="70d").sort([C, "date"]))
    import duckdb
    raw = duckdb.connect("research/cache.duckdb", read_only=True)
    tax = raw.sql("SELECT company_code, effective_date, industry FROM "
                  "industry_taxonomy_pit WHERE industry IS NOT NULL "
                  "ORDER BY effective_date").pl()
    day = (feat.filter(pl.col("date") == d0)
           .join_asof(tax.sort("effective_date"), left_on="date",
                      right_on="effective_date", by=C, strategy="backward"))
    ind_med = (day.filter(pl.col("industry").is_not_null())
               .group_by("industry")
               .agg(pl.col("rev_yoy_accel").median().alias("_im")))
    day = (day.join(ind_med, on="industry", how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("_im"))
                         .alias("accel_rel")))

    el = elig.filter((pl.col("date") == d0) & pl.col("eligible")).select(C)
    pool = (day.filter(pl.col("rev_fresh_days") <= 7)
            .join(el, on=C, how="semi")
            .drop_nulls(subset=list(S_WTS)))
    med = pool["cfo_ni_ratio_ttm"].median()
    n_cov = pool["cfo_ni_ratio_ttm"].drop_nulls().len()
    if n_cov >= 0.3 * pool.height and med is not None:
        pool = pool.filter(pl.col("cfo_ni_ratio_ttm") >= med)
    expr = None
    for cname, wt in S_WTS.items():
        term = (pl.col(cname).rank() / pl.len()) ** wt
        expr = term if expr is None else expr * term
    pool = pool.with_columns(expr.alias("geo")).sort("geo", descending=True)

    adv = Advice("S(apex_revcycle_S)")
    closes = dict(panel.filter(pl.col("date") == d0)
                  .select([C, "close"]).iter_rows())
    st = load_state("s")
    st = update_state(st, holdings, d0, closes, {})
    fresh_all = dict(feat.filter(pl.col("date") == d0)
                     .select([C, "rev_fresh_days"]).iter_rows())
    peaks = st

    # 逐檔:先過硬出場規則,倖存者過「池檢」——S 只持有它自己會買的名字。
    # 池檢語義(2026-07-13 二修,使用者:「不是 S 最推薦的就該賣」):
    # (a) 在今日進場池內(新鮮+資格+現金流濾網全過)→ 合法部位,記
    #     vetted_pool 於 state,之後即使離開新鮮池也續抱到出場規則(回測的
    #     hold-until-exit 語義:S 不因出現更高排名者而中途換股);
    # (b) 不在池內且從未通過池檢 → 賣出(S 不會買的名字沒有理由持有)。
    pool_rank = {r[C]: i + 1 for i, r in enumerate(pool.to_dicts())}
    survivors: list[tuple[float, str, str]] = []  # (排序鍵, code, keep理由)
    for code in sorted(holdings):
        info = st.get(code, {})
        held = held_tdays(info.get("first_seen", d0.isoformat()), d0, dates_all)
        px = closes.get(code)
        peak = info.get("peak_close")
        fresh = fresh_all.get(code)
        if px is None:
            adv.sells.append((code, "無法取價(下市/停牌?)人工確認"))
            continue
        if fresh is None or fresh >= 26:
            adv.sells.append((code, f"訊號過期(揭露後 {'?' if fresh is None else int(fresh)} 日 ≥26,日曆日)"))
        elif peak and px <= peak * 0.65:
            adv.sells.append((code, f"移動停損(自峰值 {peak:.1f} 回落 ≥35%)"))
        elif held >= 30:
            adv.sells.append((code, f"時間止損(持有 {held} 交易日 ≥30)"))
        elif info.get("cost") and px < info["cost"] and held >= 15:
            adv.sells.append((code, f"輸家時間止損(水下且持有 {held} ≥15)"))
        elif code in pool_rank:
            st[code]["vetted_pool"] = True
            rk = pool_rank[code]
            survivors.append((rk, code, f"今日進場池 geo 排名 #{rk}(fresh={int(fresh)})"))
        elif info.get("vetted_pool"):
            survivors.append((5_000 + int(fresh), code,
                              f"既有合法部位(進場時通過池檢,fresh={int(fresh)} <26 續抱至出場規則)"))
        else:
            adv.sells.append((code, "非本策略標的(不在進場池且未曾通過池檢——"
                                    "S 不會買的名字沒有理由持有)"))
    survivors.sort()
    for i, (key, code, why) in enumerate(survivors, 1):
        if i <= 5:
            adv.keeps.append((code, f"{why}|席位 {i}/5"
                                    f"{_sizing_hint(nav, 0.20, closes.get(code), holdings[code])}"))
        else:
            adv.sells.append((code, f"超額席位(S 上限 5 檔;{why})"))
    save_state("s", st)

    # 買入清單 = 通往完全體的完整隊列(使用者要求看得到終局):
    # 「今日進場」(空位內、每日上限 2)→「⏸ 排隊」(有席位、輪明日)→
    # 「🕒 遞補」(席位已滿,等出缺)。照標記操作,持倉恆 ≤5 檔。
    n_slot = max(0, 5 - len(adv.keeps))
    rank_i = 0
    ideal_buys: list[tuple[str, str]] = []
    for r in pool.head(n_slot + 3).to_dicts():
        if r[C] in holdings:
            continue
        rank_i += 1
        detail = f"{shares_for(0.20, nav, closes.get(r[C]))}|fresh={int(r['rev_fresh_days'])}"
        if rank_i <= min(2, n_slot):
            adv.buys.append((r[C], 0.20, f"今日進場 #{rank_i}(每日上限 2)|{detail}"))
            ideal_buys.append((r[C], "今日買"))
        elif rank_i <= n_slot:
            adv.buys.append((r[C], 0.20, f"⏸ 排隊 #{rank_i}(有席位;每日上限 2,明日起依序進場)|{detail}"))
            ideal_buys.append((r[C], f"排隊 #{rank_i}"))
        else:
            adv.buys.append((r[C], 0.20, f"🕒 遞補(席位已滿,等出缺才輪到)|{detail}"))
    adv.ideal_title = "理想持倉完全體(5 席 × 20% 等權)"
    adv.ideal = [(c, "續抱") for c, _ in adv.keeps] + ideal_buys
    n_ov = 0 if ov is None else ov.height
    adv.notes.append(f"今日 fresh cohort {pool.height} 檔;決策日 {d0};"
                     f"營收事件驅動生效(首見日 override {n_ov} 筆)")
    if pool.height == 0:
        if 10 <= today.day <= 20:
            adv.notes.append("⚠ 現在是月營收揭露季但 cohort 為 0——多半是資料"
                             "未刷新(cache 還沒收到本月揭露);先跑 ① 再重看")
        else:
            adv.notes.append("(揭露季外零訊號屬正常——S 每月只在 10 日後"
                             "約一週有新訊號,其餘日子只管出場)")
    return adv


# ── Evergreen v3.3 顧問 ────────────────────────────────────────────


EG_LIVE_CFG = "research/evergreen/data/live_config.json"


def evergreen_advisor(con, holdings: dict[str, float], today: Date,
                      nav: float = 0.0) -> Advice:
    """引擎參數外部化:讀 live_config.json(EV43 年度 refit 產物)。

    refit 換檔即生效;gate/score 依 config 動態解讀,與回測引擎同語義。"""
    import json as _json
    import os as _os

    from research.apex import data

    if _os.path.exists(EG_LIVE_CFG):
        _doc = _json.load(open(EG_LIVE_CFG))
        LC = _doc["config"]
        _tag = f"live-refit {_doc.get('refit_date', '?')}"
    else:  # fallback:EV43 2026-07-13 定版
        LC = {"gate": "none", "score": "xadv_inv", "pool_months": 3,
              "h120": 0.6, "trail": 0.4, "lts": 45, "n_slots": 5, "max_new": 2}
        _tag = "live-refit(fallback 內建 2026-07-13)"
    NS, MN = int(LC["n_slots"]), int(LC["max_new"])
    W = 1.0 / NS

    reg = pl.read_parquet("research/evergreen/data/registry_v3.parquet")
    panel = data.common_stocks(data.load_panel(
        con, (today.replace(year=today.year - 2)).isoformat(),
        today.isoformat(), warmup_days=300))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    d0 = max(d for d in dates_all if d <= today)

    yms = sorted(reg["month"].unique().to_list())
    stance = {}
    for ym in yms:
        y, m = int(ym[:4]), int(ym[5:7])
        cand = [d for d in dates_all if d.year == y and d.month == m and d.day > 10]
        if cand:
            stance[ym] = min(cand)
    active = [ym for ym in yms if ym in stance and stance[ym] <= d0][-int(LC["pool_months"]):]
    pool_codes = set(reg.filter(pl.col("month").is_in(active))["code"].to_list())

    adv = Advice(f"Evergreen({_tag})")
    cur_ym = f"{d0.year}-{d0.month:02d}"
    this_stance = min((d for d in dates_all
                       if d.year == d0.year and d.month == d0.month and d.day > 10),
                      default=None)
    if this_stance and d0 >= this_stance and cur_ym not in yms:
        adv.notes.append(f"⚠ 本月({cur_ym})標記尚未執行——今日已過站位日,"
                         "池為舊池;請執行月中標記(LLM 流程)後重跑")

    feats = (panel.sort([C, "date"])
             .with_columns([
                 (pl.col("close") / pl.col("close").rolling_max(120))
                 .over(C).alias("h120"),
                 (pl.col("close") / pl.col("close").rolling_max(252))
                 .over(C).alias("h52"),
                 pl.col("trade_value").cast(pl.Float64)
                 .rolling_median(20).over(C).alias("adv20"),
                 (pl.col("close") > pl.col("close").shift(1).rolling_max(60))
                 .over(C).alias("don60"),
             ]).filter(pl.col("date") == d0))
    closes = dict(feats.select([C, "close"]).iter_rows())
    cand = (feats.filter(pl.col(C).is_in(list(pool_codes))
                         & (pl.col("h120") > float(LC["h120"]))))
    # 進場 gate(依 live config;none 直通)
    if LC["gate"] != "none" and cand.height:
        fl = (data.load_flows(con, (d0.replace(year=d0.year - 1)).isoformat(),
                              d0.isoformat())
              .filter(pl.col(C).is_in(cand[C].to_list())).sort([C, "date"]))
        inst = pl.col("foreign_diff") + pl.col("trust_diff")
        fl = (fl.with_columns([
                  (pl.col("foreign_diff").rolling_sum(5).over(C) > 0).alias("f5"),
                  (inst.rolling_sum(5).over(C) > 0).alias("inst5"),
              ]).filter(pl.col("date") == d0)
              .select([C, "f5", "inst5"]))
        cand = cand.join(fl, on=C, how="left")
        if LC["gate"] in ("f5", "inst5"):
            cand = cand.filter(pl.col(LC["gate"]).fill_null(False))
        elif LC["gate"] == "any_confirm":
            rv = (data.load_monthly_revenue(con, d0.isoformat())
                  .filter(pl.col(C).is_in(cand[C].to_list()))
                  .sort([C, "year", "month"])
                  .with_columns([
                      pl.date(pl.col("year") + pl.col("month") // 12,
                              pl.col("month") % 12 + 1, 10).alias("avail"),
                      (pl.col("monthly_revenue_yoy")
                       > pl.col("monthly_revenue_yoy").shift(1).over(C))
                      .alias("rev_accel"),
                  ]).filter(pl.col("avail") <= d0)
                  .group_by(C).agg(pl.col("rev_accel").last()))
            cand = (cand.join(rv, on=C, how="left")
                    .filter(pl.col("don60").fill_null(False)
                            | pl.col("inst5").fill_null(False)
                            | pl.col("rev_accel").fill_null(False)))
    _base = (pl.col("h52").rank() / pl.len()) * (pl.col("h120").rank() / pl.len())
    _expr = (_base if LC["score"] == "base"
             else _base * (1.0 - pl.col("adv20").rank() / pl.len()))
    cand = cand.with_columns(_expr.alias("score")).sort("score", descending=True)

    st = load_state("evergreen")
    st = update_state(st, holdings, d0, closes, {})
    h52_rank = dict(cand.select([C, "score"]).iter_rows())
    survivors: list[tuple[float, str]] = []
    for code in sorted(holdings):
        info = st.get(code, {})
        held = held_tdays(info.get("first_seen", d0.isoformat()), d0, dates_all)
        px, peak = closes.get(code), info.get("peak_close")
        if px is None:
            adv.sells.append((code, "無法取價,人工確認"))
        elif code not in pool_codes:
            adv.sells.append((code, "池籍到期/非本策略池內標的"))
        elif peak and px <= peak * (1.0 - float(LC["trail"])):
            adv.sells.append((code, f"移動停損(自峰值回落 ≥{float(LC['trail']):.0%})"))
        elif info.get("cost") and px < info["cost"] and held >= int(LC["lts"]):
            adv.sells.append((code, f"輸家時間止損(水下且持有 {held} ≥{LC['lts']})"))
        else:
            survivors.append((-(h52_rank.get(code) or 0.0), code))
    survivors.sort()  # 席位上限依 live config,排位高者優先
    for i, (negscore, code) in enumerate(survivors, 1):
        if i <= NS:
            adv.keeps.append((code, f"池內且未觸發出場(排位 {-negscore:.2f},席位 {i}/{NS})"
                                    f"{_sizing_hint(nav, W, closes.get(code), holdings[code])}"))
        else:
            adv.sells.append((code, f"超額席位(上限 {NS} 檔,排位 {-negscore:.2f})"))
    save_state("evergreen", st)

    # 同 S:買入清單 = 通往完全體的完整隊列(今日進場/⏸排隊/🕒遞補)
    n_slot = max(0, NS - len(adv.keeps))
    rank_i = 0
    ideal_buys: list[tuple[str, str]] = []
    for r in cand.head(n_slot + 3).to_dicts():
        if r[C] in holdings:
            continue
        rank_i += 1
        detail = f"{shares_for(W, nav, closes.get(r[C]))}|排位 {r['score']:.2f}"
        if rank_i <= min(MN, n_slot):
            adv.buys.append((r[C], W, f"今日進場 #{rank_i}(每日上限 {MN})|{detail}"))
            ideal_buys.append((r[C], "今日買"))
        elif rank_i <= n_slot:
            adv.buys.append((r[C], W, f"⏸ 排隊 #{rank_i}(有席位;每日上限 {MN},明日起依序進場)|{detail}"))
            ideal_buys.append((r[C], f"排隊 #{rank_i}"))
        else:
            adv.buys.append((r[C], W, f"🕒 遞補(席位已滿,等出缺才輪到)|{detail}"))
    adv.ideal_title = f"理想持倉完全體({NS} 席 × {W:.0%} 等權)"
    adv.ideal = [(c, "續抱") for c, _ in adv.keeps] + ideal_buys
    adv.notes.append(f"池 {len(pool_codes)} 檔(標記月 {active});"
                     f"濾後候選 {cand.height};決策日 {d0};"
                     f"引擎 score={LC['score']} gate={LC['gate']} "
                     f"trail{float(LC['trail']):.0%}/lts{LC['lts']}")
    return adv


# ── Serenity 顧問(live lot 六道門:讀 live ledger 的錨,與執行系統共用同一
#    規則源 exit_rules;零 LLM、零刷新、零下單)──────────────────────────
#
# 2026-07-13 重寫:舊版用「引擎回測模擬簿的成員資格」當 KEEP/SELL 判準——
# 模擬簿裡的 lot 是引擎自己更早進場的(浮盈、法人門不觸發),與帳上真實
# lot(錨=實際成交價)的判決相左。出場門是 lot 錨定的,評判必須對「你的
# lot」做,規則必須與 serenity.daily 同一份。

SER_LEDGER = "research/serenity/state/live_positions.json"
SER_OVERRIDES = "research/serenity/state/overrides.json"
SER_BRIEFS = "research/out/trading/briefs"


def serenity_advisor(con, holdings: dict[str, float], today: Date,
                     nav: float = 0.0) -> Advice:
    import json as _json

    import duckdb

    from research.serenity.daily import market_data, trading_days_between
    from research.serenity.exit_rules import evaluate_exit

    adv = Advice("Serenity(ev_v2_thesis_inst,live lot 六道門;與執行系統同一規則源)")
    led = _json.load(open(SER_LEDGER)) if os.path.exists(SER_LEDGER) else {}
    positions = led.get("positions", {})
    overrides = (_json.load(open(SER_OVERRIDES)) if os.path.exists(SER_OVERRIDES)
                 else {}).get("force_exit", {})

    # 先讀 brief 的補位代碼,一併納入查價集合(BUY 建議要能算股數)
    brief_path = f"{SER_BRIEFS}/{today.isoformat()}.md"
    brief_entries: list[str] = []
    if os.path.exists(brief_path):
        import re as _re
        for line in open(brief_path, encoding="utf-8"):
            if line.startswith("- 今日新進場"):
                brief_entries = _re.findall(r"'([^']+)'", line)
                break

    raw = duckdb.connect("research/cache.duckdb", read_only=True)
    cutoff = raw.execute("SELECT max(date) FROM daily_quote").fetchone()[0]
    codes = set(holdings) | set(positions) | {e[:4] for e in brief_entries}
    closes, yoy3, inst20, cal = market_data(raw, codes, cutoff)
    adv.notes.append(
        f"ledger as_of {led.get('as_of', '?')};評估日 {cutoff}。出場門是 lot 錨定"
        "(錨=該筆成交價/收養價),同一支股票不同錨可有不同判決;"
        "ledger 的更新由 serenity daily(執行系統)負責"
    )

    for code in sorted(holdings):
        pos = positions.get(code)
        px = closes.get(code)
        if pos is None:
            adv.keeps.append((code, "⚠ 未收養——跑 serenity daily run 讓收養協定"
                                    "接手後才受六道門管理"))
            continue
        if px is None:
            adv.sells.append((code, "無法取價(下市/停牌?)人工確認"))
            continue
        anchor = float(pos.get("anchor") or px)
        peak = max(float(pos.get("peak") or px), px)
        days_held = trading_days_between(
            cal, Date.fromisoformat(pos["entry_date"]), cutoff)
        if code in overrides:
            reason = "override:" + overrides[code].get("reason", "?")
        else:
            reason = evaluate_exit(px=px, anchor=anchor, peak=peak,
                                   days_held=days_held,
                                   inst20=inst20.get(code), yoy3=yoy3.get(code))
        detail = f"錨 {anchor:g} 現價 {px:g} 峰 {peak:g} 持有 {days_held} 日"
        if reason:
            adv.sells.append((code, f"{reason}|{detail}"))
        else:
            adv.keeps.append((code, f"六道門全綠|{detail}"))
    for code in sorted(set(positions) - set(holdings)):
        adv.notes.append(f"⚠ ledger 有 {code} 但帳上無——跑 serenity daily 對帳")

    # 進場席位由執行系統的引擎計分決定;忠實轉列今日 brief 的補位建議為 BUY
    # (等權席位 = NAV/10;⚠ 標註原樣保留——戰役十四:半觸發名字回測支持進場)
    if brief_entries:
        for item in brief_entries:
            code = item[:4]
            adv.buys.append((code, 0.10,
                             f"引擎席位補進|{item[4:] or '無警示'}|"
                             f"{shares_for(0.10, nav, closes.get(code))}"))
    elif not os.path.exists(brief_path):
        adv.notes.append("進場建議:今日 brief 未生成——要席位補進判斷請跑 "
                         "serenity daily(執行系統)")
    # Serenity 的理想形態:上限 10 席等權(MAX_POSITIONS=10,每席 ≈ NAV/10)。
    # 續抱多是常態——事件引擎只在六道門觸發才動,沒有月頻輪動。
    n_final = len(adv.keeps) + len(adv.buys)
    adv.ideal_title = f"理想持倉完全體(上限 10 席等權;本日收斂後 {n_final}/10 席)"
    adv.ideal = ([(c, "續抱") for c, _ in adv.keeps]
                 + [(c, "補位買進") for c, _, _ in adv.buys])
    return adv
