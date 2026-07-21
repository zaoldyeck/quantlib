"""三策略每日顧問核心:S / Evergreen / Serenity 的 live 評判(永不下單)。

每個 advisor 把整個帳戶視為自己的:對每檔現有持股給 KEEP / SELL(附機械
原因),並給出今日買入清單。出場評判一律 **lot 錨定、對使用者的真實持倉**
——絕不用任何回測模擬簿的成員資格當判準(2026-07-13 教訓)。

規格來源:S = research/apex/STRATEGY.md(逐條移植;per-position 狀態存
research/tri/state/,cost=收養日收盤);Evergreen v3.3 = LEDGER EV30-33
(參數外部化:`research/evergreen/data/live_config.json`,EV43 年度 refit 產物);
Serenity = live ledger 錨 × `research/serenity/exit_rules.py`(與執行系統
同一份六道門規則源)。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date as Date
from datetime import timedelta

import polars as pl

from research.trading.cost_basis import cost_of, levels_line

C = "company_code"
STATE_DIR = "research/tri/state"
#: state 內記錄「S 自己買進的名單」的 meta key(前綴 _ 者由 update_state 保留)
_S_BUYS = "_s_buys"
#: 自買紀錄保留天數(成交 T+1;留緩衝給未成交/部分成交)
_S_BUYS_TTL = 10
#: live 計劃檔目錄(premarket 每日落盤);_s_buys 的可重建來源
_PLANS_DIR = "research/trading/live/state/plans"


def _s_buys_from_plans(days: int = _S_BUYS_TTL) -> dict[str, str]:
    """從近日 live 計劃檔重建「S 自己買過哪些股票」→ {code: 決策日}。

    **state 是可重建的衍生物,不是珍貴資料**:premarket 每日把決策落盤成
    `plans/YYYY-MM-DD.json`,那才是 S 買進決策的權威紀錄。`_s_buys` 若因 VM 重建、
    狀態清理或(本機制上線前的)歷史空窗而缺漏,一律據此自癒——不必人工編輯 state。
    """
    out: dict[str, str] = {}
    try:
        names = sorted(n for n in os.listdir(_PLANS_DIR) if n.endswith(".json"))[-days:]
    except OSError:
        return out
    for name in names:
        try:
            with open(f"{_PLANS_DIR}/{name}", encoding="utf-8") as fh:
                d = json.load(fh)
        except (OSError, ValueError):
            continue                      # 壞檔跳過:自癒不得因單一壞檔而失效
        for code in (d.get("buys") or []):
            out[str(code)] = str(d.get("date") or name[:-5])
    return out


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
    """收養新持股、回填成本、移除已出清者(峰值不存 state,由 exit_replay 逐日重算)。

    cost = 收養日收盤(輸家時間止損的水線;2026-07-13 修正:先前從未寫入,
    導致 S/Evergreen 的輸家止損是永不觸發的死代碼)。舊 state 缺 cost 者
    以當日收盤回填——語義同「今日才收養」,時鐘以 first_seen 為準不重啟。
    2026-07-21:移除死狀態 peak_close——三 advisor 出場峰值一律由價格路徑重算
    (exit_replay,cum_max),此增量值從未被讀,且正是 exit_replay 警告的
    『漏跑幾天漏掉期間高點 → 止損線偏低』陷阱殘留,刪除以防被重新接回。
    2026-07-22:保留 `_` 開頭的 meta key(如 `_s_buys` 自買紀錄),否則跨日即遺失。"""
    out = {k: v for k, v in st.items() if k.startswith("_")}
    for code in holdings:
        prev = st.get(code)
        px = closes.get(code)
        if prev is None:
            out[code] = {"first_seen": today.isoformat(), "cost": px}
        else:
            entry = {**prev}
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
    # 逐檔結構化明細(報告的深度段落用;key=code)
    detail: dict = field(default_factory=dict)


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


def _s_role_action(in_pool: bool, vetted: bool, adoption_day: bool) -> tuple[str, bool]:
    """S 角色純度純函式(倖存持股未觸發出場後):→ (action, 新 vetted)。
    action ∈ {keep_pool, keep_vetted, sell_role}。

    決定性關鍵(修 2026-07-21 狀態污染):vetted(合法部位認證)**只在收養當天**
    (adoption_day)依當天池籍鎖定一次,之後恆讀不覆寫。舊碼每次「在池」都覆寫
    vetted=True → 用不同交易日重跑會累積出不同認證集(哪幾天跑過就留哪些),
    使用者實測抓到。改為收養日一次性鎖定後,同一部位重跑結果恆定。
    """
    if in_pool:
        return "keep_pool", (vetted or adoption_day)
    if vetted:
        return "keep_vetted", vetted
    return "sell_role", vetted


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

    # ── S 自買部位認證(2026-07-22 修真實事故)────────────────────────────
    # 事故:S 於 T 日決策買進(當時在新鮮池內)、T+1 成交、T+2 首次入庫存;此時
    # 新鮮度已過 7 天掉出池子 → 舊邏輯把「自己剛買的股票」判為「非本策略標的」
    # 隔天就砍(回測平均抱 17 天,live 抱 1 天)。**部位的合法性來自買進決策本身,
    # 不是事後再檢查它還在不在池內**——故以 `_s_buys`(前次執行記下的今日進場名單)
    # 認證新入庫存者,與池籍脫鉤。
    # 來源合併:計劃檔(可重建的權威紀錄)為底、state 紀錄覆蓋 → 缺漏自癒。
    _bought = {**_s_buys_from_plans(), **dict(st.get(_S_BUYS, {}))}
    for code in holdings:
        rec = st.get(code)
        if rec is not None and code in _bought and not rec.get("vetted_pool"):
            rec["vetted_pool"] = True
            rec["vetted_src"] = f"S 自買 {_bought[code]}"   # 稽核用:認證來源可回溯

    # 逐檔:先過硬出場規則,倖存者過「池檢」——S 只持有它自己會買的名字。
    # 池檢語義(2026-07-13 二修,使用者:「不是 S 最推薦的就該賣」):
    # (a) 在今日進場池內(新鮮+資格+現金流濾網全過)→ 合法部位,記
    #     vetted_pool 於 state,之後即使離開新鮮池也續抱到出場規則(回測的
    #     hold-until-exit 語義:S 不因出現更高排名者而中途換股);
    # (b) 不在池內且從未通過池檢 → 賣出(S 不會買的名字沒有理由持有)。
    pool_rank = {r[C]: i + 1 for i, r in enumerate(pool.to_dicts())}
    # 出場一律**逐日重放**(非今日快照):你沒跑報告那幾天觸發的規則也算數
    from research.trading.exit_replay import load_paths, replay, s_rule
    entries = {c: Date.fromisoformat(st.get(c, {}).get("first_seen", d0.isoformat()))
               for c in holdings}
    paths = load_paths(sorted(entries), min(entries.values()) if entries else d0, d0)
    fires: dict[str, object] = {}
    survivors: list[tuple[float, str, str]] = []  # (排序鍵, code, keep理由)
    for code in sorted(holdings):
        info = st.get(code, {})
        px = closes.get(code)
        fresh = fresh_all.get(code)
        if px is None or code not in paths:
            adv.sells.append((code, "無法取價(下市/停牌?)人工確認"))
            continue
        cost0, _b = cost_of(code, fallback=info.get("cost"))
        fire, now = replay(paths[code], entries[code], s_rule(cost0), peak_floor=cost0)
        fires[code] = fire
        held = now.days_held if now else 0
        if fire:
            od = (f"🔴 **逾期未出場**:{fire.day} 觸發(當時 {fire.price:g}),今日 {px:g}"
                  f"({(px / fire.price - 1) * 100:+.1f}%)|" if fire.is_overdue(d0) else "")
            adv.sells.append((code, f"{od}{fire.reason}"))
            continue
        action, new_vetted = _s_role_action(
            in_pool=code in pool_rank, vetted=bool(info.get("vetted_pool")),
            adoption_day=info.get("first_seen") == d0.isoformat())
        if new_vetted:
            st[code]["vetted_pool"] = True
        if action == "keep_pool":
            rk = pool_rank[code]
            survivors.append((rk, code, f"今日進場池 geo 排名 #{rk}(fresh={int(fresh)})"))
        elif action == "keep_vetted":
            survivors.append((5_000 + int(fresh), code,
                              f"既有合法部位(收養日通過池檢,fresh={int(fresh)} <26 續抱至出場規則)"))
        else:  # sell_role
            adv.sells.append((code, "非本策略標的(不在進場池且收養日未通過池檢——"
                                    "S 不會買的名字沒有理由持有)"))
    survivors.sort()
    for i, (key, code, why) in enumerate(survivors, 1):
        info = st.get(code, {})
        px = closes.get(code)
        cost, basis = cost_of(code, fallback=info.get("cost"))
        _f, now = replay(paths[code], entries[code], s_rule(cost), peak_floor=cost)
        peak = now.peak if now else None  # 峰值由價格路徑重算,不靠增量 state
        # S 的價位門只有 trail 35%;訊號過期 26 日 / 時間止損 30 日 / 輸家止損
        # (水下且 ≥15 日)都是時間門,沒有固定止盈。
        stop = peak * 0.65 if peak else None
        lv = levels_line(cost, basis, px, stop, None,
                         stop_note=f"(trail 35%,峰 {peak:g})" if peak else "",
                         take_note="(無固定止盈:靠 trail/時間出場)")
        prow = next((r for r in pool.to_dicts() if r[C] == code), None)
        adv.detail[code] = {
            "strategy": "S", "cost": cost, "basis": basis, "px": px,
            "stop": stop, "stop_note": "trail 35%", "take": None,
            "take_note": "無固定止盈(trail/時間/訊號過期)",
            "entry_date": entries[code].isoformat(),
            "days_held": now.days_held if now else 0, "peak": peak,
            "gates": [("移動停損 −35%", f"{stop:g}(峰 {peak:g})" if stop else "—"),
                      ("訊號過期 26 日", f"距最近月營收 {fresh_all.get(code)} 日"),
                      ("時間止損 30 日", f"持有 {now.days_held if now else 0} 交易日"),
                      ("輸家止損 15 日", f"{'水下' if cost and px < cost else '水上'}")],
            # S 是純量化,沒有 LLM 理由——「為什麼買」就是這六個因子的值與排名
            "factors": ({k: round(float(prow[k]), 3) for k in list(S_WTS) if prow.get(k) is not None}
                        if prow else {}),
            "geo": round(float(prow["geo"]), 3) if prow and prow.get("geo") is not None else None,
            "pool_rank": pool_rank.get(code),
            "fire_day": None, "fire_price": None, "fire_reason": None, "overdue": False,
        }
        if i <= 5:
            adv.keeps.append((code, f"{why}|席位 {i}/5{lv}"
                                    f"{_sizing_hint(nav, 0.20, closes.get(code), holdings[code])}"))
        else:
            adv.sells.append((code, f"超額席位(S 上限 5 檔;{why}){lv}"))
    # 註:save_state 移到買入清單產生之後——需先記下今日進場名單(_s_buys),
    # 明日該股入庫存時才認得出「這是 S 自己買的」(見上方自買認證段)。

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
        # 買入候選的「為什麼買」= 六因子的值與幾何平均(S 是純量化,沒有敘事)
        adv.detail.setdefault(r[C], {
            "strategy": "S", "px": closes.get(r[C]), "cost": None, "basis": "",
            "stop": None, "take": None, "gates": [],
            "factors": {k: round(float(r[k]), 3) for k in list(S_WTS) if r.get(k) is not None},
            "geo": round(float(r["geo"]), 3) if r.get("geo") is not None else None,
            "pool_rank": rank_i, "fire_day": None, "overdue": False,
        })
        if rank_i <= min(2, n_slot):
            adv.buys.append((r[C], 0.20, f"今日進場 #{rank_i}(每日上限 2)|{detail}"))
            ideal_buys.append((r[C], "今日買"))
        elif rank_i <= n_slot:
            adv.buys.append((r[C], 0.20, f"⏸ 排隊 #{rank_i}(有席位;每日上限 2,明日起依序進場)|{detail}"))
            ideal_buys.append((r[C], f"排隊 #{rank_i}"))
        else:
            adv.buys.append((r[C], 0.20, f"🕒 遞補(席位已滿,等出缺才輪到)|{detail}"))
    # 記下今日進場名單供「明日入庫存時認證自買部位」(見上方自買認證段)。
    # 保留期 _S_BUYS_TTL 天:成交需 T+1,偶有未成交/部分成交,給足緩衝;
    # 已入庫存並認證者即可移除,避免無限累積。
    _keep_from = (d0 - timedelta(days=_S_BUYS_TTL)).isoformat()
    new_buys = {c: dt for c, dt in st.get(_S_BUYS, {}).items()
                if dt >= _keep_from and not st.get(c, {}).get("vetted_pool")}
    for code, _w, reason in adv.buys:
        if reason.startswith("今日進場"):
            new_buys[code] = d0.isoformat()
    st[_S_BUYS] = new_buys
    save_state("s", st)

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
    # 池籍改用官方 canonical midmonth_membership(唯一真源;2026-07-20 修:advisor
    # 舊自寫「最近 N 月」比驗證引擎少疊一個月〔midmonth 有效疊加 2N-1 月〕、且無最後
    # 一天修正)。reg 先裁到 advisor 資料窗涵蓋得到的月份(池只需最近數月即可)。
    from research.evergreen.ev30_baseline import midmonth_membership
    reg_win = reg.filter(
        pl.col("month") >= f"{dates_all[0].year}-{dates_all[0].month:02d}")
    _memb = midmonth_membership(reg_win, dates_all, int(LC["pool_months"]))
    pool_codes = set(_memb.filter(pl.col("date") == d0)["company_code"].to_list())
    active = [ym for ym in yms if ym in stance and stance[ym] <= d0][
        -(2 * int(LC["pool_months"]) - 1):]

    adv = Advice(f"Evergreen({_tag})")
    cur_ym = f"{d0.year}-{d0.month:02d}"
    this_stance = min((d for d in dates_all
                       if d.year == d0.year and d.month == d0.month and d.day > 10),
                      default=None)
    if this_stance and d0 >= this_stance and cur_ym not in yms:
        adv.notes.append(f"⚠ 本月({cur_ym})標記尚未執行——今日已過站位日,"
                         "池為舊池;請執行月中標記(LLM 流程)後重跑")

    # 特徵/計分改用引擎唯一真源(2026-07-20 消重複;禁止 advisor 自寫定義)
    from research.evergreen.engine import feat_cols, score_expr
    feats = (panel.sort([C, "date"])
             .with_columns([feat_cols()[k].alias(k)
                            for k in ("h120", "h52", "adv20", "don60")])
             .filter(pl.col("date") == d0))
    closes = dict(feats.select([C, "close"]).iter_rows())
    cand = (feats.filter(pl.col(C).is_in(list(pool_codes))
                         & (pl.col("h120") > float(LC["h120"]))))
    # 進場 gate(依 live config;none 直通)
    if LC["gate"] != "none" and cand.height:
        fl = (data.load_flows(con, (d0.replace(year=d0.year - 1)).isoformat(),
                              d0.isoformat())
              .filter(pl.col(C).is_in(cand[C].to_list())).sort([C, "date"]))
        inst = pl.col("foreign_diff") + pl.col("trust_diff")
        # asof 語義:取「≤ 決策日的最新一筆」,與回測引擎的 row_latest_before 一致。
        # 曾用 `date == d0` 嚴格對齊(2026-07-15 修正):法人表(16:00 才發布)只要
        # 比報價表落後一天,join 就全 null → fill_null(False) → **閘門把所有候選
        # 靜靜砍光**,live 比回測嚴格卻無人察覺。fail-closed 用錯地方就是無聲失效。
        fl = (fl.with_columns([
                  (pl.col("foreign_diff").rolling_sum(5).over(C) > 0).alias("f5"),
                  (inst.rolling_sum(5).over(C) > 0).alias("inst5"),
              ]).filter(pl.col("date") <= d0).sort([C, "date"])
              .group_by(C).agg([pl.col("date").last().alias("flow_date"),
                                pl.col("f5").last(), pl.col("inst5").last()]))
        lag_src = fl["flow_date"].max() if fl.height else None
        if lag_src is not None and lag_src < d0:
            adv.notes.append(f"ℹ 法人資料止於 {lag_src}(決策日 {d0};T86 16:00 才發布)"
                             "——閘門以最新可得日判定")
        fl = fl.drop("flow_date")
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
    cand = cand.with_columns(score_expr(LC).alias("score")).sort("score", descending=True)

    st = load_state("evergreen")
    st = update_state(st, holdings, d0, closes, {})
    h52_rank = dict(cand.select([C, "score"]).iter_rows())
    survivors: list[tuple[float, str]] = []
    from research.trading.exit_replay import evergreen_rule, load_paths, replay
    eg_entries = {c: Date.fromisoformat(st.get(c, {}).get("first_seen", d0.isoformat()))
                  for c in holdings}
    eg_paths = load_paths(sorted(eg_entries), min(eg_entries.values()) if eg_entries else d0, d0)
    for code in sorted(holdings):
        info = st.get(code, {})
        px = closes.get(code)
        if px is None or code not in eg_paths:
            adv.sells.append((code, "無法取價,人工確認"))
            continue
        if code not in pool_codes:
            adv.sells.append((code, "池籍到期/非本策略池內標的"))
            continue
        cost0, _b = cost_of(code, fallback=info.get("cost"))
        # 逐日重放(非快照):trail 與輸家止損都是路徑相依的
        fire, _now = replay(eg_paths[code], eg_entries[code],
                            evergreen_rule(cost0, float(LC["trail"]), int(LC["lts"])),
                            peak_floor=cost0)
        if fire:
            od = (f"🔴 **逾期未出場**:{fire.day} 觸發(當時 {fire.price:g}),今日 {px:g}"
                  f"({(px / fire.price - 1) * 100:+.1f}%)|" if fire.is_overdue(d0) else "")
            adv.sells.append((code, f"{od}{fire.reason}"))
        else:
            survivors.append((-(h52_rank.get(code) or 0.0), code))

    survivors.sort()  # 席位上限 NS,排位高者優先
    for i, (negscore, code) in enumerate(survivors, 1):
        info = st.get(code, {})
        px = closes.get(code)
        cost, basis = cost_of(code, fallback=info.get("cost"))
        _f, now = replay(eg_paths[code], eg_entries[code],
                         evergreen_rule(cost, float(LC["trail"]), int(LC["lts"])),
                         peak_floor=cost)
        peak = now.peak if now else None  # 峰值由價格路徑重算,不靠增量 state
        stop = peak * (1 - float(LC["trail"])) if peak else None
        lv = levels_line(cost, basis, px, stop, None,
                         stop_note=f"(trail {float(LC['trail']):.0%},峰 {peak:g})" if peak else "",
                         take_note=f"(無固定止盈:靠 trail/池籍 {LC['pool_months']} 月出場)")
        adv.detail[code] = {
            "strategy": "Evergreen", "cost": cost, "basis": basis, "px": px,
            "stop": stop, "stop_note": f"trail {float(LC['trail']):.0%}", "take": None,
            "take_note": f"無固定止盈(trail/池籍 {LC['pool_months']} 月)",
            "entry_date": eg_entries[code].isoformat(),
            "days_held": now.days_held if now else 0, "peak": peak,
            "gates": [(f"移動停損 −{float(LC['trail']):.0%}", f"{stop:g}(峰 {peak:g})" if stop else "—"),
                      (f"輸家止損 {LC['lts']} 日", f"持有 {now.days_held if now else 0} 日、"
                                                 f"{'水下' if cost and px < cost else '水上'}"),
                      (f"池籍 {LC['pool_months']} 月", "在池內")],
            "fire_day": None, "fire_price": None, "fire_reason": None, "overdue": False,
        }
        if i <= NS:
            adv.keeps.append((code, f"池內未觸發出場(排位 {-negscore:.2f},席位 {i}/{NS}){lv}"
                                    f"{_sizing_hint(nav, W, closes.get(code), holdings[code])}"))
        else:
            adv.sells.append((code, f"超額席位(上限 {NS} 檔,排位 {-negscore:.2f}){lv}"))
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
    from research.serenity.exit_rules import (ABS_STOP, TAKE_PROFIT, TIME_DAYS,
                                              TRAIL, evaluate_exit)

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

    from research.trading.exit_replay import load_paths, replay, serenity_rule

    entries = {c: Date.fromisoformat(positions[c]["entry_date"])
               for c in holdings if c in positions}
    paths = load_paths(sorted(entries), min(entries.values()) if entries else cutoff, cutoff)
    for code in sorted(holdings):
        pos = positions.get(code)
        px = closes.get(code)
        if pos is None:
            adv.keeps.append((code, "⚠ 未收養——跑 serenity daily run 讓收養協定"
                                    "接手後才受六道門管理"))
            continue
        if px is None or code not in paths:
            adv.sells.append((code, "無法取價(下市/停牌?)人工確認"))
            continue
        anchor = float(pos.get("anchor") or px)
        entry = entries[code]
        # **逐日重放**(非今日快照):規則在你沒跑報告的那幾天觸發也算數
        fire, now = replay(paths[code], entry, serenity_rule(anchor), peak_floor=anchor)
        if now is None:
            adv.sells.append((code, "無價格路徑,人工確認"))
            continue
        peak, days_held = now.peak, now.days_held
        reason = None
        if code in overrides:
            reason = "override:" + overrides[code].get("reason", "?")
        elif fire:
            reason = fire.reason
        stop = max(anchor * (1 - ABS_STOP), peak * (1 - TRAIL))
        stop_note = "成本 −15%" if anchor * (1 - ABS_STOP) >= peak * (1 - TRAIL) else f"高點 −20%,持有期最高 {peak:,.6g}"
        take = anchor * (1 + TAKE_PROFIT)
        cost, basis = cost_of(code, fallback=anchor, fallback_basis="adopted_close")
        # 給人看的說法:每一道門講「離觸發還有多遠」與「現在是安全還是危險」
        def _dist(level: float) -> str:
            d = (level / px - 1) * 100
            return f"{'跌' if d < 0 else '漲'} {abs(d):.1f}% 到 {level:,.6g}"

        inst_ok = (now.inst20 or 0) >= 0 or px >= anchor
        rev_ok = now.yoy3 is None or now.yoy3 >= 0
        gates = [
            ("絕對停損(成本 −15%)", f"{_dist(anchor * (1 - ABS_STOP))} 觸發"),
            ("移動停損(高點 −20%)", f"{_dist(peak * (1 - TRAIL))} 觸發(持有期最高 {peak:,.6g})"),
            ("止盈(成本 +60%)", f"{_dist(take)} 觸發"),
            ("時間門(50 個交易日)", f"已持有 {days_held} 日,還有 {max(TIME_DAYS - days_held, 0)} 日"),
            ("法人動向", f"法人近 20 日{'買超' if (now.inst20 or 0) >= 0 else '賣超'} "
                        f"{abs(now.inst20 or 0) / 1000:,.0f} 張"
                        f"{' ✓ 安全(要法人賣超又虧損才觸發)' if inst_ok else ' ⚠️ 已成立(法人賣超且虧損)'}"),
            ("營收動向", (f"近 3 個月營收年增 {now.yoy3:+.0f}%"
                         f"{' ✓ 安全(轉負才觸發)' if rev_ok else ' ⚠️ 已轉負'}")
                        if now.yoy3 is not None else "無營收資料"),
        ]
        adv.detail[code] = {
            "strategy": "Serenity", "cost": cost, "basis": basis, "px": px,
            "stop": stop, "stop_note": stop_note, "take": take, "take_note": "",
            "entry_date": entry.isoformat(), "days_held": days_held, "peak": peak,
            "gates": gates,
            "fire_day": fire.day.isoformat() if fire else None,
            "fire_price": fire.price if fire else None,
            "fire_reason": fire.reason if fire else None,
            "overdue": bool(fire and fire.is_overdue(cutoff)),
        }
        detail = levels_line(cost, basis, px, stop, take, stop_note=stop_note)
        detail += f"|持有 {days_held}/{TIME_DAYS} 日(時間門)"
        if reason:
            od = (f"🔴 **逾期未出場**:規則已於 {fire.day} 觸發(當時 {fire.price:g}),"
                  f"今日 {px:g}({(px / fire.price - 1) * 100:+.1f}%)|" if fire and fire.is_overdue(cutoff) else "")
            adv.sells.append((code, f"{od}{reason}{detail}"))
        else:
            adv.keeps.append((code, f"六道門全綠{detail}"))
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
