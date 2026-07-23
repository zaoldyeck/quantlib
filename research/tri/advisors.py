"""三策略每日顧問核心:S / Evergreen / Serenity 的 live 評判(永不下單)。

每個 advisor 把整個帳戶視為自己的:對每檔現有持股給 KEEP / SELL(附機械
原因),並給出今日買入清單。出場評判一律 **lot 錨定、對使用者的真實持倉**
——絕不用任何回測模擬簿的成員資格當判準(2026-07-13 教訓)。

規格來源:S = research/apex/STRATEGY.md(逐條移植;per-position 狀態存
var/state/positions/,cost=收養日收盤);Evergreen v3.3 = LEDGER EV30-33
(參數外部化:`research/evergreen/data/live_config.json`,EV43 年度 refit 產物);
Serenity = live ledger 錨 × `research/serenity/exit_rules.py`(與執行系統
同一份六道門規則源)。
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from dataclasses import dataclass, field
from datetime import date as Date

import polars as pl

from research.trading.cost_basis import cost_of, levels_line
from research import paths

C = "company_code"
#: 路徑一律以 **repo 根**為錨,不依賴 cwd(同 exit_replay 慣例)。
_REPO = Path(__file__).resolve().parents[2]
STATE_DIR = str(paths.STATE_POSITIONS)

# ── per-position 帳戶事實(首見日 + 成本)──────────────────────────
# **這裡只存帳戶的事實,不存任何判斷。**「這是不是 S 的部位」「該不該出場」
# 一律由市場資料每次現算(見 s_advisor 的進場錨),故 state 清掉只影響成本顯示。


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
    2026-07-22:保留 `_` 開頭的 meta key(向後相容)。**state 只存帳戶事實**
    (何時首次持有、成本);「這是不是 S 的部位」「該不該出場」全部改由市場資料
    每次現算(見 `entry_anchors`),故清空 state 只影響成本顯示,不影響交易判決。"""
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

#: 策略正名(2026-07-22 定名,唯一真源:對人顯示一律用它)。
#: 代號 `apex_revcycle_S` 是研發期的戰役編號(apex 戰役 / revenue-cycle / 變體 S),
#: 不是給人看的名字;引擎檔名、血統審計、回測產物維持代號不動,只換對外稱呼。
#: 名字要說出策略的本質:alpha 源自台股獨有的**每月營收強制揭露**,動作是在揭露後
#: 7 日內買進營收正在加速的名字,抱到訊號過期。
S_NAME = "月報動能"
S_FULL = "台股月營收揭露動能策略"
S_CODE = "apex_revcycle_S"

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


def pool_history(feat: pl.DataFrame, elig: pl.DataFrame,
                 tax: pl.DataFrame) -> pl.DataFrame:
    """S 進場池的**逐日**成員 + geo 分數 → (date, company_code, geo, rev_fresh_days)。

    今日的買進候選與歷史部位的「進場錨」共用**這一份**池定義(引擎唯一真源):
    池的定義只有一個,問「今天誰能買」與問「這檔當初哪天被選中」是同一支函式的
    不同切片。逐條與 STRATEGY.md 一致——營收新鮮 ≤7 日、通過流動性/上市資格、
    六因子齊全、當日 cfo/ni 中位數閘(覆蓋率 ≥30% 才啟用),再算幾何排名分數。
    """
    day = feat.sort("date").join_asof(
        tax.sort("effective_date"), left_on="date", right_on="effective_date",
        by=C, strategy="backward")
    # accel_rel = 營收加速度減去**同日同產業**中位數(產業未知者無此欄 → 稍後被剔除)
    ind_med = (day.filter(pl.col("industry").is_not_null())
               .group_by(["date", "industry"])
               .agg(pl.col("rev_yoy_accel").median().alias("_im")))
    cand = (day.join(ind_med, on=["date", "industry"], how="left")
            .with_columns((pl.col("rev_yoy_accel") - pl.col("_im")).alias("accel_rel"))
            .filter(pl.col("rev_fresh_days") <= 7)
            .join(elig.filter(pl.col("eligible")).select(["date", C]),
                  on=["date", C], how="semi")
            .drop_nulls(subset=list(S_WTS))
            # defense-in-depth(2026-07-23 稽核 D-apex-s-live):drop_nulls 不剔除 inf/NaN,
            # rank 會把它排到頂端污染選股;六因子一律要求有限值(rev_seq 護欄已治本)。
            .filter(pl.all_horizontal([pl.col(c).is_finite() for c in S_WTS])))
    # 現金流品質閘:當日池內中位數以上;覆蓋率不足 30% 的日子不啟用(避免用少數樣本殺全池)
    cand = cand.with_columns([
        pl.col("cfo_ni_ratio_ttm").median().over("date").alias("_med"),
        pl.col("cfo_ni_ratio_ttm").is_not_null().sum().over("date").alias("_ncov"),
        pl.len().over("date").alias("_h"),
    ])
    gate = (pl.col("_ncov") >= 0.3 * pl.col("_h")) & pl.col("_med").is_not_null()
    cand = cand.filter(~gate | (pl.col("cfo_ni_ratio_ttm") >= pl.col("_med")))
    expr = None
    for cname, wt in S_WTS.items():
        term = (pl.col(cname).rank().over("date") / pl.len().over("date")) ** wt
        expr = term if expr is None else expr * term
    return cand.with_columns(expr.alias("geo"))


def entry_anchors(ph: pl.DataFrame, acquired: dict[str, Date]) -> dict[str, Date]:
    """每檔持股的**策略進場日** = 它進到帳戶之前(含當天)最後一次入池的那一天。

    使用者定調(2026-07-22):「一旦標的入池,就應該以當時的出場條件制定計劃——
    什麼時間點、什麼價格被選中,就從那時建立的出場條件決定現在該不該出場」。
    S 叫你買、你買了,那就是 S 的部位;它的出場鐘從**被選中那天**開始走。

    **為什麼用池籍推、不用成交紀錄檔**:成交 jsonl 在誰下單就留在誰身上(本機一份、
    VM 一份、互不同步),拿它當身分依據會讓兩台機器對同一部位給出相反判決——
    2026-07-22 實測:本機判 2466「續抱」、VM 判「賣出」。池籍是市場資料的函數,
    任何機器、任何時間重算都得到同一個答案(stateless、可重現)。

    回傳 {code: 進場日};**不在其中者 = 這兩年從未入池 → 不是 S 的標的**。
    """
    if not acquired:
        return {}
    ref = pl.DataFrame(
        {C: list(acquired), "_by": [acquired[c] for c in acquired]},
        schema={C: pl.Utf8, "_by": pl.Date})
    hit = (ph.select([C, "date"]).join(ref, on=C, how="inner")
           .filter(pl.col("date") <= pl.col("_by"))
           .group_by(C).agg(pl.col("date").max().alias("entry")))
    return {r[C]: r["entry"] for r in hit.to_dicts()}


def s_advisor(con, holdings: dict[str, float], today: Date,
              nav: float = 0.0) -> Advice:
    import os

    from research.apex import data
    from research.apex.assemble import apply_avail_override, build_features

    # 月營收事件驅動生效:資料庫首見即納入(不等法定 10 日)。
    # 歷史回測維持保守 10 日語義;此 override 僅 live 決策使用。
    fs_path = f"{paths.RECORDS}/revenue_first_seen.parquet"
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
               # 分母護欄(2026-07-23 稽核 D-apex-s-live):前三月營收合計為 0 時 +inf/NaN
               # 會被 rank 排到頂端污染選股(與 strategy_s.py 同治;未定義成長率該 null)。
               pl.when(pl.col("monthly_revenue").rolling_sum(3).shift(3) > 0)
               .then(pl.col("monthly_revenue").rolling_sum(3)
                     / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
               .otherwise(None)
               .over(C).alias("rev_seq"),
           ]))
    rev = (apply_avail_override(rev, ov)
           .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance="70d").sort([C, "date"]))
    import duckdb
    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    tax = raw.sql("SELECT company_code, effective_date, industry FROM "
                  "industry_taxonomy_pit WHERE industry IS NOT NULL "
                  "ORDER BY effective_date").pl()
    # 逐日池籍(唯一真源):今日的買進候選 = 今日切片;歷史部位的進場錨 = 過去切片。
    ph = pool_history(feat, elig, tax)
    pool = ph.filter(pl.col("date") == d0).sort("geo", descending=True)

    adv = Advice(S_NAME)
    closes = dict(panel.filter(pl.col("date") == d0)
                  .select([C, "close"]).iter_rows())
    st = load_state("s")
    st = update_state(st, holdings, d0, closes, {})
    fresh_all = dict(feat.filter(pl.col("date") == d0)
                     .select([C, "rev_fresh_days"]).iter_rows())

    # ── 進場錨:每檔持股「被 S 選中」的那一天 ────────────────────────────
    # 一旦入池,那天就是 S 的進場點;出場鐘與價位門全部從那天算起(見 entry_anchors)。
    # 從未入池者才是外人。**身分與出場全由市場資料決定,零 state、跨機一致。**
    acquired = {c: Date.fromisoformat(st.get(c, {}).get("first_seen", d0.isoformat()))
                for c in holdings}
    anchors = entry_anchors(ph, acquired)
    _want = pl.DataFrame({C: list(anchors), "date": list(anchors.values())},
                         schema={C: pl.Utf8, "date": pl.Date}) if anchors else None
    entry_px = ({r[C]: r["close"] for r in
                 panel.join(_want, on=[C, "date"], how="inner")
                 .select([C, "close"]).to_dicts()} if _want is not None else {})

    pool_rank = {r[C]: i + 1 for i, r in enumerate(pool.to_dicts())}
    # 出場一律**逐日重放**(非今日快照):你沒跑報告那幾天觸發的規則也算數。
    # 進場價用**錨日收盤**而非你的成交價——出場規則是策略的屬性(回測的
    # `peak_close = entry_close`),成本是帳戶的屬性,兩者不得互相污染。
    from research.trading.exit_replay import load_paths, replay, s_rule
    entries = {c: anchors.get(c, acquired[c]) for c in holdings}
    local_paths = load_paths(sorted(entries), min(entries.values()) if entries else d0, d0)
    survivors: list[tuple[tuple, str, str]] = []  # (席位排序鍵, code, keep理由)
    for code in sorted(holdings):
        px = closes.get(code)
        fresh = fresh_all.get(code)
        # 無營收資料者 fresh=None;下方文案用 int(fresh) 會拋 TypeError →
        # 當日整份計劃產不出來(交易 > 文案),故先正規化為可顯示值。
        fresh_i = int(fresh) if fresh is not None else -1
        if px is None or code not in local_paths:
            adv.sells.append((code, "無法取價(下市/停牌?)人工確認"))
            continue
        if code not in anchors:
            adv.sells.append((code, "非本策略標的(S 有紀錄以來從未選中它——它不是"
                                    "S 買的,也不會是;卡在哪一關可跑 "
                                    "`python -m research.tri.pool_trace " + code + "`)"))
            continue
        entry, epx = entries[code], entry_px.get(code)
        if epx is None:
            # 錨日取不到收盤 → 止損/輸家門檻無從建立。**寧可交人工,不可靜默地
            # 用一組較弱的規則跑下去**(那等於偷偷放寬風控)。
            adv.sells.append((code, f"進場錨 {entry} 取不到收盤價,出場門檻無法建立"
                                    "——人工確認"))
            continue
        fire, now = replay(local_paths[code], entry, s_rule(epx), peak_floor=epx)
        if fire:
            od = (f"🔴 **逾期未出場**:{fire.day} 觸發(當時 {fire.price:g}),今日 {px:g}"
                  f"({(px / fire.price - 1) * 100:+.1f}%)|" if fire.is_overdue(d0) else "")
            adv.sells.append((code, f"{od}{fire.reason}(S 進場錨 {entry}"
                                    f"{f' @ {epx:g}' if epx else ''})"))
            continue
        rk = pool_rank.get(code)
        why = (f"S 標的(進場錨 {entry}{f' @ {epx:g}' if epx else ''};出場規則未觸發)"
               + (f"|今日仍在進場池 #{rk}" if rk else f"|fresh={fresh_i}"))
        # 席位先到先得(與回測一致:部位佔著席位直到出場規則觸發),同日再比今日排名
        survivors.append(((entry.toordinal(), rk or 9_999), code, why))
    survivors.sort()
    for i, (key, code, why) in enumerate(survivors, 1):
        info = st.get(code, {})
        px = closes.get(code)
        # cost = **帳戶**的成本(顯示損益用);epx = **策略**的進場價(出場門檻用)
        cost, basis = cost_of(code, fallback=info.get("cost"))
        epx = entry_px.get(code)
        _f, now = replay(local_paths[code], entries[code], s_rule(epx), peak_floor=epx)
        peak = now.peak if now else None  # 峰值由價格路徑重算,不靠增量 state
        # S 的價位門只有 trail 35%;訊號過期 26 日 / 時間止損 30 日 / 輸家止損
        # (水下且 ≥15 日)都是時間門,沒有固定止盈。
        stop = peak * 0.65 if peak else None
        # 「水下」判定與規則同基準:輸家門吃**還原(總報酬)價**(now.px),不是原始
        # 收盤——高殖利率持股除息後原始價看似水下、但總報酬未破成本(見 exit_replay
        # 總報酬正規化)。損益/現價顯示仍用原始 px(帳戶真實損益,股利另以現金入帳)。
        tr_px = now.px if now else px
        underwater = epx is not None and tr_px is not None and tr_px < epx
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
                      ("輸家止損 15 日", f"{'水下' if underwater else '水上'}(對進場錨)")],
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
    # 買入清單 = 通往完全體的完整隊列(使用者要求看得到終局):
    # 「今日進場」(空位內、每日上限 2)→「⏸ 排隊」(有席位、輪明日)→
    # 「🕒 遞補」(席位已滿,等出缺)。照標記操作,持倉恆 ≤5 檔。
    n_slot = max(0, 5 - len(adv.keeps))
    rank_i = 0
    ideal_buys: list[tuple[str, str]] = []
    # 先剔除已持有再取窗口。**順序不可顛倒**:若對整個池取 head(n_slot+3) 再跳過
    # 已持有,當 S 已經抱著池內前幾名時,窗口會被自己的持股佔滿 → 明明有空席卻
    # 一檔都不推薦(2026-07-22 實測:持有池內第 1~4 名,空 1 席卻產不出候選)。
    avail = pool.filter(~pl.col(C).is_in(list(holdings))) if holdings else pool
    for r in avail.head(n_slot + 3).to_dicts():
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

SER_LEDGER = f"{paths.STATE}/serenity/live_positions.json"
SER_OVERRIDES = f"{paths.STATE}/serenity/overrides.json"
SER_BRIEFS = f"{paths.OUT}/trading/briefs"


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

    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
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
    local_paths = load_paths(sorted(entries), min(entries.values()) if entries else cutoff, cutoff)
    for code in sorted(holdings):
        pos = positions.get(code)
        px = closes.get(code)
        if pos is None:
            adv.keeps.append((code, "⚠ 未收養——跑 serenity daily run 讓收養協定"
                                    "接手後才受六道門管理"))
            continue
        if px is None or code not in local_paths:
            adv.sells.append((code, "無法取價(下市/停牌?)人工確認"))
            continue
        anchor = float(pos.get("anchor") or px)
        entry = entries[code]
        # **逐日重放**(非今日快照):規則在你沒跑報告的那幾天觸發也算數
        fire, now = replay(local_paths[code], entry, serenity_rule(anchor), peak_floor=anchor)
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
