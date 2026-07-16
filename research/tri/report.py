"""每日投資報告的排版層——寫給投資人看,純程式、零 LLM。

**主檔只回答投資人的五個問題**,每檔一張卡片(6-8 行):
1. 今天要做什麼? 2. 這筆賺賠多少? 3. 我為什麼持有它?
4. 什麼事會讓我賣? 5. 現在離出場多遠(還能虧多少)?

原始存證(策展當時查閱的新聞、歷史標記、英文論點原文)一律進**附錄**——
可查證但不擋路。2026-07-16 的教訓:把資料庫倒出來不叫詳細,那叫沒整理;
而且「現在的理由」必須是**現在有效的**理由——兩年前到期的標記放主檔會誤導。

距離一律用人話:「跌 20% 到 6,800 觸發停損」,不是「現價距 +25%」——後者
角度反了,還會讓人**高估安全邊際**(同一天寫出來的實際錯誤,故記於此)。
"""

from __future__ import annotations

from research.tri.advisors import Advice
from research.tri.rationale import evergreen_labels, serenity_thesis

MAX_MATERIALS = 4
MAX_EXCERPT = 300


def _has_cjk(s: str) -> bool:
    return any("一" <= ch <= "鿿" for ch in (s or "")[:30])


def _money(x: float | None) -> str:
    return f"{x:,.6g}" if x else "—"


def _to_trigger(px: float | None, level: float | None) -> str:
    """從現價還要漲/跌多少才碰到這條線——投資人要的是這個,不是兩價的比值。"""
    if not px or not level:
        return "—"
    d = (level / px - 1) * 100
    return f"{'跌' if d < 0 else '漲'} {abs(d):.1f}% 到 {_money(level)}"


def _verdict_of(adv: Advice | None, code: str) -> tuple[str, str] | None:
    if adv is None:
        return None
    for c, why in adv.keeps:
        if c == code:
            return "續抱", why
    for c, why in adv.sells:
        if c == code:
            return "賣出", why
    for c, _w, why in adv.buys:
        if c == code:
            return "買入", why
    return None


def _active(v: tuple[str, str] | None) -> bool:
    return bool(v and v[0] in ("續抱", "買入"))


def _why_hold(code: str, v_ser, v_ev, v_s, d_s: dict | None) -> list[str]:
    """「現在」有效的理由——過期的標記不算(它在附錄,標明是歷史)。"""
    out: list[str] = []
    th = serenity_thesis(code)
    if th and _active(v_ser):
        # 用實際內容判斷是否白話,不用欄位來源判斷:收養註記 > 中文論點 >
        # 英文速記(僅英文時才標警語,策展層已補中文就不再誤掛「待補」)。
        if th.thesis_note:
            body = th.thesis_note
        elif _has_cjk(th.source_note):
            body = th.source_note
        else:
            body = (f"{th.source_note}(⚠ 註冊表僅英文速記,尚未補中文說明;"
                    f"瓶頸層:{th.bottleneck_layer})")
        out.append(f"- **為什麼持有**(Serenity 策展,信念度 {th.conviction}/5):{body}")
    labs = evergreen_labels(code)
    if labs and _active(v_ev):
        lab = labs[0]
        out.append(f"- **為什麼{'買' if v_ev[0] == '買入' else '持有'}**"
                   f"(Evergreen {lab.month} 標記,信念度 {lab.conviction}/5):"
                   f"{lab.theme} — {lab.event}")
    if d_s and d_s.get("geo") and _active(v_s):
        out.append(f"- **為什麼{'買' if v_s[0] == '買入' else '持有'}**(S 純量化):"
                   f"月營收加速六因子綜合排名池內第 {d_s.get('pool_rank', '?')} 名"
                   f"(geo {d_s['geo']};營收年增加速、創 52 週新高、站上 20 日高位…)")
    if not out:
        out.append("- **為什麼持有**:⚠ 三個策略現在都不會買它——這是系統接手前的持股,"
                   "或你在系統之外的判斷")
    return out


def _why_sell(code: str, v_ser, v_ev) -> list[str]:
    th = serenity_thesis(code)
    if th and th.invalidation_criteria and _active(v_ser):
        return [f"- **什麼會讓我賣**(要事實,不是新聞情緒):{th.invalidation_criteria}"]
    labs = evergreen_labels(code)
    if labs and labs[0].invalidation and _active(v_ev):
        return [f"- **什麼會讓我賣**:{labs[0].invalidation}"]
    return []


def _distance_line(det: dict | None) -> list[str]:
    if not det or not det.get("px"):
        return []
    px = det["px"]
    bits = []
    if det.get("stop"):
        bits.append(f"{_to_trigger(px, det['stop'])} 觸發停損(規則:{det.get('stop_note', '')})")
    bits.append(f"{_to_trigger(px, det['take'])} 觸發止盈" if det.get("take")
                else "無固定止盈(靠移動停損/時間出場)")
    return [f"- **離出場多遠**:{'|'.join(bits)}"]


def stock_card(code: str, name: str, shares: float, advices: dict[str, Advice]) -> str:
    """主檔卡片——30 秒讀完就知道這檔的處境。"""
    ser, ev, sa = advices.get("Serenity"), advices.get("Evergreen"), advices.get("S")
    d_ser, d_ev, d_s = (a.detail.get(code) if a else None for a in (ser, ev, sa))
    v_ser, v_ev, v_s = (_verdict_of(a, code) for a in (ser, ev, sa))
    lead = d_ser or d_s or d_ev or {}
    cost, px = lead.get("cost"), lead.get("px")

    head = f"### {code} {name}"
    if shares:
        head += f"|{int(shares):,} 股"
    if cost and px:
        from research.trading.cost_basis import BASIS_LABEL
        head += (f"|成本 {_money(cost)}({BASIS_LABEL.get(lead.get('basis', ''), '?')})"
                 f"|現價 {_money(px)}|**{(px / cost - 1) * 100:+.1f}%**")
        if shares:
            head += f"({(px - cost) * shares:+,.0f} 元)"
    elif px:
        head += f"|現價 {_money(px)}"

    lines = [head, ""]
    lines += _why_hold(code, v_ser, v_ev, v_s, d_s)
    lines += _why_sell(code, v_ser, v_ev)
    lines += _distance_line(d_ser or d_s or d_ev)
    if d_ser and d_ser.get("gates"):
        checks = "|".join(v for g, v in d_ser["gates"] if g.startswith(("法人", "營收")))
        if checks:
            lines.append(f"- **系統檢查**:{checks}")
    others = "|".join(f"{k}:{v[0]}" for k, v in (("Serenity", v_ser), ("S", v_s),
                                                 ("Evergreen", v_ev)) if v)
    if others:
        lines.append(f"- **三策略怎麼看**:{others}(判決不同很正常,它們是三個獨立視角;仲裁權在你)")
    for det in (d_ser, d_s, d_ev):
        if det and det.get("fire_day"):
            lines.append(f"- 🔴 **規則已於 {det['fire_day']} 觸發【{det['fire_reason']}】**"
                         f"(當時 {_money(det['fire_price'])},今日 {_money(det['px'])})"
                         + ("——**逾期未出場,今天就賣**" if det.get("overdue") else ""))
            break
    return "\n".join(lines)


def stock_appendix(code: str, name: str, advices: dict[str, Advice]) -> str:
    """附錄:原始存證。要查證時才看——英文論點原文、歷史標記、當時的材料。"""
    out = [f"### {code} {name}"]
    th = serenity_thesis(code)
    if th:
        sourced = th.evidence_url if th.evidence_is_sourced else \
            f"⚠ 早期入冊未留出處(`{th.evidence_url}`)"
        out += ["", f"**Serenity 註冊表**(入冊 {th.active_from}|複審 {th.review_by})",
                f"- 主題 `{th.theme_id}` — {th.theme_name}|瓶頸層:{th.bottleneck_layer}",
                f"- 論點原文:{th.source_note}",
                f"- 證據日 {th.evidence_date}|出處:{sourced}"]
    labs = evergreen_labels(code)
    if labs:
        lab = labs[0]
        stale = "" if _active(_verdict_of(advices.get("Evergreen"), code)) else \
            "(**池籍已到期——歷史紀錄,不是現在持有的理由**)"
        out += ["", f"**Evergreen 最近一次標記:{lab.month}**{stale}",
                f"- 題材:{lab.theme}|訊號類型:{lab.signal_type}|信念度 {lab.conviction}/5",
                f"- 事件:{lab.event}", f"- 證據:{lab.evidence}",
                f"- 失效條件:{lab.invalidation}"]
        if lab.materials:
            out += ["", f"<details><summary>當時查閱的材料({len(lab.materials)} 筆,原文照錄"
                        "——研究員速記,不是給人讀的文案)</summary>", ""]
            for m in lab.materials[:MAX_MATERIALS]:
                ex = m.excerpt[:MAX_EXCERPT] + ("…" if len(m.excerpt) > MAX_EXCERPT else "")
                out += [f"- **{m.title}**",
                        f"  - {m.source}|{m.date}" + (f"|{m.url}" if m.url else ""),
                        f"  - {ex}"]
            out += ["", "</details>"]
        if len(labs) > 1:
            out += ["", f"**標記史**(共 {len(labs)} 次)", "",
                    "| 月份 | 題材 | 信念度 |", "|---|---|---:|"]
            out += [f"| {x.month} | {x.theme} | {x.conviction} |" for x in labs]
    d_s = advices["S"].detail.get(code) if advices.get("S") else None
    if d_s and d_s.get("factors"):
        out += ["", "**S 的六因子**(純量化,無敘事)", "", "| 因子 | 值 |", "|---|---:|"]
        out += [f"| {k} | {v} |" for k, v in d_s["factors"].items()]
        if d_s.get("geo"):
            out.append(f"| **geo(幾何平均)** | **{d_s['geo']}** |")
    d_ser = advices["Serenity"].detail.get(code) if advices.get("Serenity") else None
    if d_ser and d_ser.get("gates"):
        out += ["", "**Serenity 六道門逐條**", "", "| 門 | 線/值 |", "|---|---|"]
        out += [f"| {g} | {v} |" for g, v in d_ser["gates"]]
    return "\n".join(out)


def action_block(advices: dict[str, Advice], names: dict) -> str:
    """置頂:今天非做不可的事。該賣沒賣排最前——規則早已觸發,只是你還沒賣。"""
    def nm(c: str) -> str:
        return f"{c} {names.get(c, '')}".strip()

    overdue, fired, buys = [], [], []
    for sname, adv in advices.items():
        for code, det in adv.detail.items():
            if det.get("overdue"):
                overdue.append(f"- 🔴 **賣掉 {nm(code)}** — {sname} 的【{det['fire_reason']}】"
                               f"在 {det['fire_day']} 就觸發了(當時 {_money(det['fire_price'])},"
                               f"今日 {_money(det['px'])})。**延遲不代表沒發生:今天賣**")
            elif det.get("fire_day"):
                fired.append(f"- 🟠 **賣掉 {nm(code)}** — {sname} 的【{det['fire_reason']}】今日觸發")
        for code, _w, why in adv.buys:
            if "今日進場" in why:
                buys.append(f"- 🟢 **可買 {nm(code)}** — {sname} 推薦"
                            f"({why.split('|')[1] if '|' in why else why})")
    out = ["## 🔔 今天要做什麼"]
    if overdue:
        out += ["", "### 🔴 該賣沒賣(規則早就觸發了)", *overdue]
    if fired:
        out += ["", "### 🟠 今天觸發出場", *fired]
    if not (overdue or fired):
        out += ["", "### ✅ 沒有任何持股該賣",
                "- 每一檔的出場規則都用「進場以來的每日收盤價」逐日檢查過"
                "(包含你沒跑報告的那幾天),全數未觸發"]
    if buys:
        out += ["", "### 🟢 今天可以買(買不買是你的決定)", *dict.fromkeys(buys)]
    return "\n".join(out)
