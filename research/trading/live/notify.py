"""通知層:Notifier port + GmailNotifier adapter(只用 Gmail,不開任何伺服器)。

下單把關語義(使用者定調 2026-07-21):**預設執行、一鍵取消**。盤前信帶一個
「🛑 取消今日執行」按鈕(`mailto:` 連結,點了 = 寄一封標好取消的信給自己);開盤前
VM 用同一組 Gmail app-password 走 IMAP 檢查收信匣有無那封信,有就中止。全程只用
Gmail(SMTP 送 + IMAP 收),不開對外網頁、不暴露任何憑證。

**money-path 失效政策**:`is_cancelled` 若因 IMAP/登入故障無法確認,**上拋例外**——
呼叫端(execute)必須「無法確認取消 → 安全起見今日不交易」,絕不在讀不到取消信時
擅自假設「沒取消」而照送真錢單。

換通知管道(Telegram/LINE…)只需另寫一個實作 `Notifier` 的 adapter,不動 execute。
"""
from __future__ import annotations

import html
import imaplib
import os
import smtplib
import ssl
from datetime import date as Date
from email.message import EmailMessage
from typing import Protocol, runtime_checkable
from urllib.parse import quote

from research.brokers.fubon import load_env_file
from research.tri.advisors import S_CODE, S_FULL, S_NAME
from research.trading.live.version import current as _deployment

_SMTP_HOST, _SMTP_PORT = "smtp.gmail.com", 587
_IMAP_HOST, _IMAP_PORT = "imap.gmail.com", 993
#: 取消信主旨模板;主旨內嵌交易日 → 天生 day-specific,昨日的取消不會誤觸今日
CANCEL_SUBJECT_TMPL = "CANCEL-S-{date}"
#: 保留股「確認賣出」主旨模板;內嵌代碼+日 → 每檔每日獨立確認
CONFIRM_SELL_SUBJECT_TMPL = "CONFIRM-SELL-{code}-{date}"


@runtime_checkable
class Notifier(Protocol):
    """通知 port。send=盤前計劃/成交回報;is_cancelled=開盤前取消檢查。"""

    def send_plan_email(self, plan, names: dict[str, str]) -> None: ...
    def is_cancelled(self, date_str: str) -> bool: ...
    def send_fill_summary(self, date_str: str, summary_html: str,
                          summary_text: str) -> None: ...


class GmailNotifier:
    """Gmail app-password 的 SMTP 送信 + IMAP 取消檢查。"""

    def __init__(self, user: str, app_password: str, to: str | None = None):
        self.user = user
        self.app_password = app_password
        self.to = to or user  # 預設寄給自己

    @classmethod
    def from_env(cls) -> "GmailNotifier":
        load_env_file()
        user = os.environ.get("GMAIL_USER")
        pw = os.environ.get("GMAIL_APP_PASSWORD")
        if not user or not pw:
            raise ValueError(
                "缺 GMAIL_USER / GMAIL_APP_PASSWORD(需 Gmail 兩步驟驗證後產生的"
                " app-password;存 research/.env 或 Secret Manager)")
        return cls(user, pw, os.environ.get("GMAIL_TO") or user)

    # ── SMTP 送信 ──────────────────────────────────────────────────
    def _send(self, subject: str, html_body: str, text_body: str) -> None:
        msg = EmailMessage()
        msg["From"] = self.user
        msg["To"] = self.to
        msg["Subject"] = subject
        msg.set_content(text_body)                       # 純文字後備
        msg.add_alternative(html_body, subtype="html")   # HTML 主體
        ctx = ssl.create_default_context()
        with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT, timeout=30) as s:
            s.starttls(context=ctx)
            s.login(self.user, self.app_password)
            s.send_message(msg)

    def send_plan_email(self, plan, names: dict[str, str], settle=None) -> None:
        _dep = _deployment()
        subject = f"[{S_NAME}] {plan.date} 交易計劃"
        if not plan.has_actions:
            subject += "(今日無自動下單腿)"
        if settle is not None and settle.shortfall > 0:
            subject += f"　⚠️ 交割款不足約 {settle.shortfall:,.0f} 元"
        if _dep.drifted:
            subject += "　⚠️ 程式版本不一致"
        self._send(subject,
                   render_plan_html(plan, names, self.to, settle),
                   render_plan_text(plan, names, settle))

    def send_fill_summary(self, date_str: str, summary_html: str,
                          summary_text: str) -> None:
        self._send(f"[{S_NAME}] {date_str} 執行結果", summary_html, summary_text)

    def send_text(self, subject: str, text: str) -> None:
        """通用純文字通知(年度 refit 報告等):純文字 + monospace HTML 後備。"""
        esc = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        self._send(subject, f"<pre style='font-family:ui-monospace,monospace'>{esc}</pre>", text)

    # ── IMAP 取消 / 確認檢查 ───────────────────────────────────────
    def _inbox_has_subject(self, subject: str) -> bool:
        """收信匣是否有指定主旨的信。IMAP/登入故障 → 上拋(呼叫端 fail-safe)。"""
        im = imaplib.IMAP4_SSL(_IMAP_HOST, _IMAP_PORT)
        try:
            im.login(self.user, self.app_password)
            im.select("INBOX")
            typ, data = im.search(None, "SUBJECT", f'"{subject}"')
            if typ != "OK":
                raise RuntimeError(f"IMAP search 失敗:{typ}")
            ids = data[0].split() if data and data[0] else []
            return len(ids) > 0
        finally:
            try:
                im.logout()
            except Exception:  # noqa: BLE001 - logout 失敗不影響判定結果
                pass

    def is_cancelled(self, date_str: str) -> bool:
        """收信匣有無今日取消信。IMAP/登入故障 → 上拋(呼叫端 fail-safe 不交易)。"""
        return self._inbox_has_subject(CANCEL_SUBJECT_TMPL.format(date=date_str))

    def is_sell_confirmed(self, code: str, date_str: str) -> bool:
        """保留股是否已收到今日『確認賣出』信。故障 → 上拋(呼叫端保守:不賣)。"""
        return self._inbox_has_subject(
            CONFIRM_SELL_SUBJECT_TMPL.format(code=code, date=date_str))


# ── 純函式:計劃 → HTML / 純文字(presentation;可獨立測試)───────────


def _nm(code: str, names: dict[str, str]) -> str:
    n = names.get(code)
    return f"{code} {n}" if n else code


def cancel_mailto(to: str, date_str: str) -> str:
    """取消鈕的 mailto 連結:點了 = 寄一封標好取消主旨的信給自己。"""
    subject = CANCEL_SUBJECT_TMPL.format(date=date_str)
    body = f"取消 {S_NAME} {date_str} 今日執行"
    return f"mailto:{to}?subject={quote(subject)}&body={quote(body)}"


def confirm_sell_mailto(to: str, code: str, date_str: str) -> str:
    """保留股『確認賣出』鈕:點了 = 寄一封確認信,VM 才會賣這一檔保留股。"""
    subject = CONFIRM_SELL_SUBJECT_TMPL.format(code=code, date=date_str)
    body = f"確認賣出保留股 {code}({date_str})"
    return f"mailto:{to}?subject={quote(subject)}&body={quote(body)}"


def _money_rows(legs, side: str, names: dict[str, str]) -> str:
    """明細列:代碼/名稱 + 最近收盤 + 金額;賣出腿另附成本、ROI、持有損益。"""
    e = html.escape
    out = []
    for l in legs:
        if l.side != side:
            continue
        px = f"{l.px:,.2f}" if l.px is not None else "—"
        if side == "buy":
            out.append(f"<li><b>買 {l.shares} 股</b>　{e(_nm(l.code, names))}"
                       f"　<span style='color:#52606d'>收盤 {px}"
                       f"　約 −{abs(l.net):,.0f} 元(含費)</span></li>")
        else:
            roi = "" if l.roi is None else (
                f"　<b style='color:{'#047857' if l.roi >= 0 else '#b91c1c'}'>"
                f"ROI {l.roi:+.1%}(損益 {l.pnl:+,.0f} 元)</b>")
            cost = "" if not l.cost else (
                f"　<span style='color:#52606d'>成本 {l.cost:,.2f}"
                f"{e(' ' + l.cost_basis) if l.cost_basis else ''}</span>")
            out.append(f"<li><b>賣出全部 {l.shares} 股</b>　{e(_nm(l.code, names))}"
                       f"　<span style='color:#52606d'>收盤 {px}"
                       f"　約 +{l.net:,.0f} 元(扣費稅)</span>{cost}{roi}</li>")
    return "".join(out) or "<li>（無）</li>"


def render_plan_html(plan, names: dict[str, str], to: str, settle=None) -> str:
    e = html.escape
    _dep = _deployment()

    def _rows(items, fmt):
        return "".join(f"<li>{fmt(x)}</li>" for x in items) or "<li>（無）</li>"

    if settle is not None:
        buys_html = _money_rows(settle.legs, "buy", names)
        sells_html = _money_rows(settle.legs, "sell", names)
    else:
        buys_html = _rows(plan.buys, lambda c: f"<b>買 1 股</b>　{e(_nm(c, names))}")
        sells_html = _rows(plan.sells, lambda c: f"<b>賣出全部</b>　{e(_nm(c, names))}")
    manual_html = _rows(
        plan.manual_review,
        lambda x: f"⚠️ {e(_nm(x[0], names))}　<span style='color:#b45309'>{e(x[1])}</span>")
    keeps_html = _rows(plan.keeps, lambda x: f"{e(_nm(x[0], names))}　{e(x[1])}")
    queued_html = _rows(plan.queued, lambda x: f"{e(_nm(x[0], names))}　{e(x[1])}")
    notes_html = _rows(plan.notes, lambda s: e(s))
    mailto = cancel_mailto(to, plan.date)

    money_block = ""
    if settle is not None:
        warn = ""
        if settle.shortfall > 0:
            warn = (f"<div style='background:#fef2f2;border:1px solid #fecaca;"
                    f"border-radius:8px;padding:12px 14px;margin-top:10px'>"
                    f"<b style='color:#b91c1c'>⚠️ 資金可能不足,請補入交割款</b><br>"
                    f"買進需 {settle.buy_cost:,.0f} 元,帳戶可用 {settle.cash:,.0f} 元,"
                    f"<b>缺口約 {settle.shortfall:,.0f} 元</b>。"
                    f"<span style='color:#52606d'>(賣出款 T+2 才入帳,故不計入)</span></div>")
        sign = "+" if settle.net_change >= 0 else "−"
        money_block = (
            "<h3>資金試算</h3>"
            "<table style='border-collapse:collapse;font-size:14px'>"
            f"<tr><td style='padding:3px 14px 3px 0;color:#52606d'>帳戶可用現金</td>"
            f"<td style='text-align:right'><b>{settle.cash:,.0f}</b> 元</td></tr>"
            f"<tr><td style='padding:3px 14px 3px 0;color:#52606d'>預計買進(含手續費)</td>"
            f"<td style='text-align:right'>−{settle.buy_cost:,.0f} 元</td></tr>"
            f"<tr><td style='padding:3px 14px 3px 0;color:#52606d'>預計賣出(扣費稅)</td>"
            f"<td style='text-align:right'>+{settle.sell_proceeds:,.0f} 元</td></tr>"
            f"<tr><td style='padding:3px 14px 3px 0'><b>現金淨變動</b></td>"
            f"<td style='text-align:right'><b>{sign}{abs(settle.net_change):,.0f}</b> 元</td></tr>"
            "</table>"
            "<p style='color:#8a94a6;font-size:12px;margin:6px 0 0'>"
            "金額以<b>最近收盤價</b>試算(計劃於盤前產生,當下無即時報價);"
            "實際成交價由開盤後執行決定。台股 T+2 交割。</p>"
            f"{warn}")

    manual_block = ""
    if plan.manual_review:
        manual_block = (
            "<h3 style='color:#b45309'>需人工複核(不自動下單)</h3>"
            f"<ul>{manual_html}</ul>")

    protected_block = ""
    if plan.protected_sells:
        btn = ("display:inline-block;padding:6px 14px;margin-left:8px;background:#b45309;"
               "color:#fff;text-decoration:none;border-radius:6px;font-size:13px")
        prows = "".join(
            f"<li style='margin:6px 0'>{e(_nm(c, names))}"
            f"<a href='{confirm_sell_mailto(to, c, plan.date)}' style='{btn}'>✅ 確認賣出</a></li>"
            for c in plan.protected_sells)
        protected_block = (
            "<h3 style='color:#b45309'>你的保留股(策略建議賣,但預設不動)</h3>"
            "<p style='color:#52606d;margin-top:0'>這些是你指定<b>自己控</b>的股票。"
            "VM <b>預設不賣、續抱</b>;唯有你按對應「確認賣出」鈕,開盤才會賣該檔。</p>"
            f"<ul style='list-style:none;padding-left:0'>{prows}</ul>")

    return f"""\
<div style="font-family:-apple-system,'Segoe UI',Roboto,'PingFang TC','Microsoft JhengHei',sans-serif;max-width:640px;margin:0 auto;color:#1f2933;line-height:1.6">
  <h2 style="margin-bottom:2px">{S_NAME} · {e(plan.date)} 交易計劃</h2>
  <p style="color:#52606d;margin-top:0">營運模式:<b>買進每檔 1 股、賣出全部庫存</b>。你不動作 → 09:00 自動執行。</p>

  <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:14px 16px;margin:16px 0">
    <p style="margin:0 0 10px">若<b>不要</b>今日執行,請在 <b>08:55 前</b>點下方按鈕(= 寄一封取消信給你自己;VM 開盤前會讀到):</p>
    <a href="{mailto}" style="display:inline-block;padding:12px 22px;background:#c0392b;color:#fff;text-decoration:none;border-radius:6px;font-weight:bold">🛑 取消今日執行</a>
  </div>

  <h3 style="color:#166534">買入(今日進場,各 1 股)</h3>
  <ul>{buys_html}</ul>

  <h3 style="color:#991b1b">賣出(全部庫存,非保留股)</h3>
  <ul>{sells_html}</ul>

  {protected_block}

  {money_block}
  {manual_block}

  <h3>續抱</h3>
  <ul>{keeps_html}</ul>

  <details><summary style="cursor:pointer;color:#52606d">排隊 / 遞補(今日不進場,資訊)</summary><ul>{queued_html}</ul></details>

  <h3>備註</h3>
  <ul style="color:#52606d">{notes_html}</ul>

  <p style="color:{"#c0392b" if _dep.drifted else "#9aa5b1"};font-size:12px;margin-top:24px">{e(_dep.line)}<br>{S_FULL}(代號 {S_CODE})· 自動產生 · 送單一律經富邦 execution.trade,收盤未竟自動盤後掛收盤價</p>
</div>"""


def render_plan_text(plan, names: dict[str, str], settle=None) -> str:
    _dep = _deployment()
    lines = [_dep.line, "",
             f"{S_NAME} {plan.date} 交易計劃(買各 1 股、賣全部;不動作 09:00 自動執行)",
             "取消:08:55 前回覆本信(主旨含 CANCEL-S-" + plan.date + ")即中止今日執行", ""]
    lines.append("買入(今日進場):" + ("、".join(_nm(c, names) for c in plan.buys) or "無"))
    lines.append("賣出(全部庫存,非保留):" + ("、".join(_nm(c, names) for c in plan.sells) or "無"))
    if plan.protected_sells:
        lines.append("保留股(策略建議賣,預設不動,需回信確認才賣):"
                     + "、".join(_nm(c, names) for c in plan.protected_sells))
    if plan.manual_review:
        lines.append("需人工複核(不自動下單):"
                     + "、".join(f"{_nm(c, names)}[{r}]" for c, r in plan.manual_review))
    lines.append("續抱:" + ("、".join(_nm(c, names) for c, _ in plan.keeps) or "無"))
    if settle is not None:
        lines += ["", "【資金試算(以最近收盤價估;實際成交價開盤後決定,T+2 交割)】",
                  f"  帳戶可用現金 {settle.cash:,.0f} 元",
                  f"  預計買進(含手續費)−{settle.buy_cost:,.0f} 元",
                  f"  預計賣出(扣費稅)+{settle.sell_proceeds:,.0f} 元",
                  f"  現金淨變動 {settle.net_change:+,.0f} 元"]
        for l in settle.legs:
            if l.side == "sell" and l.roi is not None:
                lines.append(f"  {_nm(l.code, names)} ROI {l.roi:+.1%}"
                             f"(成本 {l.cost:,.2f} → 收盤 {l.px:,.2f},"
                             f"損益 {l.pnl:+,.0f} 元)")
        if settle.shortfall > 0:
            lines.append(f"  ⚠️ 資金不足,請補入交割款約 {settle.shortfall:,.0f} 元"
                         f"(賣出款 T+2 才入帳,未計入)")
    if plan.notes:
        lines.append("")
        lines += [f"- {n}" for n in plan.notes]
    return "\n".join(lines) + "\n"


def today_taipei() -> Date:
    """台北當日(run/trade 日)——premarket 與 execute 共用,確保取消主旨日一致。"""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Asia/Taipei")).date()
