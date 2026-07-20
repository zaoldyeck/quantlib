"""notify money-path 守護:取消鈕/主旨/HTML 渲染 + is_cancelled 判定(mock IMAP)。

Run: uv run --project research python -m research.trading.live.tests.test_notify
     或 uv run --project research pytest research/trading/live/tests/test_notify.py
"""
from __future__ import annotations

from research.trading.live import notify
from research.trading.live.s_plan import DayPlan


def _plan() -> DayPlan:
    return DayPlan(
        date="2026-07-21", buys=["2408"], sells=["9999"],
        manual_review=[("1111", "無法取價(下市/停牌?)人工確認")],
        keeps=[("2330", "六道門全綠")], queued=[("5483", "⏸ 排隊 #3|…")],
        notes=["今日 fresh cohort 5 檔"])


def test_cancel_subject_and_mailto() -> None:
    assert notify.CANCEL_SUBJECT_TMPL.format(date="2026-07-21") == "CANCEL-S-2026-07-21"
    m = notify.cancel_mailto("me@gmail.com", "2026-07-21")
    assert m.startswith("mailto:me@gmail.com?subject=")
    assert "CANCEL-S-2026-07-21" in m  # quote 保留 hyphen/alnum,主旨原樣可見
    assert "body=" in m


def test_render_html_has_button_and_names() -> None:
    h = notify.render_plan_html(_plan(), {"2408": "超豐", "9999": "某公司"}, "me@gmail.com")
    assert "🛑 取消今日執行" in h
    assert "mailto:me@gmail.com" in h
    assert "2408 超豐" in h and "9999 某公司" in h
    assert "買 1 股" in h and "賣出全部" in h
    # 需人工複核區塊要出現且標出原因
    assert "1111" in h and "人工確認" in h


def test_render_text_plain() -> None:
    t = notify.render_plan_text(_plan(), {"2408": "超豐"})
    assert "2408 超豐" in t
    assert "CANCEL-S-2026-07-21" in t
    assert "賣全部" in t or "全部庫存" in t


class _FakeIMAP:
    def __init__(self, found: bool):
        self._found = found

    def login(self, u, p):  # noqa: D401
        return ("OK", [b"ok"])

    def select(self, box):
        return ("OK", [b"1"])

    def search(self, charset, key, val):
        return ("OK", [b"1 2" if self._found else b""])

    def logout(self):
        return ("BYE", [b""])


def test_is_cancelled_mock() -> None:
    """主旨命中 → True;不命中 → False。fail-safe 例外路徑由 execute 負責。"""
    orig = notify.imaplib.IMAP4_SSL
    try:
        notify.imaplib.IMAP4_SSL = lambda h, p: _FakeIMAP(True)
        assert notify.GmailNotifier("me@gmail.com", "pw").is_cancelled("2026-07-21") is True
        notify.imaplib.IMAP4_SSL = lambda h, p: _FakeIMAP(False)
        assert notify.GmailNotifier("me@gmail.com", "pw").is_cancelled("2026-07-21") is False
    finally:
        notify.imaplib.IMAP4_SSL = orig


def main() -> None:
    for fn in (test_cancel_subject_and_mailto, test_render_html_has_button_and_names,
               test_render_text_plain, test_is_cancelled_mock):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ notify 全過")


if __name__ == "__main__":
    main()
