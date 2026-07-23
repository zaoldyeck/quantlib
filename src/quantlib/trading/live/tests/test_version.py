"""部署版本自檢守護:今天跑的碼與 repo 不一致時,必須在每天必看的信裡喊出來。

事故(2026-07-22):systemd 自更新的 `ExecStartPre=-` 把「網路抓不到」與「抓到了
但套用失敗」混為一談。當天實況是後者(VM 上 src/quantlib/records/*.parquet 屬於別的
使用者 → git reset 寫不進去 → 整條 && 斷掉),於是**連續兩輪盤前都用舊碼產生
交易計劃,而且一聲不吭**。

修法不是把 `-` 拿掉(一次網路抖動就賠掉整個交易日),而是讓計劃信自己當監視器。
本測試守護那個「喊」不會壞掉——它壞了就等於監視器瞎了,而瞎掉的監視器比沒有
監視器更危險(會讓人以為有在看)。

Run: uv run --project . python -m quantlib.trading.live.tests.test_version
"""
from __future__ import annotations

from quantlib.trading.live.version import Deployment, current


def test_same_commit_is_not_drift() -> None:
    d = Deployment(head="abc1234", remote="abc1234", subject="某個修正")
    assert not d.drifted
    assert "abc1234" in d.line and "⚠" not in d.line


def test_different_commit_is_drift_and_shouts() -> None:
    d = Deployment(head="9dbaeb1", remote="680de0f", subject="舊的")
    assert d.drifted
    assert "⚠️" in d.line and "9dbaeb1" in d.line and "680de0f" in d.line
    assert "舊碼" in d.line, "訊息必須講白『今天的計劃出自舊碼』,不能只丟兩串 hash"


def test_unknown_remote_is_not_drift() -> None:
    """從未 fetch 過(或非 git 環境)→ 無從比較,**不得**誤報漂移。
    誤報會訓練使用者忽略警報,那比沒警報更糟。"""
    assert not Deployment(head="abc1234", remote=None, subject=None).drifted
    assert not Deployment(head=None, remote=None, subject=None).drifted


def test_no_git_degrades_gracefully() -> None:
    """非 git 工作區:要有話說、不得拋——盤前任何一步炸掉,今天就沒有交易計劃。"""
    d = Deployment(head=None, remote="abc1234", subject=None)
    assert not d.drifted and "未知" in d.line


def test_current_never_raises() -> None:
    """真實環境呼叫一次:即使 git 不可用也只回 None,絕不拋。"""
    d = current()
    assert isinstance(d, Deployment)
    assert isinstance(d.line, str) and d.line


def test_email_subject_carries_drift_warning() -> None:
    """漂移時主旨要帶警告——手機通知只看得到主旨。"""
    import quantlib.trading.live.notify as n
    orig = n._deployment
    try:
        n._deployment = lambda: Deployment("9dbaeb1", "680de0f", "舊的")

        class _P:
            date, buys, sells = "2026-07-22", ["2886"], []
            protected_sells: list = []
            manual_review: list = []
            keeps: list = []
            queued: list = []
            notes: list = []
            has_actions = True

            def to_dict(self):
                return {}

        # 直接驗 renderer(不觸網):HTML 與純文字都必須帶出漂移訊息
        html_out = n.render_plan_html(_P(), {}, "a@b.c")
        text_out = n.render_plan_text(_P(), {})
        assert "版本不一致" in html_out and "版本不一致" in text_out
    finally:
        n._deployment = orig


def main() -> None:
    for fn in (test_same_commit_is_not_drift,
               test_different_commit_is_drift_and_shouts,
               test_unknown_remote_is_not_drift,
               test_no_git_degrades_gracefully,
               test_current_never_raises,
               test_email_subject_carries_drift_warning):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ 版本自檢全過")


if __name__ == "__main__":
    main()
