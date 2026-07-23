"""部署版本自檢:今天跑的程式碼,是不是 repo 上那一份?

事故(2026-07-22):systemd 的自更新步驟寫成 `ExecStartPre=-`(前綴 `-` = 失敗
不擋),用意是「網路暫時抓不到就用上次的程式碼跑,別讓整個交易日開天窗」。
用意合理,但它把兩種截然不同的失敗混為一談:

  ① 抓不到(網路/遠端故障)→ 用上次的程式碼跑,可接受。
  ② 抓到了但**套用失敗**(當天實況:VM 上 `src/quantlib/records/*.parquet` 屬於別的
     使用者 → `git reset --hard` 寫不進去 → 整條 `&&` 斷掉)→ 服務照跑**舊碼**,
     而且**一聲不吭**。當天連續兩輪盤前都是用舊碼產生交易計劃的。

這正是本輪一路在清的同一種病:**用會漂的東西當事實,而漂了沒有人知道**。

解法不是把 `-` 拿掉(那會讓一次網路抖動賠掉整個交易日),而是**讓每天必看的
計劃信自己當監視器**:信裡永遠標出今天跑的 commit;一旦與遠端不一致,連信件
主旨都會喊。偵測只用本機 git,不連網、不依賴任何 infra 設定——因為 `git fetch`
即使在 reset 失敗時也已經把 `origin/master` 更新好了(事故當下實測:HEAD 9dbaeb1、
origin/master 680de0f)。
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]


def _git(*args: str) -> str | None:
    try:
        out = subprocess.run(("git", "-C", str(_REPO), *args), capture_output=True,
                             text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None if out.returncode == 0 else None


@dataclass(frozen=True)
class Deployment:
    """今天實際在跑的版本,以及它與遠端的關係。"""
    head: str | None            # 正在跑的 commit(短碼)
    remote: str | None          # 上次 fetch 看到的 origin/master
    subject: str | None         # HEAD 的 commit 標題(給人看)

    @property
    def drifted(self) -> bool:
        """True = 正在跑的**不是** repo 上那一份(自更新沒套用成功)。"""
        return bool(self.head and self.remote and self.head != self.remote)

    @property
    def line(self) -> str:
        if not self.head:
            return "版本:未知(非 git 工作區)"
        if self.drifted:
            return (f"⚠️ 版本不一致:正在跑 {self.head},repo 上是 {self.remote}"
                    "——自更新沒套用成功,今天的計劃出自舊碼")
        return f"版本:{self.head}" + (f"({self.subject})" if self.subject else "")


def current() -> Deployment:
    return Deployment(head=_git("rev-parse", "--short", "HEAD"),
                      remote=_git("rev-parse", "--short", "origin/master"),
                      subject=_git("log", "-1", "--format=%s"))
