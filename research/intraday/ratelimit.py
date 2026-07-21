"""永豐 Shioaji 官方限流的 token bucket 實作(執行緒安全)。

**規則出處(第一手,非推測)**:https://sinotrade.github.io/tutor/limit
- 行情類(`snapshots` / `ticks` / `kbars` / `credit_enquires` / `short_stock_sources`):
  **每 5 秒最多 50 次**。
- 帳務類 25 次/5 秒;下單類 250 次/10 秒;訂閱上限 200;同一身分證最多 5 條連線;
  每日 `login()` 上限 1000 次。
- 資料流量另有每日 2 GB 上限,交易日 08:00 重置。

**罰則(這是要留安全邊際的理由)**:超流量 → 行情查詢回 null;**超頻率 → 服務停用
1 分鐘;累犯 → IP 與帳號一併封鎖**。封鎖要找客服解,代價遠高於慢一點,故預設
只用官方額度的 80%。
"""
from __future__ import annotations

import threading
import time

#: 官方行情類限制:50 次 / 5 秒
MARKET_CALLS, MARKET_WINDOW = 50, 5.0
#: 安全係數:只用官方額度的 80%(超頻罰則是停用甚至封鎖,不值得貼著上限跑)
SAFETY = 0.8


class RateLimiter:
    """token bucket:`calls` 次 / `window` 秒,平滑補充,執行緒安全。

    平滑補充(而非「每 5 秒放 50 個」)是刻意的:突發 50 次雖然在字面額度內,
    伺服器端若以更短的視窗計算就會誤判超頻。平滑到每秒約 8 次最穩。
    """

    def __init__(self, calls: float = MARKET_CALLS * SAFETY,
                 window: float = MARKET_WINDOW) -> None:
        self.rate = calls / window          # 每秒可用 token
        self.capacity = max(1.0, calls / 5) # 容許的瞬間突發(約 1 秒份)
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, n: float = 1.0) -> float:
        """取得 n 個 token(不足則阻塞)。回傳實際等待秒數(供量測)。"""
        waited = 0.0
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self.capacity,
                                   self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= n:
                    self._tokens -= n
                    return waited
                need = (n - self._tokens) / self.rate
            time.sleep(need)
            waited += need

    @property
    def per_second(self) -> float:
        return self.rate
