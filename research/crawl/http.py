"""HTTP 抓取層(stdlib urllib,零新依賴)。

TWSE/TPEx 端點多為 GET(日期在 query),MOPS 為 POST(formData)。回傳解碼後文字
或原始 bytes。暫時性網路錯誤指數退避重試;瀏覽器 UA 避免被當 bot 擋。
"""
from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
_DEFAULT_TIMEOUT = 30.0


def fetch_bytes(url: str, *, form: dict[str, str] | None = None,
                timeout: float = _DEFAULT_TIMEOUT, retries: int = 4,
                first_delay: float = 3.0) -> bytes:
    """抓 URL,回原始 bytes。form 給了 → POST(application/x-www-form-urlencoded)。

    暫時性網路/HTTP 5xx 退避重試;4xx 直接拋(重試無用)。
    """
    data = urllib.parse.urlencode(form).encode("ascii") if form is not None else None
    delay = first_delay
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers={"User-Agent": _UA})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if 400 <= exc.code < 500:
                raise  # client error:重試無用
            last_exc = exc
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last_exc = exc
        if attempt < retries - 1:
            time.sleep(delay)
            delay = min(delay * 2.0, 30.0)
    raise RuntimeError(f"抓取失敗(重試 {retries} 次):{url}") from last_exc


def fetch_text(url: str, *, encoding: str = "Big5-HKSCS",
               form: dict[str, str] | None = None,
               timeout: float = _DEFAULT_TIMEOUT) -> str:
    """抓 URL 並解碼。TWSE CSV = Big5-HKSCS;TPEx JSON / opendata = UTF-8。

    以 errors='replace' 解碼避免單一壞字元炸掉整批;呼叫端的欄位驗證會擋下真異常。
    """
    return fetch_bytes(url, form=form, timeout=timeout).decode(encoding, errors="replace")
