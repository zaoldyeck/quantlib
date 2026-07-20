"""Fubon Neo API adapter.

This module deliberately keeps live-order submission behind an explicit
`dry_run=False` switch. Strategy code should produce target positions first;
the broker adapter only translates approved orders into Fubon SDK calls.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any


DEFAULT_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
PENDING_ACTIVATION_MARKERS = (
    "無簽署完成API使用風險暨聲明書帳號",
    "連線測試成功",
    "使用權限將應於次日開通",
)


def load_env_file(path: Path = DEFAULT_ENV_PATH) -> None:
    """Load simple KEY=VALUE pairs without overriding existing environment."""
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


# ── 網路韌性層 ──────────────────────────────────────────────────────
# 2026-07-15 事故:開盤瞬間主機沒網路,`FubonSDK()` 建構子直接拋
# `ValueError: IO error: failed to lookup address information`,盤中執行器
# 整個 traceback 退出、錯過整個交易日。執行器的使命是「整天掛著撈價」,
# **暫時性網路問題絕不能終止程式**——等網路回來、重登、續管。
# 憑證/帳號/權限這類「重試也沒用」的錯誤仍必須快速失敗。

# 分類原則(2026-07-15 實測後反轉):**預設暫時性,只有明確的「重試無用」才
# 快速失敗**。理由:網路錯誤的字樣空間開放且隨 SDK 版本漂移,白名單必漏——
# 實測 Wi-Fi 掉線/路由器掛掉拋的是 `ValueError: URL error: Unable to connect
# to wss://...`,完全不含 DNS 字樣;若沿用白名單就會被判終端 → 程式死,正是
# 使用者要根除的行為。SDK(Rust 綁定 + fugle REST)把所有網路失敗都包成
# ValueError / FugleAPIError,型別無從區分,只能靠這條反向規則。

# (a) 程式 bug:重試只會無聲地把 bug 變成無限迴圈 → 必須炸出來
_PROGRAMMING_ERRORS = (AttributeError, TypeError, KeyError, IndexError, NameError,
                       ImportError, AssertionError, NotImplementedError, ZeroDivisionError)
# (b) 憑證/權限/設定:重試也不會變好
# 註:中文「憑證」= 富邦登入憑證(終端);英文 "certificate/tls" 則多半是
# captive portal(飯店/公司 Wi-Fi)的連線層問題,網路通了就好 → 暫時性。
_TERMINAL_MARKERS = (
    "密碼", "password", "憑證", "api_key", "api key", "unauthorized", "forbidden",
    "permission", "無簽署完成", "使用權限", "required for", "missing fubon",
    "returned no accounts", "帳號已", "身分證", "login error",
)


def is_transient_network_error(exc: BaseException) -> bool:
    """可等網路回來重試(True)vs 重試無用、該快速失敗(False)。

    註:TLS/憑證「連線層」錯誤刻意歸為暫時性(飯店/公司 captive portal 會產生
    憑證錯誤,網路正常後即消失);真正的憑證密碼錯誤帶「密碼/password」字樣。
    """
    if isinstance(exc, _PROGRAMMING_ERRORS):
        return False
    message = str(exc).lower()
    if any(marker.lower() in message for marker in _TERMINAL_MARKERS):
        return False
    return True


def _default_on_wait(what: str, attempt: int, message: str, delay: float) -> None:
    print(f"[net] {what} 網路異常(第 {attempt} 次):{message} —— "
          f"{delay:.0f}s 後重試,**程式保持執行,網路回來即續管**", flush=True)


def net_retry(
    fn: Callable[[], Any],
    *,
    what: str = "網路呼叫",
    should_give_up: Callable[[], bool] | None = None,
    first_delay: float = 3.0,
    max_delay: float = 30.0,
    on_wait: Callable[[str, int, str, float], None] | None = None,
) -> Any:
    """暫時性網路錯誤 → 指數退避重試至網路恢復;終端錯誤直接拋。

    `should_give_up()` 為 True 時放棄(呼叫端用它表達「這時間點之後重試已無意義」,
    例如已過盤後撮合時刻)。預設無上限重試——那正是「電腦沒網路也要留在執行狀態」。
    """
    delay = first_delay
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - 依分類決定重試或上拋
            if not is_transient_network_error(exc):
                raise
            if should_give_up is not None and should_give_up():
                raise
            attempt += 1
            (on_wait or _default_on_wait)(what, attempt, str(exc)[:120], delay)
            time.sleep(delay)
            delay = min(delay * 2.0, max_delay)


def classify_login_exception(exc: Exception) -> dict[str, Any]:
    message = str(exc)
    connection_test_success = "連線測試成功" in message
    activation_pending = connection_test_success and any(
        marker in message for marker in PENDING_ACTIVATION_MARKERS
    )
    if activation_pending:
        status = "connection_test_success_pending_activation"
    elif connection_test_success:
        status = "connection_test_success"
    else:
        status = "login_failed"
    return {
        "login_success": False,
        "status": status,
        "error": message,
        "connection_reached_broker": connection_test_success,
        "connection_test_success": connection_test_success,
        "activation_pending": activation_pending,
        "placed_order": False,
    }


@dataclass(frozen=True)
class FubonCredentials:
    person_id: str
    api_key: str | None
    cert_path: Path
    cert_password: str | None
    login_password: str | None = None
    account_index: int = 0

    @classmethod
    def from_env(cls, env_path: Path = DEFAULT_ENV_PATH) -> "FubonCredentials":
        load_env_file(env_path)
        missing = [
            key
            for key in ("FUBON_PERSON_ID", "FUBON_CERT_PATH")
            if not os.environ.get(key)
        ]
        if missing:
            raise ValueError(f"Missing Fubon environment variables: {', '.join(missing)}")

        return cls(
            person_id=os.environ["FUBON_PERSON_ID"],
            api_key=os.environ.get("FUBON_API_KEY") or None,
            cert_path=Path(os.environ["FUBON_CERT_PATH"]).expanduser(),
            cert_password=os.environ.get("FUBON_CERT_PASSWORD") or None,
            login_password=os.environ.get("FUBON_LOGIN_PASSWORD") or None,
            account_index=int(os.environ.get("FUBON_ACCOUNT_INDEX", "0")),
        )


@dataclass(frozen=True)
class StockOrderRequest:
    symbol: str
    side: str
    quantity: int
    price_type: str = "Limit"
    market_type: str = "Common"
    time_in_force: str = "ROD"
    order_type: str = "Stock"
    price: str | None = None
    user_def: str | None = None


class _SessionExpired(RuntimeError):
    """券商回「Not Login」——重登後重試安全(該呼叫確定沒送達)。"""


class FubonBroker:
    def __init__(self, credentials: FubonCredentials | None = None, *, dry_run: bool = True):
        self.credentials = credentials
        self.dry_run = dry_run
        self.sdk: Any | None = None
        self.account: Any | None = None
        # 成交即時回報的回呼:重登會換掉整個 sdk 物件(session 換新),舊註冊
        # 隨之失效,故由 broker 保存並在每次登入後自動重新註冊。
        self._on_filled: Callable[..., None] | None = None
        # session 換新通知:行情層(MarketHub)訂閱它,重登後立刻重建 marketdata
        # ——重登會讓 sdk.marketdata 消失(它只在 init_realtime() 掛上),不重建
        # 就會變成「活著但拿凍結報價下單」的殭屍(2026-07-15 審計)。
        self._on_session_replaced: Callable[[], None] | None = None
        self._login_lock = threading.RLock()
        self._login_ts = 0.0

    @classmethod
    def from_env(cls, env_path: Path = DEFAULT_ENV_PATH) -> "FubonBroker":
        load_env_file(env_path)
        dry_run = os.environ.get("FUBON_DRY_RUN", "true").lower() not in {"0", "false", "no"}
        return cls(FubonCredentials.from_env(env_path), dry_run=dry_run)

    def login(self, method: str | None = None, *,
              should_give_up: Callable[[], bool] | None = None) -> Any:
        """登入(網路韌性):暫時性網路故障 → 等網路回來重試,絕不讓呼叫端死掉。

        重登會建立全新 sdk,故登入成功後自動重新註冊 on_filled 回呼。
        憑證/權限錯誤照常拋(重試無用)。
        """
        with self._login_lock:  # 併發多腿:序列化重登,避免互相抽換 sdk
            if time.time() - self._login_ts < 5.0 and self.account is not None:
                return self.account  # 別的執行緒剛換好 session,不必再登
            acct = net_retry(lambda: self._login_once(method),
                             what="富邦登入", should_give_up=should_give_up)
            self._login_ts = time.time()
            return acct

    def _login_once(self, method: str | None = None) -> Any:
        if self.credentials is None:
            self.credentials = FubonCredentials.from_env()

        from fubon_neo.sdk import FubonSDK

        # 注意:建構子本身就會做 DNS/連線,沒網路即在此拋 IO error(2026-07-15
        # 事故點)——故整段包在 net_retry 裡。
        self.sdk = FubonSDK()
        login_method = (method or os.environ.get("FUBON_LOGIN_METHOD") or "apikey").lower()
        if login_method == "apikey":
            if not self.credentials.api_key:
                raise ValueError("FUBON_API_KEY is required for apikey login.")
            result = self.sdk.apikey_login(
                self.credentials.person_id,
                self.credentials.api_key,
                str(self.credentials.cert_path),
                self.credentials.cert_password,
            )
        elif login_method == "password":
            login_password = self.credentials.login_password or self.credentials.cert_password
            if not login_password:
                raise ValueError("FUBON_LOGIN_PASSWORD is required for password login.")
            result = self.sdk.login(
                self.credentials.person_id,
                login_password,
                str(self.credentials.cert_path),
                self.credentials.cert_password,
            )
        else:
            raise ValueError(f"Unsupported Fubon login method: {login_method}")
        if not getattr(result, "is_success", False):
            raise RuntimeError(f"Fubon login failed: {getattr(result, 'message', None)}")

        accounts = list(getattr(result, "data", []) or [])
        if not accounts:
            raise RuntimeError("Fubon login succeeded but returned no accounts")
        self.account = accounts[self.credentials.account_index]
        self._register_on_filled()  # 新 session → 重新註冊成交推播
        if self._on_session_replaced is not None:  # 新 session → 行情層重建
            try:
                self._on_session_replaced()
            except Exception as exc:  # noqa: BLE001 - 通知失敗不得擋住登入
                print(f"[net] session 換新通知失敗:{str(exc)[:80]}")
        return self.account

    def set_on_session_replaced(self, callback: Callable[[], None]) -> None:
        """登入/重登成功(sdk 換新)後的通知:行情層據此重建訂閱。"""
        self._on_session_replaced = callback

    def set_on_filled(self, callback: Callable[..., None]) -> None:
        """註冊成交即時回報(推播即刻喚醒執行器;輪詢降為備援節拍)。
        回呼由 broker 保存,重登後自動重新註冊。"""
        self._on_filled = callback
        self._register_on_filled()

    def _register_on_filled(self) -> None:
        if self._on_filled is None or self.sdk is None:
            return
        try:
            self.sdk.set_on_filled(self._on_filled)
        except Exception as exc:  # noqa: BLE001 - 無此路徑則退回輪詢,不影響交易
            print(f"[fill-push] 成交推播註冊失敗({str(exc)[:80]}),退回輪詢節拍")

    def _call_with_relogin(self, fn, *, what: str = "API 呼叫", retry: bool = True) -> Any:
        """session/網路韌性外殼。

        兩種失效:(a) session 閒置過期(2026-07-13 事故:等開盤 3 小時後查庫存
        吃到 Not Login Error 炸死)→ 重登再試一次(被拒的呼叫沒有送達,安全);
        (b) 暫時性網路故障(2026-07-15 事故:開盤瞬間斷網)→ `retry=True` 時
        無限退避等網路回來(唯讀/冪等呼叫才可以);**送單必須 retry=False**——
        「送出後才斷線」時委託可能已到交易所,自動重送 = 重複下單(真金白銀),
        該由執行器先對帳在途委託再決定。
        """
        def once() -> Any:
            res = fn()
            msg = str(getattr(res, "message", "") or "")
            if not getattr(res, "is_success", True) and "not login" in msg.lower():
                raise _SessionExpired(msg)
            return res

        def attempt() -> Any:
            try:
                return once()
            except _SessionExpired:
                self.login()
                return once()

        if not retry:
            return attempt()
        return net_retry(attempt, what=what)

    def get_order_results(self) -> Any:
        if self.sdk is None or self.account is None:
            self.login()
        return self._call_with_relogin(
            lambda: self.sdk.stock.get_order_results(self.account), what="查委託回報")

    def cancel_order(self, order: Any) -> Any:
        """撤單(網路韌性)。重試安全:撤單 idempotent,重複撤已終態單只是無效呼叫。"""
        if self.sdk is None or self.account is None:
            self.login()
        return self._call_with_relogin(
            lambda: self.sdk.stock.cancel_order(self.account, order), what="撤單")

    def modify_price(self, order: Any, price: str) -> Any:
        """改價(整股;盤中零股不得改價 → 呼叫端走刪單重掛)。"""
        if self.sdk is None or self.account is None:
            self.login()

        def _do():
            obj = self.sdk.stock.make_modify_price_obj(order, price)
            return self.sdk.stock.modify_price(self.account, obj)

        return self._call_with_relogin(_do, what="改價")

    def get_filled_history(self, start_date: str | None = None, end_date: str | None = None) -> Any:
        if self.sdk is None or self.account is None:
            self.login()
        return self._call_with_relogin(
            lambda: self.sdk.stock.filled_history(self.account, start_date, end_date),
            what="查歷史成交")

    def get_bank_remain(self) -> Any:
        if self.sdk is None or self.account is None:
            self.login()
        return self._call_with_relogin(
            lambda: self.sdk.accounting.bank_remain(self.account), what="查銀行餘額")

    def get_inventories(self) -> Any:
        if self.sdk is None or self.account is None:
            self.login()
        return self._call_with_relogin(
            lambda: self.sdk.accounting.inventories(self.account), what="查庫存")

    def build_stock_order(self, request: StockOrderRequest) -> Any:
        from fubon_neo.constant import BSAction, MarketType, OrderType, PriceType, TimeInForce
        from fubon_neo.sdk import Order

        return Order(
            buy_sell=getattr(BSAction, request.side),
            symbol=request.symbol,
            quantity=request.quantity,
            market_type=getattr(MarketType, request.market_type),
            price_type=getattr(PriceType, request.price_type),
            time_in_force=getattr(TimeInForce, request.time_in_force),
            order_type=getattr(OrderType, request.order_type),
            price=request.price,
            user_def=request.user_def,
        )

    def place_stock_order(self, request: StockOrderRequest) -> Any:
        order = self.build_stock_order(request)
        if self.dry_run:
            return {
                "dry_run": True,
                "symbol": request.symbol,
                "side": request.side,
                "quantity": request.quantity,
                "market_type": request.market_type,
                "price_type": request.price_type,
                "time_in_force": request.time_in_force,
                "order_type": request.order_type,
                "price": request.price,
                "user_def": request.user_def,
            }

        if self.sdk is None or self.account is None:
            self.login()
        # Not Login / 網路中斷重試安全:被拒的送單沒有進交易所。但「送出後
        # 才斷線」無法從這裡分辨——呼叫端(執行器)在網路復原時必須先對帳
        # 在途委託再決定是否重掛(engine._resync_working_from_broker)。
        return self._call_with_relogin(
            lambda: self.sdk.stock.place_order(self.account, order),
            what="送單", retry=False)


def redacted_account(account: Any) -> dict[str, Any]:
    def mask(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value)
        if len(text) <= 4:
            return "*" * len(text)
        return f"{text[:2]}***{text[-2:]}"

    return {
        "branch_no": mask(getattr(account, "branch_no", None)),
        "account": mask(getattr(account, "account", None)),
        "account_type": str(getattr(account, "account_type", None)),
    }
