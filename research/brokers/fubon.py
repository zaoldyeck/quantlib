"""Fubon Neo API adapter.

This module deliberately keeps live-order submission behind an explicit
`dry_run=False` switch. Strategy code should produce target positions first;
the broker adapter only translates approved orders into Fubon SDK calls.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
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


class FubonBroker:
    def __init__(self, credentials: FubonCredentials | None = None, *, dry_run: bool = True):
        self.credentials = credentials
        self.dry_run = dry_run
        self.sdk: Any | None = None
        self.account: Any | None = None

    @classmethod
    def from_env(cls, env_path: Path = DEFAULT_ENV_PATH) -> "FubonBroker":
        load_env_file(env_path)
        dry_run = os.environ.get("FUBON_DRY_RUN", "true").lower() not in {"0", "false", "no"}
        return cls(FubonCredentials.from_env(env_path), dry_run=dry_run)

    def login(self, method: str | None = None) -> Any:
        if self.credentials is None:
            self.credentials = FubonCredentials.from_env()

        from fubon_neo.sdk import FubonSDK

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
        return self.account

    def _call_with_relogin(self, fn) -> Any:
        """Fubon session 閒置會過期(2026-07-13 事故:05:57 登入、等開盤 3 小時,
        09:00 查庫存吃到 Not Login Error 直接炸死,錯過整個早盤)。任何包裝呼叫
        碰到未登入錯誤 → 重登一次再重試;重試仍失敗才交給呼叫端。"""
        res = fn()
        msg = str(getattr(res, "message", "") or "")
        if not getattr(res, "is_success", True) and "not login" in msg.lower():
            self.login()
            res = fn()
        return res

    def get_order_results(self) -> Any:
        if self.sdk is None or self.account is None:
            self.login()
        return self._call_with_relogin(lambda: self.sdk.stock.get_order_results(self.account))

    def get_filled_history(self, start_date: str | None = None, end_date: str | None = None) -> Any:
        if self.sdk is None or self.account is None:
            self.login()
        return self._call_with_relogin(lambda: self.sdk.stock.filled_history(self.account, start_date, end_date))

    def get_bank_remain(self) -> Any:
        if self.sdk is None or self.account is None:
            self.login()
        return self._call_with_relogin(lambda: self.sdk.accounting.bank_remain(self.account))

    def get_inventories(self) -> Any:
        if self.sdk is None or self.account is None:
            self.login()
        return self._call_with_relogin(lambda: self.sdk.accounting.inventories(self.account))

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
        # Not Login 重試安全:失敗的送單沒有進交易所,重登後重送不會重複
        return self._call_with_relogin(lambda: self.sdk.stock.place_order(self.account, order))


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
