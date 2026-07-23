"""富邦帳戶讀取(持股 + 現金 + NAV 估算)——premarket/execute 共用。

讀取一律唯讀(`get_inventories` / `bank_remain`),與 dry-run/live 無關(只有
`place_order` 吃 `FUBON_DRY_RUN`)。持股用現成
`positions_from_fubon_inventories`(整股+零股 today_qty,權威此刻持有);NAV 用
cache 最新收盤估市值,僅供顯示/席位提示,不影響「買 1 股」決策。
"""
from __future__ import annotations


def get_broker():
    """已登入的富邦連線(唯讀查詢與條件單同步共用)。

    送單仍受 `FUBON_DRY_RUN` 管制(在 broker 內部),本函式只負責建立連線。
    """
    from quantlib.brokers.fubon import FubonBroker

    broker = FubonBroker.from_env()
    broker.login()
    return broker


def get_holdings_cash() -> tuple[dict[str, float], float]:
    """(持股 dict[code, 股數], 可用現金 TWD)。查詢失敗照既有慣例:現金以 0 計。"""
    from quantlib.brokers.fubon import FubonBroker
    from quantlib.trading.portfolio import (
        available_balance_from_fubon_bank_remain,
        positions_from_fubon_inventories,
    )

    broker = FubonBroker.from_env()
    holdings = {str(k): float(v)
                for k, v in positions_from_fubon_inventories(broker.get_inventories()).items()}
    cash = 0.0
    bank = broker.get_bank_remain()
    if getattr(bank, "is_success", False):
        try:
            cash = available_balance_from_fubon_bank_remain(bank)
        except Exception:  # noqa: BLE001 - 餘額 0/欄位缺 → 以 0 計,不擋流程
            cash = 0.0
    return holdings, cash


def estimate_nav(con, holdings: dict[str, float], cash: float) -> float:
    """NAV ≈ 現金 + 持股市值(cache 最新收盤)。純顯示用途。"""
    if not holdings:
        return cash
    from quantlib.apex import data

    d0 = data.latest_date(con)
    codes = list(holdings)
    ph = ",".join("?" * len(codes))
    px = dict(con.execute(
        f"SELECT company_code, closing_price FROM daily_quote "
        f"WHERE date = ? AND company_code IN ({ph})", [d0, *codes]).fetchall())
    return cash + sum(holdings[c] * (px.get(c) or 0.0) for c in holdings)
