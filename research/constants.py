"""Project-wide constants — 集中所有 magic numbers 避免散落各 script。

任何要改交易成本 / 評估窗口 / 標準參數的地方都改這裡，下游 import。
歷史上這些常數散在 ~30 個 .py 檔，未來改費率時容易漏改造成 bug。
"""
from datetime import date

# === Trading costs (使用者實際券商: 國泰/富邦/永豐 e-trading 2-折) ===
COMMISSION: float = 0.000285        # buy/sell 各一次 (0.0285%)
SELL_TAX: float = 0.003              # 賣方證交稅 (0.3%)
ROUND_TRIP_COST: float = SELL_TAX + 2 * COMMISSION  # ~0.357%

# === Time constants ===
TDPY: int = 252                      # trading days per year
START: date = date(2005, 1, 3)       # full-window backtest start (21y baseline)
END: date = date(2026, 4, 25)        # full-window backtest end

# === Capital ===
CAPITAL: float = 1_000_000.0         # default starting NAV (NT$1M)

# === Risk-free rate (for Sharpe / Sortino) ===
RF: float = 0.01                     # 年化 1% (台灣定存 proxy)

# === Strategy hyperparameters (ship-default; can be overridden per script) ===
DEFAULT_TRAILING_STOP: float = 0.15  # iter_24 default fixed -15%
DEFAULT_ATR_MULTIPLIER: float = 3.0  # iter_24 ATR-relative trailing
DEFAULT_ATR_LOOKBACK: int = 14       # 標準 ATR(14)
DEFAULT_TRAIL_PCT_MIN: float = 0.10
DEFAULT_TRAIL_PCT_MAX: float = 0.25

# === Universe filters ===
MIN_ADV_NTD: float = 50_000_000      # 60d ADV 最低門檻 (NT$50M)
ELECTRONICS_INDUSTRIES: tuple[str, ...] = (
    "半導體業", "電子零組件業", "光電業", "電腦及週邊設備業",
    "通信網路業", "電子通路業", "其他電子業", "資訊服務業",
)
