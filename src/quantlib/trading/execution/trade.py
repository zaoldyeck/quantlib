"""買賣混合一行指令:--buy 與 --sell 各收逗號多檔,全部腿併發執行。

買腿預設 price-first(結構錨撈低點)、賣腿預設 urgency=exit(結構錨撈高點,
護欄 −3%);兩側整場按策略撈價、盤中永不因時間跨價,收盤未竟自動盤後掛
收盤價收尾(14:30 撮合=收盤價);持續監控、成交即印剩餘計劃、完成自行終止。何時啟動都行:盤前啟動自動等開盤、
盤中啟動立即執行。

  FUBON_DRY_RUN=false \
  uv run --project . python -m quantlib.trading.execution.trade \
      --buy "2408:2,3006:5" --sell "4973,5289" --live
"""

from ._cli import run

if __name__ == "__main__":
    run("Trade")
