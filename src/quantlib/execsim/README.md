# execsim — 回測與研究用的**成交模擬**

與 `src/quantlib/trading/execution/` 嚴格分開,兩者過去同名(`execution`)造成持續誤 import:

| | 這裡 `src/quantlib/execsim/` | `src/quantlib/trading/execution/` |
|---|---|---|
| 做什麼 | **模擬**成交:費率、稅、滑價、部分成交 | **真的**送單到富邦 |
| 用在哪 | 回測、KPI 計算、事後 TCA 對照 | 每日盤中執行 |
| 弄錯的後果 | 回測成本假設失真 | **下真錢單** |

- `broker_fee.py` — 費率 schedule(**唯一真源**;1.8 折、月成交額 100 萬以上 4 折、
  證交稅 0.3%、整股最低 20 元)。計劃信的金額試算也從這裡取,不得另立常數。
- `execution_simulator.py` — 逐筆成交模擬(滑價、部分成交)。

零股最低手續費 1 元屬**帳戶層**規則,定義在 `src/quantlib/trading/live/money.py`
(它是 live 交割試算的一部分,不影響回測的整股假設)。
