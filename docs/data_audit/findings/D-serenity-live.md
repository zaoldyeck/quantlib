# D-serenity-live — Serenity 引擎 + 出場規則(LIVE)算法學理稽核

**範圍**:`research/serenity/engine.py`(回測引擎，計分/模擬/退場真源)+
`research/trading/exit_replay.py`(live 逐日重放退場檢查)。
連帶讀了兩支的直接依賴以判定計算式正確性:`research/serenity/exit_rules.py`
(六道門單一真源)、`research/serenity/replay_2025.py`(`load_price_features` /
`score_candidates` / `revenue_report_date`)、`research/serenity/daily.py`(live 執行器
`market_data`,判定 blast radius)。

**LIVE champion** = `ev_v3_wf`:`ExitRules(take_profit=0.40, trail=0.25, abs_stop=0.15,
time_days=30, thesis_stop=True)`、`thesis_mode="inst_neg"`、`regime_guard=True`、
`max_new_per_day=3`、`adv_cap=0.20`、`weight_mode="equal"`、`tp_mode="fixed"`。
live 退場真源 = `exit_rules.evaluate_exit`(daily.py 與 exit_replay.py 都 import)。

**一句話**:退場的閾值套用、峰值高水位重算、逐日重放的 T+1 紀律、月營收 PIT
(次月 10 日可用)這些全對得上學理;但有一個會讓 live 判決失真的真 bug——
**exit_replay(以及 live daily.py)用「未還原」原始收盤價評估 trailing/絕對停損,
而設定並驗證這些門檻(abs 15% / trail 25%)的回測引擎用「還原」價**;股票一遇
除權息,原始價機械性跳空(實測 2024–2025 流動股 −8.8%~−46%),就會在「其實沒
賠錢」時假觸發停損。另有一個 live 比驗證版嚴格的分岔(live 多一道 `yoy_3m<0`
退場門、且法人門天天判 vs 回測月度判),列為 SUSPECT。

---

## BUG 1 — trailing / 絕對停損用「未還原原始價」,回測用「還原價」(除權息假觸發)

**指標**:trailing stop / 絕對停損的價格基準 —— `research/trading/exit_replay.py:77-82`
(`SELECT ... closing_price FROM daily_quote`)、`:152-159`(`cum_max(closing_price)` +
`peak_floor`);LIVE 同病:`research/serenity/daily.py:341,383,389`(`market_data` 取
`closing_price`、`peak=max(stored,px)`、`evaluate_exit(px=raw...)`)。對照的回測引擎
`research/serenity/engine.py:189,207,449,635-638` 走 `load_price_features` 的 **還原**
`close`(`fetch_adjusted_panel`)。

**學理定義**:路徑相依的價格停損(trailing stop、stop-loss)衡量的是「投資部位
價值」自峰值/成本的回落,必須用**總報酬還原價(adjusted / total-return price)**評估,
或在除權息日**同步下調停損錨與峰值**;否則除權息造成的**機械性除息缺口**會在
沒有任何經濟損失時觸發停損(股東拿到現金股利/股票股利,價值未減)。Serenity 的
`abs 15% / trail 25% / tp 40%` 是在 `engine.py` 的**還原**價空間裡設定並經 walk-forward
驗證的——live 若換到未還原價空間,量到的就不是同一回事。

**程式實作**:`exit_replay.load_paths` 直接讀 `daily_quote.closing_price`(原始收盤,
未接 `ex_right_dividend`);`replay` 以 `cum_max(closing_price)` 當 peak、`peak_floor`
=原始成交價,對這條**原始**價序套 `evaluate_exit`(abs 15% / trail 25%)。engine.py
的模擬與退場全在**還原** `close` 空間(`ret_1d`、`peak_close`、`entry_close` 皆還原)。
兩者只在「持有窗內發生除權息」時分岔——無公司行為時還原比 = 原始比,判決相同;
一遇除息,還原價無缺口、原始價有缺口。

**偏差證據(可重現)**:cache `ex_right_dividend` × `daily_quote`,2024-06~2025-09
流動名單除息日「除息前一日收盤 → 除息日收盤」的**原始**跳空:

| code | ex_date | cash_div | close_prev | close_ex | raw 跳空 |
|---|---|---|---|---|---|
| 7734 錸寶 | 2025-07-29 | 294.92 | 1360.00 | 1035.00 | **−23.90%** |
| 6231 系微 | 2024-07-11 | 89.25 | 523.00 | 477.00 | −8.80% |
| 5278 尚凡 | 2024-08-09 | 63.28 | 242.00 | 183.50 | −24.17% |
| 6577 富邦媒… | 2024-08-01 | 65.99 | 180.00 | 105.00 | −41.67% |
| 3293 鈊象 | 2024-07-24 | 750.00 | 1465.00 | 786.00 | −46.35% |

持有跨除息日的 lot:原始 px 機械下跌 23.9%(7734)> `abs_stop` 15% → 立刻假觸發
「abs_stop」;−8.8%(6231)雖不單獨破 15%,疊上任何正常波動就逼近 `trail` 25%
的峰值回落線。**還原面板(engine)無此缺口 → 回測不會出場**。即 live 退場判決與
「設定門檻的那份回測」不同源、系統性偏向「除權息季提早砍出場」。重現指令:
`uv run --project research python -c` 對 cache 跑上表 SQL(見稽核 transcript)。
台股除息旺季 6–8 月,正是 Serenity 想抱夏季贏家的窗口 → 觸發條件現實且反覆。

**修法**:`exit_replay.load_paths` 改用 `prices.fetch_adjusted_panel` 的**還原** close
(與 engine 同源),peak/px 一律還原;`peak_floor` 亦換算到還原空間。若要保留
「螢幕原始價」呈現,則於 `ex_right_dividend` 除權息日**把 anchor 與 peak 依除權息
參考價同步下調**(等價於改用總報酬序列),讓停損追蹤總報酬而非機械缺口。live
`daily.py:market_data` 同步改。並補一支 parity test:對「持有跨除息日」的合成 lot,
斷言 exit_replay 判決與 engine 還原價判決逐位一致。

---

## SUSPECT 2 — live 退場多一道 `yoy_3m<0` 門、且法人門天天判(比驗證版嚴)

**指標**:訊號(thesis)退場門 —— `research/serenity/exit_rules.py:44-47`
(第 5 門 `inst20<0 且 px<anchor`、第 6 門 `yoy3<0`)對照 `research/serenity/engine.py:592-598`
(champion `thesis_mode="inst_neg"` 分支)+ `:568-606`(thesis 只在 refresh 日評估)。

**學理定義**:專案「引擎唯一真源鐵律」(CLAUDE.md)——live 退場邏輯必須**逐位
重現**驗證過的回測 champion;正當的 live-vs-backtest 差異須明列來源。champion
`ev_v3_wf` 的 thesis 規格 = `inst_neg`。

**程式實作**:`evaluate_exit`(live,每日被 daily.py / exit_replay 呼叫)有**六**門,
其中第 6 門 `if yoy3 is not None and yoy3 < 0: return "thesis(yoy_3m<0)"` 為**無條件**
(不看 px、不看 inst)退場。engine 的 `inst_neg` 走 `else` 分支**只**評 `inst<0 且
px<entry_close`,`yoy3m_neg` 那條(`ok_map`,即 `yoy3<0`)被跳過;且 engine 的
thesis 判定**只發生在月度 refresh 日**,live 則**每天**判。

**偏差證據**:`engine.py:570-575` `if variant.thesis_mode == "yoy3m_neg" or not
latest_thesis:` 為假(champion=`inst_neg` 且 `latest_thesis` 非空)→ 走 `:576` `else`
→ `:592` 僅 `inst_neg` fire;`yoy3<0` 那條在 champion 路徑不存在。故 `exit_rules.py`
第 6 門是**任何回測 variant 都沒有的額外門**(engine 無「inst 且 yoy3 複合」的
single variant)。加上 live 法人門天天判、回測月度判 → live 的 thesis 退場**更多、
更早**觸發:一個 `yoy3<0` 但 `inst≥0`/`px≥anchor` 的部位,live 會賣、驗證回測會抱。
`research/serenity/tests/` 只有 `test_execution.py`,**無 parity test 鎖死** live↔engine
退場等價(對照 Evergreen 有 `test_engine_parity.py`)。方向上 live 較保守(較嚴),
非放大風險,但「部署的策略 ≠ 驗證的策略」,KPI 不能代表 live 行為。

**修法**:二擇一並補守護——(a)在 engine 新增「`inst_neg` + `yoy3<0`」複合
`thesis_mode`、以 daily-cadence 近似(或明列為 live-only 正當差異)重跑 walk-forward
/ DSR / permutation 驗證,通過後 live 才保留第 6 門;(b)移除 live 第 6 門、法人門
改對齊回測評估時點,使 `evaluate_exit` 與 `ev_v3_wf` 逐位一致。無論哪條,補
`test_engine_parity` 對同一 lot 序列斷言 live 六門與 engine champion 判決一致。

---

## SUSPECT 3 — `atr20_pct` 命名為 ATR 但非 Wilder ATR(缺跳空項;僅非 live 變體用)

**指標**:`atr20_pct` —— `research/serenity/replay_2025.py:196-199`,經 engine.py
`weight_mode="inv_atr"`(變體 `ev_v2_watr`,`engine.py:698-706`)使用。

**學理定義**:Wilder ATR(1978)= 14 期 True Range 的 Wilder 平滑;
`TR = max(H−L, |H−C_prev|, |L−C_prev|)`,**含跳空**(前收)兩項,平滑用 Wilder RMA
(非簡單平均)。

**程式實作**:`(high − low).rolling_mean(20).shift(1) / close`——只用當日
`H−L` 的 **20 日簡單平均 / close**,**缺** `|H−C_prev|`、`|L−C_prev|` 兩項、非 Wilder
平滑、期數 20 非 14。跳空/開盤缺口的股票其真實波動被系統性低估。

**偏差證據**:定義缺兩個 TR 分量 → 對有隔夜跳空的標的 `atr20_pct` < 真 ATR。
但僅 `weight_mode="inv_atr"`(`ev_v2_watr`)用它做反波動加權;**live champion
`ev_v3_wf` 用 `weight_mode="equal"`,完全不觸及** → 對現役策略無影響。屬命名/
定義失真(以 ATR 之名行 avg-range 之實),非 live money-path。

**修法**:要真 ATR 就改用 `True Range`(含前收兩項)+ Wilder 平滑並校期數;
若只需平均振幅代理,更名為 `avg_range20_pct` 以免誤稱 ATR。

---

## SUSPECT 4 — `peg_target_mult` 以「營收成長」冒充 PEG 的盈餘成長(僅非 live 變體用)

**指標**:`peg_target_mult` 動態止盈倍數 —— `research/serenity/engine.py:394-400`,
`tp_mode` in {`peg_target`,`peg_exit`}(變體 `ev_v2_tpdyn`/`ev_v2_pegexit`)使用。

**學理定義**:PEG = P/E ÷ **EPS 成長率(%)**;用「盈餘」成長,非營收。「PEG=1
隱含上檔 = growth/pe」本身是啟發式,非標準估值公式。

**程式實作**:`np.clip(growth / pe, 1.2, 3.0)`,其中 `growth` = `yoy_3m`(**三月營收
YoY**,非 EPS 成長)、`pe` = `price_to_earning_ratio`。以營收成長替代盈餘成長,且
上檔推導非教科書。

**偏差證據**:`valuation_by_refresh` 餵入的 growth 來自 `rev_day.yoy_3m`(營收);
高毛利/高稅負股的營收成長 ≠ EPS 成長,倍數會偏。但僅 `ev_v2_tpdyn`/`ev_v2_pegexit`
使用;**live `ev_v3_wf` 用 `tp_mode="fixed"`(固定 tp 0.40),不觸及** → 對現役策略
無影響。

**修法**:要 PEG 語義改用 EPS 成長率;或明確更名為啟發式目標倍數並註明「非 PEG、
用營收成長」。低優先(非 live)。

---

## OK(核對通過,列出以示覆蓋)

- **退場閾值套用逐位對得上學理 + 回測↔live 一致**(除上述第 6 門):
  abs `px≤anchor·(1−0.15)`、trail `px≤peak·(1−0.25)`、tp `px≥anchor·(1+0.40)`、
  time `days_held≥30 且 px≤anchor·(1−0.01)`——`exit_rules.py:36-43` 與
  `engine.py:635-652` 常數/不等式一致;符合 stop-loss / trailing-stop / 目標價的
  教科書形式。`effective_trail` 在 `theme_risk_off` 收緊為 `min(trail,0.15)`
  (`engine.py:622-624`)為文件化防禦參數。
- **峰值高水位重算正確**:`exit_replay.replay:152-156` 以 `cum_max(closing_price)`
  **自 entry_day 起**算 peak、`cum_min` 算 trough,且每次 join 後**重新 sort**
  (`:128-134` 防 polars join 不保序;2408/6446 實測錨定),`peak_floor`=成交價
  (`:157-159`,對應回測 `peak_close=entry_close` 的忠實下限)——符合 trailing 錨
  定義與 Exit Semantics Contract。engine.py 的 `peak_close` 亦為逐日還原 close 的
  running max(`:449`)。
- **逐日重放 T+1 紀律 / 無前視**:engine 每日先 mark-to-market(`ret_1d`)、次日
  收盤執行昨日排定的進出場(`:457-513`),訊號 T 收盤生成、T+1 收盤成交;
  exit_replay 進場日當天不評估(`day>entry_day`,`:175`),`days_held` 為交易日
  計數,與 engine `idx−entry_idx` 對齊。
- **月營收 PIT 對齊正確**:可用日 = **次月 10 日**(`replay_2025.revenue_report_date`
  = `month_add(...,1)` 的 10 日;`exit_replay.load_paths:108-109` `legal_avail`
  = `date(year+month//12, month%12+1, 10)`,月份進位已驗:12 月→次年 1 月)。
  首見日優先(`coalesce(first_seen, legal_avail)`,`:110-114`),event-driven 可用;
  `build_refresh_days`(`engine.py:283-293`)取 ≥11 日首個交易日,PIT 安全。
  `fresh_days`=`(date−avail)` 為**日曆**天(`:134`),與 `days_held`(交易日)單位
  不同但各自文件化;serenity live 門不用 `fresh_days`,無混用風險。
- **mark-to-market 價空間自洽**:engine 的 `close`/`ret_1d`/`peak_close`/`entry_close`
  全在同一還原價空間(`ret_1d=close/close.shift1−1`,`replay_2025.py:189`),除息
  不造成假報酬;`drawdown_252 = close/rolling_max(252)−1`(`:195`)符合回撤定義;
  `adv20 = trade_value.rolling_mean(20).shift(1)`(`:194`)有 shift(1),無當日前視;
  `inst_20d = rolling_sum(20,min5)` 正確。
- **市場 regime 門無前視**:`market_risk_off` = 0050 **還原** `adj_close < MA120`
  (`engine.py:1058-1065`,`rolling(120).mean()` 為含當日的尾端 SMA,標準技術定義,
  只用過去+當日);僅作 guard B 進場閘與 `regime_exit`(champion 不啟用 regime_exit)。
- **bootstrap 年化正確**:`_boot_p5`(`engine.py:1233-1249`)月報酬取月末 NAV
  `pct_change`,circular block bootstrap(block=6、`%n` 環繞),每樣本幾何年化
  `prod(1+r)^(12/n)−1`,取第 5 百分位=90% 單邊下界,學理正確(僅 `--sweep`
  研究用,不進 live 決策)。
- **成本會計**:`FEE_BUY=COMMISSION+0.0005`、`FEE_SELL=COMMISSION+SELL_TAX+0.0005`
  (`engine.py:77-78`,comm 0.0285% + 賣稅 0.3% + 各側 5bps 滑價),進出場對稱套用
  (`:462,504`),round-trip ~0.457%——文件化的摩擦假設,非學理偏差。turnover
  = `total_traded/CAPITAL` 為文件化近似。
- **s_rule / evergreen_rule**(`exit_replay.py:196-221`):分別對應 apex_revcycle_S
  與 Evergreen 各自規格書常數(trail 35%→`peak·0.65`、time 30/輸家 15、EV43 refit
  參數),各自正確;為與 Serenity 共放同一重放模組的**別策略**規則,不影響
  Serenity champion 判決。

## 連帶觀察(非本單位兩檔,提示主流程)

- `research/serenity/daily.py`(live 執行器,**不在本單位範圍**)的 live book 用
  **增量** peak(`:383` `max(stored_peak, px)`)+ **當日快照**評估(`:389`),這正是
  Exit Semantics Contract 要 exit_replay 取代的形態;若 launchd 漏跑幾天,峰值偏低、
  漏觸發的出場會被當沒發生。exit_replay(本單位)已正確重算峰值+重放,但它目前
  掛在 `tri.daily`(唯讀決策支援),並未閘住 live 送單。建議主流程確認 live 執行
  路徑是否應改由 exit_replay 的重放結果驅動,而非 daily.py 的增量快照。
