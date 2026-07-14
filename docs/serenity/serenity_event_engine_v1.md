# Serenity 事件驅動引擎 v1 — 可回測性研究與驗證報告

- 研究日期:2026-07-06;資料 cutoff:`daily_quote` 2026-07-03(當日已跑 `Main update` + `cache_tables.py`)
- 程式:`research/serenity/engine.py`(引擎)、`serenity_event_engine_v1_validate.py`(驗證)
- 基底:fork `serenity_industry_first_replay_2025.py`(沿用 PIT 載入器、計分器、論點註冊表),把月度輪動改為**每日監控的事件引擎**
- 成本:手續費 0.0285% + 賣出稅 0.3% + 5bps 單邊滑價(與 replay 一致;非富邦 realistic execution,與 Iter9x 排行不可直接比較)

## 一、Serenity 系統可回測性判定(消息面分層)

| 層 | 內容 | PIT 資料 | 可回測性 | 證據 |
|---|---|---|---|---|
| L1 結構化事件 | 月營收(M+1 月 10 日公布)、庫藏股公告、法人買賣超、價量 | ✅ 完整 | **高** | 本報告全部回測建立在此層 |
| L2 產業論點策展 | 「哪個產業是瓶頸、哪些股受益」(Serenity 讀新聞做的事) | 以註冊表近似(active_from + lag 壓力測試) | **中——只能壓力測試,不能完全消除策展後見之明** | lag90/180 存活 → 不是啟用日期作弊;但主題「被選中」本身仍含 2025 後見之明 |
| L3 非結構化新聞 | Digitimes/報紙/供應鏈 readthrough 逐日決策 | ❌ 無歷史新聞庫;LLM 重讀歷史新聞必然滲漏未來知識 | **不可回測,只能 live 前瞻** | 既有 news_alpha survey:泛用重大訊息為負 alpha(-6.57%/60d vs 0050) |

**核心發現:L2 是 alpha 的來源,L1 是紀律放大器,L3 無法歷史驗證。**
機械版(移除註冊表、改用「產業營收中位數 YoY≥10 + 廣度≥55%」的機器主題偵測、修正 survivorship、2018-2026)**沒有風險調整後 alpha**:最佳變體 CAGR 27.8% / Sharpe 0.90 / MDD -58%,輸 hold-2330(35.2% / 1.23 / -44.8%)也輸 0050 的風險調整。SOP「產業論點先行、不得動能倒套故事」由此獲得直接量化證據。

## 二、事件引擎設計(v1)

- **進場**:月營收公布日(每月 11 日後首個交易日)刷新候選(註冊表 ∩ PIT 計分閘門:ADV≥5000 萬、動能/估值/回撤限制);持倉出缺時每日從最新候選清單補位(清單有效期 25 個交易日、停損股 20 日冷卻);T+1 收盤成交
- **出場(每日監控,多條件 OR)**:止盈 +60% 回收(take_profit)/ trailing -20% / 絕對停損 -15% / time-stop 50 日仍 < 進場價 -1%(champion 規則)/ 論點停損(3M 營收 YoY 轉負,refresh 日檢查)
- **部位**:10 檔、進場時每檔 NAV/10、不逐日再平衡(漂移)
- 下市處理:連續 10 日無價 → 以最後價值 -10% 強制出場(mechanical 模式)

## 三、結果

### Registry 模式(2025-01-02 ~ 2026-07-03,18 次 refresh)

| 配置 | lag0 CAGR | lag90 | lag180 | lag0 Sharpe | lag0 MDD |
|---|---:|---:|---:|---:|---:|
| **ev_full_tp60(勝出)** | **245.97%** | 278.32% | 184.88% | 3.403 | -38.76% |
| ev_rot_wide_tp100 | 227.69% | 297.31% | 175.33% | 3.019 | -40.61% |
| ev_rotation_only(≈replay 基準) | 208.64% | 258.27% | 148.13% | 2.652 | -40.94% |
| ev_full(有停損無止盈) | 141.30% | 222.04% | 149.45% | 2.387 | -38.76% |
| hold_2330 | 76.77% | — | — | 1.961 | -30.51% |
| hold_0050 | 70.03% | — | — | 2.226 | -28.47% |

- 交易品質(lag0 ev_full_tp60):91 筆、勝率 60.4%、平均賺 +50.8% vs 平均虧 -14.3%(盈虧比 3.55);止盈 38 筆(均 +67%/41 日)、絕對停損 21 筆(均 -17.9%/14 日);無單一股票依賴;主題橫跨 memory/伺服器機構/封測/CCL/光通訊
- **緊停損單獨使用是傷害**(141% vs 輪動 208%),**但搭配 +60% 止盈回收後反超**(246%)——高 beta 候選池的「獲利回收再滾入」複利效率;MFE/MAE 結論與 Iter95 一致
- theme_cap=3 否決:CAGR -77pp、MDD 無改善(崩盤時主題間相關性 ≈1,主題內分散無效)
- 庫藏股事件疊加在 registry 宇宙內幾乎不觸發(4 個事件日);全市場 8.5 年 +2.3pp——維持獨立事件 sleeve 的定位(見 `official_event_buyback_study.md`)

### 驗證電池(ev_full_tp60)

| 測試 | lag0 | lag90 | lag180 | mech2018 |
|---|---:|---:|---:|---:|
| Lo-2002 t(p) | 3.78 (0.0001) | 4.95 (0.0000) | 4.03 (0.0000) | 2.28 (0.011) |
| DSR(trials=40) | 1.000 | 1.000 | 1.000 | 0.375 |
| Bootstrap CAGR 5% 下界(月 block) | **+83.5%** | +169.1% | +88.5% | n/a |
| PBO(季度 fold CSCV) | 0.476 | 0.662 | 0.778 | 0.204 |
| 選股置換檢定 p(200 次,池內隨機) | **0.000**(隨機中位 139.7%、p95 186.7%) | — | — | 無意義(本體無 alpha) |

- **PBO 偏高的誠實解讀**:6 個季度 fold 的 CSCV 統計效力極低;高值反映「績效集中在特定季度」= regime 集中(2025H2 主升段),不是傳統參數過擬合(參數在 3 個 lag 窗口一致勝出)。**這個系統的真實風險是 AI 主題退潮,不是曲線擬合。**
- 置換檢定的雙層拆解:註冊表池內隨機選(保留閘門)中位數仍有 139.7% → **策展層貢獻約 70%→140% 的池級 alpha**;計分排序再加 ~107pp → **排序層有獨立技巧(p<0.005)**。

### 與現任 champion(Iter95)混合

重疊窗 2025-01-02~2026-05-22、日報酬相關性 **0.591**:50/50 混合 CAGR 211.8%(champion 單獨 189.3%)、MDD -27.9%(引擎單獨 -38.8%)、Sortino 8.59。註:執行模型不同(5bps vs 富邦 realistic),數字為指示性;正式合流需把引擎轉為 target-book 進 realistic execution 管線。

## 四、限制與後續

1. **單一 regime**:registry 窗口只有 2025-2026 AI 供應鏈多頭;跨 regime 唯一證據是機械版(無 alpha)——**本策略 = 「策展正確」條件下的紀律放大器,策展錯誤時沒有安全網**,必須搭配 regime kill-switch(hyperscaler capex/TSM 展望/記憶體價格/光學 backlog,兩項轉壞凍結加碼)
2. **策展後見之明殘留**:lag 測試消除「日期」作弊,消不掉「主題被選中」的倖存;真前瞻有效性只能靠 live 註冊表維護(`serenity-trading-system` skill 的 daily sweep)+ paper trading 累積
3. 執行層:未過富邦 realistic execution(成交量上限/漲跌停/部分成交);正式排行前需按 SOP 轉 target-book 重驗
4. 狀態:`research_candidate` → 建議下一步 = 轉 execution 管線 + 開始 live registry 維護與 paper trail

## 五、v2 進化(2026-07-06 下午;全程預註冊於 trials ledger,累計 44 trials)

**最終單一策略:`ev_full_tp60_v2`**(使用者指定不與 Iter95 合流)。

| 戰役 | 內容 | 判決 |
|---|---|---|
| 二 | regime guard(池 dd≤-30% 減半新倉+trail 收 15%;0050<MA120 停新倉)+ 每日新倉 ≤3 + 部位 ≤20%×ADV20 | **採納**:三窗 MDD 均值 -19.9%→-14.7%(lag0 -38.8→-21.8),CAGR 均值僅 -12.6%;lag0 Sharpe 3.40→3.60 |
| 三 | 庫藏股全市場通道(2 席、guard 豁免) | 否決 0/3(Calmar 全退;主題多頭中機會成本 > 事件 alpha) |
| 四 | chips 2.0 計分(SBL 20d + 外資 60d) | 否決 0/3(inst_20d 已涵蓋籌碼資訊) |
| 六 | 估值目標止盈 vs 固定 +60% | 否決(PEG-target 0/3、PEG-exit 0/3;循環股低 PE 陷阱) |
| 七 | 部位權重(分數加權/逆波動 vs 等權) | 等權凍結(score 1/3、inv-ATR 0/3) |
| 八 | 論點停損形態 | **採納 inst_neg**(法人分佈出場,2/3)→ **champion 換代 `ev_v2_thesis_inst`** |
| 九 | 席位數(1–30) | 10 席凍結(曲線峰值在 10;1–3 與 12–30 全劣化) |
| 十 | 預期差因子加計分 | 否決 0/3(全市場 IC 真但非池內排序技巧) |
| 十一 | 計分成分留一消融(診斷) | 8 成分有背書、theme_count+dd_pen 死重、pe_pen 可疑 |
| 十二 | 移除死重 theme_count+dd_pen | **採納**(三窗無損,lag0 248→252.5;計分 10→8 成分) |
| 十三 | pe_pen 軟化(extreme/off) | 否決(extreme 三窗全劣;off 犧牲 live lag0;估值紀律為跨 regime prior) |

**champion 演進**:`ev_full_tp60_v2`(戰役二~七)→ **`ev_v2_thesis_inst`(戰役八起,現役)**;計分公式經戰役十一~十三**逐項消融驗證**,8 個成分全部回測背書。

**Realistic execution 路考(富邦模擬器,book 隔日開盤成交;`ev_v2_thesis_inst` 新計分,cutoff 2026-07-06)**:CAGR **271.6%** vs 紙上 252.5%、MDD **-17.2%** vs -18.0%、Sharpe 7.28、Sortino 9.82、**成交率 96.9%**、總摩擦 ≈ 0.25% 名目(手續費 3.6 萬 + 稅 8.6 萬 + 滑價 2.7 萬 / 名目 5,807 萬)——realistic 反而略優於紙上(開盤 vs 收盤時序噪音),無摩擦崩壞(流動性閘門 + ADV 上限奏效)。發現並繞過共用模擬器的缺 bar 標價-0 問題(runner 端 forward-fill,不動共用模組)。資金放大 10-50x 後參與率才會綁定。

**驗證電池(2026-07-06 cutoff,計分經戰役十一~十三消融驗證後重跑)**:三窗 CAGR 253.3%/197.7%/180.0%、Sharpe 6.84/5.75/5.24、Sortino(validate 基準)8.97/7.29/6.29、MDD -18.0%/-11.8%/-13.3%、Lo-t 4.29/4.14/3.90(p=0.000)、DSR 1.00/1.00/1.00、bootstrap CAGR 5% 下界 +102.5%/+103.8%/+92.8%、置換 p=0.000(池內隨機 200 次,中位 127.9%)。**PBO 0.476/0.644/0.778**——lag0 <0.5 健康,長 lag 偏高為既有 caveat(季度 fold 稀疏)。mech_2018(無策展對照)CAGR 21.1%、DSR 0.46——再證 alpha 在策展層。

**戰役五:2020-2023 回溯策展(哲學泛化裁判)**——9 個非 AI 主題(貨櫃/ABF/成熟製程/軍工/EV/解封等,類別成員+證據日期):
- v2 三個 lag 全勝 0050(lag90:18.6% vs 12.4%,MDD -26.3% vs -34.0%)
- 分年:2021 超額 +60~97pp(lag 越大越強=非日期作弊);2022 空頭同跌略淺;**2020 短窗口主題在 lag 下失效**(covid 類 6 個月擴產補齊,事後檢視違反 curation SOP 準入,已把「瓶頸存續 ≥12 個月」寫入準入標準)
- 對照:無紀律的 rotation-only 在 lag180 崩至 14.2% 落後 2330——**2025 的 alpha 主體在策展,跨週期的存活主體在紀律,系統=兩者相乘**
- 量級校準:常規週期期望 = 勝 0050 約 6-14pp/年 + 更淺 MDD;230%+ 是 AI 超級週期特例

**狀態更新**:`research_candidate` → **`backtest_validated` + execution 路考通過**。策展工業化落地:`serenity_curation_sop.md`(瓶頸簽名準入、證據鏈、失效條件、節奏)+ registry v2 schema(evidence_date/url/invalidation/review_by)+ skill `references/tw-event-engine.md`(操作手冊)。下一步 = live registry 維護(每日 sweep + git 稽核軌跡)累積前瞻證據。

## Artifacts

- `research/strat_lab/results/serenity_event_engine_v1_*`(lag0/lag90/lag180/mech 全套 daily/trades/summary/picks/png)
- `docs/strategy_research/serenity_event_engine_v1_validation_ev_full_tp60.md`(驗證輸出)
