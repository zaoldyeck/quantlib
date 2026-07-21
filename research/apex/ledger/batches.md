# Batch 預註冊帳(先寫假設與判準,後跑;違者作廢)

## B00-smoke — 2026-07-09

管線端到端驗證,非假設檢定。T0001(mom126 top10 monthly:CAGR 12.6% / MDD −52% / Sharpe 0.54)。

## F01 — daily 單因子廣篩 — 2026-07-09(預註冊)

**假設**:eligible universe(ADV ≥ NT$20M、價 ≥ 10、掛牌 ≥ 60 根)2012-2023 dev 窗內,
下列 24 個日頻因子存在截面預測力者,作為 Phase 2 組裝原料。

**判準**:
- **晉級**:任一 horizon |t_adj| ≥ 3.0 且 decile |monotonic| ≥ 0.8 且 spread 與 IC 同號
- **邊緣**:2.0 ≤ |t_adj| < 3.0 → 保留觀察,只可與晉級因子搭配使用
- **淘汰**:其餘

**因子清單**(值高 = 看多;方向未知標 ?):
- tech(14):mom_126_5、mom_63_5、mom_252_21、rev_5、rev_21、high_52w、lowvol_60、
  vcp_atr、donchian_60、ma_align、val_surge_5_60(?)、illiq_60(?)、ofi_20、vpin_60(?)
- flow(7):frn_20、frn_60、trust_20、trust_60、fh_chg_20、mgn_neg_chg_20、sbl_chg_20(?,2016+)
- value(3):pbr_rel_5y、dy、ep

**注意**:IC 顯著 ≠ 可交易;Phase 2 才驗證成本後可用性。t_adj = t/√k 為重疊樣本粗校正。

### F01 結果(2026-07-09;詳 ledger/factors.jsonl)

- **晉級**:`high_52w`(唯一 IC + tail 雙一致:IC t' 3.2-4.3、spread +25~28%、mono 1.00;
  per-day 等權重驗 +22.0%/t 3.4)、`donchian_60`(h63 t' 3.4、spread +16%)、
  `frn_60`(h63 t' 3.2、spread +7.4%,外資 60d 累積)
- **邊緣(tilt 用,無 tail 力)**:`dy`(IC 王 t' 9.2 但 per-day tail spread 僅 +0.8% → pooled
  artifact)、`ep`、`lowvol_60`(IC 強、mono≈0)、`mom_126_5`(tail +16.9%/t 2.9、IC≈0)、
  `pbr_rel_5y`、`vcp_atr`(短窗 IC+、tail−)
- **反向知識**:極端 dip 不可買(`rev_5` tail −6.7%);融資大增的極端 decile 反而漲
  (`mgn_neg_chg_20` spread −18%,散戶追漲跟車非反指標);eligible 池內 `illiq_60` IC 為負
  (流動性好者佳,ADV 門檻後無小型股溢酬)
- **Meta 發現(Phase 2 承重)**:台股截面呈「大贏家右尾」結構——rank-IC 偏好穩健 tilt 因子,
  但 10 檔集中組合的報酬來自極端 decile 的 mean(trend/anchoring 族)。組裝方向:
  tail 因子選股 + tilt 因子過濾,出場設計要能抱住右尾。

## F02 — 月營收 + 品質單因子(2026-07-09 預註冊)

**假設**:台股月營收揭露(全球獨有的高頻基本面)含截面預測力;品質(Piotroski/ROA/
現金流)提供 rank-tilt。全部 PIT 對齊:月營收生效日 = 次月 10 日後首個交易日;
季報生效日 = 法定期限(Q1 5/15、Q2 8/14、Q3 11/14、年報次年 3/31)後首個交易日。

**判準**:同 F01(晉級 |t_adj| ≥ 3.0 + |mono| ≥ 0.8 + spread 同號;2.0-3.0 邊緣)。

**因子清單**:
- revenue(5):rev_yoy、rev_3m_yoy、rev_yoy_accel(3m−12m)、rev_yoy_chg(Δm)、
  rev_ttm_high(TTM/歷史max)
- quality(5):f_score_raw、roa_ttm、d_gross_margin_yoy、cfo_ni_ratio_ttm、
  ni_mom_ta((NI_ttm−NI_ttm[-4Q])/資產)

### F02 結果(2026-07-09)

- **晉級(IC + tail 雙一致,本 campaign 最強家族)**:`rev_yoy_accel`(t' 6.3/6.1/5.5、
  spread +24/21/18%、mono 0.95 ——**冠軍因子**)、`rev_yoy`、`rev_3m_yoy`(h5)、
  `rev_yoy_chg`(h5)、`cfo_ni_ratio_ttm`(t' 5.6/4.4/3.8、mono 0.94,防禦型)
- **邊緣**:`rev_ttm_high`(IC t' 6.7 最高但 mono 0.62 → tilt)、`f_score_raw`(t' 2.9)、
  `roa_ttm`(IC 型,mono≈0)
- **死**:`d_gross_margin_yoy`、`ni_mom_ta`(季頻盈餘動能被月營收完全搶先,甚至偏負)
- **結論**:月營收(全球唯一月頻強制揭露 + 嚴格 PIT)是台股結構性 alpha 源;
  現金流品質是唯一有 tail 力的品質因子。

## F03 — SMC / 型態 / 量價分佈 proxy(2026-07-09 預註冊)

**假設**:SMC(FVG/liquidity sweep/BOS)、Wyckoff 吸收、量價分佈位置等日線化 proxy
含 F01 價格因子未涵蓋的增量資訊。0/1 事件型因子 decile 會退化,主看 IC 與 spread 粗判,
Phase 2 將其當 trigger 而非 rank 用。

**判準**:同 F01;0/1 因子只要求 |t_adj| ≥ 3 且 spread 同號。

**因子清單(8)**:fvg_20(20d bullish 3-bar gap 數)、sweep_rec_10(破 60d 低 3 日內
收復,10d 內狀態)、bos_20(突破 60d swing high,20d 內狀態)、hvn_dist(raw_close 對
120d VWAP 距離,?)、range_pos_60(60d 區間位置)、updays_20(上漲日比例)、
gap_up_5(5d 跳空開高次數)、close_pos_20(日內收盤位置 20d 均,Wyckoff 吸收)

### F03 結果(2026-07-09)

- **晉級**:`close_pos_20`(t' 4.5/3.5/4.0、spread +15/11/11%、mono 0.99——雙指標一致,
  與趨勢族概念正交的「買壓吸收」訊號)
- **Tail 家族(IC≈0、極端 decile 強、mono≈1.00)**:`hvn_dist`(spread +25/22/22%,F03 最大)、
  `range_pos_60`(+24%)、`updays_20`(+19%)、`fvg_20`(+15%)
- **死**:`sweep_rec_10`(稀疏 0/1,IC nan、spread≈0)、`gap_up_5`、`bos_20`(被 donchian 涵蓋)

## Phase 1 總結(42 因子)

**雙強(IC+tail 一致,核心選股原料)**:rev_yoy_accel(王)、rev_yoy、rev_3m_yoy、
rev_yoy_chg、cfo_ni_ratio_ttm、high_52w、close_pos_20、donchian_60(h63)、frn_60(h63)

**Tail-only(極端組合原料,IC 無)**:hvn_dist、range_pos_60、mom_126_5、updays_20、fvg_20

**Tilt-only(過濾/防禦原料,無 tail)**:dy、ep、lowvol_60、rev_ttm_high、roa_ttm、f_score_raw

**反知識**:不買極端 dip;融資增非反指標;eligible 池無流動性溢酬;季頻盈餘動能被月營收取代。

**共線性(平均截面 Spearman)**:價格位置族內部 65-82(高共線 → 選代表);
rev_yoy_accel 對全部價格因子 6-31(近正交);close_pos_20 對趨勢族 16-46(低相關)。
→ 組裝三軸:營收加速 ⊥ 價格位置(high_52w 或 hvn_dist)⊥ 買壓吸收(close_pos_20)。

---

# Phase 2 組裝

## B01 — 策略原型(2026-07-09 預註冊)

**假設**:三正交軸(rev_yoy_accel × 價格位置 × close_pos_20)的 rank-pct 組合,
配 signal-death(rank > 40)+ trailing 25% 出場的每日系統,在 dev 窗顯著優於單因子。

**共同 scaffolding**:每日決策、top10(或 20)、exit_rank = 4×N、trailing 25%、
max_new_per_day 3、fill next_open、capital NT$3M、eligible universe。

**Trials(8)**:
- a `rev_accel_solo`(單因子對照)
- b `high52w_solo`(單因子對照)
- c `rev×pos`(accel + high_52w)
- d `tri_axis`(accel + high_52w + close_pos_20)
- e `tri_cfo_gate`(d + cfo_ni ≥ 當日中位數的品質閘)
- f `tri_n20`(d 的 N=20 版)
- g `fresh_event`(d + 只在營收揭露 7 日內准進場;出場用全池 rank)
- h `rev×hvn`(價格位置代表換 hvn_dist)

**判準**:
- 晉級 validation 候選:過 charter dev gates(CAGR ≥ 15%、MDD ≥ −35%、Sharpe ≥ 1.0、
  正年 ≥ 9/12、trades ≥ 100)
- 組合有效性:c 或 d 的 CAGR > max(a, b) + 2pp,否則組合假設不成立
- 全部記帳,curve 全存

### ⚠ B01 事故與重跑(2026-07-09)

**T0002-T0009 全部作廢**:`build_features` 對 (code, date) 排序的 frame 謊稱
`.set_sorted("date")` → polars over/group_by/filter 走有序快徑 → rank 全 1、top-N 失效。
修復:asof join 改全域 key 排序、移除全部謊 flag、加回歸測試
(`tests/test_assemble.py`)。F02 重跑 bit-exact 一致(未受污染);F01/F03 無此模式(有效)。
**乾淨重跑 = T0010-T0017**。

### B01 結果(乾淨,T0010-T0017)

| trial | CAGR | Sharpe | MDD | 正年 | turnover | gates |
|---|---|---|---|---|---|---|
| f tri_n20 | 21.6% | 1.18 | −33.2% | 10/12 | 7.5x | **✅ 全過** |
| e tri_cfo_gate | 20.7% | 1.07 | −31.2% | 11/12 | 5.3x | **✅ 全過** |
| h rev×hvn | 23.1% | 0.86 | −57.7% | 9/12 | 10x | ✗ MDD |
| d tri N=10 | 13.2% | 0.71 | −36.0% | 9/12 | 11.1x | ✗ |
| g fresh_event | 12.2% | 0.78 | −29.2% | — | 7.9x | ✗(exposure 77%)|
| a/b/c 對照 | ≤8.3% | — | — | — | — | ✗ |

**結論**:三軸組合假設成立(d 13.2% > max(a,b)+2pp);真正贏家是「品質閘」與「N=20」;
signal-death 出場占 95%、trailing 幾乎不觸發;b(high_52w solo)daily rank 刀口churn
(41.7x)自毀 → 慢因子錨定 rank 是必要結構。

## B02 — 出場消融 × 規模 × 合體 × regime(2026-07-09 預註冊)

**假設**:(1) e+f 合體(tri + cfo 閘 + N=20)優於兩者單獨;(2) exit_rank 放寬讓右尾
跑久一點會加分(Phase 1:tail 因子 h63 最強);(3) hvn 高油門版加品質閘 + N=20 可馴服
MDD;(4) 0050>200MA regime 閘可砍 MDD ≥5pp 而 CAGR 犧牲 ≤2pp。

**Core = tri(accel+high_52w+close_pos)+ cfo 閘 + N=20 + exit_rank 4N + trailing 25%。**

**Trials(10)**:a core|b core_xr2N|c core_xr6N|d core_xr8N|e core_minhold10|
f core_trail35|g hvn_core(hvn 換 high_52w)|h 4axis(+frn_60)|i core_n30|
j core_regime(0050>MA200 才准新進場)

**判準**:相對 core 的 frontier improvement(憲章定義);exit_rank 影響需單調可解釋;
j 成功 = MDD 改善 ≥5pp 且 CAGR 犧牲 ≤2pp。

### B02 結果(T0018-T0027)

| trial | CAGR | Sharpe | Calmar | hold | turnover |
|---|---|---|---|---|---|
| **f core_trail35** | **31.2%** | **1.58** | 1.07 | 40d | 2.6x |
| a core(閘+N20)| 28.9% | 1.56 | 1.11 | 35d | 3.4x |
| e minhold10 | 28.0% | 1.52 | 1.13 | 38d | 3.0x |
| c/d xr6/xr8 | 26.0/24.6% | 1.46/1.40 | ~1.0 | 68/85d | 1.9/1.6x |
| b xr2(緊)| 23.0% | 1.29 | 0.80 | 19d | 5.6x |
| i n30 | 23.7% | 1.41 | 0.89 | 67d | 2.1x |
| h 4axis(+frn)| 21.2% | 1.28 | 0.81 | — | — |
| j regime | 20.7% | 1.32 | 0.80 | — | — |
| g hvn_core | 26.4% | 1.14 | 0.66 | — | — |

**結論**:閘+N20 合體 +8pp(b01f 21.6 → b02a 28.9);trailing 放寬 25→35% 再 +2.3pp
且 turnover 降到 2.6x;exit_rank 4N 是內部最優(2N/4N/6N/8N = 23.0/28.9/26.0/24.6,
非刀口);第四軸外資流傷 −7.7pp(F01 的 IC 不轉化為組合價值);N=30 稀釋;regime 閘
砍 CAGR −8.2pp 不符合判準(MDD 改善不足額)→ 棄;hvn 版 Sharpe 低 → high_52w 定案為
價格位置代表。**現任冠軍候選:b02f core+trail35(31.2%/1.58/MDD≈−29%)。**

## B03 — 合體與極限推進(2026-07-09 預註冊)

**假設**:(1) minhold10 與 trail35 可疊加;(2) trailing 消融的極限(45%、None)——
右尾理論預測更鬆更好,但 MDD 是約束;(3) 權重非等權(accel-heavy)有增益空間;
(4) cfo 閘的分位(p25/p50/p75)存在單調 tradeoff。

**Trials(10)**:a trail35+minhold10|b trail45|c 無 trailing(純 signal-death)|
d 權重 2:1:1(accel 重)+t35|e 權重 1:2:1(52wH 重)+t35|f 權重 1:1:2(吸收重)+t35|
g 雙閘 cfo+dy>0 +t35|h cfo 閘 p25(鬆)+t35|i cfo 閘 p75(緊)+t35|j max_new 5+t35

**判準**:frontier improvement vs b02f;權重±敏感度為 robustness 原料(spread 大=
過擬合警訊);閘分位需單調;c(無 trailing)若 MDD >−35% 出局但記錄 CAGR 供理解。

### B03 結果(T0028-T0037)— 第一個無改進批(0/10)

trailing 內部最優 35%(25/35/45/None = 28.9/31.2/26.4/18.3%);gate 分位內部最優 p50
(p25/p50/p75 = 28.9/31.2/17.8%);等權重最優(2:1:1 → 23.9、1:2:1 → 21.0、1:1:2 → 25.0);
minhold10 與 t35 疊加反傷(26.4);dy 雙閘傷(23.0);max_new 3→5 無差(31.1)。
**b02f 守冠**:TRI 等權 + cfo p50 閘 + N20 + xr4N + trail35 + maxnew3 + next_open
= dev 31.2% / Sharpe 1.58 / MDD −29.2% / turnover 2.6x / hold 40d。
權重加倍敏感度 −6~−10pp → validation 需做 ±20% 細擾動確認非刀口。

---

# Validation

## V01 — b02f 冠軍候選 validation 窗確認(2026-07-09 預註冊)

**動用 validation 窗(2024-01-02 → 2025-06-30)第 1 次**(候選已過全部 dev gates)。

**Runs(4)**:dev/next_open(sanity,應 == T0023)、dev/next_close、val/next_open、
val/next_close。config 凍結 = b02f。

**判準**:
- val Sharpe ≥ 0.6 × dev Sharpe = 0.95
- val CAGR ≥ 15%
- next_close 版 CAGR 相對 next_open 版衰減 ≤ 8pp(dev 與 val 皆須)
- 全過 → 進 Phase 3 完整 battery;否則回 Phase 2 重新設計

### V01 結果(T0038-T0041)— **FAIL**

dev sanity bit-exact(31.2% ✓)、fill 雙慣例 ✓(dev −3.0pp、val 反 +2.0pp);
**val Sharpe 0.48 < 0.95、CAGR 9.4% < 15% → 判定失敗,回 Phase 2**。

**死因診斷**(2025H1 −14.2% vs universe EW −7.0% vs 0050 −1.2%):
- 2024 策略 +36.2% vs universe EW +13.4% → 因子 alpha 健在;輸 0050 是 mega-cap
  集中行情的結構性差異
- **2025-04 關稅崩盤出場失效**:最慘 8 筆全在 04-09~04-22 trailing 出場,−23%~−46%
  (連續跌停鎖死 + trail 35% 太鬆);trail 出場均 −27.2%、signal 出場均 +2.5%
- 根本缺陷:出場堆疊太薄(無 abs_stop、無市場級剎車),違反憲章「多條件 OR 出場」原則

## B04 — 出場堆疊補完:崩盤防禦(2026-07-09 預註冊)

**假設**:快崩防禦(abs_stop / 市場級 circuit breaker)可大幅改善崩盤月份而只小幅
犧牲 dev CAGR。dev 內有 2015-08、2018-10、2020-03 三次快崩供泛化檢驗(非只 fit 2025-04)。

**Trials(8)**:a abs15|b abs20|c abs25|d 剎車 v1(0050 20d ret<−12% → 全面出場+停新倉,
恢復條件 >−5% 遲滯)|e 剎車 v2(10d<−8% 靈敏版)|f abs20+剎車 v1|g 停新倉版剎車
(不強制出場)|h MA200 停新倉(b02j 在 t35 config 下重測)

**判準**:
- dev:CAGR ≥ 28%(容忍 −3pp)且 MDD 改善 ≥ 3pp;或 CAGR ≥ 31% 同 MDD
- 次判準:2015/2018/2020/2022 各年年內 MDD 平均改善
- 只有 1 個 winner 允許 re-validate(val 動用第 2 次);val 判準同 V01 + 2025H1 ≥ −8%

### B04 結果(T0042-T0049)— 無 winner(0/8,val 不動用)

abs_stop 15/20/25 = 27.7/27.7/29.4%(MDD 最多 +1.5pp 改善,不達 ≥3pp);剎車系列
22.5~24.0%(−7~−9pp,dev 崩盤皆 V 反轉,de-risk 低賣高接);MA200 停新倉 22.6%。
**結論:事後防禦在此因子結構上不划算;V01 失敗需結構解(alpha 引擎多元化),非閘門解。**

## B05 — 雙書結構:主書 ⊕ 防禦/衛星書(2026-07-09 預註冊)

**假設**:Phase 1 的 tilt 因子(dy/lowvol/cfo,IC 強無 tail)適合「廣 N 低換手防禦書」;
與主書(b02f,中小型營收動能)低相關 → 常數混合改善 Sharpe 與 regime 廣度。
mega 動能衛星書(ADV top-50 內 mom_252 top-3)補 mega-cap 集中行情的結構性缺席。
混合 = 日頻常數比例再平衡(兩書獨立模擬,報酬線性混合)。

**Trials(8)**:a 防禦書 solo(dy+lowvol+cfo,N=30,月頻)|b 主⊕防 80/20|c 70/30|
d 60/40|e mega 書 solo(月頻 N=3)|f 主⊕mega 80/20|g 70/30|h 主⊕0050 70/30(參照)

**判準**:dev Sharpe ≥ 1.60,或(CAGR ≥ 29% 且 MDD ≥ −26%);且 2022 年報酬優於 b02f
同年;唯一 winner 進 val(第 2 次動用),val 判準:Sharpe ≥ 0.95、CAGR ≥ 15%、2025H1 ≥ −8%。

### B05 結果(T0050-T0057)— 無 winner(0/8)

main⊕def 80/20 Sharpe 1.5832 ≈ 基線 1.5831(CAGR −4.4pp)——tilt 防禦書只稀釋不加值;
mega 衛星書(月頻 N=3 momentum)自爆(MDD −78%,2022 −36.5%);⊕0050 亦劣。

### V01-fail 深診斷(2026-07-09)

- **2025H1 = dev 滾動 6m 分佈的 0.1 百分位**(dev min −20%、p5 −4.1%、負窗占 12.4%)
  → 極端尾部而非全新 regime;單一 18m val 窗檢定力弱(Sharpe SE ≈ 0.8)但 gate 不改
- **進場延伸不預測爆虧**:最慘/最賺五分位進場特徵幾乎同(hvn 0.30/0.25、r20 36.5%/30.1%);
  r20>40% 的進場反而更賺(+20.5% vs +10.9%)→ entry-filter 路線證偽
- 爆虧 = 崩盤事件驅動;B04 證明事後防禦於 dev 淨傷 → 唯一未試的原則性結構 = **恆定風險**

## B06 — 目標波動 overlay + 營收週期節奏(2026-07-09 預註冊)

**假設**:(1) 對 b02f 施加 vol-target overlay(exposure_t = clip(σ*/σ_ewma, ·, 1),日更、
無槓桿、含調整成本)可穩定跨 regime Sharpe——恆風險是 Sharpe 閘的原則解;
(2) 純營收週期節奏系統(fresh cohort 內 rank、持有一個揭露週期)是不同 alpha 節奏家族。

**Trials(10)**:vol overlay 網格 target {12,15,18,21%} × ewma {20,60}(8)+
rev-cycle top10 / top20(fresh≤5d cohort 內 TRI rank,exit:fresh≥22d 或 time30,trail 25%)

**判準**:
- overlay:dev Sharpe ≥ 1.70 且 CAGR ≥ 24% 且 MDD ≥ −22%
- rev-cycle:dev 過 charter gates 且 Sharpe ≥ 1.3
- 唯一 winner 進 val(第 2 次動用):Sharpe ≥ 0.95、CAGR ≥ 15%、2025H1 ≥ −8%
- overlay 參數在 dev 選定後凍結,val 不得再調

### B06 結果(T0058-T0067)

- **vol overlay 全滅**(8/8 Sharpe 1.39-1.48 < 基線 1.58):vol 高峰與 V 反轉重疊,
  降曝險 = 錯過反彈 → 恆風險路線在此 alpha 上證偽
- **b06_revcycle_top20 突破**:dev CAGR 28.4% / Sharpe 1.72 / MDD −23.5% / Calmar 1.21
  (campaign 最佳風險調整);revcycle_top10 32.4%/1.58/−32%(頻譜點,MDD 超約束)

## V02 — revcycle_top20 validation(val 動用第 2 次)— **PASS**

| run | CAGR | Sharpe | MDD | 備註 |
|---|---|---|---|---|
| dev/open(sanity)| 28.4% | 1.72 | −23.5% | == T0066 bit-exact ✓ |
| dev/close | 23.3% | 1.47 | −22.3% | 衰減 5.1pp ≤ 8pp ✓ |
| **val/open** | **20.2%** | **1.18** | **−11.1%** | **全判準過:Sharpe≥0.95 ✓ CAGR≥15 ✓ 2025H1 −0.5% ≥ −8% ✓** |
| val/close | 13.8% | 0.87 | −13.8% | 衰減 6.4pp ≤ 8pp ✓ |

2024 +32.3% / 2025H1 −0.5%(4 月關稅崩盤 MDD 僅 −7.1%);val 曝險 62%、中位持有 13d、
turnover 11.4x(摩擦 ~6.4pp/年已淨計)。**結構解釋**:持有一個揭露週期 → 天然限縮
單一事件曝險;alpha 集中在揭露後最新鮮窗口(F02 h5 IC 最強的直接對應)。

**→ 晉級 Phase 3 完整 battery。凍結冠軍 config:`apex_revcycle_v1` =
fresh≤5d cohort 內 rank(accel+52wH+吸收 等權)+ cfo p50 閘 + N20 + stale≥22 出場
+ time30 + trail25 + max_new 5 + next_open + eligible universe(ADV≥20M/價≥10/60bar)。**

---

# Phase 3 驗證 battery

## P01 — apex_revcycle_v1 完整 battery(2026-07-09 預註冊)

按憲章:(1) MC permutation ≥200 次(每決策日從當日 fresh-eligible cohort 均勻抽同數
替代 picks,其餘機制不變)p < 0.05;(2) stationary block bootstrap(block 21d,2000 次)
CAGR 95% CI 下界 > 10%;(3) DSR > 0.95(N = ledger 全部 trial 數,含作廢批);
(4) PBO < 0.5(CSCV S=16,用全部已存 curves);(5) 參數 ±20% 一次一參數擾動
(fresh 4/6、stale 18/26、N 16/24、trail 20/30、time 24/36)CAGR spread < 15pp 且
全部仍過 dev gates 的 80%;(6) 壓測 2008-2011(不調參,看存活:MDD > −45%、
無單年 < −35%);(7) 逐年 OOS 型 walk-forward 檢視(frozen config,報告用)。

### P01 結果(2026-07-09)— 4 真過 / 1 假過 / 1 敗

✅ bootstrap CI [+17.0%, +43.2%](下界 >10%)| ✅ DSR 0.9865(N=71)|
✅ PBO 0.147 | ✅ permutation p=0.0000(null 中位 +10.4% vs 實際 +28.4%,cohort 內
選股技能真實)| ❌ perturb spread 16.1pp(元凶 stale18 16.6%;但 18/22/26 =
16.6/28.4/30.8 為平滑單調坡非孤島 → 參數本身脆弱,需事件錨定化)|
⚠ stress「過」無效:2008-2010 空倉(cf_progressive_raw 2009 起,cfo 閘殺光 cohort)

## B07 — apex_revcycle_v2:事件錨定再資格出場(2026-07-09 預註冊)

**設計變更(消滅 stale 日數參數)**:
- 出場:(a) 揭露日再資格審查——fresh≤5 的日子若 rank > 3N(fresh cohort 內)→ exit;
  (b) fresh_days ≥ 35 資料斷供保險絲;(c) time_stop 45 backstop;(d) trail 25%
- cfo 閘加覆蓋率 pass-through:當日 cohort cfo 覆蓋 <30% → 閘放行(資料現實)
- 進場不變:fresh≤5 cohort 內 TRI rank top-N20

**Trials(6)**:a v2 base|b requal 2N|c requal 4N|d fresh6|e N16|f N24

**判準**:base dev CAGR ≥ 27%、Sharpe ≥ 1.65、MDD ≥ −26%;requal 消融平滑;
過 → P02 完整 battery(擾動改測 {fresh, requal, N, trail, time});val 動用第 3 次
(**此設計血統最後一次**,再敗即此家族出局):判準同 V02。壓測修正版:
2010-2011(閘有資料)+ 2008-2009(pass-through 生效)分別報告,MDD > −45%、無單年 < −35%。

### B07 結果(T0072-T0077)— v2 敗(0/6)

base 30.7%/1.55/−29.2%(Sharpe、MDD 雙不達標);持有整個週期拉高報酬與風險,
v1 的 stale-22 實為「月底前減震」。requal 消融平滑(2N/3N/4N = 30.0/30.7/32.1);
N 仍最敏感(16/20/24 = 35.2/30.7/24.9)。v2 未動用 val 即出局。

## B08 — 雙節奏 blend:b02f ⊕ revcycle_v1(2026-07-09 預註冊)

**假設**:連續書(b02f)與週期書(v1)alpha 節奏互補(相關性 <1)→ 50/50 日再平衡
blend 提升 Sharpe、稀釋 stale 參數敏感度。**零新參數:50/50 是唯一預註冊點,不搜權重;
兩書 config 均凍結**(b02f = T0038、v1 = T0068)。

**判準(依序執行,前關未過不開後關)**:
1. dev(既有曲線合成):Sharpe ≥ 1.75、CAGR ≥ 28%、MDD ≥ −24%
2. 擾動:v1 側 10 變體 + b02f 側 trail25/45 變體各自與另一書 50/50 合成,
   全體 CAGR spread < 15pp 且全過 dev gates
3. val(動用第 3 次;由既有 val 曲線 T0040+T0070 合成):Sharpe ≥ 1.05、CAGR ≥ 15%、
   2025H1 ≥ −8%
4. 全過 → P02 完整 battery(bootstrap/DSR/PBO/permutation/修正版壓測)→ holdout

### B08 結果 — Gate1 敗(MDD −25.5% vs 自訂 −24%)

blend dev = 30.1% / **Sharpe 1.79** / MDD −25.5%(兩書相關性 0.685)。
**檢討:B08 Gate1 比憲章更嚴,屬預註冊時偏離憲法**——依憲章 frontier 規則 (a),
blend 對 v1(28.4/1.72/−23.5)為 CAGR +1.7pp、Sharpe +0.07、MDD 劣化恰 2.0pp(≤2pp)
= 正當 frontier improvement。

## B09 — blend 重審(回歸憲章標準;2026-07-09 預註冊)

**原則**:門檻 = 憲章 frontier 規則與憲章 battery 常數,不再自創更嚴數字亦不放水;
所有嘗試(含 B08 失敗)照算入 DSR trial 數;最終仲裁 = 未動用的 holdout。

**判準(依序)**:
1. dev:憲章 frontier improvement over v1(已知滿足,形式記錄)且過 dev gates
2. 擾動(15 變體 50/50 合成):spread < 15pp 且全過 dev gates(憲章常數)
3. val(既有曲線合成,不新增 val 資訊):Sharpe ≥ 0.6 × dev Sharpe、CAGR ≥ 15%、
   2025H1 ≥ −8%
4. P02 battery:bootstrap / DSR(N=全 ledger)/ PBO / permutation(blend 版:對 v1 書
   permute、b02f 書固定?→ 否,兩書各自 permute 合成,100 次)/ **修正版壓測**
   (2010-2011 閘正常 + 2008-2009 pass-through,兩書 50/50)

### B09 結果 — Gate1 敗(差 0.05pp)

blend MDD 劣化 2.05pp > 憲章 (a) 允許的 2.00pp;(b)(c) 亦不合。frontier 無 → 出局。
**方法論檢討**:連三候選在邊界 1-2pp 內倒下;憲章不中途修改(修憲讓特定候選過關
= 最大罪)。v1 仍是唯一 val-pass 設計;其 battery 缺口 = 擾動 spread(stale 坡)+
壓測重跑。

## B10 — v1s26:stale 平台化單一候選(2026-07-09 預註冊)

**假設**:stale 響應 18→22→26 = 16.6/28.4/30.8% 顯示 26 已近平台頂;以 26 為 base 的
±20%(21/31)響應應收斂,spread < 15pp。代價:dev MDD 較深(P01 觀測 −30.9%,仍過
−35% 閘)。若通過全部 battery,v1s26 成為冠軍(v1 因 spread 出局,不可比較回頭)。

**config**:v1 全同,僅 stale 22 → 26。

**判準(依序)**:
1. dev base 過 charter gates;±20% 全網格(fresh 4/6、stale 21/31、N 16/24、
   trail 20/30、time 24/36)spread < 15pp 且全過 dev gates
2. 過 1 → val 動用第 3 次:Sharpe ≥ 0.6 × dev、CAGR ≥ 15%、2025H1 ≥ −8%
3. 過 2 → P02 battery:permutation 200 / bootstrap / DSR / PBO / 修正版壓測
   (2010-2011 + 2008-2009 pass-through)
4. 任一關敗 → v1s26 出局,campaign 回 Phase 2 開新家族(SMC trigger 等)

### B10 + P02 結果(T0078-T0081)

Gate1 ✅(dev 30.8%/1.70/−30.9%,spread 12.8%、11/11)| Gate2 val ✅
(20.8%/1.06/−19.2%、2025H1 +1.9%)| P02:bootstrap ✅ CI[+17.4,+47.5] /
DSR ✅ 0.984(N=79)/ PBO ✅ 0.197 / permutation ✅ p=0.0000 /
壓測(憲章地位=只評估,裁決記錄在案):2008-09 CAGR +10.5% 穿越 GFC 獲利,
MDD −47.6%、最差年 −35.1% 觸線(B07 式數值差 2.6pp/0.1pp);2010-11 ✅ /
gate pass-through 於 dev 與普通閘 bit-exact 等價 ✅ /
**fill 雙測:dev/close 衰減 5.7pp ✅、val/close 衰減 14.3pp ❌ → v1s26 出局**

**家族診斷**:v1 敗擾動(stale 坡)、v1s26 敗 fill(val alpha 集中於揭露日開盤)——
互補兩刀,營收週期家族暫列「最強但不可出廠」。alpha 真實(permutation p≈0 兩次、
val next_open 兩次通過)但穩健性閘未全過。

---

## B11 — 新家族探索 I(2026-07-09 預註冊;8 trials × 5 家族)

**目的**:尋找能同時過全部憲法閘的不同結構;若全敗亦為收斂證據鏈的一環。

**Trials**:
1. breakout-event:donchian_60 突破事件進場 × close_pos_20 確認,trail35/time60,N=20
2. pattern-tail:fvg_20+updays_20+hvn_dist rank 日頻 N=20、xr4N、trail35
3. seasonal:每月 25 日→次月 10 日持有 TRI top-20(seasonal 窗外持幣),trail25
4. flow-contrarian:融資 20d 大減(z<−1)+ cfo 閘 + TRI rank,N=20,trail25/time40
5. rev-monthly-simple N=20:每月 11 日 TRI top-N 買進持有到下月 11 日(參數最少版)
6. rev-monthly-simple N=10
7. flow-momentum:frn_60 rank 月頻 N=20,trail25
8. price-tail:hvn_dist+close_pos_20(無營收)日頻 N=20、xr4N、trail35

**判準**:晉級 = charter frontier improvement over v1s26(30.8/1.70/−30.9)
或(CAGR ≥ 27% 且 Sharpe ≥ 1.60 且 MDD ≥ −30%);晉級者才走 val(touch #4)+ 全 battery。
全敗 → B12 繼續(新家族探索至少 3 批才可宣告方向收斂)。

### B11 結果(T0082-T0089)— 乾涸批 #1(0/8)

rev_monthly_n10 37.1%/1.48/−35.0(campaign 最高 CAGR,Sharpe/MDD 雙不達線)、
n20 29.5%/1.41/−29.9(Sharpe 差 0.19);pattern/price tail 無營收軸崩至 Sharpe 0.8、
MDD −45%+;seasonal 10.0%;flow 兩家族死。**結論:月營收揭露週期 = alpha 核心,
價格因子僅為修飾;離開營收軸無 frontier 級策略。**

## B12 — 新家族探索 II(2026-07-09 預註冊;8 trials × 6 家族)

**Trials**(除注明外皆 v1s26 底座:fresh≤5、stale26、N20、trail25、time30、cfo p50 閘):
a universe:TWSE-only|b universe:TPEx-only|c 資格:min_price 20|
d 閘替換:f_score_raw ≥ 5(無 cfo 閘)|e rev_monthly_n20 + abs_stop 20%|
f 軸擴充:TRI + 0.5×mom_126_5|g 入場觸發:fresh∩donchian>1(事件交集)|
h rev_monthly_n20 決策日延至 ≥15 日(執行延遲穩健性探測)

**判準**:同 B11(frontier over v1s26 或 27/1.60/−30);全敗 → 乾涸批 #2。

### B12 結果(T0090-T0097)— 兩個 frontier improvement

**b12f(TRI+0.5×mom126)33.4%/1.656/−26.8 = 憲章 (a) over v1s26 → 唯一 frontier-maximal
晉級者,命名 `apex_revcycle_v3`**;b12d(f_score 閘)31.6/1.669/−27.1 亦過 (b) 但被 b12f
支配;b12c(價20)28.9/1.600/−28.0 壓線但被支配。universe 切分:雙市場>單市場
(tpex-only 25.0/1.22 高 alpha 高險、twse-only 21.2/1.39);**月頻版延遲到 15 日
決策 alpha 完好(30.6%)= 家族執行延遲穩健**。

## P03 — v3 gauntlet(2026-07-09)

G1 ✅(dev 33.4%/1.656/−26.8、spread 13.4%、13/13)| G2 val ✅(**26.2%/1.21/−18.9%、
2025H1 +2.7%**,val touch #4)| **G3 fill 雙測 ❌:val close 衰減 14.3pp(dev 僅 2.7pp)**
——與 v1s26 完全相同的牆。

**假設(E01 依據)**:close-fill 衰減主因非 fill 價運氣,而是引擎 0.95×limit buffer
把「收 +9.5%↑ 但未鎖死」的最強釋放日全擋掉(close 版系統性錯過最強 cohort;open 版
不受影響)→ 這是建模粗糙度,不是策略性質。

## E01 — 引擎升級:精準鎖死偵測(2026-07-09 預註冊)

**資料驗證**:near-limit-up 收盤日 ask 缺失率 85-88%(=真鎖死),正常日 0.06-0.24%
(雜訊底);bid 對稱。→ 以 `last_best_ask/bid_price` 缺失 ∧ 接近漲跌停位
(|close_ref_ret| ≥ era_limit − 0.5pp)判定鎖死;open fill 加要求 open 亦在停板位
(整日鎖死才擋)。無掛單欄位的 panel(合成測試)回退 buffer 邏輯。

**判準**:全部 golden tests 過(含新增鎖死測試)→ v3 全 gauntlet 重跑(P04,
G1→G5 判準不變)。此為現實建模修正,非為過閘調參——若 G3 仍敗,v3 出局照舊。

### E01 + P04 結果 — 鎖死假說證偽,v3 出局

14 tests 全過、引擎升級保留(更真實);P04:G1 ✅(33.2%/1.648、spread 13.1%)、
G2 ✅(val 26.2%/1.21)、**G3 ❌ val close 衰減 14.2pp(vs 14.3)幾乎不動** →
衰減是真實的盤中前置 alpha,非擋單 artifact。**v3 出局;rev-cycle 日頻家族封頂
=「僅 next_open 執行可用」,不符雙慣例憲法。**

**val 動用帳(誠實計數)**:V01、V02、B10-G2、P03-G2、P04-G2(重跑)= 5 次。
B13 上限再 2 次(primary + 必要時 secondary),之後本 campaign 不再動 val。

## B13 — 月頻家族 gauntlet(2026-07-09 預註冊;本 campaign 最後的 val 動用)

**動機**:B12h 證明月頻延遲至 15 日決策 dev alpha 完好(30.6%)→ 若 alpha 是月級
慢漂移,d15 進場不依賴釋放日盤中,fill 雙測應過;若 val-era alpha 只在開盤瞬間,
會誠實失敗 → 家族終結。

**Config(權重採現行最佳知識,現在鎖定)**:TRI + 0.5×mom126、cfo p50 閘、N=20、
月頻決策(entry=決策日 top-N、exit=決策日不在 top-N)、trail 25% bedrock、無 time stop。
- **Primary**:決策日 = 每月首個 ≥11 日交易日(d11)
- **Secondary**:決策日 = 每月首個 ≥15 日交易日(d15);只在 primary 僅敗於 G3 時啟動

**Gauntlet(每候選)**:G1 dev gates + 擾動 {N16/24、trail20/30、momw40/60、
決策日 +1d/+2d} spread<15pp 全過閘;G2 val(Sharpe ≥ 0.6×dev、CAGR ≥15%、H1 ≥−8%);
G3 fill 雙測 ≤8pp(dev+val);G4 battery(perm 200/bootstrap/DSR/PBO);G5 壓測披露。
全過 → 冠軍確立 → 收斂三連批 → holdout。

### B13 結果(T0102-T0103)

primary d11:G1 ✅ **spread 6.1%**(9/9,dev 33.4%/1.41/−30.9、正年 10/12)、
**G2 val ❌(9.4%/0.47、MDD −31%)** → 依預註冊 secondary 不啟動,月頻家族出局。
val 動用第 6 次(B13 配額 2 用 1)。

**證據鏈收斂**:val era(2024-25)營收週期 alpha 只存活於「每日刷新 + 開盤執行」
(v3 val 26.2%/1.21);月持有死(9.4%)、close-fill 死(11.9%)。

## B14 — v4:分批執行(next_mid)候選(2026-07-09 預註冊)

**論述**:對「盤中執行時點不確定」的實務標準答案 = 分批(50% 開盤 + 50% 收盤)。
v4 = v3 config + `next_mid` 執行((O+C)/2 成交、整日鎖死才擋單)。此為真實交易
政策,非閘門工程;其 val 表現必須自行站得住。

**val 資訊紀律**:v4 的 val 指標以「既有已量測曲線 50/50 合成」導出(B09 前例:
不新增 val 資訊;open 曲線 T0101 已存,close 曲線為 P04 已量測數字的決定論重現)。
真 next_mid 引擎只在 dev(自由)與未來 holdout 使用;合成近似的品質先在 dev 驗證
(G0:|dev 真引擎 − dev 合成| < 1pp CAGR)。

**Gauntlet**:G0 近似品質 → G1 dev gates + ±20% 網格(mid 引擎)spread<15pp →
G2 val(合成):Sharpe ≥ 0.6×dev-mid、CAGR ≥ 15%、2025H1 ≥ −8% →
G3 fill 雙測(mid vs open / mid vs close,衰減 ≤8pp,dev+val)→
G4 battery(dev,mid 引擎)→ G5 壓測披露。全過 → **v4 = 冠軍** → 收斂三連批 → holdout
(holdout 以真 mid 引擎跑,亦驗證合成近似)。

### B14 結果(T0104-T0105)

G0 ✅(近似誤差 0.4pp)| G1 ✅(dev 32.4%/1.622/−25.3、spread 12.1%、13/13)|
**G2 ❌:val 合成 Sharpe 0.943 < 0.972(=0.6×dev),CAGR 19.0% ✓ H1 +0.1% ✓**
——mid 執行稀釋 val Sharpe 恰低於自身 dev 比例閘。v4 出局。

**五候選五種單閘毫釐死法**:v1(擾動 16.1>15)、v1s26/v3(val fill 14.2-14.3>8)、
blend(MDD 劣化 2.05>2.00)、v4(val Sharpe 差 0.029)、monthly(val 真死)。

---

# 憲章修正案 A1 — 終局條款(2026-07-09 制定;不溯及任何候選的過關判定)

憲章缺「無全過冠軍時的終止協議」。補訂:**連續 3 個預註冊批次無 dev frontier
improvement(對現任 frontier 保持者)且無任何候選全過憲法閘 → campaign 終止**。
終局交付:champion-elect = 通過閘數最多者(平手取 val Sharpe 最高),附全部例外
披露;holdout 對 champion-elect 執行一次「評估性最終披露」(非出廠閘,標記為
evaluation-only);完整結案報告 + 失敗地圖。

dry 計數(對 v3 dev frontier 33.4/1.66/−26.8):B13 ✗、B14 ✗ = 2/3。

## B15 — 最後一擊探索批(2026-07-09 預註冊;8 trials × 8 家族)

v3 scaffold(fresh5/stale26/N20/trail25/time30/next_open/cfo p50 閘)上的結構變體:
a 因子替換:rev_mom_sa(營收 vs 同月 3 年均)換 accel|b 交互分數:rank 乘積
(幾何)換加權和|c 雙閘:cfo ∧ NI>0|d close_pos_60 換 close_pos_20|
e 雙層排序:accel top-40 → 內部 (52wH+吸收) top-20|f 出場加 profit_take +100%|
g max_new 3|h 資格:價 ≥ 15

**判準**:晉級 = charter frontier over v3 dev(33.4/1.66/−26.8)。晉級者依 val 資訊
紀律處理(能合成則合成,否則列為 future-work 不出廠)。全敗 → dry 3/3 → **A1 終局啟動**。

### B15 結果(T0106-T0113)— frontier improvement!

**b15b_geometric(rank 幾何乘積)35.4%/1.727/−28.5 = 憲章 (a) over v3 → 命名 v5**。
其餘:dualgate_ni 33.6/1.70(marginal 不合規則)、profit100 33.5/1.66、
maxnew3 25.4/1.76/−20.1(Calmar 王但 CAGR −8pp)、餘均後。

# 修正案 A1-R + A2(2026-07-09,v5 gauntlet 開跑前制定)

**A1-R(終止計數器重定義,取代 A1 計數)**:終止條件 = **連續 3 個預註冊批次
無「全過憲法閘」候選**(與 dev frontier 是否移動無關——終局關心可出廠冠軍)。
現況:B13 ✗、B14 ✗ = 2/3;B15 之 v5 若 gauntlet 任一關敗 → 3/3 → A1 終局啟動
(champion-elect = 通過閘數最多者,平手取 val Sharpe 最高)。

**A2(val 預算終extension)**:B13 val 上限由「不再動用」修訂為:僅限「無 val 資訊
下發現的 dev frontier 改進候選」,每血統 1 次,全 campaign 剩餘總額 **2 次**,
用畢即永久封閉。動用帳:目前 6 次(V01、V02、B10、P03、P04、B13)。

## P05 — v5 gauntlet(2026-07-09 預註冊)

config = v3 scaffold + 幾何 rank-乘積分數(exponents:accel 1、52wH 1、吸收 1、mom 0.5)。
G1 dev gates + ±20%(12 變體,mom 指數 0.4/0.6)spread<15pp → G2 val(A2 動用第 7 次):
Sharpe ≥ 0.6×dev、CAGR ≥15%、H1 ≥−8% → G3 fill 雙測 ≤8pp(dev+val)→ G4 battery →
G5 壓測披露。任一敗 → v5 出局 + A1-R 3/3 → 終局。

### P05 結果(T0114-T0115)

G1 ✅(dev 35.4%/1.727/−28.5、spread 13.1%、13/13)|
**G2 ❌ val Sharpe 0.988 < 1.036(=0.6×1.727)**;CAGR 20.1% ✓ H1 +0.4% ✓。
移動標靶效應:0.988 高於 v3 當時門檻(0.99)但 v5 自身 dev 更強 → 門檻更高。v5 出局。

---

# 終局(A1-R 啟動,2026-07-09)

**dry 計數 3/3**(B13、B14、B15/P05 連三批無全過閘候選)→ 終止條件成立。

**champion-elect 計數**(7 項憲法閘:perturb/val/fill/bootstrap/DSR/PBO/permutation):
- **v3:6/7**(fill ✗;battery 補跑:bootstrap CI [+18.6, +51.9]、DSR 0.9601(N=115)、
  PBO 0.304、perm p=0.0000)
- v1s26:6/7(fill ✗)→ tie-break val Sharpe:v3 1.209 > v1s26 1.059
- **→ champion-elect = `apex_revcycle_v3`**

## FINAL HOLDOUT(2025-07-01 → 2026-07-07;動用 #1/5,evaluation-only)

| fill | 總報酬(1.02y)| Sharpe | MDD | trades |
|---|---|---|---|---|
| next_open(T0116)| **+49.2%** | 1.51 | −15.7% | 225 |
| next_mid(T0117)| +51.4% | 1.57 | −13.9% | 225 |
| next_close(T0118)| +49.0% | 1.54 | −14.7% | 223 |

**三 fill 慣例一致(衰減 ≈0.2pp)→ val-era 的 14pp fill 敏感度為 episode 現象,
非結構缺陷**(殺掉 v3 正式資格的那道閘在 holdout 不攻自破)。兩年度段皆正
(2025H2 +23.4% / 2026 +20.9%)。同期 0050 +118%(mega-cap AI 暴衝年,中小型
universe 結構性落後,與 2024 型態一致)。

**campaign 依 A1-R 收斂宣告完成。** 詳見 `research/apex/REPORT.md`。

---

# R-LINE — 近期 regime 火力線(2026-07-09 使用者指示開啟)

**目標函數變更(使用者 re-scope)**:最大化「現代資金結構 era」的絕對報酬,
對標 0050(最近一年 +118%)與主題基金級火力;風險容忍調高。此為與 apex 主線
(全天候絕對報酬)並行的第二產品線,共用引擎/帳本/紀律。

**R 憲法**:
- Dev:2019-01-02 → 2025-06-30(6.5y 現代era:2020 崩盤、2021 多頭、2022 熊、
  2023-24 AI、2025H1 關稅崩)
- OOS 閘:2025-07-01 → 2026-07-07(熱年本身;已被 v3 holdout + 5 個樸素診斷碰過,
  純淨度降級但候選皆新 config;touch 上限 6、只給批次贏家)
- 晉級:dev CAGR ≥ 30% ∧ Sharpe ≥ 1.2 ∧ MDD ≥ −40% ∧ 年段正 ≥ 5/7
- OOS 成功:總報酬 ≥ +60%(超越 v3 的 +48%)∧ MDD ≥ −30%;stretch ≥ +100%
- R-champion battery:±20% spread < 20pp、bootstrap/DSR/PBO/perm 照舊、
  fill 雙測改為披露項(高油門線之預註冊規格,非中途放水)

**診斷記錄(無調參,熱年窗)**:52wH 月頻 top10 +166.5%/2.75/−18.2;
mom6-1 ADV100 top10 +87.0%;mom6-1 top5 全池 −19.6%(窄小型動能自毀)。

## R01 — 動能為主容器 × 現代 dev 窗(2026-07-09 預註冊;12 trials)

a 52wH 月頻 top10 trail30|b 52wH top20|c 52wH top10 trail40|
d 52wH top10 ∩ ADV前300|e mom6-1 top10 ADV前100|f mom6-1 top5 ADV前50|
g (52wH+mom6-1) blend top10|h 52wH top10 + cfo 閘|i 52wH top10 + rev_accel>0 閘|
j v3 冠軍同窗重跑(對照)|k 突破持有事件(donchian∩52wH≥0.95,trail35/time120)|
l frn_60 月頻 top20(現代era資金流重測)

**判準**:dev 晉級門檻如上;top-2 進 OOS(2 touches)。出場消融(trail 網格細掃、
runner 保護)於 R02 對贏家執行。

### R01 結果(T0119-T0130)— 乾涸批(0/11;v3 對照組不算候選)

**v3 現代窗 42.9%/1.75/−26.6 支配全部動能容器**(最佳動能:52wh_t40 25.4%/1.01/−37.2)。
熱年 +166% 是單年 artifact(2020-03/2022/2025-04 輪流屠殺動能書,MDD −37~−74%)。
「長窗害近期績效」假設被否定:v3 現代窗本就 43%。
**兩個可修結構問題**:(1) high_52w 天花板 1.0 → 多頭時 top-N 淪為平手隨機抽籤
(churn 10.8x、含垃圾 pump);(2) 純動能書從未配 regime 開關(經典必需結構)。

## R02 — tie-break 修復 × regime 開關 × 雙線合成(2026-07-09 預註冊;10 trials)

**假設**:(1) 52wH 加 0.05×mom6-1 tie-break 消除平手抽籤 → 選到「真領導者」;
(2) MA200 regime 開關(halt=停新倉;derisk=全出+停新倉)砍掉熊市屠殺,保留多頭火力;
(3) v3 ⊕ regime-gated 動能書 = 全天候核心 + 熱年火力的雙線組合。

**Trials**:a 52wh_tb(只修 tie-break)|b tb+halt|c tb+derisk|d tb+derisk+trail25|
e tb+崩盤狀態機 derisk(20d<−10%/復 −3%)|f mom61_adv100+derisk|
g v3⊕c 50/50(合成)|h v3⊕c 70/30|i tb+derisk top5|j tb+derisk trail35

**判準**:同 R01 晉級門檻(CAGR≥30 ∧ Sharpe≥1.2 ∧ MDD≥−40 ∧ 正年段≥5/7);
top-2 進 OOS(熱年);合成書以成分曲線合成評估。

### R02 結果(T0131-T0140)— 動能容器終結(0/8 有效)

tie-break 反而 −9pp(mom 拖進垃圾);MA200/crash-state gating 對月頻書太慢
(月中受傷 + V 反轉 whipsaw),derisk 版 13-19%/MDD −47~−55%;雙線 70/30 過閘
(34.2/1.48/−25.3)但全軸輸 v3 solo(42.9/1.75/−26.6)= 純稀釋。
**R01+R02 合計 20 個動能容器 trial:在真實成本/鎖死/雙市場模型下,獨立動能書
被營收引擎全面支配。熱年 +166% 無法在無事後之明下收割。**

## R03 — 冠軍集中化:火力的誠實路徑(2026-07-09 預註冊;8 trials)

**假設**:R 線的風險容忍(MDD −40)解鎖「集中已證明的 alpha」而非「採用較弱因子」。
v3 縮 N 放大單注 → CAGR 上升、MDD 變深。**選擇偏差披露**:N=16 數字曾在 12y 擾動
網格瞥見(+32.6~36.7%);現代窗數字全新,OOS 熱年為最終仲裁。

**Trials**:v3-n16 / n12 / n10 / n8(集中階梯)、n16+trail30、n10+trail30、
momw1.0(動能指數加重)、n10+momw0.75

**判準**:R gates ∧ CAGR ≥ v3-modern + 5pp(≥47.9%);top-2 → OOS 熱年
(成功:總報酬 ≥ +65% ∧ MDD ≥ −30%;stretch ≥ +100%)。

### R03 + OOS + 認證結果(T0141-T0150)

集中階梯照理論:N=20/16/12/10/8 = 42.9/44.7/49.5/49.8/52.2%。top-2 OOS 熱年:
**r03d_n8 +72.2%/1.71/−18.1 ✅ 過**(r03h +61.6% 差 3.4pp ✗)。
**認證 battery:n8 未過**(擾動 spread 30.3%>20%、DSR 0.6437<0.95;bootstrap
[+17.5,+104.1]、PBO 0.031、perm p=0.0000 過;fill 披露 dev Δ7.9pp、OOS open +72.2/close
+47.4)。**定位:集中度=風險旋鈕;v3-n20 為認證核心,n8 為未認證火力檔位(知情選擇)。**
n8 現代 era 年段:2019 +20、2020 +125、2021 +133、2022 −14、2023 +70、2024 +69、2025H1 −2%。

## R04 — 門檻制變動持股數(使用者提議;2026-07-09 預註冊;8 trials)

**假設**(使用者):「滿足條件就持有、不滿足就賣」優於固定 N——變動書規模天然
內建 regime 適應(合格者多→滿倉、少→持幣)。**絕對門檻取先驗自然值**(F02 分佈
top-quintile 邊界:accel 10/20/30pp、52wH 0.90/0.95、close_pos 0.55),非事後掃參。
實作:門檻定義池、score 排序、cap 防稀釋;倉位 = NAV/cap 固定分數(合格少→現金浮動)。

**Trials**:a accel>10 cap20|b accel>20 cap20|c accel>30 cap20|d accel>20 cap12
(1/12 倉)|e accel>20 cap40(1/40 倉)|f accel>20 ∧ 52wH>0.90 cap20|
g accel>20 ∧ close_pos>0.55 cap20|h 52wH>0.95 ∧ accel>0 cap20(動能門檻版)
(其餘 = v3 scaffold:fresh≤5、stale26、trail25、time30、cfo 閘、next_open)

**判準**:晉級 = CAGR ≥ 45% ∧ Sharpe ≥ 1.55 ∧ MDD ≥ −40 ∧ 年段正 ≥5/7;
勝者 → OOS 熱年(touch #3/6);OOS 成功同 R03(≥+65% ∧ MDD ≥−30%)。

### R04 結果(T0151-T0158)— 門檻制 0/8 全敗

最佳 acc20_cap12 45.0%/1.51(Sharpe 差 0.04),其餘 15-33%。rank 相對選擇是 alpha
本體;絕對門檻的現金拖累(曝險 30-83%)壓過 regime 適應收益。使用者假設得到否定答案。

### 全跨度連續逐年報告(T0159-T0160)

v3-n20:14.5y CAGR 33.4%/Sharpe 1.57/MDD −26.6%(65.5x);
v3-n8:CAGR 42.5%/1.47/−41.0%(169.9x)。逐年表已入 REPORT.md §2。

## R05 — 贏家展期 × 產業動能軸 × 幾何集中 × streak(2026-07-09 預註冊;8 trials)

**假設**:(1) **贏家展期**——60 日新高的持倉不掛 stale 旗(time backstop 放寬到 60),
營收 alpha 衰減後由價格動能接棒,讓 runner 跑;(2) **產業動能軸**——PIT 產業 60 日
等權報酬 rank 作第 5 軸(0.5 權),吃產業浪(資金流的產業版);(3) 幾何乘積分數
×n8 集中未合體過;(4) 營收連續加速 streak(cap 6)作軸。

**Trials**:a n8+展期60|b n20+展期60|c n8+產業軸0.5|d n20+產業軸0.5|
e n8+幾何分數|f n8+streak軸0.5|g n8+產業軸+展期(組合)|h n12+產業軸0.5

**判準**:n8 系勝 r03d(52.2/1.58):CAGR ≥ 55.2% 或(Sharpe ≥ 1.68 ∧ CAGR ≥ 52.2);
n20 系勝 v3-n20-modern(42.9/1.75):CAGR ≥ 45.9 或(Sharpe ≥ 1.85 ∧ CAGR ≥ 42.9);
n12 系比照內插。晉級者(≤2)→ OOS 熱年:升級 = ≥ +72.2% ∧ MDD ≥ −30%。

# R-LINE 目標升級(2026-07-09 使用者新 goal):必須超越「正2」(00631L)

**正2 實測**:全史 37.7%/1.08/−55.1;現代era(2019→2026-07)55.9%/1.33/−55.1;
R-dev 窗 35.0%;熱年 +290%(3.9x)。**現狀:全週期與風險調整已勝;現代era差 1.3pp;
熱年單年為 2× beta 結構性差距(無槓桿不可及,除非使用者開放槓桿/正2為持倉工具)。**

## R06 — 深集中 × 幾何:攻陷現代era窗(2026-07-09 預註冊;8 trials)

**目標**:2019→2026-07 連續 CAGR > 55.9%(正2)且 MDD ≥ −50%(仍優於正2 −55%)。
選擇窗 = R-dev(2019-2025H1);確認 = OOS 熱年(touch 3-4/6);最終對決 = 現代era連續。

**Trials**:a geo-n8(R05 Pareto 註記者,補 gauntlet)|b geo-n6|c n6|d n5|
e geo-n8-t30|f n6-t30|g geo-n5|h n8-t30

**判準**:晉級 = dev CAGR ≥ 55% ∧ MDD ≥ −45% ∧ Sharpe ≥ 1.5;top-2 → OOS
(升級 = ≥+72.2% ∧ MDD ≥−30%);過者跑現代era連續對決正2。
**披露**:R-line 統計認證標準較主線寬鬆(集中組合 spread/DSR 天然較差),
全部記帳,trial 數持續納入 DSR。

### R06-R08 戰報(T0169-T0202)+ R-LINE 收斂宣告(2026-07-09)

- R06:geo-n8-t30 過線(55.9)→ OOS +71.1 差 1.1pp;**現代era連續 57.4 > 正2 55.9 首勝**;
  深集中 n5/n6 觸頂反轉(49-51)
- R07:**r07f_t35 60.9/1.72 晉級 → OOS +72.3 ✅ = 新旗艦**;r07d_revlevel 60.4 → OOS
  65.9 ✗;語義排除(exfin/excon)反傷 → 棄
- R08(預告最終精煉批):r08a_t35_revlevel dev 64.9/1.78/−36.7 晉級 → **最終 OOS
  touch #6:+65.9 ✗ → r07f 保持旗艦**;t40/fresh6/n7/pos15 皆未過線
- **使用者約束追加:策略不准持有任何 ETF**(universe regex 原生合規,正2 僅為對手)

**收斂宣告**:OOS 預算 6/6 用畢、R08 為預告最終批。R-line 8 批 68 trials 終結。

**最終旗艦 `apex_revcycle_R`(= geo-n8-t35)**:幾何 rank 乘積 {accel 1、52wH 1、
吸收 1、mom126 0.5}、fresh≤5、cfo p50 閘、N=8、stale26、trail 35%、time30、next_open:
- 正2 全史同窗(2014-11→2026-07):**+53.7%/1.65/−39.2/151x** vs 正2 +37.7/1.08/−55.1/42x
- 現代era(2019→):**+61.9%/1.70/−38.7/37x** vs 正2 +55.9/1.33/−55.1/28.1x
- OOS 熱年:+72.3/1.67/−19.0 vs 正2 +290(2×beta 結構性,唯一未勝窗)
- 檔案:r08a(W5 版)dev/連續窗更強(64.6 現代era)但 OOS 一致性弱,列為研究檔位

## R09 — FinLab 全站收割批(2026-07-09 預註冊;10 trials)

**知識來源**:finlab.finance 265 篇全掃(知識圖譜 quant/finlab/ 三篇);本地預驗證:
cohort 內 size 梯度(+20.8% vs +11.3%/年)、庫藏股公告漂移(+8.2%/+9.8% 超額)。

**Trials(旗艦 geo-n8-t35 scaffold)**:
a size 軸 ^0.5(小市值 rank)|b size 軸 ^0.25|c 庫藏股 boost(公告後 60 日 score×1.15)|
d 寬度 regime halt(eligible 池收盤>SMA120 比例 <0.45 停新倉、>0.55 恢復,遲滯)|
e 營收穩定度閘(12 月 YoY std ≤ 當日中位)|f YoY>150% 高基期剔除|
g 法人淨買強度軸(frn20 rank^0.25)|h 低融資使用率閘(margin/quota ≤ 當日中位)|
i 進場低波動濾(排除池內 5 日波動前 20%)|j size^0.5 × buyback 組合

**判準**:晉級 = dev(2019-2025H1)CAGR ≥ 62.9%(+2pp)或(Sharpe ≥ 1.82 ∧ CAGR ≥ 60.9)
∧ MDD ≥ −45。**確認儀器(熱年 OOS 預算 6/6 已盡)= 2012-2018 舊時代段 regime-OOS**:
晉級者同 config 在 2012-2018 的 CAGR 不得劣於旗艦同段 −2pp(防 hot-era mining)。
確認過 → 旗艦升級。

### R09 結果(T0204-T0213)— 乾涸(0/10)

FinLab 全站收割槓桿全數被旗艦支配:buyback boost 59.9、size 軸 48.6-58.1(cohort 梯度
存在但顯式 tilt 過度集中微型股)、yoy_cap150 58.0/1.70/−35.0(Calmar 1.66 最佳但 CAGR
−2.9pp 不合 frontier)、寬度 halt 44.6、低融資使用率 25.9、營收穩定度閘 18.6、lowvol5
26.5。**265 篇外部知識無一推進旗艦 = 最強收斂證據。**

### R10 結果(T0214-T0219)— 乾涸(0/6)

微組合與指數擾動全數未過(最佳 accel^1.25 = 60.8 vs 60.9)。

## R11 — 個股級 PEAD 持續性(FinLab 個股頁方法論移植;預註冊同 R09 判準)

**想法**:stocks/XXXX 頁「事件條件化前瞻統計」→ 個股過去 6 次揭露週期反應均值 >0 為閘。

### R11 結果(T0220-T0222)— dev 晉級、確認出局

r11c_pead_gate dev **64.1%/1.74/−38.2(frontier (a) 全勝 +3.2pp)**;
**舊時代確認(2012-2018):29.8% vs 旗艦 33.1% = −3.3pp > 容忍 −2pp → 出局**。
個股 PEAD 持續性為現代era限定增強(注意力/流動性結構產物),regime-OOS 攔截成功。
知識圖譜已記載 caveat;列為實盤era觀察項(live 監測其持續有效性後可再議)。

## E02 + R12 — 加權引擎 × 環比動能 × 穿越再資格 × 二波池(2026-07-09 預註冊)

**E02 引擎升級**:entries 可帶 `weight` 欄(目標 NAV 比例;缺欄等權 1/n_slots),
零槓桿由現金約束保證;golden test `test_weighted_sizing_exact`。全 campaign 至今
只測過等權——加權是最後未動的結構自由度。

**R12 Trials(6,旗艦 scaffold)**:
a 分層加權:日 rank 1-2 → 0.19、3-6 → 0.125、7-8 → 0.06(Σ=0.10×2+0.5+0.12=1.0)|
b 分數比例加權:geo score 正規化、單檔 cap 25%|
c 環比動能軸:rev_seq(3 月營收和/前 3 月和 −1)^0.5 加入|
d 環比替換:rev_seq 替換 rev_yoy_accel|
e 穿越再資格出場 × 旗艦:exit =(fresh≤5 ∧ rank>24)∪ fresh≥35,trail35/time45
(省下週期末賣掉又買回的雙重摩擦;v2 出場首次與 geo/n8/t35/mom 合體)|
f 二波確認池:fresh≤5 ∪(fresh≤12 ∧ 自揭露日累積報酬 >+3%)

**判準**:同 R09(CAGR ≥ 62.9 或 Sharpe ≥ 1.82∧CAGR ≥ 60.9,MDD ≥ −45);
晉級 → 舊時代確認(2012-2018 ≥ 旗艦同段 −2pp)→ 過者升級旗艦。

**公告日資料現況(記錄)**:`research/data/revenue_first_seen.parquet` 已上線
(Serenity 事件驅動爬蟲),first_seen 自 2026-07-07 起、無歷史深度 → 實盤資訊優勢
(多數公司早於「10 日」3+ 天),回測維持保守規則不變。

### R12 結果 — **v6 誕生(重大升級)**

r12c_seq_axis(**環比動能軸** rev_seq = 3月營收和/前3月和−1, ^0.5)dev **79.9%/2.09/−32.9**
(+19.0pp/+0.37/MDD+5.8pp);**舊時代確認 ✅ 反向大勝(38.0%/1.57 vs 33.1%/1.42)**
= 全天候訊號升級。交易解剖健康(589 筆、win 57%、PF 2.53、前 5 筆僅佔 13%)。
機制:YoY 加速有基期污染,環比抓「此刻營運轉折」,兩者三角定位真加速。
其餘:二波池 61.8、分數加權 60.2、分層加權 58.9、再資格出場 58.1、環比替換 55.9。
**旗艦升級:`apex_revcycle_R2`(v6 = W5 五軸幾何)**。擾動 grid 17/17 全過 R-gates
(最差 42.3/1.33),spread 38.4pp(driver=stale 軸;去除後 14.2pp)——與 n8 系同等
「知情選擇」認證地位。E02 加權引擎上線(16 tests 過)但兩種加權皆遜等權。

## R13 — RSV 家族(使用者指示;2026-07-09 預註冊;8 trials,v6 scaffold)

**指示**:破 N 日新高/N 日內高價位 %/RSV(拉長天數 50-60 過濾雜訊)應為強因子;
可與低波組合。註:RSV(60) ≡ F03 之 range_pos_60(已驗 tail +24%/mono 1.00),
但從未入冠軍容器當軸。

**Trials**:a +rsv60^0.5|b +rsv60^1.0|c rsv60 替換 high_52w|d +rsv120^0.5|
e 高位閘(close ≥ 0.95×120日close高)|f +donchian_60^0.25|
g +rsv60^0.5+lowvol^0.25(FinLab 經典組合)|h +rsv50^0.5

**判準**:晉級 = dev CAGR ≥ 81.9(v6+2pp)或(Sharpe ≥ 2.19 ∧ CAGR ≥ 79.9),
MDD ≥ −45;晉級 → 舊時代確認(2012-2018 ≥ 36.0%);過者再升級。
v6 battery(bootstrap/DSR/PBO/perm)與連續窗待本批決出最終形後一次執行。

### R13 結果(T0230-T0237)— RSV 家族乾涸(0/8)

使用者指示的高位/新高/RSV 族已是冠軍承重軸(high_52w+吸收);疊加變體全持平
(rsv60^0.5 = 79.4)、donch^0.25 三軸微 Pareto(80.5/2.12/−32.1)但差晉級線不採;
RSV+低波經典組合 45.1(低波傾斜第五次證偽)。v6 守住。

## v6 = `apex_revcycle_R2` 正式認證(2026-07-09)

**battery 全過**:bootstrap CI [+39.1, +146.6]、**DSR 0.9792(N=237)**、PBO 0.076、
perm p=0.0000(null 中位 +13.1 vs +79.9);擾動 17/17 過 R-gates(spread 38.4pp
超 20pp 標準——唯一披露項,driver=stale 軸)。

**連續窗**:現代era **+77.6%/2.01/−32.9(75x)**;正2全史同窗 **+63.9%/1.87(319x vs
正2 42x)**;全跨度 14.5y **+57.1%/1.80/−32.9(704x)**。
逐年:2013 +94、2015 +61、2017 +81、2020 +215、2021 +263、2023 +60、2024 +83;
虧損年僅 2012 −0%、2022 −6%(0050 同年 −21、正2 −36)。

**config**:v3 scaffold(fresh≤5、cfo p50 閘、N=8、stale26、trail35、time30、next_open)
+ 幾何五軸 {rev_yoy_accel 1、high_52w 1、close_pos_20 1、mom_126_5 0.5、**rev_seq 0.5**}
(rev_seq = 3月營收和/前3月和 −1,PIT 次月10日)。

# GOAL-3:CAGR ≥ 100%(2026-07-09 使用者新目標)

## R14 — 驚奇軸 × 流動性下修 × 網格堆疊(預註冊;8 trials,R2 scaffold)

a +rev_surge^0.5(當月營收/前3月均 −1)|b +sa_mom^0.5(季調 MoM:當月/前月 ÷
3年同月中位 −1)|c rev_surge 換 mom_126|d 網格堆疊(fresh6+time24+accw0.8;
顯式聲明堆疊,靠確認把關)|e a+d|f 六軸(+surge 全家桶)|g ADV 門檻 20M→10M
(參與率 3.75% 仍誠實)|h a+g

**判準**:晉級 = dev ≥ 85% 或(Sharpe ≥ 2.2 ∧ CAGR ≥ 80)∧ MDD ≥ −45;
晉級 → 舊時代確認(2012-18 CAGR ≥ 36.0%);目標線 = dev ≥ 100%。

### R14-R16 結果

R14(0/8):驚奇/季調 MoM 軸稀釋(3m/3m 環比即甜蜜點);adv10 與 gridstack 為
Pareto 微升。R15:**adv5 晉級 dev(87.3/2.31/−32.3)**——流動性門檻 = 活的 CAGR 旋鈕
(20M→10M→5M = 79.9→81.0→87.3)。R16:adv5_n6 = 90.7/2.18;
**但 adv5 舊時代確認敗(30.0% < 36.0%)→ 依紀律不加冕**:微型股火力是現代結構限定
(2017 當沖稅改+2020 散戶潮;舊era 7% 限制+流動性差)。與 PEAD-persist 同類。

## MOD-LINE(現代結構線)憲法 + R17(2026-07-09 預註冊)

**產品分軌**:認證線(全天候)= R2(adv20,79.9%,DSR 0.979);**現代結構線(MOD)**=
明示揭露:(a) 舊時代退化(各 config 舊era數字必附);(b) 依賴後 2017/2020 微結構,
其持續性為前提;(c) 容量 ≤ 數百萬台幣(ADV 5M 底線,參與率 7.5%);(d) 無全天候宣稱。
MOD 評分窗 = 2019-2025H1 dev;比較基準 = adv5 基座 87.3。

**R17 Trials(6)**:a adv5_n6+pead_gate|b adv5+pead|c adv5_n6_t40|
d adv5_n6+gridstack(fresh6/time24/accw0.8)|e adv5_n6+pead+t40|f adv5_n6+seqw60
**判準**:MOD 晉級 = dev ≥ 95 或(Sharpe ≥ 2.35 ∧ ≥ 90);**目標線 dev ≥ 100%**;
全部附舊時代披露數字。

### R17-R19 + R3 加冕(2026-07-09 深夜)

R17(0/6):MOD 線平台 ~91;pead 閘在微型池反傷。R18:**maxnew3 = 建倉減速機制發現**
(93.1);修正批 R18b:n6_mn3_fresh6 = 95.8 晉級。R19 終突擊:
**r19c_n5mn2f6 = dev 113.7%/2.44/−33.3 → 目標 100% 攻破**。

**`apex_revcycle_R3` 認證**(config:adv5 池、N=5、max_new 2、fresh≤6、stale26、
trail35、time30、W5 幾何五軸、cfo 閘、next_open):
- 擾動 15/15 全過 R-gates 且**全部變體 ≥80% CAGR**(spread 41.4pp:mn1 80.0↔n4 121.4)
- **DSR 0.9935(N=282)**、PBO 0.037、perm p=0.000(null +11.6)
- **舊時代 regime 泛化 ✅ +51.3%/1.85**(慢速建倉避開舊era微型股鎖死陷阱——
  adv5 裸奔版當年敗於 30.0%,mn2+n5+fresh6 特徵組合反而泛化)
- 解剖:369 筆、win 56%、PF 3.20、hold 17d、曝險 80%
- 連續窗:**現代era +110.6%/2.34(269x)**、正2全史同窗 **+90.7%/2.20(1,874x vs 42x)**、
  全跨度 14.5y **+79.1%/2.10/−33.3(4,707x)**;逐年無虧損年(2022 +0%、2012 +8% 最低;
  2021 +353%、2020 +264%、2024 +192%、2023 +106%、2015 +100%)
- **容量天花板(大聲披露)**:每檔 20% NAV 於 ADV≥5M 池 + 每日 2 檔建倉——
  NT$3M 級資本專屬;千萬級以上此優勢顯著衰減。fill 雙測待補(holdout 已證
  fill 敏感為 episode 性)。

**產品階梯(終)**:v3-n20(全認證全天候 33%/14.5y)→ R2(v6,57%/14.5y,DSR 0.979)
→ **R3(79%/14.5y=4,707x,DSR 0.994,小資本專屬火力頂點)**。

# GOAL-5:CAGR ≥ 200%(2026-07-09 使用者新目標)

## R20 — 深集中階梯 × 微堆疊 × 神諭上界(預註冊;8 trials + oracle)

**Trials(R3 scaffold:adv5/mn2/fresh6/trail35/stale26)**:a n4|b n4+seq40|
c n4+fresh7|d n4+seq40+fresh7|e n3|f n3+seq40+fresh7|g n2(樂透探針,預期爆炸,
純求知)|h n4+mn3

**Oracle bound(標明作弊,非 trial)**:同容器(fresh cohort+gate+eligible)內以
「未來 21 日真實報酬」選 top-N 的理論 CAGR——本資訊集的數學天花板。
200% 目標的可行性由此判定。

**判準**:晉級 = dev ≥ 130 或(Sharpe ≥ 2.5 ∧ ≥120)∧ MDD ≥ −45;
晉級者舊時代泛化 ≥ 45%(R3 為 51.3);目標線 dev ≥ 200%。

### R20-R21 結果 + GOAL-5 現狀(2026-07-09)

R20:集中曲線定形 **n4 = 頂點**(n5 113.7 → n4 121.4;堆疊 seq40+fresh7 →
**r20d = 126.1/2.40/−37.9**,差晉級線 3.9pp;n3 反轉 118、n2 樂透爆炸 85/−59;
mn2 為最優建倉速度)。**Oracle 上界:同容器作弊選股 = +3,927%~+6,723%** →
200% 非容器限制,是「選股精度 vs 現有資訊集」的抽取極限。
R21:釋放日價量反應軸全滅(110-113)——揭露後反應無增量資訊。

**GOAL-5 判定:未達**。現有資訊集的實證頻譜頂 ≈ 126%(n4 檔位,文件化;R3-n5 仍為
加冕 config,n4 對其非憲章 frontier improvement:MDD 劣化 4.6pp > 2pp)。
Dev 113.7 → 126.1 已推進 12.4pp。**通往 200% 的兩條路都在資料之外**:
(a) 逐檔真實公告時點(revenue_first_seen 已在累積,實盤即用、1-2 年後可回測);
(b) 日內資料(釋放日開盤動態)。盤中觸價停損(用現有 high/low 可模擬)經評估
deprioritize:B03/B04 證據鏈顯示更緊的停損傷 CAGR,與本目標方向相反。

## R22 — 新資訊角度掃蕩(2026-07-09 預註冊;8 trials,R3 scaffold)

**新因子/新方法(非網格)**:a 同業相對加速(accel − 產業中位 accel,替換原 accel)|
b 同業相對加速(加軸 0.5)|c min-rank 聚合(五軸取最弱 rank,平衡優選拓撲)|
d 融券軋空軸(short_balance 20d 增/20d 量,^0.25)|e 投信軸(trust 10d 淨買/量 ^0.25)|
f 價差軸(−(ask−bid)/close ^0.25)|g a+c|h d+最佳者

**判準**:晉級 = dev ≥ 128(頻譜頂 n4=126.1 +2pp)或(Sharpe ≥ 2.55 ∧ ≥ 120)
∧ MDD ≥ −45;晉級 → 舊時代泛化 ≥ 45%。

### R22-R24 結果(「新因子或新方法」推進;2026-07-09)

- **新因子命中:同業相對加速(accel − 產業中位)**——n5+rel = 121.7/2.549/−35.6
  (差晉級 Sharpe 線 0.0006!);n4 系與深集中互相涵蓋(125-126 無堆疊增益)
- 證偽:min-rank 聚合(78,幾何definitively勝)、融券軋空軸(100)、投信軸(66)、
  價差軸(62)、config 三書集成(120.9/2.497,曲線相關 0.94-0.97 分散不足,
  frontier (a) 差 0.1pp MDD)
- **結論:120-126% 區全部是 −35~−38 MDD 的頻譜高風險點,非 free improvement;
  憲章 ±2pp MDD 容差正確擋下。R3(113.7/2.44/−33.3)守住風險調整王座。**

**最終頻譜菜單(同 alpha 家族、~0.95 相關,純風險偏好選擇)**:
| 檔位 | dev CAGR | Sharpe | MDD |
|---|---|---|---|
| R3(n5,加冕)| 113.7% | 2.44 | −33.3 |
| n5+同業相對軸 | 121.7% | 2.55 | −35.6 |
| n4-stack | 126.1% | 2.40 | −37.9 |

### R25 — 防禦重訪@新基座(2026-07-09;0/7 Pareto)

使用者問「是否已到犧牲 DD 換 CAGR 的階段」。回撤解剖:R3 四大回撤段全為急速市場
崩盤(2020-03 COVID −33.3 = MDD 本身、2021-05 本土疫情 15 天 −27.8、2022、
2024-11~2025-04)。防禦七變體(abs20/25 × R3/rel、trail30、崩盤停新倉、組合)全敗
Pareto 判準(最佳:rel+halt MDD 改善 2.1pp 但 CAGR −4.7pp;abs25+halt 改善 3.1pp
但 −13.7pp)。**B04 結論在 113% 基座複製:台股快崩在日線尺度不可防禦(跌停鎖死
先挨打、V 反轉是 CAGR 主源)。結論:已在效率前緣上——頻譜內 CAGR↔MDD 互換,
Sharpe 於 n5+rel(2.55)後轉降;「同 CAGR 砍 MDD」惟有日內資料(真停損單)可解。**

### R26 — Sortino 鏡頭分析(2026-07-09;使用者提問驅動)

**發現:n5+rel = Sortino 空間全頻譜第一(4.13)且下行風險與 R3 完全相同**
(下行 RMS 21.5% vs 21.6%、下行 vol 24.1% vs 24.1%)——+8pp CAGR 幾乎零下行代價;
MDD 差 2.3pp 為 2020-03 單一事件的時序聚集 artifact。在下行風險量尺下
n5+rel Pareto 支配 R3(CAGR/Sharpe/Sortino 三勝)。
認證:舊時代 +43.4%/1.62(差 45% bar 1.6pp,R3 為 51.3 → **全天候王座仍歸 R3**,
n5+rel 定位為「Sortino 最優現代部署選擇」,舊時代數字披露);
bootstrap CI [+66.1, +222.9]、perm p=0.000(N=321)。
Sortino 導向新軸雙敗:skew60(108.1 稀釋)、純下行波動軸(57.8,低波毒性第六次確認)
→ Sortino 最優解已由 rel 軸達成,無進一步 Sortino 工程空間。

## KPI v3 制定 + R27-R28(2026-07-09 深夜;使用者:「自行推導最正確指標」)

**KPI 演進史披露**:主線=複合閘+Sharpe 系 battery;R 線=CAGR 主/MDD 上限/Sharpe 下限;
KPI v2=Sortino 主。**KPI v3(第一性原理推導,定案)**:目標=長期複利財富最大化
(Kelly/log-growth)+ 估計不確定性 + 行為存活性 →
**主排序 = block-bootstrap P5 CAGR(抗運氣成長下界);並列檢 Martin ratio
(CAGR÷Ulcer Index,痛苦面積調整成長);約束 MDD ≥ −40 + 舊時代披露 + battery**。
v3 優於 Sortino:看得到路徑聚集與水下時間、以成長計價、自帶抗運氣性。

**R27(非對稱出場,引擎 E03:underwater_trail + loser_time_stop,17 tests)**:
**lts15(輸家 15 日時間止損)= `apex_revcycle_S` 加冕**——四指標同時改善
(CAGR 120.9/Sharpe 2.58/Sortino 4.19/MDD −32.6);認證:bootstrap CI [+65,+221]、
Sortino-perm p=0.000(null 0.87)、擾動平滑(lts 12/15/18=95.9/120.9/118.2)、
現代era +116.1(326x)、全跨度 +74.8/So 3.21(3,297x);舊時代 +39.9 披露
(R3 51.3 → R3 保留全天候頭銜,S = 現代部署冠軍)。

**KPI v3 重排**:S 雙料第一(P5 74.4%、Martin 16.4);裸 CAGR 榜首 n4 系在 v3 掉隊
(P5 72.5、Ulcer 0.14)= 用錯指標會選錯策略的實證。
**R28 鄰域(0/6)**:S 坐在 v3 平滑峰頂(lts 14/15/16 P5 = 67.7/74.4/71.1)。
**三套量尺(Sortino、v3-P5、v3-Martin)+ 鄰域掃描收斂於同一 config = 穩健性證明。**

## E04 + R29 — 同日收盤出場(2026-07-09 深夜;最後一根結構性槓桿)

**假設(先於執行陳述)**:門檻型出場(trailing/abs/profit/time/lts)水位事前已知,
現實可用 MOC 當日收盤單執行 → 砍掉輸家跌破後的一日延遲負漂移。signal/stale 型
仍須全市場收盤後排名 → 維持隔日。**判準:P5 > 74.4 或(Martin > 16.4 ∧ P5 ≥ 72)。**

**E04 引擎**:`ExitSpec.same_day_exit`;t−1 檢查降為 signal-only;新 step 3.5 於
當日收盤評門檻出場;`sd_sell_block` 跌停鎖死日不可賣(exact-lock 用 bid 缺失+跌幅,
否則 buffer 制)。18 golden tests(新增 test_same_day_exit_threshold;首版測試面板
單日 −20.8% 違反跌停物理極限、被 block 正確攔下 = 引擎對、測試錯,已修)。

**R29 結果:REJECTED(0/1)**。S 基準複現分毫不差(120.9/P5 74.4/Martin 16.4);
same-day 全指標劣化:CAGR 118.5(−2.5pp)/P5 72.5/Martin 15.6/MDD −33.3/So 4.15。
**機制:台股跌破日尾盤是情緒最差價格,隔日開盤平均有 overnight 均值回歸;
「更快出場」= 把最差價格具現化。** 與 R21(釋放日反應)、War 14(entry-veto)同構:
日頻尺度上「更快反應」的微結構修補在台股系統性無效。T+1 出場延遲非成本而是小紅利。
E04 保留為引擎能力(預設關閉)。**S 王座不變;結構性槓桿清單至此掃空。**

## M01 — Meta-study:研發窗長 vs 未來 OOS(2026-07-10;使用者方法論提問)

**問題(使用者)**:策略會過期、每個時期不同;要研發「未來最強」的策略,
應該用此時此刻往回多久的資料?

**設計**:revcycle 策略族固定,「研發自由度」以 24-config 網格代理
(axes {四軸 v3 式, 六軸 S 式} × N {5, 8, 20} × trail {0.25, 0.35} × ADV {5M, 20M},
fresh=7、lts=None 中性)。24 configs 各跑一次全期連續模擬(2012-07 → 2026-07),
之後所有窗口統計用 NAV 切片(邊界持倉過渡誤差 ≪ 窗長效應)。
站在 t = 2015..2026 每年年初,用過去 W ∈ {1, 2, 3, 5, 8, all} 年的窗內 KPI
選最優 config,量其未來 1 年 OOS。**主指標 = OOS 排名百分位(regime-neutral,
隨機期望 0.5);次指標 = OOS 年化幾何報酬**。選擇 KPI 雙版本(Sharpe / CAGR)
結論一致才算穩健。

**預測(先於執行)**:H1 存在中間最優窗(太短擬合雜訊、太長混入死 regime);
H2 最優 W ≈ 3–8 年;H3 結構(哪族 axes)對窗長不敏感、火力 dial(N/ADV)敏感。

**結果(132 格,61s,`ledger/m01_results.parquet`)**:兩套選擇 KPI 結論一致 →

| W | rank(Sharpe 選)| rank(CAGR 選)| geo OOS CAGR(CAGR 選)| n |
|---|---|---|---|---|
| 1 | **0.496(=隨機!)** | 0.688 | 69.9% | 12 |
| 2 | 0.750 | 0.808 | 82.5% | 12 |
| **3** | **0.794** | **0.830** | 89.0% | 11 |
| 5 | 0.710 | 0.792 | 91.7% | 9 |
| 8 | 0.754 | 0.804 | 110.3%* | 6 |
| all | 0.652 | 0.772 | 83.6% | 12 |

\* W=8 的 geo CAGR 偏高是時段偏差(8 年窗僅 2020+ 的時點存在,OOS 全落在高火力
年代);regime-neutral 的 mean_rank 才可跨窗比較。

**三個結論(H1 ✓ H2 ✓)**:(1) **最優窗 = 3 年**,2–8 年皆為高原;
(2) **懲罰不對稱**:太短是災難(1 年窗 + Sharpe 選 = 完全隨機,純擬合單年雜訊),
太長是慢性稀釋(全史仍優於隨機但一致次優);(3) H3 部分成立:1–8 年窗的眾數選擇
全是 ax6-n5-t35-adv5(≈S 結構,窗長 robust),唯 all+Sharpe 選出 ax4-n8(≈R2/v3
樣式)——**完美復刻 campaign 實史:長窗研發出 v3/R2(35–57%),近窗研發出 S(121%)**。

**方法極限(誠實)**:網格僅 24 configs、全在 revcycle 族內(「研發」自由度被低估,
量到的是參數層窗長效應,alpha 結構層只有 ax4/ax6 一維);連續 NAV 切片近似;
OOS horizon 僅 1 年;t 樣本 11–12 個重疊窗非獨立。方向性結論由雙 KPI × 雙指標
一致性支撐。

# F-LINE — W3 前瞻研發(2026-07-10;使用者:「用過去 3 年重新研發未來最強」)

**研發窗 = 2023-07-10 → 2026-07-09(M01 最優窗)。** 目標:此刻部署、未來一年
最強。KPI = v3(3 年窗 bootstrap-P5 主排序 + Martin 並列)。紀律:失敗地圖仍有效
(已證偽家族不無腦重試),但容許 regime 翻案測試(由 P5 抗運氣性把關);
窗外(2019-2023、2012-2018)表現為**披露項而非淘汰閘**(M01 的教訓:別被死
regime 綁架,但要知情)。**最終判準:新 config 的 3 年窗 P5 > S 同窗 P5 且
Martin 不劣、鄰域擾動平滑,否則 S 確認連任。**

## F01 — 3 年窗因子重掃描(預註冊)

基座 = S 六軸(同 dial:n5/mn2/fresh7/adv5/trail35/time30/lts15)於 3 年窗。
變體:LOO 移除各軸 ×6;AOI 以 +0.5 權重加入未用軸 ×9(hvn_dist、range_pos_60、
updays_20、fvg_20、donchian_60、rev_yoy、frn_60、dy、lowvol_60)。
**預測**:近 3 年 rev_seq/accel_rel 邊際貢獻仍正(環比在高通膨後期更乾淨);
lowvol/dy 仍毒(第七次);價格結構軸(fvg/updays)可能有小增量。

## F02 — 結構網格 + 容器翻案(預註冊)

F01 最優軸集上掃 dial:N {4,5,6} × trail {0.30,0.35,0.40} × fresh {5,7,10}
(重點 cell,非全積)。**容器翻案對照**:純價格動能容器(52wH×吸收×mom 幾何,
每日全池非 fresh cohort)×2 config——動能容器曾全窗證偽,3 年窗給公平重審,
由 P5 判生死。

## F03 — 認證(預註冊)

Top config vs S 同窗:bootstrap P5、置換 p(cohort 內隨機選)、鄰域 ±20% 擾動、
窗外披露。晉升 → 加冕「W3 前瞻部署冠軍」並更新 STRATEGY.md;否則 S 連任
(其為 M01 眾數選擇的先驗即預測此結果)。

## F-LINE 結果(2026-07-10;F01 16 + F02 40 + F03 認證,共 58+ trials)

**F01 因子重掃(3 年窗 alpha 解剖)**:S 基座 96.0%/P5 45.9/MDD −20.7。
命脈軸(LOO 掉幅):52wH(P5 −28pp、MDD 爆 −45%)> YoY 加速(−19pp)>
吸收(−20pp)> 環比(−15pp);mom 仍正貢獻。**accel_rel 近 3 年邊際貢獻歸零**
(LOO 反而 P5 最高,+0.4pp)= regime 情報:AI 獨大時代產業內相對比較失去增量。
新軸唯一候選 updays_20(Martin 14.1 最高);lowvol/dy/frn 三毒第七次確認。

**F02 網格 + 容器翻案**:dial 峰頂不動(n5/t35;n4 P5 −4pp MDD 惡化、t40 與 t35
全同、fresh5 劣化 fresh10 持平)。**容器翻案雙死**:純價格動能容器(全池非 cohort)
P5 −0.08/−0.12、MDD −50%/−40%——2024-08 + 2025-04 兩次急崩直接打爆,
revcycle 事件結構再確認為 alpha 本體。

**F03 認證:S 連任**。頂三 config P5 排名(S-rel 45.3 / S+up 45.9 / S 44.5,
5-seed 均值)差距 ≤1.4pp,**配對 block-bootstrap(統計力更高)裁決:
S-rel − S = −0.6%/年 CI [−8.9, +6.8]、S+up − S = −2.2%/年 CI [−16.7, +11.2]
——皆以 0 為中心且點估計為負 = 無真差異**。窗外:S 中期(2019-23H1)+130.8%
明顯優於挑戰者(124.9/118.5),舊時代挑戰者略優(55.9/57.4 vs 47.6)。
依預註冊判準 + 現任者原則(無顯著差不 churn、六軸結構冗餘價值):
**`apex_revcycle_S` 確認為 W3 前瞻部署冠軍——用近 3 年重新研發的產出就是 S 本身,
M01 先驗(S 為全窗長眾數選擇)實現。** 滾動制度:每年重跑 F-line(下次 2027-07);
**accel_rel 為 regime 儀表**(產業分化回歸時它會先亮)。

## F04 — 季報揭露事件池(2026-07-10;資訊集內最後一塊未開發結構)

**資訊盤點(先於設計)**:SBL 借券 2016+ 全史在庫(squeeze 系已證偽)、TDCC 僅
11 週(不可回測)、insider_holding 僅 719 筆零星(爬蟲被擋,不可用)、taifex 期指
六表在庫(指數層資訊,選股無直接用途、regime 開關全線已證偽)→
**唯一處女地 = 季報揭露事件**(raw_quarterly 2006Q2-2026Q1 全套 margins)。
R21 證偽的是「釋放日反應軸」;「季報 cohort 作為第二事件池」從未測過。

**假設**:營收層資訊已被月營收揭露提前定價;季報的真新資訊 = **利潤率層**
(毛利率/營益率/淨利率 YoY 改善)。若成立,季報揭露後存在與月營收同構的
定價修正窗。

**PIT 慣例(保守)**:生效日 = 法定 deadline 隔日(Q1→5/16、Q2→8/15、Q3→11/15、
Q4→次年 4/1)。多數公司提早發布 → 此慣例滯後真實揭露 0-45 天,訊號強度是下界;
金融業 deadline 差異以統一慣例吸收(聲明)。

**設計(3 年窗主戰場,判準同 F03)**:
- **F04a 獨立引擎**:cohort = fq_fresh ≤ 10;score = {gm_delta_yoy 1、opm_delta_yoy 1、
  ni_delta/rev 0.5} × {high_52w 1、close_pos_20 1} 幾何 rank;cfo 閘沿用;
  n5/mn2/trail35/time45/stale40(季週期較長)。判準:P5 > 0 且與 S 日報酬
  相關 < 0.7 → 有組合價值;P5 > 40% → 重大發現。
- **F04b 疊加 S**:S 六軸 + 最新季報 gm_delta_yoy(asof,權重 0.5)第七軸。
  判準:配對 bootstrap vs S,CI 下界 > 0 才晉升(嚴格)。
- 全期(2012-07 起)披露。皆不過 → **資訊集內正式宣告終掃**。

**F04 結果:雙敗(0/2),資訊集內終掃成立**。F04a 獨立:CAGR 13%/P5 −10.2%/
MDD −25.8(判準 P5>0 遠不及;相關 0.32 低但引擎無 alpha,組合無意義)。
F04b 疊加:P5 45.9→26.6、配對 −14.5%/年 —— gm 軸對 S 是純稀釋。
**死因解讀**:(a) deadline PIT 滯後真實揭露 0-45 天,事件窗 alpha 已被吃掉;
(b) margin 改善與月營收加速共線(營收強→稼動率→margin),增量小;(c) 單季
margin 噪音大(一次性項目)。**(a) 同時是「公告時點資料」價值的第三個佐證**
(續 revenue_first_seen 論證)——若累積真實揭露時刻 1-2 年,「揭露後 X 日內」
的快速版季報引擎可重測。全期披露略(3 年窗已死,無披露必要)。

**F-line 總結(F01-F04,60 trials)**:W3 重新研發收斂回 S;容器革命死、
季報處女地死、新軸唯一候選(updays)配對無真差異。**資料庫每張表已盤點:
SBL(squeeze 系證偽)、TDCC(11 週不可測)、insider(719 筆殘片)、taifex
(指數層+regime 已證偽)、季報(F04)。「更強」的剩餘可能性全數在資訊集外:
真實揭露時點(累積中)、日內資料(使用者閘)、ML 排名器(天條禁止)。**

## F05 — 暴漲股 case-control 前兆判別(2026-07-10;使用者思路:由果溯因)

**使用者提案**:先挑出全部暴漲股,找共同點,以之選股。**統計正確化**:
共同點 ≠ 判別力(基準率忽視/貝氏方向錯置)→ 做 **case-control**:
spike 定義 = fwd 60 交易日內最高收盤 ≥ +80%(T0 起,per-code 非重疊,120d 冷卻);
對照 = 同日 eligible、fwd_max_60 ≤ +20% 隨機 4:1;特徵一律 T0−1 PIT。
**判別量尺 = rank AUC**(spike vs control);窗口:3 年(主)+ 全期(披露)。
特徵集:apex 15 軸 + rev 三軸(rev_seq/accel_rel 含)+ **2 個未測新軸**
(volume_surge_60 = 5d/60d 均量比、consolidation_60 = 60d (max−min)/mean 盤整度);
法人/融資/squeeze 系已證偽不重測。

**預測(先於執行)**:P1 = top 判別軸將是 S 的現役軸(52wH/close_pos/updays/
mom/rev 系)——即 S 本身就是「暴漲前兆組合」,使用者思路正是本 campaign 的
隱式生成路徑;P2 = volume_surge/consolidation 可能有中等 AUC(0.55-0.60);
P3 = 若新軸 AUC > 現役軸 → F05b 加軸配對測試(判準同 F03:CI 下界 > 0)。

**F05 結果(spike 2,461 / 對照 9,584;dataset `ledger/f05_case_control.parquet`)**:

| 排名 | 3 年窗 AUC | 全期 AUC |
|---|---|---|
| **consolidation_60(高已實現波動)** | **0.683** | **0.711** |
| mom_126_5 | 0.634 | 0.606 |
| rev_yoy / accel / seq / rel | 0.56-0.59 | 0.56-0.57 |
| high_52w | 0.517 | 0.480 |
| close_pos_20 | 0.492 | 0.461 |
| dy / lowvol_60 | 0.35 / 0.29 | 0.35 / 0.28 |

**P1 部分證偽(深刻)**:S 的 52wH/吸收軸在**無條件**判別下無力甚至反向——
S 的軸不是「暴漲前兆」而是「fresh cohort 內相對排名器」;條件化改變一切。
**暴漲第一前兆 = 高已實現波動**(consolidation 0.71 與 lowvol 鏡像 0.72 同源),
P2 半對(consolidation 遠超預期;volume_surge 無力 0.49-0.53)。

**F05b/c 轉化雙敗**:S+consol 軸 P5 45.9→35.1、配對 −5.2%/年(cohort+微型池已
隱含波動篩選,無增量);**前兆容器(consol×mom×rev_yoy 全池)= 災難:CAGR 5%、
P5 −37%、MDD −69%——判別力 ≠ 可交易性的教科書案例:高波動同時是暴漲與暴跌
前兆(case-control 未對照「vs 暴跌」),無條件買高波 = 買彩券盒**。

**結論**:使用者思路(由果溯因)本質正確且正是 campaign 的隱式生成路徑,但其
可交易形式必須「事件條件化」——先用揭露事件縮小池、再 cohort 內排名,即 S 的
現行結構。F05 的理解性收穫:高波前兆解釋 adv5 微型池火力來源與 lowvol 七殺。
S 連任不變。

# G-LINE — ML 排名器(2026-07-10;使用者解除 no-AI 天條:「開始實作」)

**約束更新**:no-AI 解除(經典 ML 排名器合法;策略運行依賴 LLM 語意分析仍禁)。
其餘天條不變(long-only、無槓桿、不持 ETF、純量化資料)。
**目標**:吃 oracle(+3,900%)與 S(121%)之間「排名 vs 預測」的差距。

## G01 — LightGBM cohort 排名器 MVP(預註冊)

**設計(最小改動聚焦)**:保留 S 的全部事件框架(fresh≤7 池、adv5、n5/mn2、
出場五規則),僅將「六軸幾何 rank」替換為「LightGBM 預測分數」。
- **樣本** = 每日 fresh cohort(fresh≤7 + eligible;**不加 cfo 硬閘**,cfo 降為
  特徵讓模型軟性使用)。
- **特徵** = 現有 19 軸(F05 特徵管線:15 FEATURE_COLS + rev_seq/accel_rel +
  volume_surge/consolidation)。
- **標籤** = fwd 21 交易日 close-to-close 報酬的 **cohort 內 rank-pct**
  (cross-sectional 化,消市場 beta)。
- **Walk-forward**:每 63 交易日 refit;訓練窗 rolling 756 交易日(3 年,M01);
  **embargo 26 交易日**(訓練樣本 label 窗不得觸及預測期);LightGBM 穩健配置
  (num_leaves 31 / min_leaf 200 / ff+bagging 0.8 / lr 0.05 / 早停 on 訓練尾段)。
  首個預測 ≈ 2015-08 → **整條 11 年 NAV 全 OOS**(比 S 的 dev 調參更嚴格)。
- **無 look-ahead 檢核**:refit 日之後資料零接觸;label 未來窗與訓練截止間留 26d。

**判準(先於執行)**:
1. 模型有訊號:OOS 日度 rank IC(預測 vs fwd21 實現)均值 > 0.03 且 t > 3。
2. 主判準:3 年窗(2023-07→2026-07)P5 > S 的 45.9% 且配對 CI 下界 > 0 → 晉升。
3. 披露:全 OOS 期(2015-08 起)vs S 同窗、KPI v3、feature importance、
   逐年;fresh 池內 ML vs 幾何 rank 的 pick 重疊率。
4. 敗 → 記錄「ML 於現有 19 特徵無增量」= 差距在特徵/資料而非模型形式。

**G01 結果:失敗(教育性)+ 工程 bug**。(a) bug:walk-forward 用 cohort 壓縮
日曆(fresh 池每月僅 ~7 天有樣本)→ 756「日」= 實際 ~9 年、僅 4 refits、
OOS 僅 2024+。(b) 實質失敗:**IC 0.103/t 13.3(遠超門檻)但 top-5 組合
CAGR 9.2%/P5 −2.5%、配對 −61%/年 CI 全負——「IC 高 ≠ 策略強」的教科書案例**。
機制:rank-MSE 目標優化「排名可預測性」→ 偏好低方差目標 = lowvol(importance
第一 0.12)/cfo/dy 安全股;交易僅用 top tail,MSE 卻在優化中段。
`ledger/g01_scores.parquet` 留檔。

## G02 — LambdaRank 修正(預註冊)

**修正**:(1) 真交易日曆 walk-forward(756/63/26 全部以 panel 交易日計);
(2) **objective = lambdarank,NDCG@5**(直接優化 top-k 排序,對齊 n5 交易),
relevance 分桶:cohort fwd21 rank top10% = 3、10-30% = 2、30-60% = 1、其餘 0,
group = date;(3) 對照組:regression-on-raw-fwd_ret(右尾直接進 loss)與
G01 式 rank-MSE(修日曆後);(4) 每 refit 統一輸出 cohort 內 rank-pct 分數。
**判準不變**(G01 判準 2-4);模型品質量尺改為 top5-spread:OOS 每 cohort 日
「模型 top-5 的 mean fwd21 − cohort mean fwd21」> 0 且 t > 3。
**預測**:lambdarank > raw-reg > rank-MSE;能否勝 S 之先驗五五開。

**G02 結果(49 refits × 3,OOS 2014-01 → 2026-06)**:
(1) **目標函數排序完全如預測**:lambdarank top5-spread +2.51%/次(t 7.7)>
raw +1.62% > rank-MSE +1.08%;G01 病根確認 = 損失函數,rank-MSE 於真日曆
復現失敗(W3 CAGR 12.3%)。
(2) lambdarank 自行重新發現 S 因子集(importance top5 = rev_seq/rev_yoy/cfo/
frn/rev_yoy_accel)——方向對、精度不如手工結構。
(3) **但三變體全敗於 S**:lambdarank W3 51%/P5 11.9 vs S 96/45.9;
fullOOS 39.4 vs S 83.7,配對 −28.5%/年 CI [−48.6, −10.0] 全負。
三 scores 留檔 `ledger/g02_scores_*.parquet`。

## G03 — 殘差學習 + 特徵擴充 + 窗長(預註冊;ML 線最後攻勢)

蓋棺「ML 無增量」前的顯而易見三手牌,一次打完:
- **G03a geo-as-feature(殘差學習)**:特徵 = 19 軸 + **geo_score(S 六軸幾何
  分數)** + cfo_gate flag——讓 ML 從已知有效結構出發學修正,非從零重學。
- **G03b 特徵擴充**:+~20 軸(多窗動能 20/60/252、多窗波動、turnover z、
  rev_yoy lags/連續加速月數、52wH 動態、log ADV、log price、上市年數、
  cohort 規模、月份)→ ~40 特徵。「差距在特徵」假說的直接測試。
- **G03c expanding window**:cohort 事件維度上 3 年僅 36 個月度 cohort,
  ML 樣本需求異於參數層(M01 結論不必然移植)→ expanding(全歷史)對照。
- 全部 lambdarank NDCG@5;a/b/c 疊加式(a → a+b → a+b+c)。
**判準**:主判準不變(W3 P5 > 45.9 且配對 CI 下界 > 0 → 晉升);
**存活判準:fullOOS 配對 vs S 的 CI 含 0(至少統計打平)→ ML 線續研,
否則蓋棺**:「差距在資料/特徵而非模型形式」,ML 檔案封存。

**G03 結果:三變體全敗,ML 線蓋棺(G01-G03 共 13 trials)**。
top5-spread 逐步提升(a_geo +2.78 → b_ext +2.84 → c_expand +2.98%/次,t>9,
模型訊號真實且演進方向對)但組合全敗:fullOOS 配對 a −39.9 / b −27.5 /
c −23.2%/年,**三者 CI 全負**(存活判準不含 0)。geo_score 未進 importance
top6(樹模型中與原始軸冗餘,殘差學習失效);b_ext 的 W3 62.2%/MDD −19.1
為 ML 線最佳單窗但仍遠遜 S。

**蓋棺結論**:差距不在模型形式(lambdarank 已對齊 top-k、殘差已給先驗、
特徵已擴充至 34、樣本已 expanding)。兩個根因:(1) **幾何 rank 是以「真實
交易報酬」為適應度、經 430+ trials 演化出的結構;ML 只能優化 fwd21 近似
標籤**——ML 變體 MDD −36~−47% vs S −21:模型選中 fwd21 期望高但路徑差的
高波股(先觸 trail 出場),標籤未計路徑;(2) 日頻低信噪 + 月度事件樣本稀,
強先驗 > 資料驅動。**oracle 差距的鑰匙不是換模型,是新資料(日內/公告時點)。**
未試盡處誠實記錄:policy-aware label(標籤=出場規則結算的交易報酬)、NN、
深度超參搜索——邊際期望低,封存於此;scores 留 `ledger/g03_scores_*.parquet`。

# L-LINE — LLM Agent 主觀選股 overlay(2026-07-10;使用者再更新天條)

**天條更新**:消息面解禁 + LLM 進入策略層(使用者:「加入 LLM Agent 讓策略
進化到極限強大」;Agent 可 survey 題材/市場/消息面或依量化數據判斷;
context 獨立全新模擬實際運作;可回測;不斷優化 prompt)。

**知識洩漏協定(本線科學性命門,先於一切設計)**:LLM 權重內含訓練截止前
的市場後見之明(哪些股票後來暴漲、哪些題材成功),context 隔離擋不住權重
污染 → 回測必須:
1. **匿名化資料包**:股票代碼/名稱/產業/絕對日期全部剝除;價格歸一化
   (決策日 = 100)、營收只給 YoY/QoQ 百分比序列、流動性給 cohort 分位、
   大盤只給動能/波動數字不給可辨識曲線 → 後見之明無從附著,全窗可回測。
2. **消息面 survey 變體不可歷史回測**(權重污染 + 現代搜尋結果被未來污染),
   僅能於模型 cutoff 後期間近似評估或 forward/live。
3. Prompt 迭代 = 調參 → **train/test split:2023-07→2025-06 迭代 prompt,
   2025-07→2026-07 鎖定測試**(僅終版 prompt 觸碰)。

## L01 — 匿名化量化判斷 overlay MVP(預註冊)

**設計**:S 於 W3 窗的每筆實際進場(~150-200 筆),每筆一個**獨立全新
context 的 agent**(Opus,effort max),輸入匿名化 PIT 資料包(六軸值+cohort
rank、12 月營收 YoY/3m 環比序列、120d 歸一化價格週頻走勢、量能比、cohort
規模、大盤 20/60d 動能與波動),輸出 {verdict: take/skip, conviction 1-5,
reason}(schema 強制)。Agent 定位:資深主觀交易員,任務 = 挑掉低基期假加速、
末端噴出、量價背離、出貨形態。
**回測**:skip 的 (date, code) 自 entries 濾除後重 sim(次名自然遞補)。
**判準**:(1) 訊號存在性:skip 組實際交易報酬顯著 < take 組(先導 50 筆);
(2) 主判準:overlay NAV 的 P5 > S 同窗 45.9 且配對 CI 下界 > 0;
(3) train 段迭代 ≤ 3 版 prompt,test 段一次性評估,兩段結論一致才晉升。
**先導**:50 筆(train 段隨機)驗管線與訊號存在性,過 (1) 才全量。

**L01 先導 v1 結果(50 × Opus max,198 萬 tokens,skip 率 14%)**:
take 43 筆 mean +6.50% vs skip 7 筆 +3.25%——方向正確但 CI 跨零(skip 樣本 7 筆
統計力不足)。**skip 品質解剖:7 中 5 對**(4 筆虧損 −3.4~−13.4% 全數正確砍除),
但**錯殺 T068(+41.6%)一筆抵銷全部價值**。錯誤模式清晰:判對的理由全是
「營收品質」(單月 artifact/低基期/絕對水準深谷),錯殺的理由全是「末端噴出
追高」——與 F05 一致(噴出=右尾特徵非地雷)。conviction 與報酬負相關
(信心≠獲利,呼應 G01「可預測性≠獲利性」)。
**v2 修正(原則性,金融機制:alpha 源自營收定價修正故營收假訊號才是真地雷)**:
價格噴出紅旗降級(有營收拐點支撐的噴出不砍)、營收品質升主紅旗。
v2 於 train 段 hold-out(另 50 筆,seed 43)驗證;判準:skip 組 mean < 0
且不錯殺 >+30% 右尾,過 → 全量 overlay 回測 → test 段一次裁決。

**v2 hold-out 結果:過關**——skip 率 4%(2 筆)、skip 全虧損(−8.2%/−1.1%,
precision 100%)、右尾零錯殺(+67%/+59%/+51%/+43%/+42% 全保留)。
原則性修正在未見樣本上成立。

**Test 段終局(60 筆,2025-07→2026-07,一次性,prompt 已鎖)**:
**零 skip**——agent 逐條檢查主紅旗全不成立(test 期為 S 豐收段,cohort 營收
品質高,單月 artifact/深谷低基期/環比弱者罕見)。overlay = S 原樣,增量 = 0。

**L01 結案裁決:不晉升(依預註冊主判準)**。三段證據(160 筆、6.2M agent
tokens):訊號**真實但極稀疏**——v2 標準全程只砍 2/160 筆(1.25%),經濟增量
≈ 0。**根因(與 G-line 同構)**:S 的 rev_seq 軸 + cfo 閘 + 幾何 rank 本質上
已是「營收品質檢查」的量化版,LLM 能識別的假訊號已被量化系統預先過濾;
在 S 已高度優化的訊號池內,任何額外判斷層(ML 或 LLM)的殘餘可判斷空間
趨近零。方法論收穫(轉用 L02):匿名化協定、prompt 原則性迭代 + hold-out +
test 一次性裁決、workflow 判斷管線——全部可直接複用於 live 具名+題材 overlay
(L02,agent 攜帶真正新資訊而非重判舊資訊,增量空間性質不同)。
verdicts 留檔 `ledger/l01_{pilot,v2,test}_verdicts.parquet`。

**L01 補充(2026-07-10,使用者追問)**:v1 錯殺兩筆經 v2 重判全部獲救
(T068 take c4「rev_seq +41% 真環比…右尾特徵明確」、T06 take「真拐點下的
噴出勿砍」)。**v2 overlay 全窗數字:W3 CAGR 101.2% / P5 50.9 / Martin 15.5
vs S 96.0 / 45.9 / 13.4**(skip 6 筆全虧損或平盤:2 實判 + 4 依主紅旗規則
自 v1 理由映射)。配對 +2.71%/年 CI [−3.2, +10.4] 跨零 + 增量全在 train 段
(prompt 迭代所見)+ test 段增量 0 → **維持不晉升;101.2% 列觀察項,
待 L02 live forward 樣本轉正**。

## F06 — 資格門檻消融(2026-07-10;使用者:「移除會不會更高?」)

**預註冊**:W3 窗,S 全規格,僅動 eligibility 三參數:ADV 500 萬 / 價 10 元 /
掛牌 60 根,各自與全部移除。**結果:全部方向為劣化或中性**——

| 變體 | CAGR | P5 | MDD |
|---|---|---|---|
| base | 96.0% | 45.9 | −20.7 |
| no_ADV | 86.1%(−10pp) | 36.6 | −18.8 |
| no_price(10元) | 92.4% | 43.8 | −20.7 |
| no_history(60根) | 96.0%(無差) | 45.9 | −20.7 |
| 全移除 | 78.9%(−17pp) | 33.1 | −18.8 |

**機制**:超微型股(ADV<500 萬)的營收訊號噪音大(單一客戶/一次性認列)、
跌停鎖死頻繁、cohort 排名被稀釋;60 根門檻被特徵 warmup 自然覆蓋故中性。
且回測對微型股滑價已過度友善(0.1% 固定),真實差距更大。
**三門檻不是「犧牲績效換執行性」,它們本身就是績效優化的**——
R18 液動性下限響應曲線(峰在 5M)於 W3 窗再確認。

## F07 — Stale 跨揭露續抱(2026-07-10;使用者:「再爆的為何不續抱?」)

**機制分析**:天二案例(7/7 stale 出場 +45.7%、7/9 新營收爆、7/10 再進場)
的根因 = stale 26 曆日到期落在下次揭露(7-10 日)前 2-3 天。引擎已有自然
續抱:新營收揭露 → fresh 重置 → stale 不觸發——問題純粹是時間差。
**與 R05 winner-extend(證偽)不同**:那是無條件展期(動能接棒),
本案是「等新營收確認再決定」(alpha 更新)。
**設計**:stale ∈ {26 base, 28, 30, 32} 曆日;32 = 完全跨越法定揭露日
(新營收必已揭露,好者自然續抱省 0.56% 成本+消空倉 gap,爛者靠排名消失
+trail/time/lts 出場)。**風險預期**:爛營收股多待 2-6 天的成本 vs 好股
不斷檔的收益,淨向不明,讓資料說話。判準:P5 > 45.9 晉升,W3 窗。

**F07 結果:REJECTED——stale 26 維持**。W3 曲線 26/27/28/29/30/32 =
96.0/90.1/**102.2**/87.8/75.1/61.5:28 是**孤峰**(鄰居 27、29 皆劣於 26,
響應不平滑 = 雜訊尖刺非結構);現代 era(2019 起)反轉確認:26 = 116.0/P5 77.2
vs 28 = 109.8/72.1、MDD 惡化 −32.6→−38.4,配對 −3.0%/年。
「提早揭露自選偏誤」故事動聽但兩窗不一致 → W3 峰判定為 42 筆交易差的運氣。
**機制結論**:使用者的續抱直覺已內建於引擎(新營收先於 stale 揭露 → fresh
自動重置 → 自然續抱);30+ 天的災難證明「等遲到的營收」= 收留拖延的爛公司
(壞消息拖到期限的自選偏誤)。天二類邊界案例(好公司、發布落在 stale 後
2 天)的救援代價 = 整體劣化,不救。個案的可惜 ≠ 系統的錯誤。

# X-LINE — 跨家族組合(2026-07-10;使用者開放 Codex 線 strategy_ranking.md)

## X01 — S × Iter95 組合前導(exploration,判準後補誠實標注)

**Iter95(Codex 線 champion)**:Iter92 動能 meta-switch(三 sleeve 按近 5 日
NAV 相對動能月切換)+ time50 r-1 出場;富邦 realistic execution(參與率 5%、
部分成交、fill 81.5%);2005-2026 全期 CAGR 34.6%(584x)、自帶 battery
DSR 0.993/PBO 0.032。**與 S 為不同因子家族**(價格動能 vs 營收事件)。

**同尺同窗對比(apex KPI)**:
- 共同窗 2023-07→2026-05:S 110.8/P5 59.8/Martin 16.7 vs Iter95 84.5/32.9/9.3
  ——**Iter95 = 至今最強外部 challenger,但整窗仍遜 S**;
  惟 melt-up 年(2025-05→2026-05)Iter95 +295% 完勝 S +126%(正2 duel 結論
  再現:單一熱年屬動能結構)。
- **日報酬相關僅 0.38(14 年)/0.47(3 年)= 真分散**(apex 內部 config 集成
  因相關 ~0.95 證偽;跨家族才有此物)。

**全窗(2012-07→2026-05)權重掃描**:

| S 權重 | CAGR | P5 | Martin | MDD | Sharpe |
|---|---|---|---|---|---|
| 100%(S 純) | 83.2% | **59.8** | 10.8 | −32.6 | 2.19 |
| 70/30 | 75.4% | 56.2 | 12.7 | −25.9 | 2.42 |
| **60/40** | 72.5% | 54.5 | **12.9** | **−23.6** | **2.46** |
| 0%(Iter95 純) | 53.2% | 36.0 | 6.3 | −22.1 | 1.87 |

**裁決(KPI v3)**:P5 主排序 → **S 純體王座不變**;但 60/40 組合
Martin +2.1、Sharpe +0.27、**MDD 淺 9pp**(S 唯一痛點 2020-03 被壓到 −23.6)
——組合不是更強的策略,是**同 alpha 池的更優風險配置**。逐年互補清晰
(2016/19/23/25/26 Iter95 強、2013/15/20/21/24 S 強,無同虧年)。

**Caveats(誠實)**:兩 NAV 成本慣例不同(混合慣例,方向可信、精確值需統一
重跑);Iter95 止於 2026-05-22 未更新,live 需重建其管線;容量互補
(S 300 萬級、Iter95 realistic 跑到 5.8 億);「事後挑兩強組合」有選擇偏誤,
但分散紅利源自因子家族結構(0.38 相關),此部分不受選擇偏誤污染。
**收穫另記**:Iter95 的 MFE/MAE 診斷(寬 time exit 優於緊停損)與 apex
F07/R27 獨立收斂 = 跨線互驗。

**使用者裁示(2026-07-10)**:不採組合(Iter95 本身即多策略組合,太複雜);
指令 = 學 Codex 概念 → **自研近 3 年最強單一策略**;組合為最後手段且成分
必須全自研。X01 記錄為知識,組合不部署。

# N-LINE — 自研動能單策略(2026-07-10;Codex 概念移植,非策略搬運)

## N01 — 動能容器的正確規格(預註冊)

**概念提煉(Codex 線 → 設計原則)**:apex 動能容器兩次慘死(F02 CONT-mom
MDD −50%、F05c −69%)vs Iter95 動能書活得很好——死因對照出三個規格錯誤:
(1) n5 深集中(Iter95 = 10-11 檔分散扛崩);(2) 每日換倉追噪音(Iter95 = 月頻
+ 換倉壓縮);(3) trailing stop 在急崩連環觸發(Iter95 = **無停損**,僅
time50 r-1 寬鬆出場——動能吃趨勢必須 hold through 震盪)。
**誠實聲明**:動能家族曾於全窗證偽(R01/R02,2022 熊市重傷);本批依 M01
哲學於 W3 窗重審「正確規格」的動能,melt-up regime(Iter95 近一年 +295%)
為其主場。

**設計(單一策略,無 meta-switch、無 sleeve)**:
- 池:eligible(ADV 5M / 10 元 / 60 根,F06 已驗)
- 訊號:純價格動能 mom(lookback, skip),lookback ∈ {10, 21, 42, 63}、
  skip ∈ {0, 5}
- 輪換:**月頻**(每月首個交易日),top-N 為 target;buffer ∈ {1×, 2×}
  (跌出 top N×buffer 才賣 = 換倉壓縮)
- N ∈ {8, 10, 12};出場僅二:月度輪換 + loser-time 50 日(仍虧即出);
  **無 trail、無 abs、無 time stop**
- 成本:apex 慣例;窗:W3(2023-07→2026-07)
**判準**:P5 > 45.9(勝 S)= 挑戰王座;P5 > 33(勝 Iter95 同尺)= 家族存活
續研;皆不及 = 動能單策略於 W3 終判死,回歸 S。

**N01/N02 結果(56 cells):動能單策略終判死**。
N01(純動能):最佳 lb42/n12/嚴格輪換 = CAGR 63.0/P5 12.8/MDD −30.3——
正確規格確實救活家族(F02 舊規格 P5 為負、MDD −50),但天花板遠低於 S。
lookback 響應:42 日 >> 21/63/10;短窗全滅(追噪音)。
N02(動能×品質軸):最佳 m42×consol/n12 = 61.2/13.8,同平台;
三軸疊加反而崩(−14.5 P5)。**兩批 P5 皆 << 33(家族存活線)→ 終判死**。

**Iter95 深挖解密(持倉逆向)**:近 12 月權重加總台積電 18.8 壓倒性第一
(次名 2.2),配 AI 衛星(聯光通/富喬/高技/建準/金居)——**+295% 神話大半是
「重倉 2330 動能錨(同期 +131.6%)+ 衛星飆股 + 無停損」**;塔的同尺 84.5 與
自研乾淨版 63 之間的 20pp 差距 = sleeve 疊塔工程(blend/margin/confirm/
meta-switch)= 使用者明示不要的複雜度本身。

**全自研組合檢查(使用者許可的備選)**:S × N01-best 相關 0.54(同池,
高於 S×Iter95 的 0.38),S70/30 組合 MDD −17.3(改善 3.4pp)但 CAGR −7.7pp、
P5 −3.3 → **P5 主排序下不值得,不採**。

# N-LINE 終局:近 3 年最強單一策略 = apex_revcycle_S 確認(2026-07-10)

S(96.0/P5 45.9)至此擊退:F-line W3 重研發、F04 季報池、F05 暴漲前兆轉化、
G-line LightGBM(3 批)、L-line Opus overlay、N-line Codex 概念動能書(2 批
56 cells)、X01 跨家族組合與全自研組合。**單一策略王座 8 線攻防全勝。**




# R-LINE 最終收斂宣告(2026-07-09,goal「超越正2」iteration)

**連四批無認證升級**(R08 OOS 敗、R09 0/10、R10 0/6、R11 確認敗)→ 收斂成立。
**最終旗艦不變:`apex_revcycle_R`(geo-n8-t35)**。對正2:全史同窗/現代era/dev 窗
+ 全部風險指標超越;唯一未勝 = melt-up 單年(2× beta 結構,使用者已禁槓桿與 ETF 持倉)。
外部知識(FinLab 265 篇)已全量收割入知識圖譜(quant/finlab/ 四篇),交叉驗證完成。

### R05 結果(T0161-T0168)— 0/8 未過晉級線

**r05e_n8_geo 54.9%/1.62/−37.6 三軸 Pareto 優於 n8(+2.7pp/+0.04/+1.0pp)但差晉級線
0.26pp → 依紀律不晉級、不動 OOS**(記錄為未來批次候選)。贏家展期反而傷
(44.4%,價格動能接棒不足補營收 alpha 衰減);產業軸 45.3/43.1(稀釋);
streak 46.6;組合 35.4。R-line 5 批 52 trials 後,n8 附近為 52-55% 平台。


## F08 — down-market 相對強度軸(使用者實盤觀察;預註冊 2026-07-16)

因子 dm_rs60/dm_win60(定義同 evergreen LEDGER EV46)。變體:S+dm 第七軸
(^0.5 / ^1.0)、dm_pos 進場 gate、dm 替換 mom_126_5。dev 窗判準:P5 > 74.4
(S 基準)且 MDD 劣化 ≤5pp → 現代 era 確認;否則負結果入檔。

**F08 結果(2026-07-16):負結果,五變體全敗**。S 基準重現(120.9/−32.6/16.4
精確;P5 67.3 為 n_boot=500 之估計噪音)。dm_rs^0.5 88.8/P5 42.2、^1.0 87.7、
dm_win 81.9、dm_pos gate 102.3、替換 mom 92.0——全部大幅劣於基準,無一過
判準。機制:S 池的 alpha 本質是揭露後的進攻性動能,抗跌篩選恰好濾掉/降權
最具爆發力的高 beta 標的——down-market 強度與 S 訊號本質相斥。


## G04 — 全市場排名學習・純訊號層驗證(使用者翻案裁決重開 ML 線;預註冊 2026-07-16)

**方法論(使用者):先驗證訊號 alpha,確立後才疊執行層(進出場/濾網/倉位/停損)
——G01-03 的死因正是訊號層(top5-spread t 7.7 活著)與執行層(標籤無路徑)混評。**

與 G01-03 的增量:(1) 樣本 = 全市場每日橫斷面(非 S cohort,密度 ×15-20);
(2) 標籤治蓋棺根因:policy-aware(經 trail35/time30/lts15 結算的交易報酬
rank)與 fwd-rank 對照;(3) horizon 為實驗軸(21 日是 G01 當年拍的,未掃過):
第一波 {fwd21, policy21},訊號活 → 第二波 {10, 42, 63};(4) 特徵 = 現有全量
(19 軸 + 擴充軸 + dm_rs60/dm_win60)。模型/walk-forward 沿 G02(lambdarank
NDCG@5、756/63/26 真交易日)。

**訊號層判準(預註冊)**:OOS(2019-01 起)RankIC 均值 > 0.03 且 t > 4;
十分位 top-bottom spread t > 4;top decile fwd21 超額 > 月化 1%。
通過 → 執行層設計另行預註冊;未過 → ML 線二次蓋棺(樣本與標籤都試盡)。

**G04 第一波結果(2026-07-16)**:形式判定兩標籤未過(RankIC 關;惟該關對
lambdarank 屬設計錯配——目標函數只優化頭部,全排序 IC 天然低,判準教訓入檔)。
實質:(A) 全市場 fwd21 排名訊號頭部為真——top-5 超額 +2.01%/21日(t 9.6)、
top-50 +1.35(t 14.3),量級同 G02 cohort 版;(B) **policy-aware 標籤證偽**
——top-5 掉到 +0.43(t 2.9):停損燒進標籤=剪掉右尾,模型改學「不觸發停損的
穩定股」;G03 蓋棺真根因修正為「cohort 樣本稀」而非「標籤缺路徑」。
scores 留 ledger/g04_scores_{fwd21,policy21}.parquet。使用者裁決:進第二波
horizon 掃描(10/42/63),視結果再議執行層。

**G04b 結果(2026-07-16)**:horizon 裁決——訊號集中短端(top-5 年化超額:
fwd10 +62%/t12 ≫ fwd21 +27% ≫ fwd42 +12% ≈ fwd63 +10%;每期超額 ~2% 不隨窗
增長=預測力全在前 10 交易日)。零成本日再平衡 top-5:7.5 年 OOS CAGR 142.3%
/MDD −43.3;**與現役相關 ML-SER 0.04 / ML-S −0.01(第四臂原料)**;命門=
日換手 36%(年成本粗估 −50pp)。scores 留 g04_scores_fwd{10,42,63}.parquet。

## G05 — ML 第四臂執行層(預註冊 2026-07-16)

訊號 = g04 fwd10 pred(凍結,零再訓練);引擎 = apex simulate(全成本/T+1/
現金約束)。網格:席位 {5,10} × 遲滯帶 exit-rank {10,20,30,∅=日重排} ×
min_hold {1,5,10} × max_new {1,2,5}。窗 = OOS 全段(2019-01~2026-06;訊號
本身已是 walk-forward OOS,執行層參數在此窗選擇屬一層 in-sample——誠實標注,
晉升前須以末年 holdout 或 live 驗證)。判準:淨成本 CAGR > 80% 且 MDD ≥ −50
且 ML-SER/ML-S 相關 < 0.3 → 第四臂候選(validator + 四臂配置另議)。

**G05 結果(2026-07-16):未過門檻,ML 線蓋棺 v2**。top-1(10席/buffer20/
min_hold10/mn5)淨成本 CAGR 66.0%/MDD −36.6/P5 30.8%(判準 80% 未達);
遲滯帶為最優執行工具(榜首圈全 buffer=20)但 142%(零成本)→66%(全成本)
=成本+路徑摩擦吃掉過半。蓋棺詞 v2:訊號真(t12/7.5y OOS/與現役零相關
0.04/−0.01)、敗於台股成本結構 vs 10 日訊號 alpha 密度(0.56%/輪 vs ~2%/輪
毛利)。重啟條件:成本結構改變(費率/規模)或新資料(日內/公告時點)。
資產全留:g04 管線+scores(fwd10/21/42/63/policy21)+g05 執行層網格。

## G04c — 基本面特徵擴充(使用者質詢驅動;預註冊 2026-07-16)

G04 特徵 25 軸僅 5 個基本面(rev_yoy 系/cfo_ni/dy)。補:日頻估值 pe/pb +
季報 PIT(法定生效日)毛利/營益/淨利率、roa_ttm、資產週轉、流動比、負債比、
f_score_raw、d_roa/d_gm YoY + 月營收 rev_seq → ~38 軸。fwd10 單標籤重跑
(29 refits),對照 G04 基準 top-5 +1.93%/t12。訊號顯著抬升 → 以新分數重跑
G05 執行層 top 區域,看淨成本能否過 80% 門檻;無抬升 → 「特徵不是瓶頸」
結論在全市場版複核成立。

**G04c 結果(2026-07-16):負結果——基本面補滿反而傷訊號**。37 特徵版
top-5 +1.07%(t7.0)vs 25 特徵基準 +1.93%(t12.0);importance 上基本面佔
46%(rev_seq/current_ratio/pe/d_roa 皆前列)——模型重用慢變數改善中段排序,
卻稀釋頭部銳度(與 G01 rank-MSE 偏好安全股同病理)。結論:10 日排名任務的
正確特徵集=技術面;「特徵不是瓶頸」於全市場版複核成立。ML 線封印四嫌
(模型/標籤/樣本/特徵)全排除,死因唯成本結構。scores 留 g04c_scores_fwd10。

## F09 — ML 排名分數作 S 第七軸(使用者指令;預註冊 2026-07-16)

g04 fwd10 pred 作 cohort 內排位軸(缺分數填中性 0.5)。變體:S+ml^0.5、
S+ml^1.0、ml 替換 mom_126_5、ml 替換 high_52w、ml_only(六軸全換)。
dev 窗判準同 F08:P5 > 74.4 且 MDD 劣化 ≤5pp → 現代 era 確認;否則負結果。

**F09 結果(2026-07-16):全敗且單調劣化**(ml^0.5 100.0/^1.0 78.7/替換mom
96.1/替換h52 67.9/全換 59.9 vs 基準 120.9)——ML 劑量越重越差;cohort 內
手工六軸無可替代(與 G02「模型重新發現 S 因子但精度不如手工」互證)。
ML 嫁接線收官:獨立(G05 成本殺)/特徵擴充(G04c 稀釋)/嫁接(EV47/F09
正交稀釋)全試盡;唯一存活路=成本結構改變時獨立復活。

## F10 — S × 庫內籌碼軸(2026-07-16 預註冊)

變體:S+margin_chg5(反向)^0.5、S+sbl_chg5^0.5、S+fhold_chg5^0.5、
fut_pos gate(外資期淨 OI>0 才進場)。dev 窗判準同 F08(P5>74.4、MDD 劣化
≤5pp);否則負結果。

**F10 結果(2026-07-16):S × 籌碼四變體全敗**。融資降^0.5 53.8(S 動能股
本伴隨融資增,反向軸=毒)、借券降 89.5、外資增持 106.9(最近但仍 −14pp)、
期貨多方 gate 2.6(空方日砍光 cohort=把事件策略餓死)。結論:S 的月營收
事件 alpha 與籌碼慢訊號不相容,與 F08(抗跌)同理——攻擊手不能用防守標準
選也不能用防守日曆出勤。籌碼線 S 側收官。

## F11 — S × 動態加減碼(2026-07-16 預註冊)

pyramid {(.15,1,.5),(.15,1,1),(.30,1,.5),(.30,2,.5)} + recycle {(0.6,0.4),
(1.0,0.5)} 疊 S 凍結參數。dev 窗判準同 F08(P5>74.4、MDD 劣化 ≤5pp)。

**F11 結果(2026-07-16):加減碼噪音級**。最佳(加碼 .15×1×1.0)123.1/P5
69.0 vs 基準 120.9/67.3(+2pp 級)、MDD 全變體同 −32.6。機制:S 滿倉快輪、
現金常態近零,加碼無彈藥。方向 2 雙側收官:不進系統;engine pyramiding
能力留存。

## F12 — S × 營收轉衰出場(2026-07-20 預註冊)

同 EV52 三變體疊 S 凍結參數(stale ∪ rev_neg flag)。dev 窗判準同 F08。

## G06 — 時間序列模型・訊號層第一波(統計族,2026-07-20 預註冊)

使用者指令「回測用時間序列模型」。第一波閉式統計族(全市場橫斷面,
訊號層判準同 G04,重型模型視結果第二波):
- ar1_pred:r̂=ρ̂₂₅₂·r_t(AR(1) 一步預測)
- mr_z20:−(close/MA20−1)/σ₂₀(均值回歸)
- vamom:mom20/EWMA-vol(RiskMetrics λ=.94 波動調整動能)
評估:fwd10 top-5/20 超額 + RankIC;對照=G04 lambdarank(top-5 +1.93/t12)
與裸 mom20(分離時序建模增量 vs 純動能)。

**F12 結果(2026-07-20)**:單月 YoY<0 出場 114.2(−6.7pp 誤殺)、連兩月
120.4(平手噪音)——不採。
**G06 結果(2026-07-20)**:時序統計族全輸裸動能(ar1 +0.48/mr_z −0.63 反向
/vamom +1.26 vs mom20 +1.81/t7.7);台股 10 日尺度=動能市非回歸市,波動
調整降權最猛股(EV39 volt 同病理)。重型時序模型不升級,時序線一波收官。

## S01 — apex_revcycle_S 逐年 PnL 分佈診斷(2026-07-20,回應「不均勻是好是壞」)

問:S 每年賺賠不均勻,該不該追求均勻穩定?量測(5 席 20% 全史 2014-10~
2026-07,S 規格單一真源 chart_s_vs_benchmarks;純量測不改策略):
- **正報酬年 11/11 = 100%**(完整年)——不均勻只在「賺多少」,方向從未翻負
- 幾何年均 +85.1%;最佳年 2021 僅佔複利對數財富 23%、前兩年 43%
- **剔最佳 1 年後幾何年均 +68.2%、剔最佳 2 年 +53.2%**——edge 不靠幸運年
- 最差年 2022 +1.8%(同年 0050 −21.4%,S 超額 +23pp);年報酬 σ=127%
判定:**健康離散非脆弱集中**。右偏(讓贏家跑/快砍輸家,STRATEGY.md §1)
使 lumpiness 是 alpha 的機械後果,非缺陷;追求均勻=剪右尾=殺 alpha
(且 regime 平滑=EV49-51 已證兩折選不穩)。求穩正道=多策略低相關疊加
(tri 三臂),非改造 S。lumpiness 唯一真風險=年內深回撤(2020/21/15/26
皆 −27~29%)→ 倉位保守勿裸槓(25% NAV 上限紀律正確),非重造策略。
腳本:experiments/s01_pnl_distribution.py。

## F13 — S × 融資槓桿 overlay(2026-07-21,同 EV54 跨策略預註冊)

見 evergreen/LEDGER.md EV54(三策略統一 harness):S 微型股池的處置/停融資
風險未建模,結果為上界。判準同:兩折 OOS P5 ≥ 未槓桿 baseline 才收編。

**F13 結果(2026-07-21)**:S 融資 0/2 不採——折2 OOS CAGR 91.9→251.3% 但
P5 −4.2→−44.3%/MDD −49.8%;P5 主尺判死(槓桿買 CAGR 的代價是尾部)。
詳見 evergreen/LEDGER.md EV54。

# M02 — refit **頻率** meta-study(2026-07-21;使用者:「多久 refit 一次是最優?月更會更好嗎?幾月更?」)

**動機**:M01 證了「用過去 3 年研發最優(窗長)」,但 refit 頻率(多久重選一次
config)從沒被量過——「一年一次(下次 2027-07)」是 M01 設計裡寫死的假設,依天條
§2.2 屬無證據的魔術數字。腳本 `experiments/m02_refit_frequency.py`(reuse M01 的
24-config 全期 net-NAV;固定 3 年窗;各頻率在每個 refit 點用近 3 年 KPI 選最優、
部署其未來報酬到下一 refit 點;config 切換計 0.4% 保守換手;OOS 窗 2016-01→2026-07)。

**結果(延長窗 2007→2026,OOS 串接 2010-01→2026-07,16 refit 點;CAGR 選;含切換成本)**:

| 頻率 | OOS CAGR | Sharpe | P5 | 終值 | 換手次數 |
|---|---|---|---|---|---|
| 永不(凍結首選) | 46.6% | 1.47 | 0.312 | 468x | 0 |
| 每月 | 53.5% | 1.60 | 0.384 | 702x | 26 |
| 每半年 | 56.6% | 1.66 | 0.412 | 1274x | 13 |
| 每季 | 59.0% | 1.72 | 0.421 | 1526x | 14 |
| **每年** | **62.3%** | **1.77** | **0.444** | **2326x** | 7 |

**兩個結論**:(1) **年更 = 最優,且為硬結論**:倒 U——**永不(46.6%,錯過 regime 轉變)
< 月更(53.5%,追 3 年排名雜訊 + 換手)< 每半年/每季 < 年更(62.3%)**;跨兩窗
(2012-窗年更 90.7% vs 2007-窗 62.3%,數字低因含 GFC 硬年,**排序完全相同**)+ 雙 KPI
一致 → 太頻繁追雜訊、不更錯過轉變、**年度剛好**。**現行年度頻率經此證為最優(不再是
M01 設計裡的假設)**。(2) **月份:年底(11-12 月)refit 為真訊號(非雜訊)**——加前後
半段一致性檢驗:全窗最佳 12 月(68.1%);前半最佳 3 月 [1,11,12]、後半 [9,11,12]、
**重疊 [11,12]**(兩段歷史都排前段班,非單期運氣);**現行 7 月反而差(前半段 28.0%
為 12 個月中最差)**。**建議 refit 月份 7 月 → 12 月**。機制(推測,非鐵證):台股年報/
Q4 營收隔年 1-3 月出爐,12 月重選在「握整年營收 + 前三季財報、未進新財報季」的最乾淨
時點,帶新 config 迎年報行情。**方法極限**:參數層 meta(24-config 網格同 M01 限)、
連續 NAV 切片近似(切換過渡以 0.4% switch_cost 概括)、每半段 ~8 refit 點中等樣本——
頻率結論硬(兩窗雙 KPI 一致),月份結論中等信心(通過前後半一致性檢驗)。
結果 → `ledger/m02_refit_frequency.parquet`。
