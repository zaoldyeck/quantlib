# 乾淨資料領域知識全面重驗 campaign(2026-07-24)

**Goal(使用者設定)**:2026-07-21 舊輪掃描(海龜/SMC/TPO/估值/純財務書等,結論全輸 S)跑在
**汙染資料 + 舊方法論**上;資料修復後 Evergreen −35pp 證明汙染實質改變回測。故在**乾淨資料 +
第一性原理框架**(`first_principles_framework.md`)上把使用者的領域知識全部重驗:每項要嘛證明
能讓 S 更強(過出廠閘門),要嘛以乾淨資料證據證偽落地。

**基準**:S 乾淨資料全跨度 **CAGR +82.3% / Sortino 3.28 / Calmar 2.40 / MDD −34.3% /
bootstrap 下界 +51.8%**(dev 窗 2019-2025H1 基準 115.0%)。

## 總判決先講:乾淨資料上舊結論全部成立——S 王座無挑戰者,且其出場結構已是最優形態

六類 × 全部重跑(harness reuse,約 20 分鐘 wall-clock,全背景並行),**沒有任何一項在乾淨資料
上翻案**。汙染修復傷了 Evergreen(−35pp),但**沒有讓任何被證偽的方法復活**——它們的失敗是
結構性的(訊號本身無截面預測力),不是資料雜訊造成的假陰性。

| # | 類別 | 重跑 harness | 乾淨資料結果 | 判決 |
|---|---|---|---|---|
| ① | 財務分析法 | q01(17 因子)+ q03(嫁接) | 最強仍是穩定性(gm_vol8 IC .057/spd 14.7%);嫁接 S 全劣化(115%→51~76%) | 證偽 |
| ② | 技術分析 | t01(9 因子)+ f03(SMC 8 因子) | 全部 h21 IC≈0;SMC(FVG/sweep/BOS)零訊號 | 證偽 |
| ③ | 估值模型 | v01(7 因子)+ v03(嫁接) | 五線譜完美反指標(mono −1.00);PEG 負;cfo_yield 唯一可看但嫁接敗(見下) | 證偽 |
| ④ | 交易系統 | t02 + n01 + f11 + s_structure×2 + MAEMFE | 海龜/神奇公式全滅;abs 停損 5 位置全劣化;止盈證偽;加碼噪音級 | 證偽 |
| ⑤ | 純財務書 | q02(成長/品質/穩定 ×N×出場) | 成長 19.6%/MDD −60%;穩定書 13.9%/Sharpe 1.01——能賺但遠輸 S | 證偽(替代策略) |
| ⑥ | 回測框架 | 第一性原理流程本身 | edge 檢驗先行 + D1 全史 + D2 KPI 已拍板並全程使用 | 已內建 |

## ① 財務分析法(F/Z/O-score、杜邦、accruals、GP/A、穩定性…17 因子)

q01 乾淨資料 h21 IC:**毛利穩定 gm_vol8_neg +0.057(spd +14.7%, mono .93)、盈餘穩定 ni_vol8_neg
+0.055** 為最強;cfo_ta +0.028;其餘(Z''、O-score、杜邦三項、ROE、F-score、accruals)IC<0.03 或
spread<8%。**「財務極度穩健」的可交易形態 = 穩定性,不是分數**——舊輪最大發現在乾淨資料上重現。
但 spread 14.7% 仍遠低於 S 現有因子(high_52w +26.9%、rev_accel +21.6%)。

q03 嫁接實測(dev 窗,S 基準 115.0%):+gm_vol8 第七軸 76.4%、+ni_vol8 72.2%、+cfo_ta 72.0%、
閘替換 51.4%、雙閘 71.0%——**全部大幅劣化**。第五次驗證同一定律:**防禦性因子裝進進攻性策略
= 拆引擎**(S 賺營收剛公布的爆發動能,穩健因子的本能是避開最爆的股票)。

## ② 技術分析(傳統指標、SMC、TPO、VWAP/錨定 VWAP、形態學)

t01:RSI/KD/MACD/布林/MFI/OBV/vwap20_dev/**avwap_dev(錨=月營收法定生效日的事件錨定 VWAP)**
全部 h21 IC≈0(−0.019~+0.013)。表面高 spread(RSI +13.9% 等)全是動能重疊——decile 單調來自
與動能共線,IC 無增量。boll_bw_neg IC 正但 spread 負 = 中段雜訊。

f03 SMC/形態:FVG、liquidity sweep、BOS、gap、updays 全零訊號(h21 IC −0.008~+0.013);
hvn_dist(高量價位距離 = **TPO POC 的日頻 proxy**)h21 IC +0.004。close_pos_20(S 已有)仍最強。

**TPO/Market Profile 誠實聲明**:真 TPO 需日內資料;現有 1 分 K 僅持倉股 ~10 檔/月(執行紀錄
用途),**截面因子檢驗不可行**(需每日數百檔)。日頻 proxy(hvn_dist)已測=無訊號。若未來全市場
日內資料可得再升級實測。

## ③ 估值模型(DCF、PEG、五線譜、EV/EBIT、E/P、B/P、CFO 殖利率)

v01 乾淨資料:**五線譜(fiveline_z_neg)decile spread −21~−25%、單調 −1.00 = 完美反指標**
(「跌破趨勢線買」買到的全是下跌趨勢股;反著用=動能,S 已有)。PEG 負(−11%);E/P、EV/EBIT
IC 正但 spread≈0~負(中段 rank 雜訊,極端不分離=不可交易);DCF proxy spd +4.6% 弱。
**cfo_yield 唯一可看**(IC .040/spd +9.1%)→ v03 嫁接實測(dev 窗,S 基準 115.0%):
+cfo_yield 第七軸 **66.1%**、閘替換 cfo_ni→cfo_yield **50.1%**——全劣化。估值類最後一個
存活者也嫁接敗,單因子可看 ≠ 裝進 S 更強(同 q03 定律)。

## ④ 交易系統(海龜、亞當理論、神奇公式、動態加減倉、止盈止損、移動止損、MAEMFE、持倉數)

- **海龜(Donchian 55/20 與 20/10 + 2N ATR)**:全期 +5.0% / −4.7%,MDD −58.6% / −88.7%——全滅。
- **神奇公式(Greenblatt EY×ROC)**:全期 **−2.0%**(台股景氣循環股構成,低 PE 陷阱)——全滅。
- **亞當理論**(純價格慣性):n01 純動能容器乾淨資料重跑(lookback×skip×N 全掃)——最佳變體
  CAGR ~50%、**P5≈0.6%(regime 一致性崩)** vs S 同窗 96.0/P5 45.9,遠輸;其突破式變體=海龜
  Donchian 已滅;其慣性形態=S 的 high_52w/close_pos_20 已內建(IC 已驗)。定案:純價格慣性
  單獨成軍遠不如「慣性 × 營收催化」的 S。
- **MAEMFE(新工具 `strat_lab/s_maemfe.py`)**:贏家 MAE P50 −1.5%/P10 −8.8%(贏家很少深回檔);
  反事實顯示 −10% 停損「贏家中槍 7.1% vs 輸家攔截 38.2%」看似分離——**但 whole-strategy 實測
  abs10 CAGR −23pp(59.6%)**:反事實忽略了 trail35 本來會救活多數中槍交易 + 停損後的再進場
  機會成本。**方法論教訓:MAE/MFE 分佈證據不能直接推出場規則,必須 whole-strategy 驗證。**
- **絕對停損全域證偽**:abs ∈ {10, 12.5, 15, 20, 25}% 五個位置全部劣化(59.6~79.1% vs 82.3%),
  MDD 全部沒改善(−33~−36.5%)。S 的贏家常先回檔再噴,絕對停損必殺贏家。
- **止盈證偽**:profit_take 40%/60% → 70.9%/80.0%(MFE 反事實一致:+40% 救 0% 輸家、封頂 11% 贏家)。
- **動態加減倉噪音級**:f11(加碼×4、減碼×2、組合)與 s_structure2 pyramid 全變體 ±1pp 內
  (最佳組合 116.5% vs 基準 115.0%,Martin 15.1 vs 14.8)——方向微正但不過 D2 判準(需
  Sortino+Calmar+下界同時 ≥)。
- **持倉數**(今晨 s_structure):slots 8/10 → CAGR 60%/50%,Calmar 崩——證偽。
- **移動止損**:trail 25%(緊)無改善;trail35 = canonical 已是最優。

**定案:S 的出場結構(trail 35% + time 30/15、無 abs、無止盈、5 slots)在乾淨資料上就是最優形態
——每個備選都被系統性證偽,而且這次有 MAEMFE 分佈層 + whole-strategy 雙層證據。**

## ⑤ 純財務書(不看股價)

q02 乾淨資料:成長書 G20 = **19.6%**/MDD −59.7%/Sharpe 0.80(W3 41.6%);品質書 Q20 = 10.9%;
**穩定書 S*20 = 13.9%/MDD −26.9%/Sharpe 1.007**(全 campaign 品質側最佳風險調整)。
「不看股價選股能賺」在乾淨資料上成立(成長 ~20%/年),但遠輸 S(82.3%),且 MDD 更深。
作為第二臂:X02 舊輪判零價值(相關 0.44、報酬差距過大);乾淨資料上報酬差距更極端(6×),
結論不需重跑即成立。

## 附判:S 自身唯一改進候選「去 accel_rel」——配對檢定定案為噪音級(不改 canonical)

早前 s_optimize/s_variant_validate 顯示 -accelrel 每指標略優(CAGR 82.3→83.9、Calmar 2.40→2.83),
依 IC 證據(accel_rel 無加分)列為候選。出廠閘門判定(`strat_lab/s_accelrel_gate.py`):
- **配對 block-bootstrap**(對高相關曲線最有統計力):年化日報酬差 **+0.9%,95% CI [−4.7%, +6.1%]
  跨 0、P(差≤0)=0.378** → 優勢屬噪音級,點估計的「每指標略優」不具統計顯著性。
- 權重擾動(5 因子各 ±20%,10 組):CAGR spread 僅 **7.8%**(<15pp)→ S 結構對權重不敏感(穩健,
  這同時是 canonical 的 robustness grid 通過證據)。

**定案:不改 canonical S。**「乾淨資料上完全沒有進步空間」至此含 S 自身微調亦成立。

## 方法論收穫(這輪 campaign 本身的產出)

1. **汙染修復不會讓被證偽的方法復活**:壞資料傷的是「真 alpha 的量測」(Evergreen −35pp、
   S KPI 重錄),不是「無 alpha 方法的判決」——訊號不存在時,資料再乾淨也還是不存在。
2. **MAE/MFE 反事實 ≠ 系統效果**(新教訓,四類第 6 次重現「單元證據 ≠ 組合價值」):
   單因子 IC ≠ 組合價值(mom_126_5)、防禦因子 ≠ 進攻策略加分(q03)、分佈分離點 ≠ 停損改善
   (abs10)——**一切出場/結構改動只認 whole-strategy + D2 KPI**。
3. 台股此資訊集(日 K + 月營收 + 季報)最強 alpha 源仍是**月營收強制揭露制度**,S 坐在源頭上。

## 附:harness 清單(全部可重跑;乾淨資料世代)

q01_financial_scores / q02_pure_financial_books / q03_stability_graft / t01_technical_factors /
t02_turtle_magic / f03_smc_pattern_factors / f11_pyramid / v01_valuation_factors /
v03_cfoyield_graft / n01_momentum_single(apex/experiments)+ strat_lab/s_maemfe /
s_structure / s_structure2 / s_optimize / s_variant_validate / edge_accel_rel /
methodology_decisions / candidate_edges。
