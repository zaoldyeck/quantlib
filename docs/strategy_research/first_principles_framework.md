# 第一性原理策略研發框架(思維歸零;2026-07-24)

使用者定調:資料校正完成後,**策略研發的流程與框架從頭來過——思維歸零、從第一性原理出發**;
**工具、因子、程式碼可重用不重寫**,但「怎麼研發一個策略」這件事要重新想清楚。本文是那個歸零
後的框架,承接 `phase3_methodology_scrutiny.md`(3.1-3.3)並升級成完整研發流程。

## 為什麼要歸零(第一性原理的起點)

舊策略(S/Evergreen/Serenity)在**汙染資料**上研發,且研發流程本身有未經檢驗的假設。資料校正
後實測:Evergreen −35pp(舊 KPI 部分是資料 bug 撐的)、S +82%(edge 是真的)。這證明:**在髒
資料 + 未歸零的流程上,無法分辨「真 edge」與「過擬合/資料假象」**。歸零不是丟掉成果,是把
「怎麼確認一個東西是真 alpha」這件事重建到可信。

## 框架的七條不變式(每條都有出處,違反即不合格)

1. **先有 edge,才有策略**。每個策略必須指名一個**具體的市場無效率**(為什麼這個 edge 存在、
   為什麼會持續)。答不出 edge = 不是策略,是曲線擬合。(天條:架構選擇要有回測,不是觀察統計)

2. **edge 要有自己的回測**。動手建策略前,**直接檢驗那個 edge**(該因子的 IC / 分位 / 持續性 /
   換手後淨值)。「看起來相關」不是 edge 的回測。否定或採用一個 edge,都要對它自己的樣本做直接
   檢驗,不借別的型態的錨。(天條:否定假設前先對它自己的樣本檢驗)

3. **每個參數/規則都要有證據**。分層線、門檻、slot 數、止損位、資訊源取捨——任何影響行為的數字
   都要有**可重現的量測**支撐,不得憑直覺/美感。拿不出證據就不加,把決定權留給不變式或物理界限。
   說不出「這值從哪次量測來」= 魔術數字,刪掉或去量。(天條 §2.2)

4. **穩健選參,不是最佳化參數**。3.1 已證:在 train 期最佳的參數,套 OOS 反而更差(擬合雜訊)。
   一律用**多參數等權/ensemble/參數不敏感設計**,禁止 grid-search 挑單點最佳當出廠流程。

5. **OOS-robust KPI 為主目標**,不是 in-sample 點指標。主目標 = walk-forward OOS 幾何成長率
   (pooled/median OOS log-CAGR)+ **跨 regime/年代一致性**(最差年、OOS 下界)。真 DSR/PBO 需
   campaign 多設定曲線矩陣 T×K(`apex/validate.py`),一開始就要多設定跑。

6. **硬性前提**(全部要過,不可豁免):成本後、PIT(無前視)、next-open 成交、選參樣本 ≠ 驗證樣本
   (樣本外/保留集)。「好到不像話」的數字優先假設自己設計錯(前視?),不是發現金礦。

7. **出廠閘門**(ship gate,`quantlib-strategy-validator`):walk-forward OOS + MC permutation p<0.05
   + bootstrap 95% 下界 >10% CAGR + DSR>0.95 + PBO<0.5 + robustness grid(±20% 參數 CAGR spread
   <15pp)。禁止只報 in-sample number 就聲稱有 alpha。

## 研發流程(歸零後的順序,每一步落地 repo + 完成即 commit 含負結果)

```
① edge 假設        指名一個具體市場無效率 + 它為什麼存在/持續(寫成一句話 thesis)
② edge 直接檢驗    該 edge 的原始因子:IC(alphalens)/分位單調性/持續性/衰減——先證它有訊號
③ 建構塊驗證       組成策略的每個規則(選股/過濾/出場/sizing)各自有回測證據(該規則下的樣本對照)
④ 穩健組合         等權/ensemble 合成建構塊,參數少且每個有出處(不掃單點最佳)
⑤ OOS 驗證         walk-forward + regime 一致性 + 多設定 DSR/PBO;成本後、PIT、next-open
⑥ 出廠閘門         過 §7 全部關卡才准 paper-trade;任一不過 = 回 ② 或證偽落地
```

**與舊流程的差異**:舊流程常是「挑一組因子 → grid-search 調參到 in-sample 漂亮 → 宣稱有 alpha」。
歸零後:**先證 edge 本身有訊號(②),每個規則各自有回測(③),穩健選參(④),OOS 才是主目標(⑤)**。
順序反過來——證據先行,不是先建再找理由。

## 可重用的工具/因子/程式碼(不重寫)

- **因子**:`apex/factors.py`(月營收加速度/產業相對強弱/品質等)、`strat_lab/raw_quarterly.py`
  (Piotroski F-Score/cfo_ni,PIT)、`industry_taxonomy.py`(真 PIT 產業分類)。
- **引擎**:`apex/engine.py`(simulate)、`evergreen/engine.py`;**唯一真源鐵律**——回測邏輯只一份實作。
- **驗證**:`apex/validate.py`(block-bootstrap/DSR Bailey-LdP/PBO CSCV)、`apex/metrics.py`(perf_stats)。
- **資料**:`prices.py`(還原價,DRIP+FC1+減資)、`execsim`(逐筆成本)、`db.connect()`(乾淨 cache)。
- **驗證輔助**:`verify/`(parser_check/raw_integrity 等——資料地基已驗)。

工具是驗證過的資產,直接用;**要歸零的是「怎麼用它們研發+確認一個策略」的思維與流程**。

## 基準參照(乾淨資料;非目標,是對照起點)

- **S(apex_revcycle_S)**:全跨度 CAGR +82.3% / Sharpe 2.10 / MDD -34.3% / bootstrap 下界 +51.8% /
  P(虧損) 0.000(`apex/revalidate_corrected.py`,2026-07-24 乾淨資料)。edge 為真(非資料 bug)。
- **Evergreen**:−35pp(315%→280%)——舊 KPI 部分為資料汙染撐出,示範「為何要歸零」。
- **fresh_v1 品質+動能 baseline**:Sharpe 0.69(generic 起點;市場級 regime 過濾證偽,需個股級風控)。

**下一步**:依流程 ①→② 為每個候選 edge 寫 thesis + 做 edge 直接檢驗,證據落地。S 雖強,仍要用
歸零流程重新確認其 edge(②)與每個建構塊(③)是否各自有證據,而非整包接受。
