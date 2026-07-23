# D-bankruptcy-models — 破產/財務危機模型 + DuPont + 品質因子 學理稽核

**範圍**:`research/apex/experiments/q01_financial_scores.py`（Altman Z''、Ohlson O、
DuPont、GPOA、accruals 的 IC 廣篩)+ `q02_pure_financial_books.py` / `q03_stability_graft.py`
（品質/穩定書)+ 欄位源 `research/strat_lab/raw_quarterly.py`。

**結論(白話)**:上線用的品質因子(GPOA、accruals、cfo/資產、獲利波動)定義都對、可信。
但只出現在 q01 研究廣篩、沒進生產策略的兩個破產分數有真 bug:Altman Z'' 把「保留盈餘」
用「權益−股本」硬湊,結果把 114 家已經累積虧損公司的虧損訊號抹掉(而真保留盈餘欄位其實
抓得到);Ohlson O 把「連續兩年虧損」寫成「連續兩季虧損」,10.8% 的樣本判定不同。這兩支
分數在 q01 量到的 IC 不可信,任何據以「接受/否決 Z''、O-score」的結論都要重跑。

**scope 重要事實**:`z_pp`(Altman Z'')與 `o_score_neg`(Ohlson O)全 repo 只出現在 q01,
`evaluate_factor` 是每日截面 rank-IC 廣篩;**兩者未進任何 shipped 策略**。生產路徑用到的
品質因子(q02 品質書的 GPOA/accruals/f_score/cfo_ni/asset_g;q03/S 穩定族的 cfo_ta/ni_vol8/
gm_vol8)定義正確。故 bug 影響「因子研究的接受/否決結論」,不影響現役 S 策略的計分。

---

## BUG

### 1. Altman Z'' X2:保留盈餘用「權益−股本」代理,抹掉累虧訊號
- **學理**(Altman 1993/1995 Z''):X2 = 保留盈餘 / 總資產。保留盈餘衡量累積獲利/存活年資,
  是 Z 系列對「年輕、累虧」公司最靈敏的鑑別項。
- **實作**(q01:71-72,90):`re_ta = (total_equity − capital_stock)/total_assets`,等於
  資本公積 + 保留盈餘 + 其他權益 + 非控制權益,把 paid-in capital 全灌進來。
- **證據**(cache 2023Q4,twse+tpex,1,951 家,同 COALESCE 口徑):
  - 中位 `|proxy − 真RE| / |真RE|` = **57.4%**。
  - **114 家** proxy_re > 0 但真保留盈餘 < 0 —— 累積虧損被 IPO 資本公積完全遮蔽。
  - X2 項截面 Spearman(proxy, 真值) = **0.758**。
  - 最惡例 6854:`x2_true = −8.18`(嚴重危困)vs `x2_proxy = +1.25`(看起來健康),
    單項擺盪 9.4 分,而 Z'' 危困界約 <1.1。
  - 真「保留盈餘（或累積虧損）」科目確實在 `bs_concise_raw`(已查證),代理可避免。
- **修法**:`raw_quarterly.py` BS_TITLES 增 `'retained_earnings': ['保留盈餘（或累積虧損）','保留盈餘']`,
  重建 parquet;q01 `re_ta = pos(retained_earnings)/total_assets`。

### 2. Ohlson INTWO:「連兩年虧損」被寫成「連兩季虧損」
- **學理**(Ohlson 1980 JAR,第 8 項):INTWO = 1 若淨利連續兩個**年度**為負,係數 +0.285。
- **實作**(q01:101):`(ni_q<0) & (ni_q.shift(1).over(C)<0)` —— `ni_q` 是單季、`shift(1)` 是
  前一季,判的是連續兩**季**。
- **證據**(raw_quarterly.parquet 全樣本 102,474 firm-quarters):程式版(2 季)點火 16,637、
  學理版(2 年)點火 14,712;判定不同者 **11,035 = 10.8%**(6,480 筆誤報、4,555 筆漏抓真連兩年虧損)。
- **修法**:`(ni_ttm<0) & (ni_ttm.shift(4).over(C)<0)`,或兩財年年度淨利皆 < 0。

---

## SUSPECT

### 3. Altman Z'' X3:EBIT 用營業利益代理,漏掉業外
- **學理**:X3 = EBIT/總資產,EBIT = 稅前淨利 + 利息費用,**含**業外損益。
- **實作**(q01:65,91):`ebit_ttm` = 營業利益 TTM,排除全部營業外損益、未加回利息。
- **證據**(cache FY2023 twse 1,034 家):中位 `|EBIT_true − op_income|/|op_income|` = 21.2%;
  Spearman = 0.916;**86 家(8.3%)正負號翻轉**(營業虧損但含業外後 EBIT 轉正,或反之)。
  「稅前淨利（淨損）」「減：利息費用」皆在 `is_progressive_raw`(已查證),真 EBIT 可算。
  營業利益是業界常見 EBIT 代理故列 SUSPECT。
- **修法**:IS_TITLES 增 pretax + interest,`ebit = pretax + interest`(TTM)。

### 4. Ohlson CHIN:淨利變動用季位移而非年位移
- **學理**:CHIN = (NI_t − NI_{t−1})/(|NI_t|+|NI_{t−1}|),NI 為**年度**淨利,年對年。
- **實作**(q01:103-104):lag 用 `shift(1)`=前一季 TTM。分子展開 = 本季單季NI − 去年同季單季NI,
  分母卻用 TTM 量級,尺度不一致;根因同 INTWO(年度動態項套季頻)。
- **修法**:lag 統一 `shift(4)`,分子分母皆年對年 TTM 口徑。

### 5. DuPont 三因子不自洽(週轉率年初資產 vs 槓桿/ROE 期末資產)
- **學理**:ROE = 淨利率 × 週轉率 × 權益乘數,須恆等,三項同一 TA/Equity 口徑。
- **實作**:margin=ni/rev、turnover=rev/**TA年初**(raw_quarterly.py:215 Piotroski 口徑)、
  leverage=**TA期末**/equity、roe=ni/equity期末。相乘 = ni·TA_end/(TA_begin·eq) ≠ ni/eq。
- **緩解**:q01 把三項**各自當獨立因子**做 IC(從不相乘),故不失真、每項各自合法;僅
  「DuPont 分解」標籤學理不自洽。另 roe/leverage 用期末權益(非教科書平均)屬常見簡化。
- **修法**:若當分解用,週轉率與槓桿統一資產口徑;若維持獨立因子,turnover 改名標明「年初資產」。

### 6. 權益/淨利含非控制權益(嚴格歸母 ROE 語意有別)
- total_equity=權益總計(含 NCI)、ni=繼續營業單位本期淨利(含 NCI);嚴格 ROE 應用歸母權益/歸母淨利。
- 分子分母同含 NCI 時內部自洽(= 總權益報酬率),故 SUSPECT-低。歸母口徑欄位皆可得。

---

## OK(符合學理 / 刻意合理近似)

- **GPOA**(q01:66,77;q02:104,109):`gp_ttm/total_assets` = Novy-Marx (2013) GP/A,毛利/總資產、
  高=好不取負,完全正確。生產路徑(q02 品質書)沿用同定義。
- **accruals_neg**(q01:76;q02:110):`−(ni_ttm−cfo_ttm)/total_assets` = 現金流量表法總應計
  (Sloan 1996 / Collins-Hribar 2002),取負後高=好,定義與符號皆正確。期末 vs 平均總資產屬簡化。
- **Ohlson SIZE 省 GNP 平減 + FFO≈CFO**(q01:95,100):GNP 平減對同日全體是常數,截面 rank 不變
  (q01 IC 為每日 rank 相關),docstring 已誠實揭露且推理正確;FFO≈CFO 因無折舊欄位屬資料受限之
  標準代理。對 rank-IC 用途皆可接受。
- **文件小瑕疵**:docstring「O-score 算 7/9 項」低估 —— 程式其實 9 項全實作(2 項近似),建議更新註解。
