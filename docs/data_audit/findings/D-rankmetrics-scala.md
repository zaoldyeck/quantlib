# D-rankmetrics-scala — 因子評估統計量(IC / rank-IC / t-stat)學理稽核

**範圍**:`src/main/scala/strategy/RankMetrics.scala`(+ 孿生複本 `FactorResearch.scala::RankMetricsEx`)
+ research 端「alphalens 使用」。
**判定**:**BUG**(核心統計量算對,但「未來報酬」用未還原價,是學理硬傷)。

---

## 白話總結

IC 的數學本身沒問題:它確實是每個換股日「因子分數 vs 未來報酬」的**橫截面 Spearman
等級相關**,ties 用 mid-rank 修正、t 值用 `mean/(std/√n)`、自由度 `n−1`,全部合格。

**真正的病在「未來報酬」怎麼算**:`forwardReturns` 直接拿 `daily_quote.closing_price`
(未還原的原始收盤價)做 `(p1−p0)/p0`。台股每年 7–9 月除權息旺季,**單月就有約三分之一
的股票除權息**,原始收盤價會在除息當天掉一個股利(平均 −3.99%、最大 −28.86%),但這筆
股利其實是配到你手上的現金——**真正的總報酬沒少,帳面報酬卻被砍掉約 4%**。於是殖利率、
價值這類「愛挑高配息股」的因子,它們排在前面的股票在夏天的未來報酬被系統性低估,**IC 被
壓低、跨月 IC 波動變大 → t 值被雜訊稀釋**。結論(哪個因子有 IC)在牽涉配息的因子上不能
全信。純量化冠軍 apex 的 Python 版(`research/apex/factors.py`)已經用**還原價 + T+1 起算**
做對了,偏偏這份 Scala 研究工具沒有,而且同一個錯在 `RankMetricsEx` 又抄了一份。

至於 scope 裡說的「research 端 alphalens 使用」——**alphalens 在整個 repo 裡從沒被 import
過**,只在 CLAUDE.md / agent / skill 文件裡被當成「標準 IC 工具」宣傳,實際跑的是手寫的
`pl.corr(..., method="spearman")`。所以那條線是紙上規格,沒有對過答案。

---

## 逐一算式對照

### 1. BUG — 未來報酬用未還原原始收盤價(非總報酬)

- **檔案**:`RankMetrics.scala:112-154`(`forwardReturns`),原始價出現在 :127 `closing_price`、
  :135 `p0`、:149 `(e.p1 - s.p0)/s.p0`;**同一缺陷第二份**:`FactorResearch.scala:148-186`
  (`RankMetricsEx.forwardReturns`,`Main research` 批次因子 IC 走這條)。
- **學理定義**:因子評估的「forward return」是**總報酬(total return)**——含現金股利再投資、
  且對減資/分割做參考價還原。Grinold & Kahn《Active Portfolio Management》IC = corr(score,
  *realized active return*);Alphalens `get_clean_factor_and_forward_returns` 也是對**還原/
  再投資後**價格算 forward return。原始價報酬(price return)只在無股利、無資本行動時才等於
  總報酬。
- **程式實作**:`(p1 − p0)/p0`,p0/p1 皆為 `daily_quote.closing_price`(專案鐵律載明此欄為
  **未還原原始價**,故有 `research/prices.py::fetch_adjusted_panel`)。除息日原始收盤價掉一個
  股利、減資日參考價重設,`forwardReturns` 全部當成真實漲跌。
- **可重現證據**:
  - 除權息集中度:`2023-07-04→2023-08-01`(約 21 交易日)TWSE 有 **410 / 1198 檔(34%)**
    除權息,平均參考價下修 **−3.99%**、最大 **−28.86%**、平均現金殖利率 3.99%
    (`SELECT count(*), avg((pre-ref)/pre) FROM ex_right_dividend ...`,見稽核 SQL)。
  - 個股實證:2330 於 2023-09-14 除息,`ex_right_dividend` pre=541 → ref=538(配 3 元);
    `daily_quote` 2023-09-13 收 541、9-14 收 550——原始價把配掉的 3 元直接吃掉,`forwardReturns`
    完全看不到這 3 元。
  - 減資:TWSE 每年 15–31 件(2021:27、2022:31、2023:26),參考價重設(如 2323 2022-10-24
    pre 6.53 / ref 6.20)被當成 −5% 真跌。
  - 對照組:`research/apex/factors.py:4` 明文「Forward return … **調整價**,T+1 起算,零
    look-ahead」——冠軍 Python 引擎已做對,Scala 這份沒有。
- **影響**:與殖利率/價值因子相關 → 夏季月份 IC 被系統性壓低、IC std 被灌大 → mean IC 偏低、
  t 值偏保守;跨月方向不穩。血徑限縮於**凍結的 Scala 研究輸出**(`Main strategy` / `Main
  research`),不在 Serenity/apex 實盤路徑;但它就是「判斷因子有沒有 IC」的那把尺,尺歪結論歪。
- **修法**:forward return 改用還原價——除息以 `ex_right_ex_dividend_reference_price /
  closing_price_before_ex_right_ex_dividend` 的比例、減資以 `post_reduction_reference_price /
  closing_price_on_the_last_trading_date` 的比例接續調整(即 `prices.fetch_adjusted_panel`
  的 Scala 對應),或直接讓 Scala 研究命令退役、統一走 Python `prices.py`。兩份複本一起修。

### 2. SUSPECT — 「research 端 alphalens」是紙上規格,從未被 import

- **學理定義**:Alphalens 的 `factor_information_coefficient` 預設用 `scipy.stats.spearmanr`
  (rank IC),forward return 由傳入的**價格面板**算——面板須為還原/再投資價才正確。
- **程式實作**:`alphalens-reloaded>=0.4.6` 列在 `research/pyproject.toml`,且 CLAUDE.md:182/364、
  `.claude/agents/quantlib-factor-researcher.md`、`.claude/skills/quantlib-factor-test/SKILL.md:51-62`
  都把它當「標準 IC/quantile 工具」。但 `grep -rn alphalens research/ --include=*.py` **零命中**
  ——所有實跑 IC 都是手寫 `pl.corr(..., method="spearman")`(apex/factors.py:78、g04/g06、
  valuation_replay_2025.py:540)。
- **證據**:全 repo `.py` 對 `import alphalens` / `al.utils` / `create_*_tear_sheet` 皆無命中;
  唯一出現處為文件與 skill 指示。
- **影響**:scope 指定要查的「alphalens 使用」實際不存在;skill Step 4 規定的 alphalens 路徑
  從未與手寫路徑對過答案(漂移風險),且其 `prices_wide` 若傳原始 close,forward return 會
  複製 Finding 1 的股利偏差。
- **修法**:要嘛真的接上 alphalens 並與手寫 IC 做一次逐值對照當守護,要嘛把文件/agent/skill
  的「alphalens」改標為手寫 polars 管線,別讓紙上工具冒充事實來源。

### 3. SUSPECT — spearman/forwardReturns 兩份複本 + 死碼,違反「引擎唯一真源」

- **學理/工程**:同一計算只該有一份(CLAUDE.md「引擎唯一真源鐵律」)。複本必漂移。
- **實作**:`RankMetrics.spearman/averageRanks/forwardReturns`(:81-154)在
  `FactorResearch.scala::RankMetricsEx`(:119-187)整份被抄一次。**已經漂移**:
  `RankMetrics.scala:126` 多算了一個 `rn_desc` 視窗欄位,但只有 :147 用 `rn_asc`——
  `rn_desc` 是純死碼;`RankMetricsEx`(:155)正確地沒抄它。
- **證據**:`grep -n rn_desc` 於兩檔——RankMetrics 有(:126,未被 :147 消費),FactorResearch 無。
- **修法**:`RankMetricsEx` 併回 `RankMetrics` 只留一份;順手刪 `rn_desc`。

### 4. SUSPECT(輕微)— IC 以 asOf 收盤(T)起算,回測/契約卻 T+1 進場

- **學理**:IC 的 forward return 應從「訊號可執行的時點」起算。訊號用到 T 收盤資料 →
  最快 T+1 才可成交。
- **實作**:`RankMetrics.scala:135-142` p0 = ≤asOf 最近收盤(即 T 收盤);報酬含 T→T+1 這段
  不可捕捉的位移。CLAUDE.md「Asof +1 day shift」與 apex(`close[T+1]` 起算)都用 T+1。
- **性質**:**非前視**(分數不含未來資料),但慣例不一致,會小幅高估可交易訊號。
- **修法**:forward return 改從 T+1 收盤起算,與執行對齊。

### 5. SUSPECT(低)— 未來窗口用「2×horizon 日曆日」框「horizon 交易日」,農曆年會漏樣本

- **實作**:`RankMetrics.scala:130` 窗口 = `asOf + 2*horizonDays 日曆日`,取 `rn_asc=horizonDays`
  第 N 個交易日。21 交易日平常 ≈ 29–31 日曆日,但跨農曆年(封關約 9 天)可能 >42 日曆日 →
  該股當月無 `rn_asc=21` → 被靜默剔出當月 IC。
- **證據**:純日曆換算 + 台股 CNY 封關;剔除只影響樣本數,不偏誤被納入者的報酬值。
- **修法**:窗口放寬(如 3×)或直接以交易日計數選第 N 日。

### 6. OK — Spearman rank IC 定義正確(tie 修正)

- **定義**:Spearman ρ = 兩序列**等級**的 Pearson 相關;有 ties 時須用 mid-rank + Pearson-on-ranks
  (簡化式 `1−6Σd²/(n(n²−1))` 只在無 ties 成立)。
- **實作**:`averageRanks`(:96-108)對等值給 mid-rank((i+j)/2+1),`spearman`(:81-93)對兩組
  rank 算 Pearson。即 tie-corrected Spearman = 橫截面 rank IC。分母 `denX/denY` 為 0 時回 0,防
  常數序列除零。**與學理相符**。

### 7. OK — IC 序列 t 值:公式與自由度正確

- **定義**:對 IC 時間序列檢定 H0: E[IC]=0 的單樣本 t = mean/(s/√n),s 為樣本標準差(ddof=1),
  df = n−1;等價於 Grinold-Kahn 的 IC t = IR·√n(IR=mean/std)。
- **實作**:`summarize`(:72-77)mean = Σic/n;std = √(Σ(ic−mean)²/(n−1))(**ddof=1 正確**);
  tStat = mean/(std/√n)。`n<2` 回 std=0/t=0,std=0 時 t=0 防除零。apex/factors.py:86 同式。
  顯著門檻 t≥2.0 標為 rule-of-thumb(≈ 常態 1.96),已在註解聲明,屬**刻意近似 → OK**。

### 8. OK — 自身對齊無前視;非重疊窗口使 t 值 IID 假設成立

- 分數在 asOf 用 ≤asOf 資料;forward return `date > asOf`(嚴格未來)——**score 端無未來洩漏**。
- 換股採 `RebalanceCalendar.monthlyAfterDay`(月頻,約 21 交易日/次),horizon=21 → 相鄰月的
  未來窗口幾乎不重疊 → IC 序列近似獨立,簡單 t(無 Newey-West)成立。
- **Caveat**:t 值 IID 是「horizon ≈ 換股間隔」的巧合,非程式保證;若呼叫端傳入 horizon > 間隔
  (如週頻換股配 21 日 horizon),窗口重疊會**膨脹 t**,此時需 Newey-West/有效樣本數修正。

### 9. OK(功能缺口,非錯)— 分位數單調性未實作;IR 未具名輸出

- `RankMetrics.scala` 只算 IC + t,**沒有** decile/quantile 報酬單調性檢定。非算錯,是沒做;
  Python `research/apex/factors.py:88-99` 已正確實作 decile spread(橫截面 rank 分 10 桶取均值)。
- IR = mean(IC)/std(IC) 未列為具名欄位,但 tStat = IR·√n 已內含,**非偏差**。

---

## 已檢查清單

- 精讀 `RankMetrics.scala`(155 行,spearman/averageRanks/summarize/forwardReturns 全式)
- 精讀 `FactorResearch.scala`(individualICs/pairwiseCorrelations + RankMetricsEx 複本)
- 對照 `Main.scala:286`(horizonDays=21)、`RebalanceCalendar.monthlyAfterDay`(月頻換股)
- 盤點 research 端實際 IC 實作:apex/factors.py(Spearman+decile+t,調整價 T+1)、g04/g06、
  valuation_replay_2025.py、sprint_a(已自我聲明為「同期一致性、非 forward IC」)
- 確認 alphalens 全 repo `.py` 零 import(只在文件/agent/skill 出現)
- DB 量化除權息集中度(2023-07 窗口 410/1198=34%、平均 −3.99%、最大 −28.86%)
- DB 驗證 `daily_quote` 為未還原(2330 2023-09-14 除息 3 元隱形於原始收盤)
- DB 盤點減資頻率(TWSE 15–31 件/年)與參考價重設樣本
- 確認 `rn_desc`(RankMetrics.scala:126)為死碼、複本已漂移
