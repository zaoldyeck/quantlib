# D-valuation — 估值模型(DCF / PEG / 五線譜 / valuation view)學理稽核

範圍:`src/main/resources/sql/view/8_valuation.sql` + `9_valuation_1q.sql` +
research 端估值程式(`src/quantlib/apex/experiments/v01_valuation_factors.py`、
`src/quantlib/evergreen/ev18_make_packs.py`)。

## 一句話結論

那兩張 SQL 估值檢視(`valuation` / `valuation_1q`)的樂活五線譜和 DCF 有真的學理
錯,但**目前沒有任何生產程式在讀它們**;實際上線用的估值(Evergreen 的 PE/PB
三年百分位、research 的對數殘差五線譜、兩階段 CFO DCF)反而都算對。所以錢沒被
汙染,但這兩張檢視不能當估值真源用——要嘛照下面修,要嘛標為棄用。

**Verdict: BUG**(單位內含已證實的學理錯誤公式)。

## 消費者盤點(為何錢沒被汙染)

- `grep` 全 `src/main/scala/` 與 `research/`:**查無** `from valuation` / `valuation_1q` /
  `dcf_10y` 等消費者。這兩張 SQL 檢視是遺留物,未接進任何策略或報表。
- 生產 Evergreen packs 的估值走 `ev18_make_packs.py::valuation()`,**自己**用
  `stock_per_pbr` 算 PE/PB 三年百分位,不碰上述檢視。

---

## BUG 級(三項,都在 SQL 檢視)

### 1. 樂活五線譜的「標準差」用錯:原始價 SD 而非迴歸殘差 SD

- 位置:`8_valuation.sql:41-49`、`9_valuation_1q.sql:42-50`(`stddev(closing_price)`)。
- 學理:線性迴歸通道 / 樂活五線譜(薛兆亨、tivo168《五線譜投資術》)的 ±σ、±2σ,
  σ 是**價格對趨勢線的殘差標準差**(Excel 原版 `STEYX`=估計標準誤)。數學上
  `σ_resid² = σ_price²·(1−R²)`。
- 實作把 σ 取成**原始收盤價**的標準差,中心線卻是迴歸趨勢線 → 中心=去趨勢、
  頻寬=含趨勢,口徑打架,通道被系統性撐寬。
- 可重現證據(cache `daily_quote`,twse,最近 3.5 年、n≥600 的 1198 檔,
  `regr_r2(close, epoch(date))` 估 R²,頻寬膨脹=`1/√(1−R²)`):

  | 分位 | R² | 通道寬 vs 教科書 |
  |---|---|---|
  | 中位 | 0.392 | **1.28×** |
  | p75 | 0.651 | 1.69× |
  | p90 | 0.819 | 2.35× |
  | p95 | 0.880 | 2.89× |

  58.1% 的股票通道至少寬 1.2 倍、35.3% ≥1.5 倍、16.4% ≥2.0 倍。**趨勢越乾淨撐
  越兇**,而通道正是給趨勢股用的 → 穩步上漲的股票幾乎永遠踩不到過寬上軌,
  `evaluation` 恆為 0、`price_err` 失真。
- 內部反證:`v01_valuation_factors.py:92-111` 的 `fiveline_z_neg` 就用對數價 OLS
  **殘差**標準化 z,是正解。SQL 錯、Python 對。次要:SQL 用線性價,對數價才對。
- 修法:σ 改殘差標準差 `sqrt(var_samp(close) − slope²·var_samp(x))`(或用
  `regr_*` 併 SSE `/(n-2)` 對齊 STEYX);建議同時改對數價,和 `fiveline_z_neg`
  併成單一真源。

### 2. `eps_growth_rate_10y` 視窗排序寫成 desc → 亂序 / 前視

- 位置:`8_valuation.sql:101-103`、`9_valuation_1q.sql:103-105`。
- 3y/5y 用 `order by year, quarter`(遞增、trailing,正確);只有 10y 這條多了
  `desc`。`order by year, quarter desc` = year ASC + quarter DESC,是非時序亂序,
  `39 preceding` 取到錯亂季度的混合;若本意是完整反時序,則 `39 preceding` 變成
  **當期之後 39 季 = 未來 = 前視**。兩種解讀都壞。此值餵進 `dcf_10y` / `dcf_10y_err`。
- 修法:去掉 `desc`,統一 `order by year, quarter rows between 39 preceding and current row`。

### 3. DCF 成長率用「算術平均 YoY」而非幾何 CAGR → 灌水合理價

- 位置:`8_valuation.sql:94-100`、`9_valuation_1q.sql:96-105`。
- 學理:複利折現的 g 應是幾何 CAGR `(EPS_t/EPS_{t−N})^(1/N)−1`;算術平均 ≥ 幾何
  (AM-GM),把算術平均當 g 餵 `(1+g)^t` 系統性高估內在價值。
- 證據:玩具序列 EPS=[1,1.5,1.2,2.0,1.8,2.5],算術平均 YoY=**0.2511**、幾何
  CAGR=**0.2011**,g 灌水約 5pp(相對高 25%),經十年複利放大後 `dcf_Ny` 明顯偏高。
  另 `eps/lag(eps,4)−1` 在盈餘近 0 會爆量、被離群值主宰。內部反證:`dcf_proxy`
  的 `g3y=(rev_ttm/rev_ttm.shift(12))^(1/3)−1` 就是正確幾何 CAGR。
- 修法:`g_Ny = (eps/lag(eps,4N))^(1/N) − 1`,並對 `eps≤0` 端點做域外處理。

---

## SUSPECT 級(有聲明的近似 / 偏離定義)

### 4. DCF 終值是「有限 10 年年金」而非 Gordon 永續

`8_valuation.sql:104-128`。第二段 `eps·x^N·y·(1−y^M)/(1−y)`(M=10)是有限成長
年金,第 20 年後價值歸零。以程式常數 r=0.12、g_t=0.04 實算:10 年年金
`Σy^s=6.804` vs Gordon 永續 `y/(1−y)=13.000` → 終值段只擷取永續的 **52.3%**、
短少 47.7%。教科書兩階段 DCF 終值=`CF_N(1+g_t)/(r−g_t)`。`dcf_proxy` 用的才是
正確 Gordon 永續。另 r=12% 全市場一體適用、無個股化,屬無證據常數。

### 5. DCF 折現 EPS 而非 FCF/股利

`8_valuation.sql:116-128`。EPS 全額當可分配、又假設它靠再投資成長 → 重複計入
再投資來源,對成長股高估。教科書 DCF 折現 FCFF/FCFE/股利。research 端至少用 CFO,
較接近可分配現金。屬零售式簡化,但偏離定義。

### 6. `peg_inv` 用營收成長而非盈餘成長

`v01_valuation_factors.py:62,87-88`。PEG 的 g 學理上是盈餘成長,這裡 `g_yoy` 取自
`rev_ttm`(營收 YoY)。docstring 誠實註明「營收成長」,但 `ledger/batches.md:1809`
又寫成「NI 成長」,文件內部矛盾(碼是營收)。此因子已被 IC 判死
(`factors.jsonl` peg_inv h63 mean_ic −0.017、spread −12%),不進生產。

### 7. `ev_ebit_inv` 的 EV 用總負債而非淨負債

`v01_valuation_factors.py:58,86`。`tl = 流動+非流動負債`(含應付、預收等非附息
營業負債)且未扣現金。教科書 EV=市值+附息淨負債−現金。用總負債對銀行/壽險/大量
預收的公司會把 EV 灌到荒謬。docstring 已聲明「無現金扣除」,僅供截面 IC rank。

---

## OK 級(逐項核對通過,含正面發現)

- **`fiveline_z_neg`**(`v01:92-111`):對數價 400 日滾動閉式 OLS 殘差標準化 z,
  逐項推導核對正確;是修 SQL 五線譜的內部參照真源。
- **`dcf_proxy`**(`v01:81-90`):兩階段 5 年 + 正確 Gordon 永續終值 + 幾何 CAGR +
  CFO 基準。caveat 皆有聲明(營收 CAGR 套 CFO、r/g 常數、CFO 未扣 capex),作 rank OK。
- **Evergreen PE/PB 三年百分位**(`ev18_make_packs.py:108-125`):`date < t0` 嚴格
  無前視、756≈3 年、經驗百分位含等於為標準定義。這是**實際上線**的估值特徵,正確。
- **SQL 月→季 PIT 映射**(`8_valuation.sql:215-220`):逐月對照台股法定公告期限
  (Q1 5/15、Q2 8/14、Q3 11/14、年報 3/31),全程保守、無 look-ahead。正面發現。
- **`ep`/`bp`/`cfo_yield`**(`v01:49-51,85`):1/PE、1/PB(gated >0)、CFO/mcap,
  標準殖利率。
- **`per_err`/`pbr_err`/`dividend_yield_err` + `rank() x`**:自訂相對便宜度分數,
  符號一致(便宜=+1)、min-max 置中標準化;OLS 斜率與當期擬合值對 x 平移不變,
  全域 rank 不致偏。caveat:hi/lo 用 3.5 年極值當界,對單日離群值敏感。

---

## 給主流程的修復優先序(僅結論,不動碼)

1. 若要保留 SQL `valuation`/`valuation_1q` 為可用真源:修 #1(五線譜殘差 σ)、
   #2(10y 排序)、#3(幾何 g),並決定 #4 終值要不要改 Gordon。
2. 若不打算再用:在檔頭標 `-- DEPRECATED, superseded by research fiveline_z_neg /
   dcf_proxy`,避免有人誤當估值真源接線。
3. 順手修 #6 的 ledger 標註使其與碼一致(營收成長)。
