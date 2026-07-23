# B-view-7_sheet_music 財務定義與算式稽核

**一句話結論:這份數字「算得對」但「意義有偏」,而且它根本不是財務報表算式。**
`7_sheet_music.sql` 不是 ROE / ROIC / 毛利率那種財報 view,而是一張**純價格的線性回歸
通道圖**(俗稱「五線譜」):對每檔股票近 3.5 年收盤價跑一條回歸趨勢線,上下各畫 ±1σ、
±2σ 五條線。我逐位重算過,view 算的就是它宣稱的東西(零誤差)。**唯一實質問題**:五條線
的「中線」是那條**斜的回歸線**,但「線距(σ)」用的卻是收盤價**繞水平均值**的標準差,不是
繞回歸線的殘差標準差——兩者對不上,導致會漲的股票通道被畫寬 **2.0~2.4 倍**,「碰到最上線
= 超漲」這個訊號幾乎永遠不會觸發。因為現在**沒有任何程式引用這張 view**,實害暫時是零。

所以財報稽核那幾項(TTM 累計差分、PIT 前視、ROE 用期末還期初、合併 vs 個體)在這裡
**全部不適用**——這檔沒碰任何財務報表。別把「沒抓到財報 bug」誤讀成「財報算式通過」。

---

## 稽核對象

| 項目 | 位置 |
|---|---|
| view 定義 | `src/main/resources/sql/view/7_sheet_music.sql`(view 名 `sheet_music_3y6m`)|
| 資料來源 | `daily_quote`(market ∈ {twse, tpex},近 3.5 年,只用 `closing_price` + `date`)|
| 建立機制 | `Task.scala:93-100 createViewsAndMaterializedViews()`(掃 `sql/view/*.sql` 依檔名排序建立)|
| 現況 | PG 已存在,`SELECT count(*) FROM sheet_music_3y6m` = 2453 列 |
| 消費者 | **無**。全庫 grep `sheet_music`(scala/py/sql/md/conf)只命中定義檔本身 |

## 這張 view 到底算什麼(逐欄公式)

對每檔 `company_code`,取近 3.5 年(`current_date - 3年6月`)的收盤價序列,令
`pos = rank() over (order by date)`(交易日序號,1..n),跑最小平方回歸:

- `slope = regr_slope(closing_price, pos)` — 每個交易日的價格斜率(元/日)
- `intercept = regr_intercept(closing_price, pos)`
- `sd = stddev(closing_price)` — **收盤價的樣本標準差(繞其水平均值)**
- 用 `distinct on (company_code) order by pos desc` 收斂到**最新一個交易日**(`maxpos`)

輸出五條線,全部在 `pos = maxpos` 這一點求值:

| 欄位 | 公式 | 含義 |
|---|---|---|
| `highest` | `slope·maxpos + intercept + 2·sd` | 趨勢線 +2σ |
| `high` | `slope·maxpos + intercept + 1·sd` | 趨勢線 +1σ |
| `tl` | `slope·maxpos + intercept` | 趨勢中線(trend line)|
| `low` | `slope·maxpos + intercept − 1·sd` | 趨勢線 −1σ |
| `lowest` | `slope·maxpos + intercept − 2·sd` | 趨勢線 −2σ |

`closing_price`(最新收盤)拿來跟這五條線比,看「現在價格落在通道哪一格」。

**逐位驗證(自己重算 vs view 輸出,完全吻合)**:

```
1101: maxpos=838  tl=22.67  high=27.81  highest=32.96   (view 一字不差)
2330: maxpos=838  tl=1858.13 high=2371.73 highest=2885.33 (view 一字不差)
```

## 唯一實質問題:σ 用錯基準(SUSPECT)

**中線是斜的回歸線,σ 卻是繞水平均值量的。**回歸通道(Raff channel / 標準誤差通道,
TradingView「Linear Regression Channel」等)的線距應該是**殘差**(價格 − 回歸線)的標準差
或標準誤差;這裡用 `stddev(closing_price)` 量的是價格**繞其平均值**的分散,把「趨勢本身的
移動」也算進了通道寬度。

數學關係:`σ_殘差 = σ_價格 · sqrt(1 − r²)`。會漲的股票 r² 高,兩者差距就大。實測四檔:

| 代號 | view 的 sd(繞均值)| 正確殘差 sd(繞回歸線)| r² | 倍率 |
|---|---|---|---|---|
| 2330 台積電 | 513.60 | 215.22 | 0.824 | 2.4× |
| 1101 台泥 | 5.14 | 2.18 | 0.821 | 2.4× |
| 2317 鴻海 | 51.70 | 25.55 | 0.756 | 2.0× |
| 2412 中華電 | 7.06 | 3.47 | 0.758 | 2.0× |

**後果**:通道被畫寬 2~2.4 倍。以 2330 為例,view 的 `highest = tl + 2·513.6`,正確應該是
`tl + 2·215`;等於把「超漲線」畫到高得離譜的地方,價格幾乎永遠碰不到 ±2σ。作為「這檔相對
自己趨勢是不是被拉太開」的選股訊號,趨勢股會恆顯「通道正中央」,訊號幾乎失效;只有沒方向、
盤整(r²≈0)的股票才勉強正確(因 r²→0 時 σ_殘差 ≈ σ_價格)。

**為何列 SUSPECT 而非 BUG**:這檔沒有文件寫明「通道寬度的定義」,也沒有任何消費者;若作者
本意就是「回歸線 ± 原始價格 σ」,那它就是照做、不算 bug。但「中線用 A、線距用 B」這件事本身
在統計上就是不一致的,這點可證。**修法**:`sd` 改成殘差標準差,例如
`stddev(closing_price - (slope·pos + intercept))`(需先算 fitted 值,兩趟)或用
`σ_價格 · sqrt(1 − regr_r2(closing_price, pos))` 一趟近似。

## 查了、沒問題的項目(OK)

- **逐位算式正確**:1101 / 2330 手算 tl/high/highest 與 view 完全一致;`regr_slope(Y,X)`
  的 (Y=price, X=pos) 引數順序正確,slope 是 d(price)/d(pos)。
- **沒有除法 → 沒有 ±inf / NaN 風險**。「分母保護」一項在此 N/A。degenerate 輸入
  (<2 個非空價)時 `regr_*` / `stddev` 回 NULL 不報錯:2453 列中僅 **1 列**(某新上市稀疏碼)
  五條線為 NULL,不 crash。
- **跨市場合併是良性的**:近 3.5 年有 7 個 company_code 同時出現在 twse 與 tpex
  (4736/5236/6423/6446/6472/6589/8476)。這些是**上櫃轉上市**(TPEx→TWSE)的同一家公司,
  price 分區只用 `company_code`,等於把該公司完整價格史接起來。驗證 4736:tpex 到
  2023-12-21 收 163.5 → twse 從 2023-12-22 開 161,連續、無跳空;`(company_code,date)`
  重複列 = **0**(無同日雙市場疊列、無雙計)。台股代號跨市場唯一,故此合併正確而非污染。
- **`rank()` 當 pos 安全**:因 `(code,date)` 零重複,rank 沒有 tie,pos 就是乾淨的 1..n;
  `distinct on ... order by pos desc` 因此確定性地取到最新一日。
- **無前視**:view 只用 ≤ 今天的收盤價,不碰任何未來或財報公告日資訊。

## PIT 備註(by-design,非 bug)

view 用 `current_date` 動態取「最近 3.5 年」,所以**每次查詢結果隨當天變動**、無法還原到
過去某一天的樣貌(不可歷史重放)。對一張「看現在價格在通道哪裡」的即時篩選 view 這是設計本意,
不是前視偏誤;但若未來要拿它做回測,得先把 `current_date` 參數化成 as-of 日,否則沒有 PIT 語義。

## 怎麼查的(可重跑)

1. 精讀 `7_sheet_music.sql` 全 35 行,寫下每欄分子/分母/期間/單位。
2. 全庫 grep `sheet_music` 找消費者(僅命中定義檔)+ 讀 `Task.scala` 確認 view 建立機制。
3. `psql` 確認 view 存在、`daily_quote` schema、跑樣本股 2330/2317/1101 看輸出。
4. **量化 σ 錯基準**:一趟 SQL 同時算 `stddev(price)`(view 用的)與
   `stddev(price − (slope·pos+intercept))`(殘差),對四檔得 2.0~2.4× 並用 r² 對上關係式。
5. **驗證跨市場/重複風險**:查「同碼跨 twse+tpex」(7 筆,皆遷市)與「同碼同日重複列」(0 筆),
   抽 4736 看遷市邊界價格連續。
6. **逐位 parity**:獨立重算 1101/2330 的 tl/high/highest = view 輸出,零誤差。
7. 查 NULL 線比例(1/2453)、確認全 view 無除法。
