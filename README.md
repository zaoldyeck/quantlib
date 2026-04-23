# QuantLib-Scala: 專案技術規格書

## 1. 專案概要 (Project Overview)

本專案是一個使用 Scala 編寫的金融數據資料庫，專注於下載、處理並儲存台灣股市 (TWSE, TPEx) 的各類公開數據。它會自動從台灣證券交易所、證券櫃檯買賣中心及公開資訊觀測站等來源爬取資料，並將其整理後存入 PostgreSQL 資料庫中，以利後續的量化分析與策略回測。

---

## 2. 專案架構 (Project Structure)

```
.
├── data/                  # 存放爬取下來的原始 CSV/HTML/JSON 檔案
│   ├── daily_quote/
│   │   ├── tpex/
│   │   │   └── 2024/      # 按年份存放的資料
│   │   └── twse/
│   ├── financial_analysis/ # 不按年份存放的資料
│   └── ...
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── application.conf # 核心設定檔
│   │   │   └── sql/             # SQL 視圖定義
│   │   └── scala/
│   │       ├── db/          # Slick 資料庫 Schema 定義
│   │       ├── reader/      # 資料讀取與解析器
│   │       ├── setting/     # 各類數據的爬取設定
│   │       ├── util/        # 輔助工具
│   │       ├── Crawler.scala  # 網路爬蟲，負責執行下載
│   │       ├── Job.scala      # 組合高階任務
│   │       ├── Main.scala     # 程式主入口
│   │       └── Task.scala     # 高階任務定義 (如何、何時下載)
│   └── test/
└── README.md              # 本說明文件
```

---

## 3. 核心概念與資料流程 (Core Concepts & Data Flow)

本專案的運作流程可分為「設定」、「任務」、「爬取」、「讀取」四個主要階段。為了讓流程更清晰，我們將以 `pullDailyQuote`（下載每日股價）為例，貫穿整個說明。

### 階段一：設定 (Configuration)

一切的起點是設定。設定分散在 `.conf` 檔案和 `setting` 套件中。

#### `application.conf` 設定詳解

這是專案的基礎設定檔，定義了所有資料來源的 URL 和本地儲存路徑。以每日股價為例：

```hocon
data = {
  dir = "./data" // 所有資料的根目錄
  dailyQuote = {
    twse = {
      // TWSE 每日股價的 API 端點，注意結尾的 `date=` 是參數 placeholder
      file = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=csv&type=ALLBUT0999&date="
      // TWSE 每日股價的本地儲存基礎路徑
      dir = ${data.dir}"/daily_quote/twse"
    }
    tpex = {
      // TPEx 每日股價的 API 端點
      file = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&o=csv&se=EW&d="
      // TPEx 每日股價的本地儲存基礎路徑
      dir = ${data.dir}"/daily_quote/tpex"
    }
  }
}
```

#### `setting` 套件運作實例

`setting` 套件讀取 `.conf` 的設定，並將其物件化，供後續流程使用。

1.  **`DailyQuoteSetting.scala`**:
    ```scala
    // 當 `Task` 需要 2024-07-01 的資料時，會這樣實例化
    case class DailyQuoteSetting(date: LocalDate = LocalDate.now) extends Setting {
      // 建立一個給 TWSE 用的 Detail 物件
      val twse: TwseDetail = new TwseDetail(LocalDate.of(2004, 2, 11), None, date) {
        val file: String = conf.getString("data.dailyQuote.twse.file") // 讀取 .conf 的 URL
        val dir: String = conf.getString("data.dailyQuote.twse.dir")   // 讀取 .conf 的路徑
      }
      // 建立一個給 TPEx 用的 Detail 物件
      val tpex: TpexDetail = new TpexDetail(...)
      // 將兩個市場的 Detail 物件打包成一個序列
      val markets = Seq(twse, tpex)
    }
    ```

2.  **`TwseDetail.scala` / `TpexDetail.scala`**:
    這些是 `Detail` 的具體實作，負責根據不同市場的規則，將日期格式化並附加到基礎 URL 後面，產生最終可供下載的完整 URL。
    ```scala
    // TwseDetail.scala 簡化版
    abstract class TwseDetail(..., endDate: LocalDate) extends Detail(...) {
      private val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      def url: String = {
        val endDateString = endDate.format(dateFormatter) // 2024-07-01 -> "20240701"
        // 將基礎 URL 和格式化後的日期組合起來
        this.file + endDateString // "https://...&date=" + "20240701"
      }
    }
    ```

### 階段二：任務 (Task)

`Task.scala` 負責發起、協調和管理所有下載任務。它決定了**何時**以及**對哪些範圍**的資料發起請求。

#### `pullDailyQuote` 流程追蹤

1.  **`job.pullAllData()`** 呼叫 **`task.pullDailyQuote()`**。
2.  `task.pullDailyQuote()` 實例化一個 `DailyQuoteSetting` 物件，並遍歷其 `markets` 列表（`twse` 和 `tpex`）。
3.  對於每一個 `detail` 物件（例如 `twse`），它會呼叫 `pullDailyFiles(detail, ...)`。
4.  `pullDailyFiles` 是**避免重複下載**的核心。它的第一步是呼叫 `detail.getDatesOfExistFiles`。
5.  `detail.getDatesOfExistFiles` 執行以下操作：
    a.  取得基礎路徑，例如 `data/daily_quote/twse`。
    b.  使用 `deepFiles` **遞迴地**掃描此路徑下的**所有檔案和子目錄**。這確保了它能找到 `twse/2024/` 或 `twse/2023/` 等年份資料夾內的檔案。
    c.  對每個找到的檔案，解析其檔名 (`2024_7_1.csv`) 以取得日期。
    d.  **驗證檔案有效性**：只有當檔案**不是一個 HTML 錯誤頁面**時，才被視為有效。空檔案是有效的，代表該日休市。
    e.  最後回傳一個包含所有有效日期的 `Set[LocalDate]`。
6.  `pullDailyFiles` 接著產生一個從 `firstDate` 到今天的所有日期序列。
7.  它使用 `filterNot` 從日期序列中**移除**上一步驟中回傳的「已存在日期」。
8.  對於剩下的「待下載日期」，`pullDailyFiles` 會一一呼叫 `crawler.getFile(...)` 來執行下載。

### 階段三：爬取 (Crawling)

`Crawler.scala` 是實際執行網路下載的底層元件。它只關心如何根據傳入的 `Detail` 物件把檔案抓回來。

1.  `crawler.getFile(detail)` 被呼叫。
2.  **組合儲存路徑**：`Crawler` 會判斷資料類型。對於每日資料，它會將**年份**附加到基礎路徑後，形成最終的儲存路徑，例如 `data/daily_quote/twse/2024`。
3.  **執行下載**：使用 `detail.url`（已由 `Setting` 組合完畢）發起 HTTP 請求。
4.  **儲存檔案**：將下載的檔案流寫入上一步組合好的年份資料夾中，檔名由 `detail.fileName` 定義 (e.g., `2024_7_1.csv`)。

#### 特殊處理

對於 `capital_reduction` 和 `ex_right_dividend` 這兩類一次下載一個大檔案的資料，`Crawler` 在下載完成後，會額外執行一個拆分步驟：讀取該 CSV 檔的內容，並根據每一筆資料的日期，將其分類後，分別寫入到各個對應的年份子目錄中。

### 階段四：讀取 (Reading)

當原始檔案被下載到 `data` 目錄後，`reader` 套件負責解析它們，並將結構化後的資料存入資料庫。

1.  **`job.readAllData()`** 呼叫 **`tradingReader.readDailyQuote()`**。
2.  `readDailyQuote` 同樣會呼叫 `DailyQuoteSetting().getMarketFilesFromDirectory`，此方法一樣使用 `deepFiles` **遞迴**掃描所有年份的子目錄來獲取檔案列表。
3.  它會遍歷所有找到的 `.csv` 檔案。
4.  **解析 CSV**：由於來自不同來源 (TWSE/TPEx) 的 CSV 格式可能會有細微差異（例如欄位順序、表頭文字），`Reader` 中包含了針對這些差異的處理邏輯。
5.  **映射至 Schema**：解析出的資料會被映射到 `db.table.DailyQuoteRow` 這個 case class。
6.  **寫入資料庫**：最後，使用 Slick 將這些 `DailyQuoteRow` 物件批次插入到資料庫的 `daily_quote` 表格中。

---

## 4. 資料庫 Schema (`db/table`)

`db.table` 套件中使用 Slick 定義了所有資料庫表格的結構。每個檔案大致對應一個資料庫表格。

| 表格 (Table)                      | 描述                                     | 主要資料來源 (範例)        |
| --------------------------------- | ---------------------------------------- | ------------------------------ |
| `BalanceSheet`                    | 資產負債表 (詳細)                        | MOPs                           |
| `ConciseBalanceSheet`             | 資產負債表 (簡明)                        | MOPs                           |
| `CapitalReduction`                | 減資                                     | TWSE, TPEx                     |
| `DailyQuote`                      | 每日股價                                 | TWSE, TPEx                     |
| `DailyTradingDetails`             | 三大法人買賣超                           | TWSE, TPEx                     |
| `ETF`                             | ETF 列表                                 | TWSE                           |
| `ExRightDividend`                 | 除權除息                                 | TWSE, TPEx                     |
| `FinancialAnalysis`               | 財務分析                                 | MOPs                           |
| `IncomeStatement`                 | 綜合損益表                               | MOPs                           |
| `Index`                           | 每日指數                                 | TWSE, TPEx                     |
| `MarginTransactions`              | 融資融券                                 | TWSE, TPEx                     |
| `OperatingRevenue`                | 營業收入                                 | MOPs                           |
| `StockPER_PBR_DividendYield`      | 本益比、股價淨值比、殖利率               | TWSE, TPEx                     |

---

## 5. 資料儲存結構

根據數據的特性，主要有兩種儲存結構：

1.  **按年份存放**: 大部分的每日數據、區間數據會按年份存放在對應的子目錄下，以避免單一資料夾內檔案過多。
    -   **範例**: `data/daily_quote/twse/2024/`
    -   **資料類型**: `daily_quote`, `daily_trading_details`, `index`, `margin_transactions`, `stock_per_pbr_dividend_yield`, `operating_revenue`, `balance_sheet`, `income_statement`, `capital_reduction`, `ex_right_dividend`

2.  **不按年份存放**: 對於數據量較小，或本身沒有按日更新的資料，則直接存放在其類別的根目錄下。
    -   **範例**: `data/financial_analysis/twse/`
    -   **資料類型**: `financial_analysis`, `etf`, `financial_statements`

---

## 6. 如何執行

主要的執行入口點是 `Main.scala` 或 `Job.scala`。

-   `job.pullAllData()`: 執行所有資料的下載任務。
-   `job.readAllData()`: 執行所有已下載資料的讀取與資料庫寫入任務。
-   `job.updateData()`: 依序執行 `pullAllData` 和 `readAllData`。 