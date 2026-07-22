# 台股量化交易策略研發 SOP

最後更新：2026-05-18

資料層狀態：`var/cache/cache.duckdb` 已包含 `industry_taxonomy_pit`。研究時必須以當次實際查到的 `daily_quote`、`stock_per_pbr`、`daily_trading_details`、`operating_revenue` 與 `industry_taxonomy_pit` cutoff 為準，不得沿用本文件文字中的舊日期。

本文件是台股量化策略研發的正式作業流程。任何新策略、策略改版、參數 sweep、文件排名或自動交易前檢查，都必須依這份 SOP 執行。目的不是最大化紙面績效，而是建立可重現、point-in-time、含成本、可轉成真實交易的策略。

## 1. 核心原則

1. **先定義投資問題，再寫策略**
   - 交易方向：long-only。
   - 持股數：預設不再用固定檔數作為 reject gate。每個策略仍必須回報觀測最大持股、平均持股、成交率、流動性與換手風險；只有當使用者重新指定上限時，持股數才恢復為硬限制。
   - 執行限制：以真實可成交邏輯為準，不使用 signal-day close 假設買進。
   - 評估基準：必須與 0050、主動式 ETF、既有最佳策略在同一資料截止日與同一執行假設下比較。

2. **資料正確性優先於模型複雜度**
   - 價格必須經 `research/prices.py` 產生 total-return-equivalent adjusted OHLCV。
   - 產業分類必須使用 `industry_taxonomy_pit`，不得直接使用 `operating_revenue.industry` 當策略分類。
   - 財報、營收、籌碼與產業分類都要遵守 point-in-time，不能把未來資料套回過去。

3. **策略必須能解釋賺的是什麼錢**
   - 每個候選策略都要能說明 alpha 來源：品質、成長、動能、產業相對強度、籌碼、風險承擔、流動性補償或事件反應。
   - 如果績效主要來自單一個股、單一產業、單一年代或單一參數，不能視為 robust。
   - 對「結構性瓶頸 / Serenity-style」這類策略，研究順序必須是產業 thesis 先行：先定義需求驅動、供應鏈層級、瓶頸類型與候選股，再由程式做營收、估值、動能、流動性與風險 double-check。不得先由程式掃出高動能股票，再事後硬套產業故事。

4. **研究與上線分階段**
   - `research_candidate`：想法或 exploratory result。
   - `backtest_validated`：嚴格專業回測通過。
   - `execution_ready`：target-book / order-level 實作可重現，且與 validated NAV 對齊。
   - `live_pilot`：小資金實盤測試。
   - `production_scaled`：允許大資金與自動化執行。

## 2. 每次研究前的資料流程

### 2.1 檢查資料新鮮度

任何策略結論、排名、KPI、ETF 比較或自動交易目標產生前，都要先查：

```bash
uv run --project research python - <<'PY'
from db import connect
con = connect()
for table, col in [
    ("daily_quote", "date"),
    ("daily_trading_details", "date"),
    ("margin_transactions", "date"),
    ("stock_per_pbr", "date"),
    ("foreign_holding_ratio", "date"),
    ("sbl_borrowing", "date"),
]:
    print(table, con.sql(f"select max({col}) from {table}").fetchone()[0])
print("operating_revenue", con.sql("select max(year * 100 + month) from operating_revenue").fetchone()[0])
print("industry_taxonomy_pit", con.sql("select max(effective_date) from industry_taxonomy_pit").fetchone()[0])
PY
```

如果 PostgreSQL 或 DuckDB cache 落後，先更新資料：

```bash
sbt "runMain Main update"
uv run --project research python research/cache_tables.py
```

### 2.2 資料建置後的最低驗證

```bash
uv run --project research pytest research/tests/test_db.py research/tests/test_prices.py research/tests/test_industry_taxonomy.py -q
```

最低接受條件：

- `research.db.connect()` 可平行 read-only 開啟。
- 還原價格測試通過。
- `industry_taxonomy_pit` 可以 point-in-time as-of join，且不會退回 latest-static 分類。

## 3. 產業分類標準

正式產業分類來源是 `research/industry_taxonomy.py` 建出的 `industry_taxonomy_pit`。

來源與依據：

- 原始來源：MOPS 月營收資料中的官方產業別欄位。
- 判定依據：TWSE / TPEx 官方產業類別規則，核心是主要業務、營收占比與交易所核准分類。
- cache 表：`industry_taxonomy_pit`。
- join 方式：依 `market, company_code, date` 做 backward as-of join。
- effective date：使用月營收發布代理日，也就是下一月 13 日附近，保守避免未來資料。

策略程式不得直接：

```sql
SELECT DISTINCT ON (company_code) company_code, industry
FROM operating_revenue
ORDER BY company_code, year DESC, month DESC
```

這種 latest-static 做法會把現在分類套回歷史，對產業輪動框架是不合格的。

## 4. 策略假設提出

每個新策略都要先寫清楚：

1. **Alpha 假設**
   - 例如：營收加速 + 股價突破 + 法人參與代表基本面被市場重新定價。
   - 不可以只說「這樣回測比較高」。

2. **適用市場結構**
   - 台股散戶占比、漲跌幅限制、除權息、月營收公布、外資 / 投信 / 自營商交易行為、借券與融資融券。

3. **可失效條件**
   - 例如：產業集中過高、成交量不足、事件訊號被市場提前反映、交易成本吞噬、單一年代有效。

4. **交易規則**
   - universe。
   - entry signal。
   - exit signal。
   - MFE / MAE diagnostic：先看既有持倉在持有期間的最大有利波動與最大不利波動，再決定停利、停損、追蹤停利或 time stop 是否有統計理由。
   - position sizing。
   - max positions。
   - industry cap / single-name cap。
   - cash handling。
   - execution timing。

### 4.1 產業 thesis-first 策略的可回測格式

產業研究先行的策略可以回測，但前提是把質性判斷轉成 point-in-time thesis registry。Registry 是人工研究輸入，不是程式掃描結果，最低欄位為：

| 欄位 | 說明 |
|---|---|
| `theme_id` | 產業 thesis ID，例如 `ai_ccl`、`advanced_packaging` |
| `theme_name` | 可讀名稱 |
| `bottleneck_layer` | 供應鏈瓶頸層，例如 CCL、CoWoS、探針卡、光通訊 |
| `active_from` / `active_until` | 這個 thesis 在回測中何時可被使用；不得早於證據可觀測日 |
| `company_code` | 候選股 |
| `conviction` | 研究員事前信心分數，用於排序輔助，不得由未來績效反推 |
| `source_note` | 證據摘要；正式升級前要補 source URL、文件日期、重點摘錄 |

回測器只能在 `active_from <= signal_date <= active_until` 的候選池內選股。程式的角色是 double-check：

- 月營收是否支持 thesis。
- 股價是否已進入相對強勢或至少沒有明顯失效。
- 估值是否過度 priced for perfection。
- ADV 是否足夠成交。
- 法人籌碼是否支持或至少沒有極端背離。
- 單一 theme / 產業集中度是否過高。

#### 4.1.1 結構性瓶頸股的估值 overlay

Serenity-style 策略不能只用靜態 PE / PB 判斷貴便宜，因為真正的結構性瓶頸股常常會經歷基本面上修與估值 re-rating。估值層的目的不是排除所有高 PE，而是回答：

```text
目前價格隱含的成長率，是否仍低於產業 thesis 與基本面資料可支持的成長率？
```

正式估值研究優先使用：

1. **PEG**
   - 用 PE 除以可支持成長率。
   - 成長率不得只用單月營收，應至少混合 3 個月營收 YoY、淨利 TTM YoY、毛利率變化。
   - 適合作為 Serenity 候選池內的主排序估值因子。

2. **Reverse DCF implied-growth gap**
   - 先用目前 PE 反推市場隱含的 5 年 EPS / owner-earnings CAGR。
   - 再用營收、淨利、毛利率估計可支持成長率。
   - `supported growth - implied growth` 越高，代表市場尚未完全定價 thesis；若為負，代表市場價格需要比基本面更樂觀的劇本才合理。

3. **DCF + PEG blend**
   - 正式研究版優先使用 blend，而不是單一估值公式。
   - 目前可重跑範例：

```bash
uv run --project research python research/experiments/serenity_valuation_methods_replay_2025.py --start 2025-01-01
```

目前 2025-至今驗證結果：

- `PEG`、`reverse DCF gap`、`DCF upside`、`DCF + PEG blend` 在 63 / 126 日 forward-return IC 上都為正。
- `PE band` 不適合作為結構性瓶頸股主估值法，因為歷史 PE 區間常會錯殺 re-rating。
- `gross-profit yield` 即使在單次 NAV 回測中最高，也不能單獨採用；若 IC 為負，應視為持倉/產業暴露效果而非穩定估值 alpha。

目前的可重跑範例：

```bash
uv run --project research python research/experiments/serenity_industry_first_replay_2025.py --start 2025-01-01
```

此範例是 SOP 可回測性的第一版，使用 `research/experiments/serenity_industry_thesis_registry_2025.csv` 作為 thesis registry。若要升級成可列入正式策略排名的結果，必須把 registry 補成嚴格研究日誌：每筆 thesis 要有可驗證的證據日期、來源連結與當時可得的 analyst note。

## 5. Backtest 實作標準

1. **價格**
   - 必須使用 `research/prices.py` 的 adjusted OHLCV。
   - 不得用 raw close 做 NAV。

2. **執行**
   - 預設 signal 在收盤後可知，隔日開盤或更保守價格成交。
   - 若使用收盤成交，必須明確標為 diagnostic，不可列為正式結果。

3. **成本**
   - 買賣手續費、賣出證交稅、滑價假設都要列入。
   - 滑價可依成交金額、ADV 或 fixed bps 建模，不可默認 0。

4. **持股與現金**
   - 持股數要每日檢查。
   - 未使用資金預設為現金，不得自動補成 0050，除非策略明確定義現金替代資產。

5. **Exit layer**
   - 停利、停損、追蹤停利、breakeven stop、time stop 都必須是策略邏輯的一部分，不得只在 broker 端臨時掛條件單。
   - Exit rule 必須放在 realistic execution simulator 內驗證，與一般換倉使用相同的手續費、交易稅、滑價、成交量限制、漲跌停阻擋與部分成交規則。
   - 若同一日 OHLC 同時碰到停利與停損，日線資料無法知道真實先後順序時，預設採保守的 stop-first 假設。
   - 不得因為加入 exit rule 提高紙面 CAGR 就升級；必須同步檢查 OOS CAGR、最近一年 CAGR、MDD、Sortino、DSR、PBO、Profit Factor、SQN 與交易數是否合理。

6. **程式效能**
   - 大型 panel 用 Polars / DuckDB / Arrow / Parquet。
   - 只允許小型狀態機用 Python loop，例如每日持倉 bookkeeping。
   - Expensive deterministic features 要有 cache key，包含資料 cutoff、schema version、feature version、日期區間。

## 6. 驗證標準

每個 candidate 至少要過以下層級，才可以升級。

### 6.0 正式最佳化目標函數

策略最佳化不得使用單一 full-window CAGR、Total Return、Win Rate 或 Sharpe Ratio 當主目標。正式研究採用兩層式 objective：

1. **先通過硬性淘汰條件**
   - point-in-time 正確。
   - total-return adjusted OHLCV 正確。
   - next-open 或更保守成交假設。
   - 含手續費、證交稅、滑價。
   - 觀測最大持股、平均持股、成交率與流動性風險已列入報告；若使用者指定持股上限，才把上限列為硬性淘汰條件。
   - OOS CAGR 為正，且相對 0050 / 可投資 benchmark 有意義。
   - MDD / CDaR 未超過策略風險容忍上限。
   - DSR / PBO 未顯示高度 data mining。
   - 去掉最大貢獻個股或最大產業後，策略不能完全失效。

2. **再最大化 robust growth**

正式主目標是：

```text
median / pooled walk-forward OOS log CAGR
```

也就是最大化樣本外幾何成長率，而不是最大化全樣本報酬。排序時用以下指標做扣分或輔助排序：

| 指標 | 用途 | 用法 |
|---|---|---|
| `OOS log CAGR` | 主增長目標 | 主要排序方向，越高越好 |
| `Calmar` | 報酬 / 最大回撤 | 硬門檻 + 輔助排序 |
| `Sortino` | 報酬 / 下行波動 | 輔助排序，不單獨作為主目標 |
| `Ulcer Index` | 回撤深度與持續時間 | 越低越好 |
| `Ulcer Performance Index` | 報酬 / Ulcer Index | 越高越好 |
| `CDaR` | 最糟一批 drawdown 的平均痛苦 | 硬門檻 + 扣分 |
| `Tail Ratio` | 上尾 / 下尾 | 檢查左尾是否過重 |
| `K-Ratio` | log NAV 斜率穩定度 | 取代單純 equity curve R² |
| `Profit Factor` | 交易層盈虧品質 | 交易診斷，不作為 NAV 主目標 |
| `SQN` | 單筆交易分布穩定度 | 交易診斷，防止靠少數暴賺 |
| `DSR` | 多次嘗試後 Sharpe 顯著性 | overfit gate |
| `PBO` | 樣本內最佳在樣本外失效機率 | overfit gate |

R² 可以作為視覺診斷，但不得直接用 `Total Return * R²` 當正式 objective，因為低報酬慢速爬升策略也可能有很高 R²。正式框架以 `K-Ratio` 或 log NAV regression t-stat 衡量資金曲線斜率穩定度。

### 6.1 基本 KPI

每次報告都要列：

- Full-window CAGR。
- OOS CAGR。
- 最近一年 CAGR，並寫明窗口。
- OOS log CAGR。
- Sortino。
- Sharpe。
- MDD。
- Calmar。
- Ulcer Index / Ulcer Performance Index。
- CDaR。
- Tail Ratio。
- K-Ratio。
- turnover。
- trade count。
- Profit Factor。
- SQN。
- win rate / payoff ratio。
- average holding days。
- max positions。
- max single-name weight。
- max industry weight。

### 6.2 Walk-forward

- 不可用全樣本選權重後宣稱 OOS。
- 參數、權重、候選策略切換規則都要在每個 train window 內決定，再套到下一段 test window。
- 每段 OOS 都要保存 daily NAV、trades、holdings、selected params。

### 6.3 Overfit 檢查

至少包含：

- bootstrap confidence interval。
- Deflated Sharpe Ratio。
- CSCV / PBO。
- permutation 或 label shuffle sanity check。
- parameter neighborhood stability。
- leave-one-period-out。
- leave-one-industry-out。
- leave-one-top-holding-out。

所有策略必須逐步收斂到共用 validator harness，而不是每個 `iter_NN` 自己實作一套 validation。正式入口為：

```python
from validator import validate_daily_nav
```

共用 validator 必須輸出：

- full-window metrics。
- OOS metrics。
- 最近一年 CAGR。
- path-quality metrics。
- Lo Sharpe test。
- bootstrap CI。
- DSR。
- PBO。
- robust growth score。

若某策略因資料格式無法接入共用 validator，該策略只能停留在 `research_candidate`，不能升級。

### 6.4 Robustness 檢查

必跑切片：

- 2008 金融海嘯。
- 2020 疫情。
- 2022 升息 / 科技股修正。
- 2024-2026 AI / 半導體強勢期。
- 最近一年。
- 最近三年。
- 無半導體。
- 無電子。
- TWSE-only。
- TPEx-only。
- 高成本 / 高滑價。

若策略只有在單一切片極強，其他切片崩潰，不能升級。

## 6.5 多策略 PM allocator 原則

策略研發方向不應只追求「單一最強策略」。正式框架允許多策略 PM allocator，但 allocator 本身也必須被視為一個策略，接受同等驗證。

多策略架構目標：

1. 每個 sleeve 都有不同 alpha 來源。
2. sleeve 之間低相關，或在不同 regime 表現互補。
3. allocator 只能使用事前可觀測訊號。
4. 最終 portfolio 必須回報觀測最大持股、平均持股、成交率與流動性；只有使用者指定持股上限時，才把檔數作為升級門檻。
5. allocator 的參數與權重也必須 walk-forward 選擇，不可全樣本最佳化。

PM allocator 可使用的 regime 訊號：

- 0050 / 大盤趨勢。
- 市場廣度。
- realized volatility。
- drawdown state。
- 產業 leadership。
- 流動性與成交金額。
- sleeve 近期 OOS-like momentum / degradation。

研究入口：

```python
from pm_allocator import run_momentum_allocator
```

注意：NAV-level allocator 只能做研究候選。要升級到 `execution_ready`，必須做 target-book reconciliation，確認所有 sleeve 合併後的目標持倉、股數四捨五入、交易成本、成交量限制、漲跌停阻擋與券商下單約束都可重現。若使用者指定持股上限，reconciliation 也必須驗證檔數上限。

## 7. Candidate 升級門檻

### 7.1 `research_candidate`

可接受條件：

- 有清楚 alpha 假設。
- 無明顯 look-ahead。
- 初步含成本回測為正。

### 7.2 `backtest_validated`

最低條件：

- OOS CAGR 為正且顯著高於 0050。
- 最近一年 CAGR 必須列出，但不能單獨當升級理由。
- OOS MDD 在可接受範圍。
- DSR / PBO 沒有顯示高度 overfit。
- 對主要 robustness 切片不崩潰。

### 7.3 `execution_ready`

最低條件：

- target-position / order-level backtest 與 validated NAV 對齊。
- 每日 target book 可重現。
- 持股數、權重、交易金額、現金、成本、四捨五入股數都能 reconciliation。
- 券商 API dry-run 可產生正確 order plan。
- 不需要人工主觀判斷才能執行。

### 7.4 `live_pilot`

最低條件：

- 使用小資金。
- 有每日自動產生計畫、人工確認或明確授權流程。
- 有 fail-closed 行為：資料缺漏、券商異常、策略 registry 無 execution-ready 策略時不下單。

### 7.5 `production_scaled`

最低條件：

- live pilot 期間行為與回測假設一致。
- 交易成本、滑價、延遲、部分成交可被觀測與控制。
- 有風控限額、停機條件、日誌與復原程序。

## 8. 文件與結果保存

每次正式研究結束要保存：

- strategy code。
- config / params。
- data cutoff。
- feature version。
- daily NAV。
- trades。
- holdings。
- KPI summary。
- validation report。
- rejection reason 或 promotion reason。

文件位置：

- 策略研發 SOP：`docs/strategy_research/research_sop.md`。
- 策略 production 狀態與交易規則：`docs/strategy_ranking.md`。
- 產業 taxonomy 說明：`docs/data/industry_taxonomy.md`。
- 主動式 ETF 分析：`docs/active_etf_analysis.md`。
- 給一般投資人的主動式 ETF 報告：`docs/active_etf_investor_recommendation.md`。
- 自動交易與 broker 操作：`research/trading/README.md`。

## 9. 禁止事項

- 不得把最新產業分類套回全歷史。
- 不得用 raw close 回測含股息策略。
- 不得用 signal-day close 當正式成交假設。
- 不得只用 full-window 最佳績效挑 champion。
- 不得把 diagnostic NAV 當 production target-book。
- 不得把不能重現下單的策略寫成 execution-ready。
- 不得為了追高 CAGR 犧牲 point-in-time、成本、流動性或使用者明確指定的持股限制。

## 10. 下一次策略研發的起點

下一輪研發應該從已完成的資料層開始：

1. 使用 `industry_taxonomy_pit` 重建所有產業相關 features。
2. 先做 taxonomy audit 與 signal attribution。
3. 再設計策略假設。
4. 最後才跑 backtest / sweep。

本文件完成後，不代表任何新產業輪動策略已經通過驗證；它只是定義下一輪研發必須遵守的流程。
