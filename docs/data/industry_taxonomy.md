# 產業分類資料層

最後更新：2026-05-17

## 結論

台股策略研究的正式產業分類來源是 `research/cache.duckdb` 內的 `industry_taxonomy_pit`，建置程式為 `research/industry_taxonomy.py`。

策略不得直接使用 `operating_revenue.industry` 做產業輪動或同業分組，因為原始欄位包含歷史舊分類、名稱變更、合併分類與長描述文字；若直接取每家公司最新一筆 industry，會把現在分類套回歷史。

## 資料來源

原始來源是 MOPS 月營收資料中的官方產業別欄位。Scala parser 讀取月營收檔中的 `產業別：...`，寫入 PostgreSQL `operating_revenue.industry`；DuckDB cache 再把這些 rows 複製到 `operating_revenue`。

官方分類依據是 TWSE / TPEx 產業類別劃分暨調整規則，核心是公司主要業務、營收占比與交易所核准分類。

## Cache 表

`industry_taxonomy_pit` 欄位：

| 欄位 | 說明 |
|---|---|
| `market` | `twse` 或 `tpex` |
| `company_code` | 股票代號 |
| `company_name` | 公司名稱 |
| `source_year` | 月營收資料年份 |
| `source_month` | 月營收資料月份 |
| `source_ym` | `YYYYMM` |
| `effective_date` | 這筆分類在研究中可被使用的日期 |
| `raw_industry` | MOPS 原始產業文字 |
| `industry` | normalized canonical industry |
| `broad_sector` | 粗分類風險桶 |
| `is_financial` | 是否金融類 |
| `is_special_category` | 是否特殊分類，例如管理股票、存託憑證 |
| `industry_source` | 固定為 `mops_operating_revenue` |
| `taxonomy_version` | taxonomy schema / mapping 版本 |

目前 cache audit：

| 指標 | 值 |
|---|---:|
| rows | 448,922 |
| unique company codes | 2,486 |
| min effective date | 2001-07-13 |
| max effective date | 2026-05-13 |
| normalized industries | 44 |

## Point-in-time 規則

`effective_date` 採保守規則：

```text
month_start + 1 month + 12 days
```

這和月營收 feature 的發布日代理邏輯一致，目的不是精準模擬交易所何時首次知道產業分類，而是避免把未觀測到的來源檔提前套入回測。

策略 feature panel 應使用：

```python
from industry_taxonomy import attach_industry_asof

panel = attach_industry_asof(panel, con)
```

join key：

```text
market, company_code, date <= effective_date backward as-of
```

## Normalization 原則

Normalization 只做名稱歸一與官方分類演進對齊，不做主觀產業判斷。

例子：

| raw | canonical |
|---|---|
| `建材營建` | `建材營造` |
| `觀光事業` | `觀光餐旅` |
| `通訊網路` | `通信網路業` |
| `生物科技` | `生技醫療業` |
| `電子商務` | `數位雲端` |
| `金融保險業（其中金控公司...）` | `金融保險` |

早期無法可靠拆分的合併分類會保留為合併桶，例如：

- `電子工業`
- `化學生技醫療`
- `塑化紡織`
- `電機電纜`
- `水泥窯製營造`

這些合併桶是歷史來源限制，不應被硬拆成更細子產業，除非未來導入更可靠的 point-in-time 官方公司基本資料或交易所分類異動公告。

## 建置命令

```bash
uv run --project research python research/cache_tables.py
```

建置會在複製 `operating_revenue` 後產生 `industry_taxonomy_pit`，並建立：

```sql
CREATE INDEX idx_itp_code_date ON industry_taxonomy_pit(company_code, effective_date);
CREATE INDEX idx_itp_market_code_date ON industry_taxonomy_pit(market, company_code, effective_date);
```

## 驗證命令

```bash
uv run --project research pytest research/tests/test_industry_taxonomy.py -q
```

測試涵蓋：

- legacy industry label normalization。
- consolidated monthly revenue rows 優先。
- `effective_date` 正確性。
- point-in-time as-of join 不會提前套用未公布分類。

## 後續改進

更高階的 taxonomy 可以在此基礎上增加兩層，但不能取代官方 PIT 分類：

1. **官方分類異動公告表**
   - 解析 TWSE / TPEx 產業類別調整公告。
   - 補出更精確的 `effective_from` / `effective_to`。

2. **資料驅動同群 cluster**
   - 用營收、價格、法人流、供應鏈暴露建立 statistical peer group。
   - 只能作為 alpha feature 或 robustness stress，不可覆蓋官方分類。

正式產業輪動策略必須同時檢查官方分類與資料驅動分群，避免策略績效完全依賴單一 taxonomy 假設。
