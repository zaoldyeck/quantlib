export const meta = {
  name: 'parser-allgen',
  description: '把每個 Python 爬蟲 parser 擴充成能正確吃全部歷史格式世代 + 修掉所有稽核 parser bug(rebuild-from-raw 前提)',
  whenToUse: '所有歷史資料正確化:parser 只吃現行格式,舊世代(13欄/12欄…)會炸;擴充吃全世代',
  phases: [{ title: 'Extend', detail: '每源擴充 parser 吃全世代 + 對真實 raw 逐世代驗證' }],
}

const REPO = '/Users/zaoldyeck/Documents/scala/quantlib'
const UNITS = [
  { source: 'daily_trading_details', reader: 'TradingReader.readDailyTradingDetails' },
  { source: 'margin_transactions', reader: 'TradingReader.readMarginTransactions' },
  { source: 'daily_quote', reader: 'TradingReader.readDailyQuote' },
  { source: 'index', reader: 'TradingReader.readIndex(market_index)' },
  { source: 'foreign_holding_ratio', reader: 'TradingReader.readForeignHoldingRatio' },
  { source: 'ex_right_dividend', reader: 'TradingReader ex-right/dividend' },
  { source: 'stock_per_pbr', reader: 'TradingReader.readStockPerPbrDividendYield(raw 在 data/stock_per_pbr_dividend_yield/)' },
  { source: 'sbl_borrowing', reader: 'TradingReader.readSblBorrowing' },
  { source: 'capital_reduction', reader: 'TradingReader capital reduction' },
  { source: 'financial_analysis', reader: 'FinancialReader.readFinancialAnalysis' },
  { source: 'operating_revenue', reader: 'FinancialReader.readOperatingRevenue' },
  { source: 'insider_holding', reader: 'insider reader' },
  { source: 'treasury_stock_buyback', reader: 'treasury reader' },
]

const RESULT = {
  type: 'object',
  required: ['source', 'status', 'edits', 'verify'],
  properties: {
    source: { type: 'string' },
    status: { type: 'string', enum: ['DONE', 'PARTIAL', 'BLOCKED'] },
    summary: { type: 'string', description: '一句白話:擴充了哪些世代、修了哪些 bug、驗證結果' },
    generations: { type: 'string', description: '這個源有哪幾個歷史格式世代(欄數/日期範圍/差異),逐一列' },
    edits: {
      type: 'array',
      description: '對 HEAD 現況 research/crawl/sources/<source>.py 可乾淨套用的精確編輯',
      items: {
        type: 'object', required: ['file', 'old_string', 'new_string', 'why'],
        properties: {
          file: { type: 'string' }, old_string: { type: 'string' },
          new_string: { type: 'string' }, why: { type: 'string' },
        },
      },
    },
    verify: { type: 'string', description: '逐世代對真實 raw 的驗證:每個世代抽樣 parse 成功列數 + 對 audit 已知正確值的交叉核對(如 00403A Int 溢位還原、13欄自營商對位),附實際數字' },
    known_values_checked: { type: 'string', description: 'audit 列的已知正確值(具體代碼/日期/欄位),你的 parser 跑出來對不對' },
    blocked_reason: { type: 'string' },
  },
}

const PROMPT = (u) => `你是資深資料工程師。台股爬蟲的 Python parser **目前只吃現行格式**,歷史舊世代
(如 dtd 的 2012-2014 13 欄、2007-2014 tpex 12 欄)會拋 SchemaDrift 例外——這讓「從 raw 重建
cache」對歷史資料完全失效。你要把「${u.source}」的 parser **擴充成正確吃全部歷史格式世代**,
並修掉稽核記載的所有 parser bug,使 rebuild-from-raw 能產出**當下最正確的歷史資料**。

# 源:${u.source}(Scala reader 參考:${u.reader})

## 必讀(先讀再動手,走 codebase-memory-mcp)
1. **稽核結論(格式世代規格 + bug + 已知正確值)**:
   - ${REPO}/docs/data_audit/_done/A-${u.source}.json(解析層 bug + 世代)
   - ${REPO}/docs/data_audit/_done/C-*${u.source}* 或相關 C-*.json(一致性/汙染/錯日)
   - ${REPO}/docs/data_audit/findings/*${u.source}*.md
2. **現行 Python parser**:${REPO}/research/crawl/sources/${u.source}.py(只吃現行格式)。
3. **Scala reader(全世代參考)**:src/main/scala/reader/ 對應函式——它**原本就處理所有世代**
   (含 IFRS 前後、欄數變遷、市場分流),是你擴充的權威參考;但**它有稽核記載的 bug**
   (欄位錯位、Int32、name-strip 傷文字欄、日期只認檔名…),你要移植它的世代處理、**同時修掉
   那些 bug**(不可複製)。
4. **真實 raw(絕對路徑;worktree 的 data/ 是空的因 gitignore,一律用絕對路徑讀)**:
   ${REPO}/data/${u.source}/<market>/<year>/*.csv(或 audit 指出的舊路徑)。

## 你要做的
1. **列出這個源的所有歷史格式世代**(欄數、日期範圍、與現行的差異)——寫進 \`generations\`。
2. **擴充 parser 逐世代正確解析**:用「標頭內容/欄數」判世代(明確 case,不用 fallthrough),
   各世代用正確欄位對映(對照 Scala reader + audit)。**同時修掉所有 audit bug**:Int32→Int64、
   自營商三欄對位、name-strip 只清數值欄、日期用內容標題非只檔名、純配股非 0、報酬指數不亂改名…。
3. **嚴格驗證(先紅後綠,對真實 raw)**:
   - **每個世代**都抽樣至少 2 個真實 raw 檔,parse 成功且列數合理(非 0、非炸)。
   - **對 audit 列的已知正確值逐一交叉核對**(如 00403A 2026-05-12 dealers 溢位還原成正確大值、
     13 欄世代自營商淨額對到正確欄、IFRS 前 financial_analysis 欄位對正)——寫進 \`known_values_checked\`。
   - 用 Bash 實跑你的 parser(可在 worktree 寫暫時測試腳本,讀絕對路徑 raw)。
4. 回**精確 edits**(old_string 為 HEAD 現況 ${u.source}.py 逐字原文、檔內唯一;多 edit 依序套用)。

## 鐵律
- **只認可重現證據**:verify 要有實際 parse 列數 + 已知值核對數字,不空口「已支援全世代」。
- **絕不 drop/靜默跳過任何世代**:舊世代不是「跳過」,是「用對的欄位對映解析」。若某世代真的
  無法解析(格式資訊不足)→ status=PARTIAL 並列出是哪個世代、卡在哪,不假裝完成。
- **只改 research/crawl/sources/${u.source}.py**(不碰別源、不碰 Scala、不碰 cache)。
- **忠實 = 對 raw 內容**:不無中生有、不「修正」真實的來源錯(如 2832 EPS 源頭錯位要忠實搬),
  但要修**解析層**的 bug(欄位對位、型別、編碼)。

單位 JSON:${JSON.stringify(u)}`

log(`parser 全世代擴充:${UNITS.length} 源平行(每源對真實 raw 逐世代驗證)`)
phase('Extend')
const results = await parallel(UNITS.map(u => () =>
  agent(PROMPT(u), {
    label: `parser:${u.source}`, phase: 'Extend', schema: RESULT,
    isolation: 'worktree', effort: 'max', model: 'opus',
  })))
const done = results.filter(Boolean)
return {
  sources: done.length,
  by_status: {
    DONE: done.filter(r => r.status === 'DONE').map(r => r.source),
    PARTIAL: done.filter(r => r.status === 'PARTIAL').map(r => r.source),
    BLOCKED: done.filter(r => r.status === 'BLOCKED').map(r => r.source),
  },
  results: done,
}
