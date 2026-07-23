export const meta = {
  name: 'data-audit',
  description: '全資料鏈稽核:原始檔→DB 解析正確性 / 財務定義與算式 / cache 一致性與缺漏',
  whenToUse: '使用者要求複核 raw data 到結構化資料的整條鏈路;以 args.dim 指定 A|B|C',
  phases: [
    { title: 'Audit', detail: '逐單位查證,結論即時落盤' },
  ],
}

// ─── 續跑:已落盤的單位直接跳過 ─────────────────────────────────────────
// 每個單位寫一個獨立檔(docs/data_audit/_done/<id>.json),沒有共享檔案競爭。
// 中斷後重跑本 workflow:已完成的單位不會再派 agent(省 token),
// workflow 自身的 journal 再提供第二層快取。
// args 有時會以 JSON 字串抵達(視呼叫端而定)→ 兩種形態都吃,否則會靜默跑 0 個單位
const A = typeof args === 'string' ? JSON.parse(args) : (args || {})
const DIM = A.dim || 'A'
const UNITS = A.units || []
const BATCH = A.batch || 2
// 已完成的單位由呼叫端在 args.done 傳入(workflow 沙箱讀不到磁碟);據此跳過,
// 避免重跑燒 token —— 第一層續跑防護。workflow 自身的 journal 是第二層。
const DONE = new Set(A.done || [])
if (!UNITS.length) throw new Error(`args.units 為空(收到 ${typeof args});稽核不能空跑`)

const FINDING = {
  type: 'object',
  required: ['unit', 'verdict', 'summary', 'findings'],
  properties: {
    unit: { type: 'string' },
    verdict: { type: 'string', enum: ['BUG', 'SUSPECT', 'OK', 'REAL'] },
    summary: { type: 'string', description: '一句話結論(白話,講「資料能不能信」)' },
    findings: {
      type: 'array',
      items: {
        type: 'object',
        required: ['severity', 'what', 'evidence'],
        properties: {
          severity: { type: 'string', enum: ['BUG', 'SUSPECT', 'OK', 'REAL'] },
          what: { type: 'string', description: '具體問題(欄位/算式/筆數)' },
          evidence: { type: 'string', description: '可重現的證據:檔案+行號、SQL、實際值 vs 應有值' },
          fix: { type: 'string', description: '建議修法;無則留空' },
        },
      },
    },
    checked: { type: 'array', items: { type: 'string' }, description: '實際查了哪些東西(供覆核涵蓋度)' },
  },
}

const COMMON = `
你在對台股量化系統做**資料正確性稽核**。這套系統的所有回測與實盤決策都建立在
這些資料上——**資料錯了,上面每一個結論都是假的**。

## 鐵律
1. **只認證據,不認推測。**每一個結論都要附「可重現的證據」:檔案路徑+行號、
   可重跑的 SQL/Python、實際值 vs 應有值。說不出證據的疑慮寫成 SUSPECT,不寫 BUG。
2. **負結果一樣要落盤。**查過沒問題就給 OK 並列出「查了什麼」——否則下一個人
   會把同一件事再查一遍。
3. **先查文件再判錯。**台股有大量「看起來像錯、其實是真的」的現象,例如:
   金融業月營收為負(匯兌/評價損失,備註欄有說明)、營建業月營收為 0(完工比例法)、
   週六補行交易日、concise_* 表沒有 market 欄。判 BUG 前先排除這些。
   已知的真實邊界記在 docs/data/data_quality_incidents.md 與 CLAUDE.md。
4. **讀碼一律先走 codebase-memory-mcp**(專案名 Users-zaoldyeck-Documents-scala-quantlib):
   search_graph 找定義 → get_code_snippet 精讀。別直接 grep 全庫。
5. **禁止修改任何程式碼或資料。**你只產出結論;修法寫在 fix 欄位由主流程決定。

## 環境
- repo: /Users/zaoldyeck/Documents/scala/quantlib(所有指令從 repo 根執行)
- 路徑唯一真源:\`src/quantlib/paths.py\`(cache 在 paths.CACHE_DB = var/cache/cache.duckdb)
- DuckDB cache 可讀:\`uv run --project . python -c "..."\`
- PostgreSQL 可用:\`psql -h localhost -p 5432 -d quantlib -c "..."\`(32 張表)
- 原始檔在 \`data/<source>/<market>/<year>/\`

## 產出(**這一步不可省,否則中斷就前功盡棄**)
查證完成後,把結構化結論寫入
  docs/data_audit/_done/<unit_id>.json     ← 完成標記 + 機器可讀結論
  docs/data_audit/findings/<unit_id>.md    ← 人可讀報告(白話講「這份資料能不能信」)
再回傳同一份 JSON 作為你的最終輸出。
`

const PROMPT = {
  A: (u) => `${COMMON}
# 稽核單位 ${u.id}:資料源「${u.source}」的**解析正確性**

原始檔 ${u.raw_files} 個,解析程式 \`${u.reader}\`,落地表 \`${u.table || '(無對應 cache 表)'}\`。

## 你要回答的問題
**原始檔裡的每一欄,有沒有正確、完整、單位無誤地落到資料庫?**

## 做法(必須實際執行,不可只讀碼推論)
1. 讀 \`src/main/scala/setting/\` 下對應的 Setting 與 \`${u.reader}\`,寫下它宣告的
   欄位順序、型別、單位、以及它怎麼處理欄位數不同的情形。
2. **抽樣要跨時代**:從 \`data/${u.source}/\` 的**最早年份、中間年份、最近年份**
   各挑至少 2 個檔(不同 market 也要涵蓋)。台股原始檔的欄位會隨年份無聲增減
   ——只抽近期等於沒抽。
3. 用 Python 獨立解析那些原始檔(自己讀 CSV,**不要呼叫既有解析程式**——那正是
   受測對象),再與 cache/PG 中**同一天同一檔股票**的值逐欄比對。
4. 特別盯這幾類無聲錯誤:
   - **欄位錯位**(新增欄位後 fall-through 把值放錯欄)
   - **單位**(元 vs 千元 vs 張 vs 股;百分比是 0.05 還是 5)
   - **正負號**(賣超/買超、減資、負營收)
   - **編碼**(Big5/UTF-8 造成的中文欄名比對失敗)
   - **日期**(民國 vs 西元;檔名日期 vs 內容日期不符)
   - **漏欄**(原始檔有但 schema 沒接的欄位——那是白白丟掉的資訊)
5. 若發現不一致,**先確認不是上面「真實現象」清單裡的項目**再判 BUG。

單位 JSON:${JSON.stringify(u)}`,

  B: (u) => `${COMMON}
# 稽核單位 ${u.id}:財務定義與算式審查

審查對象:\`${u.artifact}\`

## 你要回答的問題
**這個算式/schema 在會計與財務學理上正確嗎?它算出來的數字能拿來選股嗎?**

## 做法
1. 精讀該檔全文,把每一個計算欄位的公式寫下來(分子、分母、期間、幣別、單位)。
2. 逐條對照學理標準檢查:
   - **TTM / 累計制**:台股損益表是**累計數**(Q3 = 前三季合計),不是單季。
     算 TTM 時有沒有正確做差分?跨年的第一季有沒有處理?
   - **時點對齊(PIT)**:用到的財報是「當時已公告」的嗎?有沒有前視偏誤
     (用了公告日之後才知道的數字)?
   - **分母保護**:負權益、零營收、極小分母有沒有處理?會不會產生 ±inf?
   - **比率定義**:ROE 用期末還是平均權益?ROIC 的投入資本含不含現金?
     毛利率的分母是營收還是淨營收?
   - **合併 vs 個體**:同一個指標有沒有混用兩種報表?
3. ${u.id === 'B-fscore-academic' ? `
   **這一項是重點**:F-Score 必須逐項對照 Piotroski (2000)
   "Value Investing: The Use of Historical Financial Statement Information to
   Separate Winners from Losers" 的**原始九項定義**:
     獲利性 4 項:ROA>0、CFO>0、ΔROA>0、CFO>ROA(應計項目品質)
     槓桿/流動性 3 項:Δ長期負債比↓、Δ流動比↑、當年未增發股本
     營運效率 2 項:Δ毛利率↑、Δ資產周轉率↑
   逐項核對:(a) 每一項的定義是否忠實 (b) 分母與期間是否一致
   (c) **多處實作是否互相一致**(先用 codebase-memory-mcp 找出所有實作位置,
   至少有 12 個檔提到 f_score/piotroski)(d) 台股語境下是否需要調整(如金融業)。
   任何偏離原始定義的地方,都要說清楚「偏離了什麼、影響是什麼、是否刻意」。` : `
   若此檔被其他程式引用,用 codebase-memory-mcp 的 trace_path 找出消費者,
   確認它們對這些欄位的假設與定義一致。`}
4. 可行時**實際跑一次查詢驗證**:抽幾檔股票、幾個季度,手算一遍與 view 的輸出比對。

單位 JSON:${JSON.stringify(u)}`,

  C: (u) => `${COMMON}
# 稽核單位 ${u.id}:cache 表「${u.table}」的一致性與缺漏

## 你要回答的問題
**DuckDB cache 與 PostgreSQL 是否一致?時間序列有沒有洞?**

## 做法
1. 比對 **schema**:cache 的欄位集合 vs PG 對應表(欄名、型別、順序)。
   \`research/cache_tables.py\` 是同步程式——確認它有沒有漏欄、漏表、型別降級。
2. 比對 **筆數**:整表 count 與逐年 count。差異要能解釋(例如 cache 只同步某些
   market 或某個日期之後)。
3. **抽樣逐欄比對**:隨機挑 3 個日期 × 5 檔股票,兩邊的每一欄值必須相同。
4. **覆蓋缺口**:列出日期序列中的缺口。判斷缺口是「休市」還是「真的漏抓」——
   休市日曆看 \`data/daily_quote/twse/<year>/\` 下的 0-byte sentinel 檔,
   或用 \`src/quantlib/data_calendar.py::is_trading_day\`。**颱風假不能從星期幾推得**。
5. **異常值掃描**:該表的關鍵數值欄有沒有不可能的值(負價格、零成交量卻有成交值、
   本益比極端值、日期在未來)。列出筆數與代表樣本。
6. 若發現真的漏抓,寫明「缺哪些日期、可從哪個 endpoint 補」到 fix 欄位
   (**不要自己去下載**,補抓由主流程統一安排)。

單位 JSON:${JSON.stringify(u)}`,
}

// ─── 執行:小批次併發,避免一次燒太兇 ───────────────────────────────
const todo = UNITS.filter(u => u.dim === DIM && !DONE.has(u.id))
log(`維度 ${DIM}:待稽核 ${todo.length} 個單位(已完成 ${DONE.size} 個略過),每批 ${BATCH} 個`)

phase('Audit')
const results = []
for (let i = 0; i < todo.length; i += BATCH) {
  const batch = todo.slice(i, i + BATCH)
  const got = await parallel(batch.map(u => () =>
    agent(PROMPT[DIM](u), {
      label: u.id,
      phase: 'Audit',
      schema: FINDING,
      effort: 'max',
    })))
  results.push(...got.filter(Boolean))
  const bugs = results.filter(r => r.verdict === 'BUG').length
  log(`進度 ${Math.min(i + BATCH, todo.length)}/${todo.length};累計 BUG ${bugs}`)
}

return {
  dim: DIM,
  audited: results.length,
  bugs: results.filter(r => r.verdict === 'BUG'),
  suspects: results.filter(r => r.verdict === 'SUSPECT'),
  ok: results.filter(r => r.verdict === 'OK').length,
  real: results.filter(r => r.verdict === 'REAL').length,
}
