export const meta = {
  name: 'algo-audit',
  description: '全專案算法學理稽核:每個計算式對照教科書/論文定義,找出偏差 + 驗證修法',
  whenToUse: '使用者要求所有算法 100% 符合學理定義',
  phases: [{ title: 'Audit', detail: '逐算法面對照學理,結論即時落盤' }],
}

const A = typeof args === 'string' ? JSON.parse(args) : (args || {})
const UNITS = A.units || []
const BATCH = A.batch || 3
const DONE = new Set(A.done || [])
if (!UNITS.length) throw new Error('args.units 為空;算法稽核不能空跑')

const FINDING = {
  type: 'object',
  required: ['unit', 'verdict', 'summary', 'findings'],
  properties: {
    unit: { type: 'string' },
    verdict: { type: 'string', enum: ['BUG', 'SUSPECT', 'OK'] },
    summary: { type: 'string', description: '一句話白話:這些算法能不能信' },
    findings: {
      type: 'array',
      items: {
        type: 'object',
        required: ['severity', 'metric', 'textbook_def', 'implementation', 'evidence'],
        properties: {
          severity: { type: 'string', enum: ['BUG', 'SUSPECT', 'OK'] },
          metric: { type: 'string', description: '指標/因子名 + 檔案:行號' },
          textbook_def: { type: 'string', description: '學理正確定義 + 出處(論文/教科書/公式)' },
          implementation: { type: 'string', description: '程式實際算法' },
          evidence: { type: 'string', description: '偏差的可重現證據:實際值 vs 學理值、具體樣本' },
          fix: { type: 'string', description: '學理正確的修法;無偏差留空' },
        },
      },
    },
  },
}

const COMMON = `
你在對台股量化系統做**算法學理正確性稽核**。使用者要求「專案中所有算法 100% 符合
學理上的定義」。你負責的檔案裡,**每一個計算式**(指標、因子、比率、統計量、還原、
成本會計)都要對照其**學理標準定義**逐一檢查。

## 鐵律
1. **先確立學理定義再看程式**。每個指標寫下它在教科書/原始論文的**精確定義**(附
   出處:公式、論文、標準)。例:Sharpe = (E[Rp]−Rf)/σp 且用超額報酬;Sortino 用
   下行標準差;IC = 橫截面 Spearman(因子, 前瞻報酬);ROIC = NOPAT/投入資本;
   MDD = min(NAV/cummax−1);CAGR = (end/begin)^(1/years)−1。
2. **只認可重現證據**。判定偏差要能舉出「實際算出的值 vs 學理值」或具體錯誤樣本
   (檔案:行號 + 一個能重現的數字)。說不出證據的疑慮寫 SUSPECT。
3. **區分「學理偏差」與「刻意的合理近似」**。有些簡化是刻意且有註解說明的(如
   年化用 252 交易日、摩擦成本假設)——那不是 BUG,標 OK 並註明。真正的 BUG 是
   **與學理定義不符且會讓結論失真**(用錯分母、錯誤年化、樣本內外洩漏、前視、
   符號錯、母體 vs 樣本標準差混用)。
4. **台股語境要納入**:年化 252 日、無風險利率可近似 0、除權息還原、漲跌幅限制。
5. **讀碼先走 codebase-memory-mcp**(專案 Users-zaoldyeck-Documents-scala-quantlib)。
6. **禁止改任何程式**;你只產結論,修法寫在 fix 由主流程驗證後施作。

## 環境
- repo /Users/zaoldyeck/Documents/scala/quantlib(指令從根跑)
- cache: paths.CACHE_DB;PG: psql -h localhost -p 5432 -d quantlib
- 可跑 Python 驗證:uv run --project . python -c "..."

## 產出(不可省)
docs/data_audit/_done/<unit_id>.json + docs/data_audit/findings/<unit_id>.md,再回傳同一份 JSON。
`

const PROMPT = (u) => `${COMMON}
# 算法稽核單位 ${u.id}:${u.title}

負責檔案/範圍:${u.scope}

要對照學理檢查的重點:${u.focus}

把該範圍內**每一個**計算式都列出來:學理定義(附出處)→ 程式實作 → 是否相符 →
偏差證據 → 修法。逐一,不遺漏。單位 JSON:${JSON.stringify(u)}`

const todo = UNITS.filter(u => !DONE.has(u.id))
log(`算法稽核:${todo.length} 個單位,每批 ${BATCH}`)
phase('Audit')
const results = []
for (let i = 0; i < todo.length; i += BATCH) {
  const got = await parallel(todo.slice(i, i + BATCH).map(u => () =>
    agent(PROMPT(u), { label: u.id, phase: 'Audit', schema: FINDING, effort: 'max' })))
  results.push(...got.filter(Boolean))
  log(`進度 ${Math.min(i + BATCH, todo.length)}/${todo.length};BUG ${results.filter(r => r.verdict === 'BUG').length}`)
}
return {
  audited: results.length,
  bugs: results.filter(r => r.verdict === 'BUG'),
  suspects: results.filter(r => r.verdict === 'SUSPECT'),
}
