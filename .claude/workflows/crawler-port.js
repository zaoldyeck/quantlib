export const meta = {
  name: 'crawler-port',
  description: '把只有 Scala 的爬蟲逐源 Python 化:忠實移植 + 修稽核 bug + 原始檔封存 + parity 測試',
  whenToUse: '全 Python 化:port Scala reader → src/quantlib/crawl/sources/',
  phases: [{ title: 'Port', detail: '逐源移植,parity 對現存 PG 逐位驗證' }],
}

const A = typeof args === 'string' ? JSON.parse(args) : (args || {})
const UNITS = A.units || []
const BATCH = A.batch || 3
const DONE = new Set(A.done || [])
if (!UNITS.length) throw new Error('args.units 為空')

const RESULT = {
  type: 'object',
  required: ['source', 'status', 'summary', 'parity'],
  properties: {
    source: { type: 'string' },
    status: { type: 'string', enum: ['DONE', 'BLOCKED'] },
    summary: { type: 'string', description: '一句話白話:port 完成度 + parity 結果' },
    files: { type: 'array', items: { type: 'string' }, description: '產出的檔案路徑' },
    parity: { type: 'string', description: 'parity 測試結果:比對筆數、逐位是否一致、已知壞日期排除說明' },
    audit_bugs_fixed: { type: 'array', items: { type: 'string' }, description: '這個 port 修掉的稽核 bug' },
    blocked_reason: { type: 'string' },
  },
}

const PROMPT = (u) => `你是資料工程師,把台股量化系統的一個爬蟲從 Scala **Python 化**。
使用者鐵律:全 Python、零 Scala 依賴;而且**不延後任何 TODO,不略過**。

# port 目標:${u.source}(Scala reader: ${u.reader})

## 背景
專案正全面 Python 化,Scala 爬蟲+reader 要退役。你負責把「${u.source}」這一源
重寫成 Python,放進現有框架 src/quantlib/crawl/sources/。

## 必讀(先讀再動手,走 codebase-memory-mcp 專案 Users-zaoldyeck-Documents-scala-quantlib)
1. **Scala reader**:src/main/scala/reader/${u.reader}(解析邏輯——你要忠實移植它
   的欄位對位、值轉換、市場分流、版型分支)。
2. **Setting**:src/main/scala/setting/(URL/formData/檔名/驗證配方)。
3. **application.conf**:該源的 endpoint。
4. **稽核結論**:docs/data_audit/_done/A-${u.source}.json 與
   docs/data_audit/_done/C-${u.table || u.source}.json(**這一源已知的所有 parser bug
   + 汙染/缺漏日期**——你的 Python 版必須把這些 bug 一次寫對,不可複製)。
5. **既有 port 樣板**:src/quantlib/crawl/sources/daily_quote.py(fetch → archive → parse
   → Sink 的標準形態)、src/quantlib/crawl/sink.py、src/quantlib/crawl/http.py、
   src/quantlib/crawl/parse.py、**src/quantlib/crawl/archive.py**(原始檔封存鐵律)。

## 產出(寫進 repo,不要只描述)
1. **src/quantlib/crawl/sources/${u.source}.py**:
   - TABLE / KEY_COLS / MARKETS 常數。
   - fetch 函式:抓取 → **archive.save_raw 先原樣落地 data/** → parse → 回 polars DF
     (欄位/型別與 cache 表同構)。**原始檔封存鐵律:順序不可顛倒。**
   - parse:忠實移植 Scala 的欄位對位與值轉換,但**把稽核發現的 bug 修對**
     (如 dtd 自營商欄位錯位、int32 溢位改 Int64、name-strip 只清數值欄、
     ex_right_dividend 配股數值要算出還原因子而非只當布林、tpex 版型用明確 case
     不用 fallthrough、日期用內容標題不只檔名)。
   - header 位置守衛(SchemaDrift fail-loud,比照 daily_quote._guard)。
2. **src/quantlib/crawl/tests/test_${u.source}_parity.py**:
   - **對現存 PG 資料逐位 parity**:獨立解析原始檔(data/${u.source}/…)→ 與 PG 該表
     逐欄比對。**PG 已知壞的日期(稽核列出的汙染/缺漏)明確排除並註明**,其餘必須
     逐位一致。這是 port 忠實度的證明(先紅後綠)。
   - 若某欄是 port 修好而 PG 是錯的(如 int 溢位、配股 0),測試要斷言 **Python 對、
     PG 錯**,並說明。

## 鐵律
- **只認可重現證據**:parity 用實際比對數字,不空口說「移植完成」。
- **不碰 PG/cache 寫入**(唯讀對照即可);不改 update.py(主流程整合時我來接線)。
- **不 port 就別假裝**:endpoint 打不通或版型太複雜當下無法忠實移植 → status=BLOCKED
  並說清楚卡在哪,不要交半成品。

單位 JSON:${JSON.stringify(u)}`

const todo = UNITS.filter(u => !DONE.has(u.source))
log(`爬蟲 Python 化:${todo.length} 源,每批 ${BATCH}`)
phase('Port')
const results = []
for (let i = 0; i < todo.length; i += BATCH) {
  const got = await parallel(todo.slice(i, i + BATCH).map(u => () =>
    agent(PROMPT(u), { label: `port:${u.source}`, phase: 'Port', schema: RESULT, effort: 'max' })))
  results.push(...got.filter(Boolean))
  log(`進度 ${Math.min(i + BATCH, todo.length)}/${todo.length};DONE ${results.filter(r => r.status === 'DONE').length}`)
}
return {
  ported: results.filter(r => r.status === 'DONE'),
  blocked: results.filter(r => r.status === 'BLOCKED'),
}
