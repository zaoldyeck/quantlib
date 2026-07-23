export const meta = {
  name: 'audit-closure',
  description: '算法稽核剩餘 BUG 逐單位修到好結案:Scala 凍結碼 + SQL view + q01,平行設計精確修法',
  whenToUse: '把 D-* 算法稽核的剩餘 BUG 全部清償(Scala/SQL/研究),ROI 序、零缺漏',
  phases: [{ title: 'Fix', detail: '每單位一 agent:讀稽核+目標檔→套修法→自驗→回精確 edits' }],
}

// 依 ROI 排序(高效益先):資料正確性 schema > 篩選閘 > 因子/指標 > 回測 > 估值 view > 研究
const UNITS = [
  { key: 'slick-schema', audit: 'D-slick-schema-full',
    files: ['src/main/scala/db/table/DailyTradingDetails.scala', 'src/main/scala/db/table/NetChangeOfPrice.scala', 'src/main/scala/db/table/ExRightDividend.scala', 'src/main/scala/reader/TradingReader.scala'],
    focus: '法人買賣超股數欄 Int→Long(溢位靜默變 0);NetChangeOfPrice.* 投影同欄寫兩次+漏漲停家數;ex_right_dividend.cash_dividend 欄名語意註解(存的是權值+息值總調整額,非純現金)。改 Slick Table 型別後 parser 對應改 toLong。' },
  { key: 'quality-screening', audit: 'D-screening-timing-scala',
    files: ['src/main/scala/strategy/QualityFilter.scala'],
    focus: 'QualityFilter 缺 DISTINCT ON,把「本季體質達標才放行」實作成「歷史任一季曾達標就永久放行」;加 DISTINCT ON 收斂到 PIT 日最新一季。' },
  { key: 'rankmetrics-scala', audit: 'D-rankmetrics-scala',
    files: ['src/main/scala/strategy/RankMetrics.scala', 'src/main/scala/strategy/FactorResearch.scala'],
    focus: 'forwardReturns 未來報酬用未還原原始收盤 → 除權息旺季系統性低估、壓低殖利率/價值因子 IC;改用還原價(與 Python apex 一致,T+1)。' },
  { key: 'metrics-scala', audit: 'D-metrics-scala',
    files: ['src/main/scala/strategy/Metrics.scala'],
    focus: 'Sortino 下行差用「下跌天數當分母 + 非 MAR 錨」→ 改 sqrt(sum(min(r-MAR,0)²)/N)×√TDPY(全期 N、MAR=0);Sharpe 分子用幾何 CAGR → 改算術年化超額(mean×TDPY 或 mean/std×√TDPY);turnover 年化分母用期初資本 → 改逐期 NAV 均值;TDPY 242 註明出處(cache 實測 ~243.6)。' },
  { key: 'signals-scala', audit: 'D-signals-scala',
    files: ['src/main/scala/strategy/Signals.scala'],
    focus: '(含 D-strategy-variants 的 Greenblatt 共病)distFrom52wHigh 的 INTERVAL 252 days 把交易日當日曆天→只看約 8 個月,改為 52 週日曆窗(~365 天)或交易日 rn 上限;greenblattROIC/earningsYield 的 EBIT 用稅前淨利當 EBIT(應為營業利益或稅前+利息);EV 的 Total Debt 用負債總計(應為有息負債);投入資本口徑(ROCE vs ROIC)。逐項對照 docstring 宣稱定義修正。' },
  { key: 'backtester-scala', audit: 'D-backtester-scala',
    files: ['src/main/scala/strategy/Backtester.scala'],
    focus: '減資(capital reduction)參考價重設完全沒處理:引擎只讀 ex_right_dividend + 2.5x 分割啟發式,386 筆減資 91% 漏接,持有到減資的股票把「股數變少股價跳高」的會計重設當真漲幅記進 NAV(實測 8103 憑空 +11.6%)。載入 capital_reduction 表(closing_price_on_the_last_trading_date + post_reduction_reference_price),減資日等比調整股數/還原,與 DRIP 同一路徑中性化。' },
  { key: 'crawler-scala', audit: 'D-crawler-scala',
    files: ['src/main/scala/Crawler.scala'],
    focus: '「休市判定」凡回應 < 50 bytes 就無條件當休市寫 0-byte sentinel,無正向開盤查核 → 27 個真交易日的 twse 融資融券/本益比/三大法人/指數被永久標成休市丟棄。加正向交易日證據(交叉其他源或日曆)才寫 sentinel,否則 [deferred] 刪檔重抓。' },
  { key: 'sql-valuation', audit: 'D-valuation',
    files: ['src/main/resources/sql/view/8_valuation.sql', 'src/main/resources/sql/view/9_valuation_1q.sql'],
    focus: '樂活五線譜「標準差」用收盤價原始標準差(該用線性迴歸殘差標準差)→ 通道被系統性撐寬、趨勢股訊號稀釋;DCF 成長率 g 用逐季 YoY 算術平均(該用幾何 CAGR);eps_growth_rate_10y 視窗排序錯(未按日期正確取 10 年端點)。' },
  { key: 'sql-finratios', audit: 'D-financial-ratios',
    files: ['src/main/resources/sql/view/3_financial_index_quarterly.sql', 'src/main/resources/sql/view/4_financial_index_ttm.sql'],
    focus: 'TTM 版總資產週轉率平均資產分母錯用 5 季前(同檔 ROA 是 4 季前的打字錯,應 4 季前=年初);兩張表現金比率分母用總資產(教科書該用流動負債);ROIC 分子用稅後淨利(該 NOPAT);負權益公司排名反轉成最佳(加 guard)。' },
  { key: 'q01-bankruptcy', audit: 'D-bankruptcy-models',
    files: ['research/apex/experiments/q01_financial_scores.py', 'research/strat_lab/raw_quarterly.py'],
    focus: 'Altman Z\'\' X2 保留盈餘用「權益−股本」代理(把 114 家累虧訊號抹掉)→ raw_quarterly BS_TITLES 增 retained_earnings,q01 改用真保留盈餘;Ohlson INTWO「連兩年虧損」寫成「連兩季」(shift 1 應 shift 4)、CHIN lag 同錯;Z\'\' X3 EBIT 用營業利益(可加回利息);DuPont 週轉率年初 vs leverage/roe 期末口徑不一致;含 NCI 口徑註明。raw_quarterly.py 是共享 canonical,只做「新增科目 title」這種加法式改動,不動既有欄。' },
]

const EDIT_SCHEMA = {
  type: 'object',
  required: ['unit', 'status', 'edits', 'verify'],
  properties: {
    unit: { type: 'string' },
    status: { type: 'string', enum: ['DONE', 'PARTIAL', 'BLOCKED'] },
    summary: { type: 'string', description: '一句白話:修了什麼、驗證結果' },
    edits: {
      type: 'array',
      description: '對 HEAD 現況檔可乾淨套用的精確編輯;old_string 需與檔案逐字相符(含縮排)且在檔內唯一',
      items: {
        type: 'object',
        required: ['file', 'old_string', 'new_string', 'why'],
        properties: {
          file: { type: 'string' },
          old_string: { type: 'string' },
          new_string: { type: 'string' },
          why: { type: 'string', description: '對應哪條稽核 BUG/SUSPECT' },
        },
      },
    },
    suspects: { type: 'string', description: '該單位所有 SUSPECT 的處置:修了哪些、哪些判定不修(附理由)——零缺漏' },
    verify: { type: 'string', description: '自驗方式與結果:Scala=worktree 內 sbt compile 結果;SQL=語法/邏輯核對;Python=pytest 或 parse 實測。附實際輸出摘要' },
    blocked_reason: { type: 'string' },
  },
}

const PROMPT = (u) => `你是資深量化工程師,把台股量化系統一個算法稽核單位的**全部** BUG 修到好結案。
使用者鐵律:所有 TODO 零缺漏、每個修法對照學理定義、只認可重現證據。

# 單位:${u.key}(稽核 ${u.audit})

## 必讀(先讀再動手)
1. **稽核結論**(精確 BUG/SUSPECT + 建議修法 + 證據):
   - docs/data_audit/_done/${u.audit}.json
   - docs/data_audit/findings/${u.audit}.md
2. **目標檔**(逐字精讀,你的 old_string 要與現況逐字相符):
${u.files.map(f => '   - ' + f).join('\n')}

## 本單位重點(依稽核)
${u.focus}

## 你要做的
1. 把稽核列出的**每一個 BUG 全部修對**(不是只修一個),對照它宣稱/學理定義。
2. **每一個 SUSPECT 都要處置**:能修就修;判定不修的,在 \`suspects\` 明確寫理由(如「屬合理簡化、方向不變」)——**零缺漏**,不准默默略過。
3. **自驗**:
   - Scala:在你的 worktree 跑 \`sbt compile\`(逾時給足,可能 10+ 分),確認你的修改編譯通過;若有對應測試就跑。把結果寫進 \`verify\`。
   - SQL(view .sql):人工核對改後 SQL 的邏輯與語法(欄位、GROUP BY、視窗、除零 guard),寫清楚為何正確。
   - Python(q01):跑 \`uv run --project research python -m pytest\`(若有測試)或實際 parse/計算一筆對照學理手算值,寫進 \`verify\`。
4. 回傳**精確 edits**(對 HEAD 現況檔可乾淨套用的 old_string/new_string):主 agent 會統一套用到主工作區並做整合編譯。old_string 必須逐字相符且在檔內唯一(含足夠上下文)。

## 鐵律
- **只認可重現證據**:改法要能對照學理定義/稽核證據說明,不空口。
- **不碰本單位以外的檔**(保持 edits 互不重疊,利整合)。
- Scala 是凍結碼(修到好結案後退役),但**仍要編譯通過**——你的 edits 不可讓專案編不過。
- **共享 canonical 檔**(如 raw_quarterly.py)只做加法式改動(新增科目/欄),不動既有欄語意,避免污染 live 財報路徑。
- 改不動(endpoint 不可得、需跨單位大改)→ status=BLOCKED 並說清楚,不交半成品假裝完成。

單位 JSON:${JSON.stringify(u)}`

log(`算法稽核結案:${UNITS.length} 單位平行修法(worktree 自驗 + 回精確 edits)`)
phase('Fix')
const results = await parallel(UNITS.map(u => () =>
  agent(PROMPT(u), {
    label: `fix:${u.key}`,
    phase: 'Fix',
    schema: EDIT_SCHEMA,
    isolation: 'worktree',   // 各自 worktree 才能獨立 sbt compile 自驗
    effort: 'max',
    model: 'opus',
  })))

const done = results.filter(Boolean)
return {
  units: done.length,
  by_status: {
    DONE: done.filter(r => r.status === 'DONE').map(r => r.unit),
    PARTIAL: done.filter(r => r.status === 'PARTIAL').map(r => r.unit),
    BLOCKED: done.filter(r => r.status === 'BLOCKED').map(r => r.unit),
  },
  results: done,
}
