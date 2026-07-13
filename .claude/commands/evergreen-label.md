# /evergreen-label — Evergreen 月中標記日

對指定月份執行 Evergreen 純質化標記(發 agent 真搜尋 → 驗收 → 落盤
registry_v3 → 重跑 tri)。**單月成本約 15–20 萬 token**;多月 backfill
先向使用者確認額度再發。

參數:`$ARGUMENTS` = 月份(`YYYY-MM`,可多個,空白分隔);**留空 = 當月**。

## 鐵律(違者停止並回報使用者)

1. 標記提示詞**逐字取自凍結檔** `research/evergreen/PROMPT_ev28_labeling.md`
   (由配套腳本組裝)。任何改動提示詞的需求 = 停下,由使用者裁決。
2. PIT 紀律:agent 只准使用站位日(含)之前的資訊;搜尋材料必須落檔。
3. 驗收不過**不落盤**;registry_v3 落盤前自動備份。
4. 已入冊月份不重標,除非使用者明示覆蓋。
5. agent 原始輸出必須存 `research/evergreen/data/label_runs/{month}.json`
   (零 token 重放資產)。

## 執行流程

```bash
# ① 組提示詞 + 確認站位日(站位日未到的月份即停)
uv run --project research python -m research.evergreen.label_monthly prompt <月份...>
```

② 以 Workflow 發標記 agent(command 觸發即為使用者授權;1 月 = 1 agent,
多月 parallel)。**JOBS 必須內嵌進 script 檔,不走 Workflow 的 `args` 參數**
(args 會被序列化成字串,`args.map` 直接爆)。作法:用 python 讀
`data/prompts/{month}.txt`,以 `json.dumps` 生成 script 檔再以
`{scriptPath}` 發射。script 形態(schema 與 EV28/EV29 完全同構):

```js
export const meta = { name: 'evergreen-label', description: 'Evergreen 月中標記', phases: [{ title: 'Label' }] }
const SCHEMA = {…同 ev28_build_pilot.py 的 SCHEMA…}
const JOBS = […{month, prompt} 由生成器 json.dumps 內嵌…]
phase('Label')
const out = await parallel(JOBS.map(j => () => agent(j.prompt, { label: `label:${j.month}`, schema: SCHEMA, model: 'opus', effort: 'max' }).then(v => v && { ...v, month: j.month })))
return out.filter(Boolean)
```

(agent 工具全開——WebSearch / WebFetch / 使用者 Chrome 皆可用,不得限制。)

③ 每月結果存檔 + 驗收 + 落盤 + 確認:

```bash
# agent 輸出寫入 research/evergreen/data/label_runs/{month}.json 後:
uv run --project research python -m research.evergreen.label_monthly validate --input research/evergreen/data/label_runs/{month}.json
uv run --project research python -m research.evergreen.label_monthly merge    --input research/evergreen/data/label_runs/{month}.json
# 重跑三策略日檢,確認 Evergreen 池已更新(標記月滾動)
uv run --project research python -m research.tri.daily
```

④ 回報使用者:各月入冊筆數與代號、驗收警示(PIT 可疑項要列出)、
registry 月份數變化、tri 報告中 Evergreen 池的變化。
