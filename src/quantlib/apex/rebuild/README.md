# apex/EV 實驗代碼考古(2026-07-13)

apex campaign 中後期(r03 之後)與 EV5–EV9 消融的實驗代碼當年以 Bash
heredoc 執行未落檔(「研發代碼永久留存」鐵律 2026-07-10 立法,正好在
apex 收官後一天)。本目錄是欠帳的清償:**592 筆缺檔 trial 的代碼已
100% 從 Claude Code 對話 transcript 逐字復原,零 LLM 成本**。

## 目錄結構

| 檔案 | 內容 |
|---|---|
| `recovered/{seq}_{batch}.py` | **逐字原版代碼**(98 段;檔頭註記來源 transcript 與時戳,內文零改動)|
| `recovered/index.json` | trial name → 復原檔對照(715/715 全覆蓋)|
| `extract_transcripts.py` | 抽取器(可重跑;三層比對:逐字名 ∪ batch 前綴動態名 ∪ 值域特徵)|
| `make_packs.py` + `packs.json` | 缺檔盤點 + 每 batch 的 golden metrics 整理(重跑驗證時用)|

## 性質與用法

- `recovered/` 是**歷史檔案庫**(log),不保證在當前 repo 直接可執行——
  部分引用當年的模組狀態(如 `assemble.entries_and_flags`)。要重跑某個
  歷史實驗:以復原檔為藍本、對當前引擎 API 微調,golden 數字在
  `ledger/trials.jsonl`(name 對照 `recovered/index.json`)。
- 歷史紀錄不可竄改:`trials.jsonl` / `batches.md` / `REPORT.md` 維持原樣;
  重跑驗證的結果另行記錄,不回寫。

## 教訓

heredoc 執行的代碼**永遠留在 transcript**(`~/.claude/projects/<proj>/*.jsonl`
的 tool_use 記錄)——真正的風險是 transcript 檔案被清理。正道仍是鐵律:
研發代碼一律先落正式檔再執行;transcript 復原是最後保險,不是流程。
