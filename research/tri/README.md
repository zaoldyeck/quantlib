# 三策略每日操盤手冊(S / Serenity / Evergreen v3.3)

你每天操盤需要的**全部指令**都在這一頁。

**兩個系統,性質不同,徹底切分:**

| | 決策支援(tri) | 執行流水線(Serenity daily) |
|---|---|---|
| 指令 | `research.tri.daily` | `research.serenity.daily run` |
| 性質 | **只給判斷,永不下單** | 完整流水線:刷新→引擎→對帳→計畫→**盤中執行** |
| 涵蓋 | S + Evergreen 評判 + Serenity 唯讀摘要 | Serenity 策略的倉位同步 |
| 觸發 | 你想看建議時 | 你決定讓 Serenity 執行時 |

兩者互不觸發:tri 只會唯讀嵌入 Serenity 已存在的 brief,絕不啟動它的流水線。

---

## 每日流程

```bash
cd /Users/zaoldyeck/Documents/scala/quantlib

# ① 三策略決策支援(讀富邦庫存+現金 → 三份獨立建議含股數;永不下單)
#    2026-07-14 起內建資料新鮮度自檢:過期會自己跑爬蟲+cache 重建(約
#    10-15 分)再評判,新鮮就直接用;--no-refresh 可跳過自檢
uv run --project research python -m research.tri.daily

# ②(可選,獨立決定)Serenity 執行流水線——會產生訂單計畫並啟動盤中執行
uv run --project research python -m research.serenity.daily run
```

①的輸出:終端 + **`research/tri/reports/YYYY-MM-DD.md`(一天一份,重跑覆蓋)**。
報告是寫給投資人看的,**純程式產生、零 LLM**——理由取自策展當時就落地的資料:

| 區塊 | 內容 |
|---|---|
| 🔔 今日必須行動 | **逾期出場**(規則早已觸發、你還沒賣:附觸發日/當時價/今日價)→ 今日觸發 → 買入 |
| 📖 逐檔深度 | 每檔一節:三策略判決 + 成本/現價/損益 + **為什麼買**(Serenity 論點原文+瓶頸層+信念度+**失效條件**;Evergreen 最近一次標記全文+**當時查閱的材料原文與連結**+標記史;S 六因子值與 geo 排名)+ **六道門逐條的線與距離** |
| 🛒 買入候選深度 | 同上規格——買進的理由也要寫完整 |
| 📋 三策略完整視角 | 各自的理想持倉完全體、續抱/賣出/買入隊列 |
| 下單指令 | 代碼自行填入的 `trade` 範例 |

**出場一律逐日重放,不是今日快照**(`research/trading/exit_replay.py`):規則在你沒跑
報告的那幾天觸發也算數——「延遲了該賣還是得賣,不能過時間了就當作沒發生」。峰值
(trailing 的錨)由價格歷史重算,與跑不跑報告無關。

**誠實標示**:Serenity 註冊表早期入冊只有 `legacy:…` 沒有真出處 → 報告標 ⚠;
Evergreen 沒標記過的檔 → 寫「全史未曾標記」而不是留白;池籍到期 ≠ 沒看過。

## 每月一次:Evergreen 標記日

每月 **10 日後的第一個交易日**,①的報告頂部會出現:

> ⚠ 本月標記尚未執行——請執行月中標記(LLM 流程)後重跑

那天在 Claude Code 輸入 **`/evergreen-label`** 即可(流程:凍結提示詞
組裝 → 發標記 agent 真搜尋 → 驗收 → 落盤 registry_v3 → 重跑 ①)。
約 15-20 萬 token,每月僅此一次。其他所有日子 ① 零 LLM(資料自檢刷新除外)。

## 富邦讀不到時(手動提供持倉)

```bash
uv run --project research python -m research.tri.daily --positions "2330:1000,2317:2000" --cash 500000
# 或 csv(欄位 code,shares):
uv run --project research python -m research.tri.daily --positions-file my_positions.csv --cash 500000
```
(`--cash` 供 NAV 與買入股數計算;富邦 API 模式會自動抓銀行餘額,也可用 `--cash` 覆蓋)

## 常見狀況

| 狀況 | 處理 |
|---|---|
| 報告顯示「cache 落後 N 天」 | ① 已內建自動刷新;若用了 --no-refresh 才會見到,拿掉該旗標重跑 |
| S 顯示「fresh cohort 0 檔」 | 揭露季外正常(訊號稀疏);揭露季內(每月 7-17 日)多半是資料未刷新,重跑 ①(自動刷新會補) |
| Evergreen 池很小(<5 檔) | 正常——空手紀律;連續數月過小才需檢查標記 |
| 持股被判「非本策略標的」 | 正常——那是該策略視角(它會換成自己的池);三份建議由你仲裁 |
| Serenity 段 book as_of 較舊 | 表示引擎近日未跑;要最新 book 就跑 ②(注意 ② 是執行系統) |
| 富邦 API 失敗 | 用 `--positions` 手動提供(見上) |

## 檔案位置

| 東西 | 路徑 |
|---|---|
| 每日報告 | `research/tri/reports/YYYY-MM-DD.md` |
| Serenity brief | `research/serenity/out/briefs/YYYY-MM-DD.md` |
| 出場狀態(首見日/峰值,自動維護) | `research/tri/state/*.json` |
| Evergreen 標記庫 | `research/evergreen/data/registry_v3.parquet` |
| 策略規格書 | S: `research/apex/STRATEGY.md`;Serenity: skill `serenity-trading-system`;Evergreen: `research/evergreen/LEDGER.md`(EV30-33 定版) |

## 三策略一覽

| | S | Serenity | Evergreen v3.3 |
|---|---|---|---|
| 本質 | 純量化(月營收加速) | 人工策展瓶頸論點 × 機械紀律 | AI 搜尋消息面標記 × 量化引擎 |
| 訊號源 | 月營收揭露(每日掃) | 論點註冊表(人工維護) | 月中 AI 標記(每月一次) |
| 持股/倉位 | ≤5 檔 × 20% | 依其計分 | ≤5 檔 × 20% |
| 出場 | 過期26/trail35/時停30/輸家15 | 八成分事件紀律 | 池籍3月/trail40/輸家45 |
| 同窗戰績(2025-01~2026-07) | 63%(此窗弱;14.5 年 +75%/年) | 235-253% / MDD −18 | 304%(區間 229-304)/ MDD −30 |
