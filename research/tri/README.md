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

# ① 資料刷新(前一晚 21:30 後或當天盤前;約 10-15 分鐘)
#    (若你今天會跑 Serenity daily,它內建刷新,這步可省)
sbt "runMain Main update" && uv run python research/cache_tables.py

# ② 三策略決策支援(讀富邦庫存+現金 → 三份獨立建議含股數;永不下單)
uv run --project research python -m research.tri.daily

# ③(可選,獨立決定)Serenity 執行流水線——會產生訂單計畫並啟動盤中執行
uv run --project research python -m research.serenity.daily run
```

②的輸出(終端 + `research/tri/reports/YYYY-MM-DD.md`):

- **S(apex_revcycle_S)**:每檔持股 KEEP/SELL(機械原因:訊號過期 26 日
  /trail 35%/時間止損 30/輸家止損 15)+ 今日 fresh cohort 買入建議
  (每檔 20% 資金,今日進場 2 檔 + backlog 候補)。**營收事件驅動**:
  資料庫一有新月報首見即納入(不等每月 10 日;回測維持保守 10 日語義)
- **Evergreen(live-refit)**:引擎參數讀 `research/evergreen/data/live_config.json`
  (EV43 年度 refit 產物;現版 score=xadv_inv 小票優先、gate=none、池籍 3 月、
  trail 40%/輸家止損 45、5 席 × 20%、每日 2 檔)。**每年 refit 一次**
  (`uv run --project research python -m research.evergreen.ev43_live_refit`)
  換檔即生效;驗證等級 Borderline,建議倉位 ≤25% NAV
- **Serenity(live lot 六道門,2026-07-13 重寫)**:讀 live ledger 的 lot 錨
  (成交價/收養價),用與執行系統**同一份規則源**(`serenity/exit_rules.py`)
  逐 lot 評六道門 → KEEP/SELL 附錨/現價/峰值;進場建議轉述當日 brief。
  舊版用引擎模擬簿成員資格當判準,會與 live 判決相左(模擬簿 lot 是引擎
  自己的錨)——已廢棄

## 每月一次:Evergreen 標記日

每月 **10 日後的第一個交易日**,②的報告頂部會出現:

> ⚠ 本月標記尚未執行——請執行月中標記(LLM 流程)後重跑

那天在 Claude Code 輸入 **`/evergreen-label`** 即可(流程:凍結提示詞
組裝 → 發標記 agent 真搜尋 → 驗收 → 落盤 registry_v3 → 重跑 ②)。
約 15-20 萬 token,每月僅此一次。其他所有日子 ② 零 LLM。

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
| 報告顯示「cache 落後 N 天」 | 先跑 ① 再跑 ② |
| S 顯示「fresh cohort 0 檔」 | 揭露季外正常(訊號稀疏);揭露季內(每月 7-17 日)多半是資料未刷新,先跑 ① |
| Evergreen 池很小(<5 檔) | 正常——空手紀律;連續數月過小才需檢查標記 |
| 持股被判「非本策略標的」 | 正常——那是該策略視角(它會換成自己的池);三份建議由你仲裁 |
| Serenity 段 book as_of 較舊 | 表示引擎近日未跑;要最新 book 就跑 ③(注意 ③ 是執行系統) |
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
