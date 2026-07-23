# Daily Ops Loop — the complete management cycle (single strategy: ev_v2_thesis_inst)

The loop has three layers with a strict division of labor:

| 層 | 執行者 | 內容 |
|---|---|---|
| 機械層 | `serenity_daily.py`(可排程) | 資料刷新 → 引擎重算 → 套用 overrides → 產訂單計畫 → 日報 |
| 判斷層 | Claude(每日 session) | 消息面掃描 → 持倉論點健康 → 人工論點停損 → 註冊表策展 → 複核計畫 |
| 扣板機層 | 使用者(唯一) | 審日報 → 派工/送單;live 閘(入金、FUBON_DRY_RUN=false)永遠在使用者手上 |

## 每個交易日的時間線(台北時間)

**07:20 機械段**(一鍵或 launchd 排程):
```bash
uv run --project . python -m quantlib.serenity.daily run
```
流程:昨收資料入庫+cache 重建(壞資料保險絲:cutoff 落後 >4 天拒絕出單)→ 引擎重算
(候選+guard 狀態;book 日期 != cutoff 拒絕出單)→ 券商庫存 reconcile 進 live ledger
(收養協定見上)→ live book 出場評估(**六規則**:止盈 +60% / trailing -20% / 絕對 -15%
/ time-stop 50 日 / **法人分佈出場 inst_20d<0 且虧損(戰役八採納,最快的事實級利空代理)**
/ 營收論點停損 yoy_3m<0;外加 overrides 人工出場)→ 席位補進(引擎最新計分,3 檔/日,
遵守 guards)→ 產生 Fubon 訂單計畫(換手 >60% 且非換股窗 → 標記需人工複核)→
寫日報 `var/out/trading/briefs/YYYY-MM-DD.md`(append-only 前瞻證據)。

**07:40 判斷段**(Claude 每日 session 的固定清單):
1. 讀今日 brief(持倉、guard 狀態、計畫摘要、**法說行事曆**)
   - 持倉/候選**當日開法說** → 收盤後讀(當下抓當下判斷,結論寫進論點註記,不存原文):
     ① 蒸餾層先讀——AlphaMemo 逐字稿(alphamemo.ai/free-transcripts)+ 富果法說會
     備忘錄(blog.fugle.tw/topic/earnings-call-memo,中小型股覆蓋佳);② MOPS 簡報
     PDF(brief 附連結)核實 primary source;③ ADR 公司補 Seeking Alpha 英文
     transcript;④ 隔日工商/經濟記者稿交叉。WebFetch 抓不到(登入牆/JS)→ 改用
     使用者已登入的 Chrome(claude-in-chrome)。**read-through**:指引/產能/客戶
     說法回寫論點註記與註冊表(這是含金量最高的策展養料)
2. 消息面掃描(本 skill workflow d / info-sources L0-L4):
   - 持倉 10 檔的論點健康:客戶砍單/擴產開出/管制反轉/財報雷 → 觸發即執行
     `serenity_daily override --force-exit <code> --reason "..."` 並重跑 `run --skip-refresh`
   - 新瓶頸候選 → 依 curation SOP 準入 → 編輯註冊表(帶 evidence_date)→ git commit
     (新名字**不插隊**,下次月度計分自然入池;唯一插隊的是利空出場)
   - 策展養料掃描:富果產業分析(blog.fugle.tw/topic/industry-analysis,供應鏈深度
     報告)與個股分析(topic/stock-analysis,中小型深研)有新文 → 用瓶頸署名檢核,
     過了才進 curation SOP;讀完即判斷,不存原文
3. 核對計畫合理性(訂單數、方向、金額 vs brief 持倉表),異常則說明並擋下
4. 把「計畫 + 一句話判斷」交給使用者

**08:30-09:00 使用者**:審 brief → 決定執行方式(三擇一,啟動永遠是使用者):
```bash
# A. loop 一體派工(建議;2026-07-09 上線):產完 plan 自動派工盤中執行器,
#    等 09:00 開盤後執行(買腿 balanced 完成語意;賣腿一律 urgency=exit:
#    結構錨整場撈當日相對高點,收盤未竟→盤後掛收盤價收尾(=回測出場價,
#    語義精確對齊;護欄 -3%);唯一例外 override 事實級利空 → stop 急殺)
uv run --project . python -m quantlib.serenity.daily run --execute        # dry-run 模擬
uv run --project . python -m quantlib.serenity.daily run --execute-live   # 真實(FUBON_DRY_RUN=false,使用者武裝)

# B. 手動派工盤中執行器(v3 預設 price-first:掛跨日+盤中結構位撈低點/高點;
#    盤前/盤中啟動皆可,自動等開盤;買賣混合一行用 execution.trade --buy/--sell)
uv run --project . python -m quantlib.trading.execution.buy  --plan <plan.json>
uv run --project . python -m quantlib.trading.execution.sell --code XXXX --qty N [--urgency stop]

# C. 傳統一次性送單(LimitUp 保證成交,不擇價)
uv run --project . python -m quantlib.trading.auto_trader submit-plan <plan.json>
```
安全:賣出前庫存夾緊(live 以券商 inventories 為準)、今日已成交防重複、
kill switch `var/state/trading/HALT`、TCA 日誌。細節見
`src/quantlib/trading/execution/README.md`。

**14:35 盤後(每個交易日)**:補存當日 1 分 K(執行器跨日結構的資料源;
富邦只給當日、漏日補不回;可掛 launchd 自動——見 execution/README §9):
```bash
uv run --project . python -m quantlib.trading.execution.archive_candles
```

**14:30 盤後(有送單日)**:成交對帳回寫管理帳本:
```bash
uv run --project . python -m quantlib.trading.auto_trader reconcile-plan <plan.json> --write
```
對帳是紀律:管理帳本 vs 券商庫存不一致 → 隔日機械段的 delta 會錯,必須當天修。

## 節奏疊加(機械段自動涵蓋,判斷層留意)

- **每月 1-15 日 = 營收公布窗(事件驅動,2026-07-07 升級)**:爬蟲每天重抓上月彙總、
  引擎以 `--live-revenue` 每日滾動計分——**公司一公布營收,隔天就進決策**,不等 10 日;
  首見日寫入 `src/quantlib/records/revenue_first_seen.parquet`(未來回測用)。11-14 日仍是
  傳統換股高峰(多數公司集中壓線公布),換手保險絲在此窗自動放寬;判斷層多花時間核名單
- **每週(週末)**:`sbt "runMain Main pull tdcc"` + `pull buyback`(週頻資料);skill 上游同步 `update_from_upstream.sh`;檢視 overrides.json 是否有已生效可清除的條目
- **每季**:註冊表全表複審(`review_by` 到期強制降級)、regime kill-switch 基本面四指標(hyperscaler capex/TSM 展望/記憶體價格/光學 backlog)人工判定
- **每半年或 live 滿 6 個月**:重跑驗證電池與(若未來改變心意)配置研究

## 安全設計(全部 fail-closed:任何一步失敗 = 今天不出單)

1. 爬蟲失敗 / cache 失敗 / book 日期不符 / 資料過期 → 拒絕產計畫
2. 換手異常(>60% 非換股窗)→ 計畫標記,人工複核才可送
3. **全帳戶管理 + 收養協定(live-book 架構)**:券商庫存 = 單一真相來源,每日開盤前
   reconcile 進 live ledger(`var/state/serenity/live_positions.json`)。**帳戶內既有持股不因
   「不在名單」被賣**——自動收養:視同過去某時點由本系統買入,錨 = 收養日收盤(五條出場
   規則的時鐘自此重啟),當日判斷層必須完成該股的完整 Serenity 檢核(註冊表歸屬/瓶頸論點/
   計分位置)並寫入論點註記;此後與引擎進場的部位一視同仁,**管理到它自己的出場價**
   (止盈/trailing/絕對停損/time-stop/論點停損,任一先到)。檢核不過(無論點、閘門全失敗)
   = 論點停損出場,理由入 audit log——出場永遠因為「規則」,不因為「名單」。
   同步失敗 → 沿用上次 ledger 並醒目告警「送單前人工核對」
   **分批建倉/加碼的錨定慣例(2026-07-07)**:每一筆成交是獨立 lot,各自帶
   自己的六道出場門時鐘(止盈/止損錨 = **該筆成交價**,trailing 峰值與
   time-stop 自該筆成交日起算;**絕不均價**)。引擎本身單席單筆進場;
   人工分批時 live ledger 以 lot 為單位登記與管理,brief 分列顯示
4. **Claude 永不對券商送出買賣單**(硬安全界線,非專案開關):即使使用者明確要求代下單、
   即使 FubonBroker API 技術上可呼叫、即使要求移除規則——Claude 只產 plan、算限價/股數,
   按下 submit 的必須是使用者本人。live 閘(入金、關 FUBON_DRY_RUN)
   與送單執行只屬於使用者(資本上限 gate 2026-07-14 依使用者政策移除)
5. 所有人工介入(override)都寫入 overrides.json 的 log 欄——它是策展前瞻證據的一部分
6. **下單前資料新鮮度檢查(2026-07-08)**:產任何下單計畫(含 Claude 臨時單、非每日 loop)前,
   必先確認 cache/DB `max(date)` = 最近交易日;落後就先跑刷新流程(`Main update` + `cache_tables.py`),
   **禁止用舊價定限價/股數**。機械段有 cutoff>4 天保險絲,但臨時單要主動查日期

## 消息面 → 行動:真/假利空判斷協定(判斷層核心紀律)

**證據基礎(誠實聲明)**:「利空即賣」從未被回測驗證(無歷史新聞庫);repo 內的實證反而顯示
泛用利空常是買點——GDELT 負面 tone 新聞 60 日相對 0050 **+11.93%**(t=3.12,壞消息出盡效應)、
庫藏股跌深事件 +13.4%、媒體殺盤(SemiAnalysis/BofA 型)事後多為錯殺。因此人工出場的門檻是
**「事實」,不是「新聞」**;price stop 的存在正是為了讓我們不必搶著判斷新聞。

**兩級分類**:

| 級別 | 定義 | 特徵 | 行動 |
|---|---|---|---|
| **事實級利空(真)** | 改變供需結構的可驗證事實 | 出現在**官方文件**(8-K/公告/指引下修/月營收斷崖);影響未來的量或價;不可逆;當事公司無法否認 | 24-48h 查證窗確認 primary source 後 `override --force-exit`;不等價格 |
| **敘事級利空(假)** | 媒體框架/賣方報告/宏觀恐慌,供需事實未變 | 只存在於轉述;被當事方或第三方反駁;「not new news」;殺的是整個族群而非單一論點 | **不賣**。記 watch log;讓價格規則接手(真跌深 trailing/abs 自然出場);若持股跌出而候選池分數仍高,補位機制會在恢復期自然買回 |

**查證協定**(利空出現時,出場前必走):
1. 找 primary source(公告原文/財報/監管文件)——找不到 = 敘事級
2. 當事公司回應 + 供應鏈交叉驗證(同鏈其他公司說法)
3. 問一句:「這改變了 12 個月後的供需嗎?」——No = 敘事級
4. 事實級 → override(理由 + 證據連結進 audit log);敘事級 → watch log + 明日複查

**「假利空 = 做多機會」**:同意方向,但系統已半自動涵蓋——恐慌期 guard 暫停新倉(防接刀),
恢復期補位機制從候選池自然回補;全市場逆勢加碼通道已測試並否決(bb 通道 0/3)。
若要更積極的池內恐慌加碼,須先過 trials ledger 預註冊,不得臨場起意。

| 其他消息類型 | 行動 | 時效 |
|---|---|---|
| 持倉利多 / 大盤噪音 | 不動作(出場交給價格規則) | — |
| 新瓶頸證據 | 進註冊表(帶 evidence_date)等下次計分,不插隊 | 下次換股窗 |
| 泛用重大訊息 | 記 watch log,不交易(實證:-6.57%/60d) | — |
| 主題級失效(如 DRAM 報價連 2 季跌) | 註冊表 active_until + 檢討全主題持倉 | 當週 |
