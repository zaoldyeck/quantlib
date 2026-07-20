# 盤中執行器使用手冊(execution)

兩支 CLI:**買入** `research.trading.execution.buy`、**賣出** `research.trading.execution.sell`。
接手 plan 檔或單檔指令,盤中盯價、依模式掛單改價、死線前完成、全程 TCA 日誌,
全部腿結束後自行終止。

**使命(v3,2026-07-09 起)**:買入盡可能接近當日低點、賣出盡可能接近當日
高點。**手動 CLI 預設 `--patience price`**:單掛在跨日+盤中結構位(昨日價值
區、日線支撐阻力、TPO VAL/POC、OB/FVG、VWAP)等市場回到便宜處,只有狙擊級
微結構訊號(全條件 AND)才主動取價;**盤中永不因時間跨價**(2026-07-14 起),
收盤未竟自動以盤後定價收尾(14:30 撮合=當日收盤價,= 回測的出場價語義)。實證(2026-07-09 實盤
TCA 反事實):同批 10 檔 balanced 實際成本 vs price 模式,**10/10 全數當日
成交、總成本省 206 bps**,重傷單(+83/+85 分位追價)全數避開。

**兩種語意,各有證據**:每日 loop 的引擎買腿顯式使用 `--patience balanced`
(完成優先)——進場擇時事件研究證明對本策略的動能型進場「等回檔」會錯過
贏家(95 筆累積報酬 1,450% → 595%);手動買賣則預設 price。這不是工具的
信念,是呼叫方各自帶著自己的回測證據選擇參數。

---

## 1. 快速開始

```bash
# 離線自測(不需憑證,驗證邏輯)
uv run --project research python -m research.trading.execution.buy --selftest

# dry-run(真實行情、模擬成交、不送單;預設模式)
uv run --project research python -m research.trading.execution.buy --code 2408 --qty 1

# 真實下單(武裝是使用者的動作:FUBON_DRY_RUN=false + --live;
# 盤前啟動自動等開盤、盤中啟動立即執行)
FUBON_DRY_RUN=false \
uv run --project research python -m research.trading.execution.buy \
    --plan research/out/trading/plans/<plan>.json --live

# 買賣混合一行指令(全部腿併發;買腿撈低點、賣腿撈高點+保證當日完成)
FUBON_DRY_RUN=false \
uv run --project research python -m research.trading.execution.trade \
    --buy "2408:2,3006:5" --sell "4973,5289" --live   # 買寫股數;賣不寫=全部庫存
```

---

## 2. 完整參數表(buy 與 sell 共用;差異註明)

### 標的與數量

| 參數 | 型別/預設 | 說明 |
|---|---|---|
| `--code` | 代碼(可多檔,可帶股數) | 與 `--plan` 二選一。**逗號多檔 = 併發執行**;每檔可寫 `代碼:股數`:`"4973:1,5289:3"`。**省略股數:買 1 股、賣全部庫存**(`4973:all` 亦可) |
| `--qty` | 預設股數(買賣通用) | 未逐檔指定 `:股數` 時的預設。**給了 → 買賣兩側都用它;不給 → 買 1 股、賣全部庫存**。逐檔 `:股數` 永遠優先。`< 1000` 走盤中零股 |
| `--plan` | 路徑 | plan JSON(格式見 §4);buy 吃全部 Buy 腿、sell 吃全部 Sell 腿,**併發執行** |

### 執行模式

| 參數 | 值/預設 | 說明 |
|---|---|---|
| `--patience` | `price`(預設)/`balanced` | **price**:整場掛結構位(昨日 VAL/POC、日線支撐阻力、TPO VAL/POC、OB/FVG、VWAP、日低近旁)撈價,微結構訊號自動升為狙擊級(全 AND),盤中只有訊號才主動取價、**永不因時間跨價**;收盤未竟→盤後掛收盤價收尾;**balanced**:階梯(被動 3 輪 → 中價 3 輪 → 跨價,12:30 死線),每日 loop 買腿顯式使用 |
| `--urgency` | `normal`(預設)/`exit`/`stop` | **sell 限定**。**exit = 系統出場預設**:結構錨(VAH/昨日高,盤中新高即時追蹤)整場撈相對高點、盤中永不因時間跨價,收盤未竟→**盤後掛收盤價收尾**(14:30 撮合=收盤價,與回測出場價語義精確對齊;護欄 −3% 鐵律:收盤破欄不掛、盤後未中籤→明日門重評);**stop** = 急殺,僅事實級利空 override:首輪即跨價;normal 吃 `--patience` |
| `--position-mode` | `auto`(預設)/`own`/`add` | 買入語意。**own**:目標=「持有 ≥ qty」,啟動先對庫存,已持有跳過、部分持有只補差額;**add**:嚴格加碼 qty(每日 loop 的 delta plan 用這個);auto:`--code` → own,`--plan` → 讀 plan 的 `position_mode` 欄,無則 add |
| `--no-micro` | flag | 關閉微結構層(OFI/VPIN/TPO/SMC/VWAP),走純階梯 |
| `--trigger-strict` | flag | **狙擊模式**(UMEE 哲學):micro 加速需全部條件 AND(止穩+竭盡/掃蕩+資金流/簿支撐+價值區),預設為 ≥3/4 |

### 價格控制

| 參數 | 預設 | 說明 |
|---|---|---|
| `--cap-pct` | 取 profile(balanced 0.8%、price 0.5%) | **護欄**:買上限 = arrival×(1+cap)、賣下限 = arrival×(1−cap);**絕不越過**,價格跑走就掛在護欄等。`0` = 絕不高於啟動價 |
| `--cap-auto` | flag | 波動自適應護欄 = 8×(1 分 K 平均振幅),夾 0.4%–2%(高波動股給空間、牛皮股鎖緊);蓋過 `--cap-pct` |
| `--deadline` | balanced 12:30;price/exit **無**(盤中永不因時間跨價) | 覆蓋盤中升級死線 HH:MM;price/exit 模式想強制盤中保底完成才需要設 |

### 時間與節奏(啟動時機免旗標:盤前啟動自動等開盤、盤中啟動立即執行、收盤後拒絕)

| 參數 | 預設 | 說明 |
|---|---|---|
| `--round-sec` | 60 | 改價輪詢週期秒(盤中零股逐分鐘撮合 → 60;實際 ±15% 抖動防狙擊) |
| `--avoid-open-min` | 3 | 開盤前 N 分鐘只被動不跨價(輪動噪音迴避;停損不受限) |

### 安全與雜項

| 參數 | 預設 | 說明 |
|---|---|---|
| `--live` | flag(預設 dry-run) | 真實下單;需 `FUBON_DRY_RUN=false`(資本上限 gate 2026-07-14 依使用者政策移除——代碼與股數本由使用者逐一給定) |
| `--allow-refill` | flag | 無視「今日已成交」續傳,強制再執行(預設會把當日成交計入進度) |
| `--slice-qty` | 整股≥2 張→1000 | 大單 TWAP 切片上限(child 依序出);零股不切 |
| `--selftest` | flag | 離線自測 |

### 環境變數

| 變數 | 說明 |
|---|---|
| `FUBON_DRY_RUN=false` | 解除模擬鎖(預設 true 永不送單) |
| `QL_STRATEGY_CAPITAL_TWD` | 執行器**不再需要**(2026-07-14);僅每日 loop 的計畫 sizing(auto_trader)使用 |
| `FUBON_PERSON_ID` 等憑證 | 已在 `research/.env`,不需重複設定 |

---

## 3. 模式選擇矩陣

| 情境 | 指令組合 |
|---|---|
| **當天想買+想賣一次下(建議手動日常)** | `trade --buy "2408:2,3006:5" --sell "4973,5289"`(買寫股數、賣不寫=全部庫存;全腿併發撈低/撈高,收盤未竟→盤後收盤價) |
| 每日 loop 的計畫單(建議日常) | loop 自動派工:買腿 `--patience balanced`(回測完成語意)、止盈腿 price、停損腿 stop |
| 手動買進(預設:買到好價) | `--code X --qty N`(price + own;結構錨定,收盤未竟→盤後收盤價) |
| 手動買進、務必今天買到 | `--code X --qty N --patience balanced` |
| 一般賣出(想賣好價) | `sell --code X --qty N`(price 預設) |
| **六道門系統出場(全部理由)** | `sell --code X --qty N --urgency exit`(整場撈相對高點,收盤未竟→盤後收盤價) |
| 事實級利空急殺(override) | `sell --code X --qty N --urgency stop`(首輪跨價,不等價格) |
| 高波動股(記憶體/生技) | 任何組合 + `--cap-auto` |

---

## 4. plan 檔格式

```json
{
  "plan_id": "manual_buy_missing_20260709",
  "position_mode": "own",              // 選填:own / add(auto 模式讀這裡)
  "orders": [
    {
      "symbol": "2383",                 // 必填
      "side": "Buy",                    // 必填:Buy / Sell
      "quantity": 1,                    // 必填(股)
      "reference_price": 5220.0,        // 選填:啟動核對顯示用
      "name": "台光電"                  // 選填:顯示用
    }
  ]
}
```

產生方式:(a) 每日 loop 自動(`auto_trader plan`,add 語意的 delta 單);
(b) 手動情境建議**產生時就過濾**——查庫存+今日成交+在途單後只留缺額
(參考 `plans/manual_buy_missing_20260709.json` 的產生腳本模式)。

---

## 5. 執行流程(你會看到什麼)

1. **交易計劃核對**:方向/模式/護欄/死線/micro/防重複 + 每腿代碼×股數×零股或整股×參考價×合計。
2. **LIVE 倒數 5 秒**(dry-run 不倒數;核對有誤 Ctrl+C,零副作用)。
3. 登入 →(未開盤則自動等到 09:00,長等待後自動刷新 session)→ **接管與續傳**(§6)→ 併發啟動全部腿。
4. **進度板**:任何腿掛單/改價/成交/接管/跳過即印「完成 n/N + 各腿狀態」。
5. **盤後收尾**:13:30 收盤仍未竟且收盤價在護欄內 → 自動掛盤後定價(零股 13:40 起收單、整股 14:00 起;14:30 一次撮合、成交價=當日收盤價);未中籤如實記錄,明日規則重評。
6. 全部腿結束(成交/放棄/中止)→ 每腿 JSON 總結(成交均價、對 arrival 滑價 bps)→ **自行終止**。

**Ctrl+C 語意**:第一次 = 全部腿本輪撤單退出;第二次 = 立刻強制(之後跑
`cancel_all` 確認無殘留)。**全域急停**:`touch research/state/trading/HALT`
(所有執行器 ≤1 輪內撤單退出;`rm` 解除)。

---

## 6. 冪等與接管(重跑永遠安全)

| 啟動時發現 | 行為 |
|---|---|
| 執行鎖存在、**持鎖 PID 還活著** | 擋下(兩個執行器不可同時管同一腿) |
| 執行鎖存在、程序已死 | 自動接手(`stale_lock_takeover`) |
| 交易所有在途同向委託 | **認領接管**續管(`takeover_working_order`) |
| 今日已有同向成交 | **計入進度**(`resume_from_today_fills`,源=當日委託回報;`filled_history` 盤中查不到當日成交——2026-07-09 實盤事故根因,已禁用) |
| own-mode:庫存已 ≥ 目標 | 跳過(`⏭ 已持有`) |
| sell:庫存不足 | 自動夾到可賣量;庫存 0 跳過 |

---

## 7. v2 智慧層(全部 TCA 可觀測)

| 技巧 | 作用 | 事件標籤 |
|---|---|---|
| microprice + OBI 自適應掛位 | 被動段:我方厚+價差寬 → 讓一檔搶排隊;對方厚 → 退檔潛伏 | `adaptive_passive` |
| 結構錨定(price 模式) | 單掛在 TPO VAL/POC、OB/FVG、VWAP、日低近旁,掃蕩時接貨 | `structure_rest` |
| **跨日結構層(v3)** | 啟動載入近 30 日日線 OHLC(cache.duckdb):昨日低/高/收、5 根分形 swing、日線 FVG、20 日極值 → 錨候選;fail-open | `daily_context` |
| **昨日價值區 prior(v3)** | 讀 1 分 K 自建歷史算昨日 VAL/POC/VAH,開盤初期今日 TPO 樣本不足時的錨 | `prior_value_area` |
| **1 分 K 自建歷史(v3)** | 每次收盤自動 dump 當日 1 分 K 到 `out/trading/candles/`(台股無免費歷史 1 分 K 端點,2026-07-09 起自我累積) | `candles_dumped` |
| 掃蕩回收快速通道 | SMC sweep 確認即刻取價 | `micro_sweep_fastpath` |
| 竭盡/回穩加速 | 逆勢主動流竭盡(滾動 90s 窗)+OFI+價值區 ≥3 類(`--trigger-strict` = 全 AND)→ 提前跨價;止穩窗自適應(OFI 強攻 60s→30s) | `micro_accelerate` |
| **啟動暖機** | REST 重放今日逐筆 ≤500 筆:VPIN 量桶與 session 極值在第一輪就有意義 | `micro_warmup` |
| VPIN 毒性減速 | VPIN ≥ 0.75 → 多等一輪(死線照樣升級) | `micro_hold` |
| 波動自適應護欄 | `--cap-auto` | `cap_auto` |
| 開盤迴避 / TWAP 切片 / 節奏抖動 | 見 §2 | — |

---

## 8. TCA 日誌與事件字典

每筆執行寫 `research/out/trading/executions/<時間>_<side>_<code>.jsonl`。
關鍵事件:`start`(arrival/collar/參數)、`place`/`cancel`/`modify_price`、
`fill`(價量/累計/剩餘)、`round`(每輪 bid/ask/掛價)、§6 與 §7 的所有標籤、
`session_end_unfilled`、`halt_detected`、`summary`(**shortfall_bps** = 對 arrival
滑價,買正=貴、賣正=便宜賣;aborted)。累積數據後用它校準參數——不拍腦袋。

---

## 9. 輔助工具與 loop 整合

```bash
# 一次性掛限價單(掛了就離開,不盯盤、不自動撤單;撤單用 cancel_all)
# 集合競價語義:賣單限價=願賣下限(成交在撮合價)→ 掛低≈保成交;掛高=等好價
uv run --project research python -m research.trading.execution.place_limit \
    --code 2408 --side Sell --qty 1 --price 448 [--live]

# 收盤後補存當日 1 分 K(預設含富邦庫存;--codes 追加候選)。
# 富邦只給當日 → 漏日補不回;建議 launchd 每日 14:35 自動跑
#(launchd/com.quantlib.archive-candles.plist,安裝指令見檔內註解)。
# 缺檔退化:昨日 VA 自動改用最近有檔日,日線結構與盤中 TPO 不受影響
uv run --project research python -m research.trading.execution.archive_candles [--codes 6488,3026]

# 列出/撤銷帳上未完成委託
uv run --project research python -m research.trading.execution.cancel_all [--code X] [--live]

# 每日 loop 一體派工(產完 plan 自動派執行器;賣腿依出場理由分流 urgency)
uv run --project research python -m research.serenity.daily run --execute        # dry-run
uv run --project research python -m research.serenity.daily run --execute-live  # 真實(三閘仍在)
```

---

## 10. 網路韌性(2026-07-15 事故後重建)

**契約:暫時性網路問題絕不終止程式**——沒網路就等,網路回來自動重登、對帳、
續管。憑證/權限/程式 bug 仍快速失敗(重試無用,得讓人看見)。

| 機制 | 作法 |
|---|---|
| 錯誤分類 | **預設暫時性**,只有明確的「重試無用」才快速失敗(程式 bug 型別、憑證/權限字樣)。反向判定的理由:網路錯誤字樣空間開放——實測 Wi-Fi 掉線拋 `ValueError: URL error: Unable to connect`、DNS 失敗拋 `IO error: failed to lookup address information`,白名單必漏 |
| 登入 | `FubonSDK()` 建構子本身就連線(事故點)→ 整段指數退避重試至網路恢復;**多腿併發序列化**(5 秒去抖),避免互相抽換 sdk |
| 唯讀查詢 | 委託/庫存/餘額:無限退避重試(冪等,重試安全) |
| **送單** | **絕不自動重送**——「送出後才斷線」時委託可能已到交易所,自動重送 = 重複下單。改由執行器:恢復 → 對帳今日成交 → 認領在途委託 → 才決定是否重掛 |
| 進度對帳 | **絕對重算**:進度 = 今日累計成交 − 基準(委託回報為權威源)。取代增量記帳——後者在「送單當下斷線」時會永遠漏帳並重複下單 |
| 行情連線 | SDK 的 `ws.connect()` 沒網路時**永不返回也不拋例外**(`while True` 忙等燒滿一核)→ 自實作有界連線(20s),逾時用 SDK 自己的逾時出口叫醒忙等迴圈 |
| 行情自癒 | SDK **沒有自動重連**(`run_forever` 未帶 reconnect)→ 看門狗:斷線事件或靜默 >180s → REST 兜底 + 重建整條行情鏈(init_realtime → 重註冊回呼 → 重訂閱);重登換 sdk 也會即刻觸發重建 |
| 報價新鮮度 | ws 靜默死亡後舊報價會永遠留在記憶體 → 逾 45 秒的報價視為過期,**寧可停手不改價**(在途限價單保留),絕不拿舊價下單 |
| 事件標籤 | `net_degraded`(斷網續管)、`quote_stale`(報價過期停手)、`waiting_for_quote`、`resync_working_order`(恢復後認領在途單)、`progress_corrected`(對帳修正) |

測試:`uv run --project research python -m pytest research/tests/test_execution_resilience.py`
(26 項,含「送單當下斷線 → 委託已成交 → 恢復後只算一次且不重掛」的故障注入)

---

## 11. 故障排除

| 症狀 | 處置 |
|---|---|
| `Not Login Error` | 已根治(2026-07-13 事故):broker 層全呼叫自動重登重試 + 長等待後開盤瞬間刷新 session |
| **沒網路 / 斷線** | 已根治(2026-07-15 事故:開盤瞬間斷網 → 整支 traceback 退出、錯過整天)。現在會印 `[net] …網路異常…程式保持執行`,網路回來自動重登續管;掛單保留、恢復後先對帳再動作。見 §10 |
| 印 `quote_stale` | 行情停擺(ws 死亡/斷網)→ 設計行為:停止改價、保留在途單,等看門狗重建行情鏈;不拿舊價下單 |
| 成交後才隔一會兒停止 | 已修(2026-07-14):成交即時推播(`set_on_filled`)瞬間喚醒,輪詢只是備援節拍 |
| 想全部停 | 第一優先 `touch research/state/trading/HALT`;或各終端 Ctrl+C |
| 強殺後怕有殘單 | `cancel_all` 列出 → `--live` 撤;重跑主程式會自動接管/續傳 |
| 「另一個執行器正在跑(PID)」 | 真的有程序活著;要換手先終止它 |
| price/exit 收盤未成交 | 先看盤後收尾(收盤價在護欄內會自動掛,14:30 撮合);盤後未中籤或破欄未掛 = 設計內結果,明日規則重評 |
| 交割金 | 現股 T+2;零股當日買進**不可當日賣**(零股無現股當沖),T+1 起可賣 |

## Legacy

原型 `smart_execution.py`(SMC/TPO/OFI/VPIN 概念來源)保存於 git 歷史
`git show 9d8b64c:research/trading/legacy/smart_execution_v0.py`;
其致命 bug(竭盡量無衰減、無死線)與修正紀錄見該檔頭註記。
