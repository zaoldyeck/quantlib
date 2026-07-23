# D-evergreen-live:Evergreen 計分引擎(LIVE)學理稽核

**範圍**:`src/quantlib/evergreen/engine.py` 的 `score_expr` / `scores` / `replay_nav`
(含其委派的 `refit` / `walkforward` / DRIP 還原路徑:`src/quantlib/apex/engine.py::simulate`、
`src/quantlib/apex/data.py::load_panel`、`src/quantlib/prices.py::fetch_adjusted_panel`、
`src/quantlib/evergreen/ev36_walkforward.py::{seg_kpi,kpis_full}`、
`src/quantlib/apex/validate.py::block_bootstrap_cagr`、
`src/quantlib/evergreen/ev30_baseline.py::midmonth_membership`)

**焦點**:計分因子定義、NAV 重放的 DRIP 與還原、refit 的樣本內外切割、walk-forward 有無洩漏

**總判定:SUSPECT**(錢路全對、可信;三處提醒皆不影響現役選型與 NAV)

## 一句話白話

現役 Evergreen 引擎的「錢路」四項全部學理正確、可以信:

- **除權息還原(DRIP)對**:報酬曲線用的是股利/減資/分割都折進價格的「總報酬還原價」,
  持有固定股數乘還原收盤 = 含息總報酬,不會像讀原始收盤那樣在除息日少算股利、出現幽靈崩跌。
- **沒有偷看未來**:每一段走查用的參數只由「該段之前」的資料選出(滾動擴張窗);因子都用
  當下已知的資料算,營收還嚴格延到次月 10 號才可見;Donchian 突破用「前 60 日」不含今日。
- **樣本內外切乾淨**:網格搜尋 + 挑選(先 Martin 再 p5)全在訓練窗做,樣本外另段驗證。

**要提醒的三件事,全都不進現役 live config、不動 NAV**:

1. 走查統計函式 `kpis_full` 裡的 **Sortino 下行風險公式寫錯**(用「下跌日的標準差」而非學理的
   「全期對 0 的下行差」,會把 Sortino 高估約一成)——但 `refit` 只讀它的 `p5`,這個 Sortino
   根本沒被用到,live config 不受污染。這是 `D-metrics-scala` 已編錄的 Sortino class-bug 的
   **第 5 個實例**(其清單漏了 `ev36_walkforward.py`)。
2. 那個 `p5` 名字叫「5% 下界」,**實際是 bootstrap 的第 2.5 百分位**——正好等於雙尾 95%
   信賴區間下界,對上 CLAUDE.md 的驗收口徑,**算對了,只是綽號取得鬆**。
3. 走查「誠實線」的**彙總 CAGR 把第一年「樣本內」那段也算進去**,會讓那條線的年化看起來比
   真正的樣本外好一點——但這段有 `in_sample` 旗標、儀表板畫成虛線標明,只有 `_wf_report`
   這支診斷列印沒扣掉。

另有兩處是**刻意且全庫一致的代理選擇**,不是錯:52 週高點用收盤(而非盤中最高)、
`adv20` 用中位數(而非平均)。

## 可重現證據

- **polars 語義實測**(本次跑):`rolling_max(3)` 含當列且前 2 列 null;
  `shift(1).rolling_max(2)` = 前 2 日最大、**不含今日**(證 `don60` 剔今日正確);
  `rank([3,1,2,2])→[4,1,2.5,2.5]`、`rank(null)→null`(暖機列 score 為 null → `drop_nulls`
  濾除,不會誤入池)。
- **DRIP 不變式**(prices.py:38-41 + `src/quantlib/tests/test_prices.py` 十項 parity):
  `adj[T]/adj[t]−1 == cumprod((close+div)/prev_close)`;open/close/high/low 同一 `adj_factor`
  後復權、`trade_value`/`volume` 保留原始。→ `simulate` 的 `next_open` 成交價與 `close`
  mark 在還原空間一致,除息日原始收盤跳水被因子抵銷。
- **選型/走查結構**(engine.py:236-240):訓練終點 `2023-07-10 / 2024-07-10 / 2025-07-10`
  恒 < 對應 OOS 起點,擴張窗;forward 段 live_config 訓練窗 `2023-07~2026-07` 早於 forward
  起點 `2026-07-10`。`_selfcheck` + `tests/test_engine_parity.py` 鎖死引擎逐位重現
  `live_config` 記錄的 train KPI(cagr 3.164 / martin 37.33 / p5 1.505),漂移即紅燈。
- **月營收 PIT**:`avail = pl.date(year+month//12, month%12+1, 10)` 代入 `month=12`→次年 1/10、
  `month=1`→當年 2/10,皆為次月 10 號(台股法定公告上限日),`join_asof` backward 取 ≤ 決策日
  最新一筆 → 無 look-ahead。
- **Sortino 偏差量級**:`kpis_full` 的 `dn.std(ddof=1)`(dn=下跌報酬)與 `D-metrics-scala`
  的 `replay/tri/live` 變體同型,在該單位 seed=42 序列實測 Sortino `0.5954` vs 學理 `0.5239`
  = **高估 +13.7%**;但 `engine.py:208` 只讀 `['p5']`,Sortino 被丟棄。
- **p5 = 2.5 百分位**:`validate.py:37` 明碼 `np.percentile(cagr, 2.5)`。

## 逐項

### OK 1 — DRIP / 除權息還原(核心錢路)
`replay_nav` 的 NAV 全由 `simulate` 在 `d.panel` 上走,而 `d.panel = data.load_panel =
prices.fetch_adjusted_panel`(標準 Yahoo 式後復權:現金股利、減資、分割因子連乘)。持有股數
固定 × 還原收盤 = 含息總報酬,學理正確。2026-07-23 FC1 修正後改用交易所「除權息前收盤/參考價」
一次涵蓋配息+配股,對純配息與舊 cash 法僅差 4e-5(20,273 筆)。**這是 CLAUDE.md 明列
「所有 NAV 模擬必須走 prices.fetch_adjusted_panel」的正確落實。**

### OK 2 — walk-forward 無前視
每段 OOS 的 cfg 由 `_refit_cached` 只在其訓練窗選出,訓練終點 ≤ OOS 起點,擴張窗;forward 段
用實際上場 live_config(訓練窗早於 forward 起點)。因子 `h120/h52 = close/rolling_max(N)`
僅含 ≤t 的價,`rank .over('date')` 為單日橫截面,故 `EvergreenData` 一次性建全窗不漏未來。
fold1/fold2 的 OOS 雖落在 live_config 訓練窗內,但各自用 fold1/fold2 自己 refit 的參數、非
live_config,**非前視**。

### OK 3 — refit 樣本內外切割
官方網格(`GATES×SCORES×_GRID_NUM`)全以 `seg_kpi(replay_nav(...train...))` 計 Martin 於
訓練窗,取前 40 再以 `kpis_full(...)['p5']`(亦訓練窗)取最大 → 上場 cfg;OOS 另段驗證。
選擇資料與驗證資料分離,兩階段選型皆樣本內,無洩漏。

### OK 4 — 計分因子定義與 PIT
`h52=close/rolling_max(252)`(52 週≈252 交易日,George & Hwang 2004)、`h120=close/rolling_max(120)`、
`don60=close>close.shift(1).rolling_max(60)`(剔今日)、`_rank=(rank/len).over('date')`(單日
百分位)、`base=h52pct×h120pct`、`xadv_inv=base×(1−adv20pct)`、`rev_accel` 次月 10 號可見、
`inst5/f5` 法人 5 日淨買(收盤後已知)。皆為專屬複合分數,定義自洽、無前視,並被驗證版 ev36
`run_cfg` 逐位沿用。`midmonth_membership`(ev30_baseline.py:20-49)以「標記月次月 10 號後首交易日」
入池、trailing `pool_months` union、未來月標記無站位日自然略過 → PIT 正確。

### SUSPECT 1 — kpis_full 的 Sortino 下行差公式(不進決策,但技術債)
`dstd = dn.std(ddof=1)`(只取下跌日、對下跌均值離差、÷(n_neg−1))≠ 學理下行差
`sqrt(mean(min(0,r−MAR)²))`(全期 N、錨 MAR=0),Sortino 高估 ~+14%。**但 refit 只讀
`['p5']`、Sortino 被丟棄**(其餘 evergreen/apex 呼叫者亦僅讀 `['p5']`),不進 live_config、
不動 NAV。修法:改 `sqrt(mean(minimum(r−MAR,0)²))×√252`,或直接 `empyrical.sortino_ratio`。
順手補齊 `D-metrics-scala` 未列的此第 5 實例。

### SUSPECT 2 — p5「5% 下界」實為 2.5 百分位(名實不符,算對)
`block_bootstrap_cagr` 回傳 `ci_lo = percentile(cagr, 2.5)` = 雙尾 95% CI 下界,**對上
CLAUDE.md「Bootstrap 95% CI lower bound」口徑,計算正確**;僅綽號「p5/5%」字面暗示第 5
百分位。refit 以 max p5 選型,對所有 cfg 一致取同一百分位 → 單調一致的選擇尺,cfg 排序不受
名實不符影響。修法:對齊 95%CI 就改名 `p2.5/95%CI-lo`;要第 5 百分位就把 2.5 改 5.0。

### SUSPECT 3 — 走查誠實線彙總混入首年樣本內段
`walkforward` 在 k==0 額外把 fold0 cfg 套回自己訓練窗(2022-07~2023-07)當首段、標
`in_sample=True`,使線自 2022-07 起。`in_sample` 旗標存在、儀表板畫虛線標明;但
`_wf_report`(engine.py:354-356)以全線(含首年樣本內)算 CAGR/MDD,未扣除 → 對外年化偏高。
屬診斷列印、非資金/選型路徑。修法:`_wf_report` 彙總時 `filter(in_sample==False)` 或分列
「含首年/純 OOS」兩數。

### OK 5 — 分段從全現金重啟 + 段界正規化(已揭露、方向保守)
每段 `simulate` 以 `cash=capital、positions={}` 從全現金起跑,段界液化重建(trailing 峰值/
持有期重置),`r=nav/nav.first()` 段首日報酬歸零(~3 日/4 年)。方向保守(段界付再進場成本、
起點數日現金拖累 → 低估而非高估),非前視;docstring 已明揭 NAV 線為分段走查、另以連續 replay
產 trades 供交易行為視角。

### OK 6 — 52 週高點用收盤、adv20 用中位數(刻意一致代理)
`h52` 以收盤 `rolling_max(252)` 代替盤中最高(橫截面一致、業界常見簡化);`adv20` 以
`rolling_median(20)` 代替平均(對爆量穩健,與 `data.eligibility:122` 同式)。`adv20` 僅用於排序
與 gate 缺值填 `1e12`、非絕對門檻,中位數 vs 平均對排序影響極小。屬台股語境合理近似,非學理偏差。
