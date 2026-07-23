# D-perf-validation — 策略驗證統計稽核（walk-forward / MC / DSR / PBO / bootstrap）

範圍：`src/quantlib/apex/validate.py` + `src/quantlib/serenity/validate.py` +
`src/quantlib/strat_lab/validator.py`（含其共用核心 `src/quantlib/strat_lab/validate_hybrid.py`
與 `src/quantlib/strat_lab/evaluation.py`）。

## 一句話結論（白話）

**同一套驗證統計有兩份實作，品質天差地別。** apex 那份（`apex/validate.py`）的
Deflated Sharpe、PBO/CSCV、block bootstrap **完全符合學理，可信**。但**現役策略
Serenity 和 strat_lab 用的另一份（`validate_hybrid.py`）壞掉兩處關鍵**：

1. **PBO 根本沒在算 PBO** — 它的「過度配適機率」是一個約等於 0.5 的擲硬幣噪音，
   跟策略有沒有過度配適完全無關。Serenity 出廠報告的「PBO 0.526」是這個壞掉的
   函式吐出來的，那個「< 0.5 才能上線」的關卡等於沒把關。
2. **Deflated Sharpe 灌水** — 多重測試的懲罰用錯了變異數，把「跨多個嘗試的
   Sharpe 離散度」偷換成「單一策略自己的估計誤差」，導致懲罰過輕、DSR 系統性
   偏高。Serenity 報的「DSR 1.00」不能當成真的過了多重測試校正。

另有三個較輕的偏差：Sharpe 用幾何 CAGR 而非 mean/std（與同檔的 Lo 檢定自相矛盾）、
Sortino 的下行標準差不是教科書定義、置換檢定 p 值少了 +1 修正（會報出不可能的 p=0.000）。

**能不能信？** apex 的數字可信；Serenity / strat_lab 報表上的 **PBO 一律不可信、
DSR 偏樂觀**，其餘指標可用但 Sharpe/Sortino 有系統性偏移。

---

## BUG 1 — `validate_hybrid.pbo_cscv` 不是 PBO/CSCV，是 ≈0.5 的噪音

- 檔案：`src/quantlib/strat_lab/validate_hybrid.py:146-162`
- 消費者（money-path）：`src/quantlib/serenity/validate.py:135`、
  `src/quantlib/strat_lab/validator.py:95`、`validate_hybrid.validate_hybrid():244`、
  以及 `evaluation.robust_growth_score`（把 pbo 當排序因子，`evaluation.py:184`）。

**學理定義（Bailey, Borwein, López de Prado & Zhu 2015, "The Probability of
Backtest Overfitting", CSCV）**：把 T×**N（N 個策略設定）**的績效矩陣切成 S 個
等長區塊，窮舉 C(S, S/2) 種 IS/OOS 對半組合；每組在 **IS 上挑出最佳設定 n\***，
再看 n\* 在 **OOS 上於 N 個設定間的相對排名**；PBO = P(IS 最佳者 OOS 落到中位數
以下) = 各組 logit λ_c ≤ 0 的比率。**核心是「跨設定選擇」**——沒有多個設定就沒有 PBO。

**程式實作**：輸入 `folds`＝**單一策略**逐年（或逐季）的 metrics dict 清單，
每個 fold 只有一個 `sortino`。迴圈裡把 folds 隨機對半，算兩半的平均 sortino，
判 `oos_sortino < median(sortinos)`（:160）。**沒有 N 個設定、沒有 IS 選擇**；
且 `is_sortino = sortinos[is_idx].mean()`（:158）算出來後**從未被使用**——判斷式
完全不看 IS。這等於在問「一組年份的平均 sortino 是否低於全體中位數」，本質是
對稱的 ≈50/50 事件，只受 sortino 分佈偏度影響，與過度配適無關。

**可重現證據**：對 200 組隨機生成的「單一策略 16 年 sortino」跑此函式，PBO
平均 **0.525**、範圍 **0.062–0.98**（純粹隨資料抖動，非過度配適訊號）；`is_sortino`
於 :158 計算後即丟棄。Serenity 出廠文件記載「PBO lag0 0.526（fold 稀疏 caveat 未解）」
——那個 0.526 正是此退化估計量的產物，"fold 稀疏" 的註解誤把病因歸給樣本少，
真因是這函式根本沒計算 PBO。

**修法**：改用 `apex/validate.py:66` 的 `pbo_cscv(returns: T×K, s=16)`——它是正確
CSCV：輸入多設定日報酬矩陣、窮舉 C(16,8) 組、IS argmax、OOS 相對排名、
P(rank<中位)。Serenity / strat_lab 需先蒐集「同一campaign 各候選設定的日報酬曲線」
堆成矩陣再餵入；單一策略的逐年 folds 無法算 PBO，應停止把它當 PBO 報告與當排序因子。

---

## BUG 2 — `validate_hybrid.deflated_sharpe` 的多重測試變異數用錯量（DSR 系統性偏高）

- 檔案：`src/quantlib/strat_lab/validate_hybrid.py:88-105`（`sigma_sr` :101、
  `e_max*sigma_sr` 當 SR0 :104）
- 消費者：`src/quantlib/serenity/validate.py:140`、`src/quantlib/strat_lab/validator.py:90-94`。

**學理定義（Bailey & López de Prado 2014, "The Deflated Sharpe Ratio"）**：
DSR = PSR(SR₀) = Z[ (ŜR − SR₀)·√(n−1) / √(1 − γ₃·ŜR + (γ₄−1)/4·ŜR²) ]，其中
**SR₀ = √(V[{ŜRₙ}]) · [ (1−γ)·Z⁻¹(1−1/N) + γ·Z⁻¹(1−1/(N·e)) ]**，
**V[{ŜRₙ}] 是「N 個嘗試各自 Sharpe 的橫截面變異數」**（真實跨設定離散度）。

**程式實作**：`sigma_sr = √((1 − γ₃·SR + (γ₄−1)/4·SR²)/(n−1))` 是**單一策略 Sharpe
估計量的標準誤**；接著 `dsr = Φ((sr_daily − e_max·sigma_sr)/sigma_sr)`，等價於
把 SR₀ 設為 `e_max·sigma_sr`，也就是**把 √(V[{ŜRₙ}]) 偷換成 sigma_sr**。這隱含
「N 個嘗試彼此只差抽樣雜訊」的假設——當你實際掃 40（Serenity）或 66（strat_lab）
個真正不同的設定時，它們 Sharpe 的離散度遠大於單一策略的估計誤差，故懲罰過輕、
DSR 系統性偏高（反保守）。此外 docstring 自稱 "López de Prado DSR"，名實不符。

**可重現證據**：同一組日報酬下 `DSR_hybrid(n=40)=0.2866`，與「apex DSR 但令
V=sigma_sr²」的 0.2866 **逐位相同**（證明 hybrid 的隱含 V[SR]=單策略估計變異數）；
把 V 換成較貼近真實的 (3·sigma_sr)²，正確 DSR 掉到 **0.0000**。方向明確：
掃越多元設定、真實 V 越大，正確 DSR 越低，hybrid 越高估。Serenity 出廠報告的
「DSR 1.00」即建立在此反保守輸入上，不能當作已通過多重測試校正。

**次生偏差（同函式）**：`sr_annual` 由 `metrics/nav_metrics` 的
`(cagr − RF)/vol` 傳入（見 SUSPECT 1），是幾何 CAGR 基礎、非 mean/std，
故餵進 PSR 的 ŜR 也偏離學理。

**修法**：改用 `apex/validate.py:44` 的 `deflated_sharpe(nav, n_trials,
sr_var_across_trials)`，並以 `apex/validate.py:95 sr_variance_from_curves(curves)`
（各候選曲線日 Sharpe 的樣本變異數）供給真實 V[{ŜRₙ}]。ŜR 亦應改用 mean/std 日
Sharpe，而非 CAGR 基礎。

---

## BUG 3 — `serenity/validate.py` bootstrap 結果被 key 名不符靜默丟成 NaN

- 檔案：`src/quantlib/serenity/validate.py:136-139`（取 boot）、`:153-154`（讀 key）；
  來源 `validate_hybrid.bootstrap_ci` 回傳 key 在 `validate_hybrid.py:139-142`。

**問題**：`years >= 4` 時走 `bootstrap_ci(rets, dates)`，其回傳 key 為
`cagr_lb / cagr_ub / sortino_lb / sortino_ub`；但 `validate_series` 讀
`boot.get("cagr_lb95", nan)` 與 `boot.get("sortino_lb95", nan)`（:153-154）
——**key 名不存在，一律回 NaN**。等於算完 1000 次 year-block bootstrap 後把
下界結果整個丟掉。只有 `years < 4` 走 `month_block_bootstrap`（key 正確為
`cagr_lb95`）才有值。

**可重現證據**：`bootstrap_ci` 原始碼回傳 `cagr_lb`（`validate_hybrid.py:139`），
serenity 讀 `cagr_lb95` → NaN。實測 `var/out/strat_lab/` 下 130 支
`serenity_event_engine_v1_*_daily.csv` 有 **30 支跨度 ≥4 年**（如 `mech_2018`
系列 2018→2026），這些在 validate.py 產表時 `boot_cagr_lb95 / boot_sortino_lb95`
欄位全是 NaN，等於「bootstrap 95% 下界 > 10% CAGR」這道關卡對長窗序列**靜默失效**。
（註：現役 registry `ev_v3_wf`、`ev_full_tp60` 窗為 2025-01→2026-07 ≈1.5 年 <4，
走 month_block_bootstrap，key 正確、不受影響——故 live 頭條數字倖免，但 mech_2018
長史穩健度列受害。strat_lab `validator.py:105` 直接讀 `boot["cagr_lb"]`，key 正確，
不受此 bug 影響。）

**修法**：`serenity/validate.py:153-154` 改讀 `bootstrap_ci` 的實際 key，或統一
兩條 bootstrap 分支的回傳 key（長窗補回 `cagr_lb95 / sortino_lb95`，語意用單邊
95% 下界＝5th percentile，而 `bootstrap_ci` 現用 2.5th percentile＝雙邊 95%，須一併對齊）。

---

## SUSPECT 1 — Sharpe 用幾何 CAGR 而非 mean/std（並與同檔 Lo 檢定自相矛盾）

- 檔案：`src/quantlib/strat_lab/validate_hybrid.py:58`、`src/quantlib/strat_lab/evaluation.py:135`、
  `src/quantlib/strat_lab/validate_full_v6.py:62`（同一份模板複製三處＝缺陷類）。

**學理定義**：Sharpe = E[Rₚ − R_f]/σₚ；年化 = (mean/std)·√252（算術平均）。
**程式實作**：`sharpe = (cagr − RF)/vol`，分子用幾何 CAGR、分母用年化 σ。幾何 <
算術（波動拖累 ≈ ½σ²），故系統性低估 Sharpe。

**可重現證據**：同一組日報酬 code(cagr 基礎)=**0.4152** vs 教科書(mean/std)=**0.5771**
（年化，差 28%）；換算日 Sharpe 0.02615 vs 0.03635。同檔 `lo_2002_sharpe_test`
於 `:76` 用**正確**的 `sr_daily = mean/std`——同一支報酬、兩個 Sharpe 定義，
內部自相矛盾；且此 CAGR 基礎 Sharpe 又被 BUG 2 的 DSR 當輸入。

**修法**：Sharpe 分子改用算術平均日報酬年化（`mean(r)·252` 或 `mean/std·√252`），
與 Lo 檢定一致；如需用超額報酬，rf 以日頻扣除。台股語境 rf≈0–1% 差異小，主偏差
來自幾何/算術，非 rf。（MDD、CAGR 本身定義正確，不動。）

## SUSPECT 2 — Sortino 下行標準差非教科書定義

- 檔案：`src/quantlib/strat_lab/validate_hybrid.py:57,59`、`src/quantlib/strat_lab/evaluation.py:120,134`、
  `src/quantlib/strat_lab/validate_full_v6.py:58-59`（同模板複製＝缺陷類）。

**學理定義（Sortino & Price 1994）**：目標下行離差 TDD =
√( (1/N)·Σ min(Rᵢ − MAR, 0)² )，對 **MAR（通常 0 或 rf）**取平方、分母是**全期數 N**。
**程式實作**：`downside = rets[rets<0]; downvol = downside.std(ddof=1)·√252`——
只取負報酬、以其**自身均值**為中心（非 MAR=0）、分母是**負報酬個數−1**（非 N）。
兩處都偏離標準 TDD。

**可重現證據**：同一組日報酬 code downvol=**0.1885** vs 教科書 TDD(MAR=0,全 N)=**0.2182**
（比值 0.864），使 Sortino 高估約 16%。此 sortino 又流入 bootstrap sortino 下界、
退化 PBO 的 folds、以及 `robust_growth_score`。

**修法**：`downvol = √(mean(minimum(r − MAR, 0)²))·√252`，MAR 取 0 或 rf/252，分母全期數。

## SUSPECT 3 — 置換檢定 p 值缺 +1 有限樣本修正（會報出 p=0.000）

- 檔案：`src/quantlib/serenity/validate.py:266`。

**學理定義（Phipson & Smyth 2010, "Permutation P-values Should Never Be Zero"）**：
有限次置換的無偏 p = (1 + #{置換統計 ≥ 觀測})/(1 + n_perm)。
**程式實作**：`p = (arr >= actual_cagr).mean()`＝#{≥}/n_perm，未含 +1，可得 p=0。
n_perm=200 時真實 p 最小應為 1/201≈0.005，卻會報 0.000（Serenity 文件即記
「置換 p=0.000」）。

**可重現證據**：公式缺 (b+1)/(m+1) 結構；n_perm=200 下任何「無置換 ≥ 觀測」即輸出
0.000，低估 p 上限 ~0.005。屬機率不可能值。

**修法**：`p = (1 + (arr >= actual_cagr).sum())/(1 + n_perm)`。（置換設計本身——
保留 gate 與 40 檔上限、只打散排名——是正確且良好的技巧，僅 p 值公式需修。）

---

## 判定為 OK 的項目（含刻意近似）

- **`apex/validate.py:44 deflated_sharpe`**：ŜR=mean/std（正確、非 CAGR 基礎）、
  γ₃/γ₄ 母體動差、denom=√(1−γ₃SR+(γ₄−1)/4·SR²)、z=(SR−SR₀)√(n−1)/denom、
  SR₀ 用**真實跨嘗試 V[SR]**。**完全符合 Bailey & López de Prado 2014**。
- **`apex/validate.py:66 pbo_cscv`**：輸入 T×K 矩陣、C(16,8) 窮舉、IS argmax、
  OOS 相對排名、P(rank<中位)＝**正確 CSCV**（呼叫點 `apex/experiments/p01_battery_revcycle.py:61`
  確以多曲線 `mat` 餵入）。
- **`apex/validate.py:23 block_bootstrap_cagr`**：circular moving-block、
  block=21（≈1 交易月）、CAGR=growth^(252/t)−1、2.5/97.5 percentile CI。正確。
  block=21 為固定慣例值（非由自相關導出，如 Politis-White 自動選長），屬可接受的
  刻意近似，非 bug。
- **`validate_hybrid.py:67 lo_2002_sharpe_test`**：var_sr=(1+½SR²−γ₃SR+(γ₄−3)/4·SR²)/n
  ＝Mertens(2002)/Lo(2002) 非常態 IID 變異數，單邊 p=1−Φ(t)。正確。
- **年化 252 交易日、rf≈0.01**：符合台股語境的刻意設定，OK。
- **`evaluation.py:56 k_ratio`（log NAV 斜率 t 值）、`drawdown_series`、mdd、
  `metrics.cagr`**：定義正確。
- **標籤語意提醒（非算式 bug）**：`strat_lab/validator.py` 的 "OOS"＝把報酬
  視窗切到 2010–2025，`walk_forward_folds` 只按年切 fold、**無 train/test 切分與
  重新擬合**；docstring 宣稱的 "5y train/1y test walk-forward" 未在 `validate_daily_nav`
  真正實作。被驗證的 NAV 若在與驗證同一資料上被挑選，DSR/PBO 需靠正確的 n_trials
  與（修好的）PBO 才能承擔選擇偏誤——目前 PBO 壞、DSR 反保守，選擇偏誤實質未被把關。
