# D-metrics-scala:績效指標(CAGR / Sharpe / Sortino / MDD / Martin)學理稽核

**範圍**:`src/main/scala/strategy/Metrics.scala` + research 端 empyrical/pyfolio 對應使用
（`research/strat_lab/evaluation.py`、`research/serenity/replay_2025.py`〔=live〕、
`research/tri/pnl_dashboard.py`、`research/apex/validate.py`）

**總判定:BUG**

## 一句話白話

會算對、能信的:**CAGR、最大回撤、Calmar、Ulcer/Martin、以及整套專業驗證
（DSR / bootstrap / PBO）**；活的 Serenity 策略對外回報的 **Sharpe 也對**（和業界標準
套件 empyrical 逐位相符）。

有問題的:**Sortino**——全專案四個實作沒有一個對得上學理定義，同一條報酬序列它們給出
`0.21 / 0.34 / 0.60`，彼此差到 3 倍，學理值是 `0.52`；活策略回報的 Sortino 是**偏高**的。
另有一支 **Sharpe（研究/凍結路徑）拿「幾何 CAGR」當分子，把 Sharpe 低估約 4 成**。
年化天數:Scala 用 242（≈實際 243.6，對），Python 全用美股 252（偏高 1.7%、且兩套不可比）。

## 可重現證據

`scratchpad/verify_metrics.py`（seed=42、2500 日、48% 下跌日的合成報酬，可重跑；
以 empyrical 為學理參考，已驗證 `empyrical.downside_risk=0.1791=textbook 手算 0.1791`）:

| 指標 | empyrical（參考） | 專案實作 | 偏差 |
|---|---|---|---|
| CAGR | 0.0630 | 0.0630（Scala/Python 幾何） | **逐位相符** |
| MDD | −0.6328 | −0.6328 | **逐位相符** |
| Sharpe | 0.3668 | replay_2025/live **0.3668** | **逐位相符** |
| Sharpe | 0.3668 | nav_metrics/Scala 幾何 **0.2072** | **−43.5%** |
| Sortino | 0.5239 | replay/tri/**live 0.5954** | **+13.7%** |
| Sortino | 0.5239 | nav_metrics 0.3364 | **−35.8%** |
| Sortino | 0.5239 | Scala 0.2093 | **−60.0%** |

TWSE 實際交易日/年（cache 2015–2025）:`[244,244,246,247,242,245,243,246,239,242,242]`，
mean **243.6**、median 244 → Scala 的 242 ≈ 正確，Python 的 252 偏高。

## 逐項

### BUG 1 — Sortino 下行標準差:四個實作全偏，且互不一致

- **學理**（Sortino & Price 1994 / `empyrical.downside_risk`）:下行差 =
  `sqrt( (1/N_total)·Σ min(0, r−MAR)² )`——對**全部 N 期**取平均（不是只有下跌那幾天）、
  以 **MAR 為錨**（不是以下跌報酬自己的均值為錨），年化 ×√TDPY；分子的 MAR 要與分母一致。
- **實作偏差**:
  - `Metrics.scala:69-72`:`sqrt(Σ_{r<0} r² / n_downside)×√242`——分母用「下跌天數」而非
    全期 N，把下行差灌大 √(N/n_d)≈1.44×；分子又用幾何 (cagr−1%)。→ **−60.0%**。
  - `evaluation.py:120,134` nav_metrics:`np.std(neg, ddof=1)×√252`——對「下跌報酬自己的均值」
    取離差（非 MAR=0）、÷(n_d−1);分子幾何 (cagr−1%)。→ **−35.8%**。
  - `replay_2025.py:495-500`（**live**）、`tri/pnl_dashboard.py:199-205`、
    `valuation_replay_2025.py:432`:`√252·mean/std(neg)`（pandas ddof=1，同樣錯錨+錯分母）;
    分子算術、rf=0。→ **+13.7%**。
- **修法**:下行差改 `sqrt( mean( minimum(r−MAR,0)² ) )×√TDPY`，對全部 N 期平均、分子分母
  同一 MAR;最省事直接 `empyrical.sortino_ratio(returns, required_return=MAR)`。Scala 同構
  （`sum(neg²)/dailyRets.size`，而非 `/downside.size`）。

### BUG 2 — Sharpe 分子用幾何 CAGR（Scala + nav_metrics）

- **學理**（Sharpe 1994 / `empyrical.sharpe_ratio`）:超額報酬取**算術平均**，
  年化 = √TDPY·(mean_daily − rf_daily)/std_daily。
- **偏差**:`Metrics.scala:67`、`evaluation.py:135` 分子用幾何 CAGR。因
  `CAGR ≈ 算術年化 − ½σ²`，波動越大壓得越低。seed=42（vol 25.6%）低估 **−43.5%**。
- **血緣註記**:此二處是研究/凍結路徑（Scala strategy 已凍結;`nav_metrics.sharpe` 是展示
  KPI，**未**進 `robust_growth_score` 目標函式——目標用 calmar/upi，走幾何 CAGR 一致、無此問題）。
  仍屬學理偏差、會對高波動策略額外扣分而扭曲跨策略比較。**live 對外的 Sharpe（replay_2025）
  是算術版、正確**。
- **修法**:分子改算術年化超額 `(mean_daily·TDPY − rf)/vol`，或直接 empyrical。

### SUSPECT 3 — 年化天數 242(Scala) vs 252(Python 全線)

實際 TWSE ≈ 243.6/年。Scala 242 對;Python 252 是美股慣例，使年化 vol 與 Sharpe/Sortino
系統性高估 √(252/243.6)−1 = **+1.7%**，且兩套 stack 產出不可直接比較。
修法:統一用實測 ~243–244（或沿用已驗證的 242），常數集中單一來源並註明「cache 實測」。

### SUSPECT 4 — 無風險利率 rf=1%(Scala/nav_metrics) vs rf=0(live/replay/apex)

同一策略的 Sharpe 隨「哪支模組算」而不同（empyrical rf=0→0.3668、rf=1%→0.3277）。
CLAUDE.md 定調 rf≈0;且 10Y 公債非理想無風險代理（應取短率）。
修法:全專案統一一種 rf 慣例並註明來源。

### SUSPECT 5 — `k_ratio` 命名

`evaluation.py:56-81` 實作 = `slope/se_slope`（純斜率 t 值，無 n 正規化），docstring 誠實
標「slope t-stat」，但鍵名 `k_ratio` 易被當文獻 Kestner K-ratio（後者對期數正規化）引用。
屬內部啟發式、標籤誠實、target 由 harness 校準，可留;建議改名或加註。

### SUSPECT 6 — Turnover 以 initialCapital 為分母

`Metrics.scala:86-89` 年化換手 = 成交名目Σ/(years×initialCapital)。成交名目隨 NAV 成長累積、
分母卻固定期初資本 → 高成長策略後期換手被高估。超出本單位核心焦點且 Scala 已凍結;
修法:分母改逐期 NAV 均值，或沿用 CLAUDE.md 的 per-rebal turnover 口徑。

### OK 清單（逐位或逐行驗證通過）

- **Sharpe 算術版**（`replay_2025.py:520` = live、apex `sr`）:=empyrical 0.3668，逐位相符。
- **CAGR**:`(end/begin)^(1/years)−1`，years=日曆天/365.25（含閏年）;=empyrical 0.0630。
- **MDD**:`min(NAV/cummax−1)`;=empyrical −0.6328。
- **年化 σ**:樣本標準差 ddof=1（Bessel）×√TDPY，母體/樣本正確。
- **Calmar**:CAGR/|MDD|，三處一致。
- **Martin/UPI + Ulcer**（`evaluation.py:123,139`）:Ulcer=sqrt(mean(dd²))（Martin & McCann 正確）、
  UPI=(cagr−rf)/Ulcer;即標題所稱「Martin」的對應實作。
- **DSR / bootstrap CAGR / PBO**（`apex/validate.py`）:逐項對照 Bailey & López de Prado
  2014/2015——SR0、skew/kurt 校正標準誤、√(T−1)、bootstrap 年化 252/t、CSCV rank<0.5 全對，
  全程日頻 SR 單位一致。專業級正確。
