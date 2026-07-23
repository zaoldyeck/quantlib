# D-scala-python-parity — Scala 凍結引擎 vs Python 現役 因子/指標定義一致性

**scope**: Scala `strategy/Signals.scala` + `Metrics.scala` + F-Score SQL view
vs research 端 `apex/*`、`evergreen/engine.py` 對應實作
**focus**: 同名因子(動量、營收成長、F-Score、價值)兩套定義是否一致;不一致哪個對;
凍結 Scala 是否被任何 live 路徑引用(是則其 bug 是 live bug)

## 白話結論

**live(現役)算法可以信**。現役單一策略是 Serenity Python(launchd 每日跑
`research.serenity.daily`),參考儀表板叫 Python 的 evergreen/apex 引擎。我實測到的
Python 指標(Sharpe、Sortino、CAGR、MDD、IC、前瞻報酬、調整價)全部符合學理定義。

**Scala strategy 整包是凍結歷史碼,沒有任何 live/排程路徑引用它**——只有人工手打
`sbt "runMain Main strategy ..."` 才會跑到,而且其中 `momentum12m1m`、`relativeStrength`、
`rsi14`、`bollingerPosition`、`revenueAccel` 連一個 Scala 策略都沒接(純死碼)。因此
**Scala 端所有學理偏差都不是 live bug**,最多只會污染「未來若有人手動跑 Scala 回測當
驗證」的那次結果。

Scala 與 Python 是**兩組互不相交的策略家族**(Scala 舊 momentum/value/MFPiot vs
Python apex-S/evergreen/serenity),同名因子本來就不要求逐位一致;真正要查的是各自
是否符合學理。查完:**Python 側乾淨;Scala 凍結側有 1 個確定的算式錯誤(Sortino 用錯
分母)+ 幾個定義偏差**,全數 off-live。

---

## 逐項

### F1 [BUG,定義,off-live] Scala Sortino 下行標準差用錯分母
- **檔案**: `src/main/scala/strategy/Metrics.scala:69-72`
- **學理**: Sortino(Sortino & Price 1994)分母 = target downside deviation
  = √( Σ min(r−MAR,0)² / **N** ),N 為**全部**期數(非只有下跌日數)。
- **實作**: `downside = dailyRets.filter(_ < 0)`;`downVol = √(Σ_{r<0} r² / downside.size)`
  ——分母是**下跌日數 n_downside**,不是總期數 N。
- **證據(可重現)**: 樣本日報酬 `[.02,-.01,.03,-.02,0,.015,-.005,-.03,.01,0]`(N=10,
  下跌 4 天):textbook 下行標準差 0.011937,Scala 0.018875(膨脹 ×1.581)→ **Sortino
  被低估 36.8%**。Python 對照 `research/apex/metrics.py:23`
  `np.sqrt(np.mean(np.minimum(rets,0)**2))` 用 ÷N,**Python 正確**。
- **修法**: 分母改總期數:`sqrt(downside.map(r=>r*r).sum / dailyRets.size) * sqrt(242)`
  (下跌日平方和不變,除以全體 N)。

### F2 [SUSPECT,定義,off-live] Scala Sharpe 用幾何 CAGR 當分子(非算術平均超額)
- **檔案**: `src/main/scala/strategy/Metrics.scala:67`(rf `:12`,年化 242 `:15`)
- **學理**: Sharpe = (E[Rp]−Rf)/σp,分子為**算術平均**超額報酬(年化)。
- **實作**: `sharpe = (cagr − 0.01) / vol`——分子用**幾何 CAGR**、rf=1%。
- **證據**: Python `research/apex/metrics.py:32` `mean/std*√252`、rf=0 = 教科書形態。
  幾何 CAGR ≈ 算術平均 − σ²/2,故 Scala Sharpe 系統性略低於教科書值;為公認
  practitioner 變體,偏差溫和。台股 rf≈0 可接受(Scala 取 1% 屬合理)。年化係數
  Scala 242 vs Python 252(√242 vs √252 差 ~2%)——Scala 註解「TWSE ~242」屬**刻意
  且有據**,OK。
- **修法**: 若要教科書一致,分子改 `dailyRets 年化算術平均 − rf`;或明註為幾何 Sharpe 變體。

### F3 [SUSPECT,定義,off-live] Scala 價格/報酬因子用**原始收盤價**(未還原),Python 用還原價
- **檔案**: `Signals.scala` `momentum12m1m:239-268`、`relativeStrength:80-99`、
  `priceReturn:202-231`、`distFrom52wHigh`、`rsi14`、`bollingerPosition`、
  `lowVolatility60d`、`rsv120d` 全讀 `daily_quote.closing_price`(raw)。
- **學理**: 動量/報酬類因子應建在**總報酬(除權息還原)價**上;台股股利/減資大,原始價在
  除息日跳空會低估真實報酬(專案自訂鐵律 `research/prices.py` 亦明文)。
- **實作**: Scala 直接用 raw close;Python `research/apex/assemble.py:61-84` 的 `close`
  來自 `data.load_panel`→`prices.fetch_adjusted_panel`(`research/apex/data.py:62`)=
  **還原價**。J-T 12-1 動量本身定義正確(skip_px≈1 月前 / base_px≈12 月前,`(p1-p0)/p0`)。
- **證據**: 定義層——同一支高股息股跨除息日,raw-close 動量 < 還原價動量;**Python 正確、
  Scala 偏差**。實務衝擊近零:`momentum12m1m`/`relativeStrength`/`rsi14`/`bollinger` 無任何
  策略消費(死碼),被接的 `rsv120d`(MFPiot)、價量因子亦僅供凍結策略。
- **修法**: Scala 若復用,價格面板改接還原價來源;或標註「凍結:未還原,勿用於現役驗證」。

### F4 [SUSPECT,定義,off-live] Scala F-Score 五個「變化」訊號用 lag(1) 季比,非 Piotroski 的年比
- **檔案**: `src/main/resources/sql/view/5_growth_analysis_ttm.sql:3-37`
  (被 `MagicFormulaPiotStrategy.scala:69` 消費)
- **學理**: Piotroski(2000)9 分,其中 ΔROA、ΔCurrent ratio、ΔLeverage、ΔGross margin、
  ΔAsset turnover 五項比較**會計年度 t vs t−1**(年比)。
- **實作**: 9 個成分**全數到位且對應正確**(roa>0、ocf>0、ocf>profit=應計、非流動負債↓=
  槓桿↓、流動比↑、股本≤前期=未增發、roa↑、毛利率↑、資產週轉↑),但五個變化項用
  `lag(...) over (order by year, quarter)` = **lag(1)=前一季**;在季頻 TTM/資產負債表上,
  年比應為 **lag(4)**。lag(1) 對 TTM 流量項是重疊 9 個月的季環比,對資產負債表項(槓桿/
  流動比/股本)更是純季比,皆非 Piotroski 年比語義,會改變哪些訊號點亮。
- **證據**: 定義層對照 Piotroski 原文;無 Python 對照(Python 只用單一 `cfo_ni_ratio_ttm`
  當品質閘,無完整 F-Score)。off-live(僅餵凍結 MFPiot)。
- **修法**: 五個變化項改 `lag(4)`(同季去年);或明註為「季環比 F-Score 變體」。

### F5 [SUSPECT,定義,off-live] Scala Greenblatt ROIC/EY 用「稅前淨利」當 EBIT
- **檔案**: `Signals.scala:602-609`(greenblattROIC)、`642-654`(earningsYield)
- **學理**: Greenblatt Magic Formula 用 **EBIT**(營業利益,資本結構中性)算 ROIC=EBIT/投入
  資本、EY=EBIT/EV。
- **實作**: EBIT 取 `title IN ('繼續營業單位稅前淨利(淨損)'...)` = **稅前淨利**(已扣利息費用、
  含業外損益),非 EBIT。專案本身有 `營業利益`(`opIncomeGrowthYoY:748` 在用),才是正確 EBIT 代理。
- **證據**: 定義層;稅前淨利 = EBIT − 利息 ± 業外,對高槓桿/業外大的公司偏離明顯,違背
  Greenblatt「跨資本結構可比」初衷。無 Python 對照。off-live(僅凍結 MFPiot)。
- **修法**: EBIT 改用 `營業利益(損失)`;EV 的市值代理 `close×capital_stock/10`(台股面額 10、
  忽略庫藏)屬合理近似、有註解,OK。

### F6 [SUSPECT,命名/定義,off-live+死碼] Scala revenueAccel 名為「加速度」實為水準比
- **檔案**: `Signals.scala:782-813`
- **學理**: 營收「加速度」= 成長率的變化(二階差分)。
- **實作**: `latest_month_rev / avg(prior 3 months rev)` = **水準比**(近月營收暴衝),非成長率
  二階差分。對照:evergreen `rev_accel`(YoY_t > YoY_{t-1},`engine.py:114-115`)、apex
  `rev_yoy_accel`(avg3 YoY − avg12 YoY,`assemble.py:100-104`)才是合格加速度代理。
- **證據**: 定義層;`revenueAccel` 零消費者(死碼)+ off-live。三種「accel」各屬不同策略、
  非 parity 要求。
- **修法**: 更名為 `revenueSurgeRatio` 或改為成長率二階差分。

### F7 [OK] 營收 YoY parity:Scala 自算比率 vs Python TWSE 百分比欄,概念同、rank 中性
- **檔案**: Scala `revenueYoY3M:30-41` / `revenueYoYLatest:579-589` 自算
  `(rev − last_year_rev)/last_year_rev`(比率);Python 讀 `monthly_revenue_yoy`,由
  `research/cache_tables.py:52` 取 TWSE `"monthly_revenue_compared_last_year(%))"`(**百分比**)。
- **證據**: cache 實測 1101/2026-06 `monthly_revenue_yoy = 32.398782`(=+32.4%),Scala 同口徑
  會得 0.324;**100× 尺度差**。但兩側皆在截面做 `rank()/len()` 百分位轉換,**尺度對排名無影響**、
  無選股偏差。概念同 = YoY;OK,僅記尺度差。

### F8 [OK] IC / 前瞻報酬(apex/factors.py)= 教科書
- **檔案**: `research/apex/factors.py:25-101`
- **學理**: IC = 截面 Spearman(因子, 前瞻報酬);前瞻報酬 T+1 起算、零 look-ahead。
- **實作**: 每日 `rank(value)`、`rank(fwd)` 後 `pl.corr`(= Spearman)✓;`forward_returns`
  = `close.shift(-(1+k))/close.shift(-1)-1`(T+1 基期)✓;`t = IR·√n` ✓。`t_adj=t/√k`(重疊
  樣本粗校正)、pooled-decile × 252/k 線性年化 = **有註明的刻意近似**,OK。

### F9 [OK] CAGR / MDD / Calmar 兩側一致且符合學理
- CAGR `(end/begin)^(1/years)−1`(年=日曆/365.25):Scala `Metrics.scala:61`、Python
  `metrics.py:27` 同。MDD `min(NAV/cummax−1)`:Scala `:113-123`、Python `:24-26` 同。
  Calmar `CAGR/|MDD|` 兩側同。全部 OK。

### F10 [OK,結構/headline] 無任何 Scala 因子/指標在 live 路徑
- **證據**: live = `research/serenity/launchd/com.quantlib.serenity-daily.plist` → 
  `python -m research.serenity.daily run`;`research/serenity/engine.py` 不 import Scala、
  不 import apex/evergreen 因子。`grep "runMain Main strategy"` 全 repo 僅命中 `Main.scala`
  (人工 CLI 入口)與一個 agent 說明 toml,無 shell/plist/cron。Scala strategy 整包凍結
  (CLAUDE.md 明文),`momentum12m1m/relativeStrength/rsi14/bollingerPosition/revenueAccel`
  於 Scala 策略層零消費者。**結論:F1 的 BUG 與 F2–F6 的偏差全部 off-live,非 live bug。**
