---
name: quantlib-backtest
description: Use this skill when the user wants to run a strategy backtest, compare variants, or parameter-sweep (e.g. "跑 apex v3", "test threshold 3% vs 5% vs 7%", "backtest my new strategy idea", "比較 A vs B"). Always uses Python — never Scala. New-strategy research goes through the apex harness (src/quantlib/apex/); legacy baselines (v4/iter 系列) reproduce via src/quantlib/strat_lab/. Interprets against ledger baselines, flags deviations, produces actionable next-step recommendation.
---

# Backtest workflow(Python-only)

**Python is canonical**. Never run Scala strategies — that package is frozen historical
reference.

**兩套 harness 的分工**:
- **新策略研究(預設)= `src/quantlib/apex/`**:事件驅動日頻引擎、PIT 對齊、雙市場、
  era-aware 漲跌停鎖死(E01 精準掛單偵測)、trial ledger + 反過擬合協議。
  憲章與判準:`src/quantlib/apex/CHARTER.md`;方法紀錄:`src/quantlib/apex/ledger/batches.md`。
- **舊 baseline 重現 = `src/quantlib/strat_lab/`**(v4/iter 系列):只為對照歷史數字,
  不再新增策略。

## Step 1: Parse request

Classify:
- **Single run**:specific strategy + window → apex:寫 `src/quantlib/apex/experiments/`
  腳本(拷貝 `p03_gauntlet_v3.py` 的 `run()` 模式);legacy → `src/quantlib/strat_lab/v4.py`
- **Parameter sweep**:apex 擾動迴圈(見 p03 的 perturb 段)或 vectorbt grid
- **Variant comparison**:2-5 策略 side-by-side → 各跑 + tabulate

## Step 2: Freshness check

`var/cache/cache.duckdb` mtime > 24h old → advise user to run `quantlib-data-refresh`
first(don't auto-refresh)。

## Step 3: Run(apex 標準路徑)

```bash
# 從 repo root 以模組模式跑(script 模式 sys.path 會斷)
cd /Users/zaoldyeck/Documents/scala/quantlib && \
  uv run --project . python -m quantlib.apex.experiments.<script>
```

最小骨架(完整 12 年雙市場回測 ~10-20 秒):

```python
from quantlib.apex import data, metrics
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

con = data.connect()
panel, feat, elig = build_features(con, "2012-01-02", "2023-12-29")
sc = blend_score(feat, elig, {"rev_yoy_accel": 1.0}, require=[...])   # (date, code, score)
entries, flags = entries_and_flags(sc, topn=20, exit_rank=80)
res = simulate(panel, entries, exit_flags=flags,
               exec_spec=ExecSpec(), port_spec=PortSpec(n_slots=20),
               exit_spec=ExitSpec(trailing_stop=0.25))
print(metrics.fmt_report("my-idea", res.nav, res.trades,
                         data.benchmark_nav(con, "2012-01-02", "2023-12-29")))
```

**鐵律**(apex 憲章):trial 一律 `ledger.log_trial(...)` 記帳 + 存 curve;batch 先在
`ledger/batches.md` 預註冊假設與判準再跑;dev 窗自由迭代,val(2024-01→2025-06)與
holdout(2025-07→)動用需依憲章紀律。

## Step 4: Interpretation

比較基準(全部 apex 憲法口徑,淨成本,dev 2012-2023):

| baseline | CAGR | Sharpe | MDD | 出處 |
|---|---|---|---|---|
| **apex_revcycle_v3(champion-elect)** | +33.2% | 1.65 | −26.6% | ledger T0100;val +26.2%/1.21、holdout +48.3%/1.51 |
| 0050 total return 同窗 | +12.2% | — | — | `data.benchmark_nav` |
| 隨機 cohort null(200 perm 中位)| +11.0% | — | — | p03 battery |

| ΔCAGR vs 相關 baseline | Interpretation |
|---|---|
| within ±1pp | 等同 — note as "on baseline" |
| 1-3pp | Material — investigate which component changed |
| >3pp | Either breakthrough or bug — triple-check(先懷疑 look-ahead / PIT 破洞)|

Sanity checks:
- Sharpe < 0.8 with CAGR > 20% → high-beta ride warning
- MDD worse than −35% → 憲章 dev gate 直接不過
- 新 idea 勝過 v3 → 必須走完整 gauntlet(±20% 擾動、val、fill 雙測、battery),
  參考 `src/quantlib/apex/experiments/p03_gauntlet_v3.py`

## Step 5: Output(Traditional Chinese)

- **Runtime**:X 秒
- **結果 table**(each strategy):CAGR / Sharpe / MDD / Calmar / turnover
- **vs baseline**:ΔCAGR / ΔSharpe(標明對照的是 v3 還是 legacy v4)
- **關鍵觀察**:3-5 句中立解讀
- **建議 next step**:探索變體 / 走 gauntlet / 丟棄
- **Commit 建議**:若值得 commit,草稿訊息

## Anti-patterns

- **Never run Scala**(`sbt "runMain Main strategy ..."`)
- 別繞過 `prices.fetch_adjusted_panel` 讀 raw close 跑 NAV(除息會低估;歷史教訓)
- 別對 (code, date) 排序的欄 `set_sorted("date")`(B01 事故:over/group_by 損壞;
  asof join 一律全域 key 排序)
- 別只報 in-sample 數字就聲稱 alpha(憲章:permutation/bootstrap/DSR/PBO + OOS)
- Don't silently run — show the exact command
- Don't paraphrase numbers — paste exact values from stdout
- Don't compare across different rebalance timings — flag timing difference first
- Don't chase bit-exactness — 10s iteration + 1pp approximation noise beats 10-min 高精度
- 若 variant Sharpe 升但 CAGR 掉 >3pp,明確標示 tradeoff
