# D-apex-s-live — S 策略六因子(LIVE money-path)算法學理稽核

- **範圍**:`research/apex/strategy_s.py` + `research/apex/assemble.py`
- **判定**:**SUSPECT**(定義全對、money-path 無失真性錯誤;僅一顆健壯性螺絲要補)
- **稽核重點**:rev_yoy_accel、high_52w、close_pos_20、mom_126_5、rev_seq、accel_rel 的定義是否學理一致;幾何排名 `rank/len` 的處理;PIT 對齊有無前視

## 白話結論(先講答案)

這條真金白銀的選股線可以信。六個因子逐一對照教科書/原始論文定義都對得上,PIT 對齊乾淨、沒偷看未來,幾何排名的做法穩健。其中四個因子我做了**逐格重算驗證**、零差異;兩個營收因子的定義忠實實作。

唯一要補的螺絲:三月序列動能 `rev_seq` 在「前三個月營收合計為 0」時會除以零變成 `+inf`,而 polars 的 `rank` 會把 `inf`(以及更糟的 `0/0 → NaN`)排到接近/絕對最頂端。這幾檔多半是建設股這類認列跳動的產業。因為總分靠 `rank/len` 收斂、不會爆掉,實際只汙染計分池 6 / 343,866 列(0.0017%),對選股結論沒有實質影響——所以**不是會讓結論失真的 BUG**,但學理上「未定義的成長率」應該 null 掉而非給最高名次,值得順手加個和 `close_pos_20` 同款的護欄。

## 逐項對照

### 1. rev_seq 三月營收序列動能 — SUSPECT

- **學理**:序列(環比)動能 = 近三月營收合計 / 前三月營收合計 − 1;分母為 0 時成長率**數學上未定義**,學理處理應排除(null)或設限,不得當成 +∞。
- **實作**(`strategy_s.py:36-39`):`rolling_sum(3) / rolling_sum(3).shift(3) − 1`,分母 = 0 → `+inf`,0/0 → `NaN`;兩者皆非 null,`drop_nulls` 不剔除,直接進 `(rank/len)` 排名。
- **證據**:重現 `run_s_full` 計分池共 343,866 列,`rev_seq` 有 **6 列 = +inf**、0 列 NaN(min −91.471、max inf)。polars `rank([1,2,inf,3,nan]) = [1,2,4,3,5]`——`inf` 得近頂、`NaN` 得**絕對最頂**(rank=n)。這幾檔皆前三月營收合計為 0(建設股認列跳動,CLAUDE.md 已載為真實現象)。`term=(rank/len)^w` 底仍有限 → 總分不炸、選股不崩;但這 6 列被指派為「最高序列動能」名次,屬學理上不該有的「未定義 → 最佳分」。影響 6/343,866(0.0017%),且需其餘 5 因子同時排名前段才會真進 top-5,對回測/live 無實質失真。
- **修法**:`pl.when(rolling_sum(3).shift(3) > 0).then(ratio − 1).otherwise(None)`(與 `assemble.py:78` 對 `high==low` 的 when/else None 同款);另在 `run_s_full` 計分前加 `df.filter(pl.all_horizontal(pl.col(c).is_finite() for c in WREL))` 作類守護,一次關掉六因子任何 inf/NaN 被排到頂端的陷阱。健壯性修補,現有結論無需重跑。

### 2. high_52w 52 週高距離 — OK

- **學理**:George & Hwang (2004, JF) `52-week high` = 現價 / 過去 52 週最高價,值域 (0,1];台股 252 交易日 ≈ 52 週。
- **實作**(`assemble.py:60`):`close / rolling_max(252)`,`close` 為 `prices.fetch_adjusted_panel` 還原(total-return-equivalent)收盤。
- **證據**:對 2330 逐格重算 2872 列 `max|diff|=0.0`;全市場值域 [0.0194, 1.0000] med 0.8275(嚴格 ≤1)。`rolling_max(252)` 預設 min_periods=252,暖機 null 不前視;含當日收盤為 as-of 標準,非 look-ahead。

### 3. mom_126_5 六月動能(跳 5 日) — OK

- **學理**:Jegadeesh & Titman (1993) 動能含短期反轉跳空 = P[t-5]/P[t-126] − 1(跳 5 日避開 Lehmann 1990 短期反轉)。
- **實作**(`assemble.py:61`):`close.shift(5) / close.shift(126) − 1`,還原價(除息跳空不扭曲動能)。
- **證據**:2330 逐格 2872 列 `max|diff|=0.0`;命名 `mom_126_5`=回看126/跳5 與實作一字不差;`shift(126)` 使暖機 null 不前視。

### 4. close_pos_20 盤中收盤位置 20 日均 — OK

- **學理**:當日收盤在日內區間相對位置(Williams %R 補數 / Stochastic %K 同類)=(close−low)/(high−low)∈[0,1],取 20 日均。
- **實作**(`assemble.py:78-83`):`when(high>low).then((close−low)/(high−low)).otherwise(None).rolling_mean(20, min_samples=10)`。
- **證據**:值域 [-0.0000, 1.0000] med 0.4458(-0.0000 為浮點雜訊)。比值對還原不變:`prices.py:476-482` OHLC 同乘一個 `adj_factor`、比值相消。此處 0/0 護欄正是 `rev_seq` 所缺之正確範式。

### 5. rev_yoy_accel 營收年增加速度 — OK

- **學理**:營收成長「加速度」= 成長率變化;此處採 MACD 式平滑代理 MA3(YoY)−MA12(YoY),文獻常見之 acceleration 操作化,語義一致(非字面二階差分,屬合理近似)。
- **實作**(`assemble.py:102-104`):`monthly_revenue_yoy.rolling_mean(3) − rolling_mean(12)`,over(code);YoY 實測為百分比(med 3.36、p99 723.91)。
- **證據**:定義忠實,min_periods=12 保守暖機。長尾 ±數百萬源自**基期效應真實資料**(例 2528 建設股 2024-12 YoY=25,266,600%,前年極小基數),非 bug;因後續轉 rank(單調變換不變)數值爆量不使計分失真;基期效應與 `rev_seq` 的 inf 同一根因(認列跳動產業),由 cfo_ni 閘 + fresh<=7 + rank 穩健性吸收。

### 6. accel_rel 減同業中位數 — OK

- **學理**:產業中性化因子 = rev_yoy_accel − median(同日同產業);中位數對極值穩健;產業別須 PIT。
- **實作**(`strategy_s.py:46-57`):`industry_taxonomy_pit` join_asof backward(effective_date ≤ date)取 PIT 產業;median over (date, industry);相減。
- **證據**:逐格重算 **5,067,841 列 `max|diff|=0.0`**;前視檢查 `effective_date > date` 列數 = **0**(產業別零前視)。用 median 對基期效應長尾穩健。

### 7. 幾何排名 rank/len — OK

- **學理**:異質因子先各自轉橫截面分位再加權;此處加權幾何平均 `∏ (rank_pct_i)^w_i`(conjunctive:懲罰任一軸弱);rank_pct 須避免恰為 0。
- **實作**(`strategy_s.py:73-77`):`term = ((rank / len).over('date')) ** w`;`∏ term`;權重 1.0/1.0/1.0/0.5/0.5/0.5。
- **證據**:polars rank 預設 average 升冪(`[10,30,20,20]→[1,4,2.5,2.5]`),因子值越大 → score 越大、單調、∈(0,1]。`rank/len` 使最小分位=1/n(**絕不為 0**),幾何積永不塌陷——正是精妙處。未除 Σw 對 top-5 選股序不變(全體同乘一正指數)。live 走此幾何積(STRATEGY.md §6),未誤用 `blend_score` 加法版本。

### 8. PIT 對齊與執行時序 — OK

- **學理**:因子只用 ≤T 資訊;月營收次月 10 日生效;T 決策 → T+1 成交。
- **實作**:`avail = date(year + month//12, month%12 + 1, 10)`;join_asof backward tol 70d;季報 5/15、8/14、11/14、次年3/31;`engine.py:230` `e_di+1` 成交。
- **證據**:avail 模運算正確(month=12 → 隔年1月10日,無 month=13 off-by-one)。回測用「次月10日」保守下界,`strategy_s.prep` 未傳 avail_override → 全程保守非前視。rolling 全預設 min_periods=window 不以部分窗前視。engine 進出場皆隔日成交,執行層零 look-ahead。月營收相鄰列 gap>1 僅 81/451,159(0.018%),row-based rolling 等同月連續。

## 重現指令

```bash
uv run --project research python -c "..."   # 見 _done/D-apex-s-live.json evidence 欄
# 關鍵:prep(con, end='2026-06-30') → 重建計分池 → 檢 rev_seq inf/nan;
#       2330 手算 high_52w/mom_126_5 vs build_features(2872 列 diff=0);
#       accel_rel 手算 vs feat(5,067,841 列 diff=0);taxonomy 前視列=0
```
