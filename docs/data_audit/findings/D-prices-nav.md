# D-prices-nav — NAV / 報酬 / 成本會計 學理稽核

**Verdict: OK**(核心 NAV/DRIP/成本數學全部符合學理;範圍內無 BUG,只有數個保守方向的 minor SUSPECT + 一個文件寫反的傳播風險)

範圍:`src/quantlib/prices.py` 的 `daily_returns`/DRIP + Python 端每日 NAV walk(`v4.py`、
`apex/engine.py`、`serenity/engine.py`)+ 手續費/稅成本(`constants.py` 及各引擎)。
Scala `Backtester.scala` 已由 **D-backtester-scala** 稽核(該處抓到減資 BUG);
本單位聚焦 Python 正典路徑。

---

## 白話總結

這條路徑上「錢怎麼滾、股息怎麼還原、買賣要扣多少費」全部算對,可以信。

- **除權息還原(DRIP)**:用的是 Yahoo/CRSP 標準的「還原收盤價」乘法法,和教科書
  加法式 `(收盤+股利)/前收盤−1` 對純現金股利實測只差約 0.5%(中華電 15 年 15 次配息
  累積差 −0.53%),而且 **Python 版還多還原了「減資」**(Scala 版漏掉、被 D-backtester-scala
  記為 BUG 的那個),比 Scala 更完整。
- **複利、CAGR、最大回撤**:逐日 `NAV = 資本 × ∏(1+日報酬)`、`CAGR=(末/初)^(1/年)−1`、
  `MDD=min(NAV/歷史高−1)`,全是教科書定義。
- **成本**:手續費買賣雙邊各 0.0285%(= 0.1425% × 2 折)、證交稅只收賣方 0.3%,
  round-trip 0.357%;event 引擎(apex/serenity)另加單邊滑價假設,方向都對、都偏保守。

要注意的小瑕疵(都不影響「能不能信」的結論,但學理上不完美):
1. **v4 第一次建倉多扣了一次賣稅 + 一次手續費**——第一筆只有「買」卻按 round-trip 收費,
   一次性多扣 0.33%,對 8 年 CAGR 影響 −0.04pp,方向保守(低估報酬)。
2. **`CLAUDE.md` 把換手成本公式寫反了**(用交集 `∩` 當分子,程式其實用「1 − 交集」),
   程式碼是對的,但文件叫人「照抄」,新策略若照文件抄會做出反向的成本(持股越穩定扣越多)。
3. **0050/ETF 賣出一律課 0.3%,法定 ETF 證交稅是 0.1%**——v4 regime 抱 0050 時多扣 0.2%/次,
   保守方向,量小。

---

## 逐一計算式對照

### 1. [OK] DRIP 日報酬 = 乘法還原收盤價比值 — `prices.py:195-212, 310-329, 434-483`

**學理定義**:單期總報酬(Fisher / CRSP holding-period return)= `(P_t + D_t)/P_{t-1} − 1`,
D_t 為除息日 t 的每股分配。等價的乘法約定(Yahoo「adjusted close」、CRSP price-adjustment
factor):除息因子 `f = (P_pre − D)/P_pre`,回乘所有過去價格,則
`adj[T]/adj[t] − 1` 即買 t 賣 T 的總報酬。

**程式實作**:`daily_returns_from_panel` 回 `close_t/close_{t-1} − 1`,其中 `close` 是
`fetch_adjusted_panel` 的還原收盤價。因子優先序(FC1,2026-07-23):
① 有交易所參考價 → `f = ref/pre`(官方因子,配息+配股一體);
② 只有現金股利 → `f = (pre − cash)/pre`;`_apply_back_adjustment` 用「逆向累乘 + forward
asof(probe=date+1)」對每個 (code,date) 套上 `∏_{ex_date>date} f_e`。

**是否相符**:相符,且屬產業標準法。乘法與加法兩約定 **僅在** `P_t = P_{t-1} − D`(除息當日
無其他市場波動)時逐項相等;否則差在「股利以哪個價再投入」的二階項
(乘法隱含以 pre-ex 基準 `1/(1−D/P_{prev})`、加法以除息收盤 `1+D/P_ex`)。

**偏差證據(可重現)**:逐事件實測(2412 中華電 2009–2024,15 次配息):
`∏(add/mult)=0.9947`,即整段僅差 **−0.53%**;2330 差 15.2pp / 3771%(相對 0.4%)、
1301 差 0.078pp。docstring 稱「mathematically equivalent」略為誇大——正確表述是
「二階近似,現金股利下 21 年 <0.5%」。`test_prices.py` 的 parity 測試比對的是 **另一個乘法
實作**(`_independent_total_return` 也是乘法),因此它保障乘法實作彼此一致,但 **未** 驗證
乘法對加法的等價——equivalence 這句話目前無測試背書。

（附帶:初次 73pp 探針落差是 **探針缺陷**——crude 加法 walk 未中性化非現金公司行動
〔2412 有 1 筆現金減資 + 一筆權值事件〕,`prices.py` 正確還原了它們。）

**修法**:非 BUG。建議把 docstring 的「mathematically equivalent」改為「二階等價
(exact iff ex-price = prev−div),cash-div 下 21y <0.5%」,並補一條「乘法 vs 加法」的
parity 測試把差異上界寫死,免得未來誤讀為逐位等價。

### 2. [OK] `prices.py` 有還原「減資」參考價重設 — `prices.py:172-179, 332-357`(相對 Scala 為強項)

**學理定義**:總報酬 NAV 要中性化「每一種」公司行動的參考價重設,含減資
(TWSE 減資恢復買賣參考價)。

**程式實作**:載入 `capital_reduction` 表,`factor = post_reduction_reference_price / pre_close`
(asof 取除權日前最後收盤),範圍守衛 `0.05 < f < 100`(彌補虧損型可到 factor 40)。逆向
累乘與股利/分割同層套用。

**是否相符**:相符,方向正確。彌補虧損型(價跳高、f>1):8103 型的 +11.6% 機械跳被還原為
~0%;現金減資型(價跌、f<1):以還原方式隱含再投入返還股款。**Python 路徑因此避開了
D-backtester-scala 記錄的 Scala 減資 BUG(350/386 筆漏接)**——同一資料在 Python 端被正確處理。

**修法**:無。（僅提醒:退還股款型 100% 精確尚需補記返還現金;參考價重設已消 ~95% 誤差,與
Scala 修法建議一致。）

### 3. [OK] 每日 NAV 複利 / CAGR / MDD / 年化波動 — `v4.py:257-268`、`apex/metrics.py:16-38`

**學理定義**:`NAV = 資本 × ∏(1+r_t)`;`CAGR=(end/begin)^(1/years)−1`,years=日曆日/365.25;
`MDD=min(NAV/cummax−1)`;年化波動 `= σ_daily × √252`(σ 用樣本標準差 ddof=1)。

**程式實作**:`navs = capital * np.cumprod(1 + rets_arr)`;`cagr=(navs[-1]/capital)**(1/years)−1`,
`years=(days[-1]-days[0]).days/365.25`;MDD 逐日 `peak=max(peak,v); mdd=min(mdd,(v-peak)/peak)`;
`vol=rets.std(ddof=1)*sqrt(252)`。`apex/metrics.py` 同構(`np.std(ddof=1)`、`runmax`、
`(v[-1]/v[0])**(1/years)−1`)。

**是否相符**:全部逐字符合教科書定義。年化用 252、無風險利率近似另計,皆為台股標準約定。

**修法**:無。

### 4. [OK] 摩擦成本雙邊 + 賣稅 0.3% + 手續費折數 + round-trip — `constants.py:9-11`、`apex/engine.py:16-18,48-50`、`serenity/engine.py:77-78,462,505`

**學理定義**:台股手續費 0.1425%(2 折=0.02850%)買賣雙邊;證交稅賣方單邊,普通股 0.3%;
round-trip = 賣稅 + 2×手續費。滑價/衝擊成本為額外可加摩擦。

**程式實作**:`COMMISSION=0.000285`、`SELL_TAX=0.003`、`ROUND_TRIP_COST=SELL_TAX+2*COMMISSION
=0.00357`。apex 引擎:買 `cash-=N×(1+comm)`、`shares=N/(px×(1+slip))`;賣
`cash+=shares×px×(1−slip)×(1−comm−tax)`,`slippage=0.001`。serenity:`FEE_BUY=comm+0.0005`、
`FEE_SELL=comm+tax+0.0005`,`proceeds=value×(1−FEE_SELL)`、`value=alloc×(1−FEE_BUY)`。

**是否相符**:相符。手續費雙邊、賣稅單邊、折數 0.0285%=0.1425%×0.2、round-trip 0.357% 全對。
滑價(10 bps / 5 bps 單邊)是明確標註的模型摩擦參數(保守,加成本),非學理錯誤。

**修法**:無。（滑價數值屬 §2.2 參數證據問題,非學理定義問題。）

### 5. [OK] 換手成本公式(穩態)+ TargetPercent 再平衡 — `v4.py:124,199,228-246`

**學理定義**:等權替換籃 rebalance,賣出比例 s、買入比例 s,成本
`= s×賣稅 + (s賣+s買)×手續費 = s×(賣稅 + 2×手續費)`。TargetPercent = 每檔目標 1/N NAV。

**程式實作**:`sold_frac=(size−overlap)/size`(size=`max(len(cur),1)`);
`cost=sold_frac×(SELL_TAX+2×COMMISSION)`。weight 由 SQL 給 `1/TOPN`(regime 抱 0050 時 weight=1.0),
權重恆和為 1、全額投資 = TargetPercent 等權。picks 以 asof `+1 day` shift 生效(T 決策、T+1 成交)。

**是否相符**:穩態公式相符;`sold_frac` 用「非重疊比例」正確(見 §7 對照文件寫反)。
`+1 day` shift 正確避免前視(與 apex `next_open` 一致;對照 Scala 同收盤成交的 SUSPECT)。

**已註記近似**(非 BUG):日報酬用固定權重 `Σ w_i·r_i`(隱含每日 rebalance 回等權),但成本
只在月度 rebal 日計——即模型免費得到日再平衡的方差縮減、不付日換手成本。差異二階、且
CLAUDE.md「Speed over bit-exact」明文接受。

**修法**:公式無需改。

### 6. [SUSPECT] 首次建倉多收「賣稅 + 一次手續費」— `v4.py:240-246`

**學理定義**:第一筆建倉是「純買」,成本應只有買方手續費 `1.0×COMMISSION`,不含賣稅、不含賣方手續費。

**程式實作**:`if prev_set is None: sold_frac=1.0`,再 `cost=sold_frac×(SELL_TAX+2×COMMISSION)`
→ 對初始純買也收了整份 round-trip。

**偏差證據**:charged=0.00357 vs 正確 pure-buy=0.000285,一次性多扣 **0.329%**;對 8.29 年
回測 CAGR 影響 **−0.0397pp**。方向:保守(低估報酬)。僅發生在首個 rebal 日。

**修法**:首個 rebal 的成本改為 `1.0×COMMISSION`(只算買方手續費);或 `first_buy_cost =
bought_frac×COMMISSION`,穩態維持 `sold_frac×(SELL_TAX+2×COMMISSION)`。

### 7. [SUSPECT] `CLAUDE.md` 換手公式寫反(文件 vs 程式;傳播風險)— 文件 `CLAUDE.md` Strategy Semantics Contract

**學理定義**:換手成本 ∝ **實際交易比例** = 非重疊比例 `1 − |prev∩cur|/TOPN`。

**文件實作**:CLAUDE.md 寫 `Per-rebal turnover cost = |prev ∩ cur| / TOPN × (SELL_TAX + 2×COMMISSION)`
並附「match src/quantlib/strat_lab/v4.py formula」。`|prev∩cur|/TOPN` 是 **交集(保留)比例**,
與程式 `sold_frac=(size−overlap)/size` 恰為互補(反向)。

**偏差證據**:依文件公式,持股完全不變(overlap=TOPN)→ 扣滿 round-trip;持股全換(overlap=0)
→ 零成本,與現實相反。**程式碼(`v4.py:245`、`iter_13_event_exit.py:162,177`)全部用正確的
非重疊比例**,無一支引擎採用文件的反向式;風險純在「未來新策略照文件抄」。全庫掃描
(`sold_frac|overlap|SELL_TAX + 2`)未見任何消費者採用反向式。

**修法**:改 CLAUDE.md 為 `(1 − |prev∩cur|/TOPN) × (SELL_TAX + 2×COMMISSION)` 與程式對齊
(此為文件修正,程式無需動)。

### 8. [SUSPECT] ETF/0050 賣出一律課 0.3%(法定 0.1%)— `v4.py:196-199` regime 0050 leg;event 引擎 `SELL_TAX` 扁平

**學理定義**:ETF(0050/0052…)證交稅為 0.1%,非普通股 0.3%。

**程式實作**:`SELL_TAX=0.003` 扁平套用。v4 純股票池以 regex `^[1-9][0-9]{3}$` 排除了 ETF
(0050 首位 0 被 `[1-9]` 擋掉)→ **股票腿正確**;但 regime 風險偏好時整檔抱 0050(weight=1.0),
切回股票時賣出 0050 仍按 0.3%。event 引擎若持有 ETF 亦同。

**偏差證據**:每次「賣出整本 0050」多扣 0.2% NAV;8 年約 10–20 次 regime 切換 → 累積 ~2–4%
(上界)、保守方向。benchmark 0050 為買抱不賣,無影響。

**修法**:依代號分流賣稅(ETF 0.1%);普通股純用途優先度低。

### 9. [OK/cross-ref] 風險調整指標 Sharpe/Sortino — 歸屬 **D-perf-validation**

`apex/metrics.py:32-33` 的 Sharpe = `mean/std×√252`、Sortino = `mean/√mean(min(r,0)²)×√252`
**皆為教科書正確**(算術均值超額報酬 / 標準差、目標半離差 MAR=0 除以全 N)。

但 `v4.py:264` 與 `strat_lab/evaluation.py:135` 的 Sharpe = `(cagr−rf)/vol` 用 **幾何 CAGR** 當
分子(分母是算術年化波動)——與教科書 `(算術均值−rf)/σ` 不一致,系統性低估 Sharpe ~σ/2;
`strat_lab/evaluation.py:120,134` 的 Sortino 下行離差用 `np.std(rets[rets<0], ddof=1)`
(對負報酬子集的「自身均值」離差、除以 n_neg),與教科書「對 MAR=0 的半離差、除以全 N」不同
(實測某 Gaussian 樣本比值 0.86x,方向依分佈而定)。**這兩項已由 D-perf-validation 立案**
(該單位明列 `Sharpe(幾何 CAGR 基礎)`、`Sortino 下行標準差` 覆蓋 `validate_hybrid.py` +
`evaluation.py`),此處僅交叉引用,不重複計列;`v4.py:264` 為同類另一實例。
