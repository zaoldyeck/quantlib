# A-index — 指數資料(`index` 表)解析正確性稽核

- 單位:`A-index`(dim A:原始檔 → DB 解析正確性)
- 原始檔:`data/index/{twse,tpex}/{year}/YYYY_M_D.csv`,共 10,149 個
- 解析程式:`src/main/scala/reader/TradingReader.scala:398-463`(`readIndex`)
- 設定:`src/main/scala/setting/IndexSetting.scala`
- 落地表:PG `public.index`(**不是** `market_index`;`market_index` 是 DuckDB cache 的視圖名)
- 稽核日期:2026-07-22
- 判定:**SUSPECT**(解析器本身乾淨,但落地資料有 947 列是別的日期的資料,另有 33 個交易日整天缺料)

---

## 一句話結論

**這張表的數字大體可以信,但有三個坑要先知道:**

1. **有 8 天的資料整片是「別的日子」的**(共 947 列)。TWSE 的網站在抓不到那天資料時,
   會把「你要的日期」印在檔頭、內容卻給另一天的數字。抓下來的檔看起來完全正常,
   驗證程式也放行了。其中 3 天(週六)根本沒開市,整天是幻影。
2. **上櫃(TPEx)指數在 2024-06-27 ~ 2024-08-12 整整 32 個交易日完全沒抓**,加上零星幾天,
   共 33 個交易日查無資料。上市(TWSE)則缺 2 天。
3. **「漲跌」「漲跌幅」兩欄不允許空值,所以原始檔寫「--」(當天沒公布)一律被寫成 0**。
   結果是 1,578 列明明是「無資料」,在資料庫裡長得像「當天平盤沒動」。做動能、
   波動度計算會被這些假 0 汙染。

**除此之外,解析器把原始檔搬進資料庫這件事做得非常乾淨**:769,751 列逐欄比對,
收盤點數 **0 筆不符**,列數進出 **0 筆遺漏、0 筆多餘**,欄位沒有錯位、單位沒有搞錯、
編碼沒有亂碼、檔名日期與內容日期 100% 一致。

---

## 受測對象宣告的欄位契約

### TWSE(`TradingReader.scala:419-440`)

原始檔(Big5-HKSCS,CRLF)結構:第 1 行標題含民國日期,之後多個區段
(`價格指數(臺灣證券交易所)` / `價格指數(跨市場)` / `價格指數(臺灣指數公司)` /
`報酬指數(臺灣證券交易所)` / `報酬指數(跨市場)` / `報酬指數(臺灣指數公司)`),
每區段一行表頭 + 資料列,最後是 `備註:` footer。

資料列欄序(2009→2026 恆定,實測無漂移):

| # | 欄名 | 型別 | reader 取用 |
|---|---|---|---|
| 0 | 指數 / 報酬指數 | 字串 | `name`(去空白、去逗號) |
| 1 | 收盤指數 | 數值 | `close`(`toDoubleOption` → 可為 NULL) |
| 2 | 漲跌(+/-) | `+` / `-` / 空 | 與第 3 欄組成 `change` 的正負號 |
| 3 | 漲跌點數 | 數值(無號) | 幅度 |
| 4 | 漲跌百分比(%) | 數值(自帶正負) | `changePercentage`(`getOrElse(0)`) |
| 5 | 特殊處理註記 | A / B / 空 | **未接**(schema 無此欄) |

列篩選:`row.size == 6 或 7` 且 `head != "指數"` 且 `head != "報酬指數"`。

### TPEx(`TradingReader.scala:441-455`)

第 1 行 `上櫃股價指數收盤行情`、第 2 行 `Data Date:民國年/月/日`、第 3 行表頭
`指數,收市指數,漲跌,漲跌幅度`,價格區資料列,`報酬指數` 分隔行 + 表頭,報酬區資料列。

| # | 欄名 | reader 取用 |
|---|---|---|
| 0 | 指數 | `name`;**報酬區會被改名成 `name.replace("指數","") + "報酬指數"`** |
| 1 | 收市指數 | `close` |
| 2 | 漲跌 | `change`(**自帶正負號**,`.toDouble` 非 Option) |
| 3 | 漲跌幅度 | `changePercentage` |

### 落地 schema(`src/main/scala/db/table/Index.scala:16-35`)

```scala
Table[(Long, String, LocalDate, String, Option[Double], Double, Double)]
// id, market, date, name, close(Option), change(非 Option), change(%)(非 Option)
// unique index (market, date, name)
```

---

## 稽核做法

1. 讀 `IndexSetting.scala`、`Index.scala`、`TradingReader.readIndex`,寫下欄序契約。
2. **獨立重寫解析器**(`docs/data_audit/scripts/A-index/indep_index.py`),
   完全不呼叫受測的 Scala 程式,自己用 Python `csv` + Big5-HKSCS 解 **全部 10,149 個原始檔**
   (不是抽樣;跨 twse 2009→2026、tpex 2016→2026 全部年份)。
3. 把 PG `index` 全表(769,751 列)匯出,與獨立解析結果 **逐 (market, date, name) 逐欄比對**。
4. 額外做語意交叉驗證:`close_t − close_{t−1} == change_t`(指數自己的算術恆等式),
   以及 `index` 有資料的日子 `daily_quote` 是否也有交易。

重跑指令:

```bash
# 先跑這支:獨立解析全部 10,149 個原始檔 -> indep_index.parquet(其餘腳本的輸入)
uv run --project . python docs/data_audit/scripts/A-index/indep_index.py
# 逐 (market,date,name) 逐欄比對(自己連 PG,不需先匯出 CSV)
uv run --project . python docs/data_audit/scripts/A-index/cmp_aligned.py
uv run --project . python docs/data_audit/scripts/A-index/probe_fields.py   # 欄位語意/單位/編碼/日期/漏欄
uv run --project . python docs/data_audit/scripts/A-index/probe2.py         # sentinel 檔、檔名 vs 內容日期
uv run --project . python docs/data_audit/scripts/A-index/probe3.py         # 歸零的實質影響、tpex 可解析性
uv run --project . python docs/data_audit/scripts/A-index/probe4.py         # index vs daily_quote 交叉
uv run --project . python docs/data_audit/scripts/A-index/probe5.py         # 缺漏交易日盤點
uv run --project . python docs/data_audit/scripts/A-index/probe6.py         # tpex 改名器破壞範圍
uv run --project . python docs/data_audit/scripts/A-index/probe7.py         # 旗艦指數嚴格一致性
uv run --project . python docs/data_audit/scripts/A-index/probe8.py         # 全表逐日內部一致性
```

(`indep_index.parquet` 是可重生的中間物,不進版控;跑第一支就會重建。)

---

## 發現

### BUG-1 — 8 天的指數整片是別的日期的資料(947 列)

`index` 表裡有 8 個 twse 日期,**每一檔指數的收盤點數與漲跌都與另一個日期一字不差**:

| 受汙染日期 | 星期 | 內容其實是哪一天 | 當日是否有交易(daily_quote) |
|---|---|---|---|
| 2015-08-29 | 六 | 2015-12-18 | 無(0-byte sentinel) |
| 2016-05-26 | 四 | 2016-01-18 | 有(949 檔) |
| 2017-08-02 | 三 | 2017-12-18 | 有 |
| 2018-08-04 | 六 | 2018-07-24 | 無 |
| 2018-09-15 | 六 | 2017-03-17 | 無 |
| 2018-10-03 | 三 | 2018-06-15 | 有 |
| 2019-07-05 | 五 | 2019-07-16 | 有 |
| 2019-09-25 | 三 | 2019-09-02 | 有 |

證據(可直接重跑):

```sql
SELECT date, close, change FROM index
WHERE market='twse' AND name='發行量加權股價指數'
  AND close IN (10634.85, 7811.18, 8257.32, 10506.52, 10995.39, 9908.69, 11087.47, 10886.05)
ORDER BY close, date;
-- 每個 close 值都恰好出現兩次:一次在受汙染日,一次在真正的來源日,連 change 都相同
```

原始檔本身就已錯:`data/index/twse/2016/2016_5_26.csv` 檔頭寫
`"105年05月26日 價格指數(臺灣證券交易所)"`,但 `發行量加權股價指數` 是 `7,811.18`
(2016 年 5 月台股在 8,400 附近,7,811 是 1 月的水位),而且檔案只有 91 行、
鄰日是 113 行(舊格式、指數檔數較少)。

**為什麼守門沒擋住**:`IndexSetting.scala:14-17` 的 `validate` 只比對「檔頭有沒有出現
我要的民國日期」。TWSE 的 fallback 回應**會把你要求的日期印進檔頭**,內容卻是別天的
——這個檢查對這個失效模式完全無效。原始碼註解描述的失效模式(回傳錯誤日期的檔頭)
只是兩種 fallback 的其中一種。

全表量化:`docs/data_audit/scripts/A-index/probe8.py` 逐日檢查
`|close_t − close_{t−1} − change_t| > 0.02` 的比例,
767,148 個可檢查列中有 2,367 列(0.31%)不一致,集中在 21 天;
扣掉「前一天本來就缺料」造成的假警報後,真正內容錯誤的就是上表 8 天 + 其後一天的連鎖。

**這不是解析器的錯**——解析器忠實地把錯檔搬進來了。根因在爬蟲的驗收條件。

### BUG-2 — 33 個交易日的上櫃指數完全沒有(其中 32 天連檔案都不存在)

以 `daily_quote` 的交易日為日曆(`probe5.py`):

- **tpex 缺 33 個交易日**:
  - `2024-06-27` ~ `2024-08-12` 共 **32 個交易日,`data/index/tpex/2024/` 下連檔案都沒有**
    (目錄裡 `2024_6_26.csv` 之後直接跳到 `2024_8_13.csv`)。
  - `2016-09-10`(週六補行交易日):檔案存在但只有 122 bytes 表頭,TPEx 當天沒公布指數。
  - `2026-05-28`:兩市場的 index 原始檔都不存在。
- **twse 缺 2 個交易日**:`2009-12-12`、`2026-03-12`,兩者都是 0-byte sentinel,
  但 `daily_quote` 當天有完整資料 → 是真交易日,index 卻沒抓到。

另外 `2026-02-26`、`2026-03-11` 兩天是**半殘檔**(8.4 KB / 155 行 vs 鄰日 16.7 KB / 287 行),
只落了 135 列而非 267 列 —— 值本身正確(與前日 close 對得起來),但少了一半的指數。

影響:任何以 `index` 當市場 regime / benchmark 的計算,在 2024 年 7 月整月會拿不到櫃買指數。

### BUG-3 — 「無資料」被寫成 0,無法與「當天真的沒動」區分

`Index.scala:29-31` 把 `change` 與 `change(%)` 宣告成 `Double` 而非 `Option[Double]`,
於是 `TradingReader.scala:433-438` 只能把無法解析的值塞 0:

```scala
val change = values(2) match {
  case "-" => Try(-values(3).toDouble).getOrElse(0D)   // 幅度是 "--" → 0
  case ""  => 0                                        // 方向欄空白 → 0(不管幅度欄是什麼)
  case "+" => Try(values(3).toDouble).getOrElse(0D)
}
val changePercentage: Double = values(4).toDoubleOption.getOrElse(0)  // "--" → 0
```

原始檔對「當天未公布」是印 `"--"`,例:

```
data/index/twse/2026/2026_7_9.csv:150
"臺灣50報酬指數","--","","--","--","",
```

實測影響(`probe_fields.py` / `probe3.py`):

| 情形 | 列數 | 落地後長相 |
|---|---|---|
| `close`、`change`、`pct` 三者原始皆為 `--` | **1,578**(860 天) | `close=NULL, change=0, change(%)=0` → 像「平盤」 |
| `pct` 原始為 `--`,但 close/change 有值 | 2,831 | `change(%)=0`;其中 2,810 列真實漲跌幅四捨五入本來就是 0.00(TWSE 用 `--` 代替 `0.00`),**21 列實際 |漲跌幅| ≥ 0.005% 被歸零** |
| 方向欄是 `+`/`-` 但幅度欄是 `--` | 5 | `change=0`(方向資訊被丟掉) |
| 方向欄空白但幅度欄有值 `0.01` | 2 | `change=0`(2014-12-27 紡織纖維類報酬指數、2018-03-31 塑膠類報酬指數) |

`close` 這一欄做對了(`Option[Double]` → NULL),`change` / `change(%)` 沒有。

### BUG-4 — TPEx 報酬指數改名器把名字弄壞(7,506 列,占 tpex 全表 4.72%)

`TradingReader.scala:447`:

```scala
val returnIndexes = spanRows._2.tail.map(values =>
  (values.head.replace("指數", "") + "報酬指數") +: values.tail)
```

TPEx 報酬區的指數名有兩種:多數是價格區的同名(`櫃買指數`、`半導體業`),
需要加上「報酬」以免撞名 —— 這部分是對的。但有 **11 檔原始名已經自帶「報酬指數」**,
被無條件再疊一次:

| 原始檔中的官方名稱 | 落地成 |
|---|---|
| `TPEx FactSet氣候韌性報酬指數` | `TPExFactSet氣候韌性報酬報酬指數` |
| `TPEx FactSet半導體氣候韌性報酬指數` | `TPExFactSet半導體氣候韌性報酬報酬指數` |
| `TPEx FactSet半導體氣候淨零優選報酬指數` | `TPExFactSet半導體氣候淨零優選報酬報酬指數` |
| `櫃買半導體領航報酬指數` | `櫃買半導體領航報酬報酬指數` |
| `富櫃200報酬正向2倍指數` | `富櫃200報酬正向2倍報酬指數` |
| `特選上櫃ESG成長/永續高股息/電子菁英/龍頭報酬指數` | `…報酬報酬指數`(4 檔) |
| `台灣5G報酬指數` / `臺灣5G+通訊報酬指數` / `特選臺灣電動車產業鏈代表報酬指數` | `…報酬報酬指數` |

```sql
SELECT count(*) FROM index WHERE market='tpex' AND name LIKE '%報酬報酬%';  -- 7506
```

值是對的、查得到,但名字不是官方名 —— 用官方名去 join 會查無資料。
另外 `.replace(" ","")` 也把 `Quality 50指數` → `Quality50指數`、
`上櫃ESG 30指數` → `上櫃ESG30指數`(5 檔),同類問題但影響較小。

歷史上尚未造成撞名丟列(全史 (market,date,exp_name) 碰撞數 = 0),
但這個改名器只要哪天讓兩檔指數對映到同一個名字,`++=` 就會撞 unique index 讓整檔匯入失敗。

### SUSPECT-1 — footer 切割條件對 2025 年以前的檔完全無效(目前零影響)

`TradingReader.scala:424`:

```scala
val csvData = source.getLines().takeWhile(! _.startsWith("備註:")).mkString("\n")
```

實測:2025 年以前的 TWSE 檔,footer 首行是 `"備註:"`(**有前導雙引號**),
`startsWith("備註:")` 為 false → footer 根本沒被切掉;2026 起改成 `備註:`(無引號)才生效。

證據:

```
data/index/twse/2020/2020_6_1.csv:165  '"備註:"'          ← takeWhile 失效
data/index/twse/2026/2026_7_9.csv:282  '備註:"'           ← takeWhile 生效
```

**目前零影響**:footer 各行都不會被解析成 6 或 7 欄的資料列
(`QuantlibCSVReader` 會跳過含 `""` 且不含 `,""` 的行;其餘 footer 行解析出來是 1~3 欄,
被 `row.size == 6 || 7` 濾掉)。獨立解析(有正確切 footer)與 DB 的列數比對 0 筆差異,
反證了這一點。但這是靠下游濾網救的,不是靠切割本身 —— TWSE 哪天改 footer 格式就會爆。

### SUSPECT-2 — `change` 的 `match` 沒有 default case

`TradingReader.scala:433-437` 只寫了 `"-"`、`""`、`"+"` 三個 case。
全史 610,757 列 twse 資料的第 3 欄取值分佈實測只有這三種
(`+` 326,109 / `-` 282,163 / 空 2,485),所以**至今沒炸過**;
但這個 `map` 跑在 `.par.foreach` 裡,一旦 TWSE 出現第四種值(例如全形 `－`)就是 MatchError,
整個 `Main update` 會掛。同理 TPEx 的 `values(2).toDouble`(非 Option)遇到非數值也會炸
(實測 159,026 列全部可解析,目前安全)。

### SUSPECT-3 — TPEx 自己把 2 檔指數的名字寫成 `null`,那段期間的資料沒進庫

`data/index/tpex/2022/2022_1_13.csv:32-33`:

```
"null","5,401.27","-1.92","-0.04"
"null","5,854.36","-10.73","-0.18"
```

`TradingReader.scala:458` 的 `.filterNot(_._3 == "null")` 把它們丟掉。
**丟掉是對的**(兩列同名會撞 unique index 讓整檔匯入失敗),但代表
2022-01-13 ~ 2022-02-14 這 16 個交易日、共 32 列的指數資料沒有落地。
根因在 TPEx 來源端。

### 觀察(非本單位,但順手抓到)— `daily_quote` 有 4 個交易日整天沒資料

`index` 有完整且**內部一致**的資料、`daily_quote` 卻是 0-byte sentinel 的日子:
`twse 2021-08-18`(三)、`twse 2025-08-15`(五)、`twse 2026-04-29`(三)、`tpex 2023-06-08`(四)。
這幾天 index 的 `close_t − close_{t−1} == change_t` 全部對得起來 → 是真交易日,
是 `daily_quote` 缺料。建議轉給 daily_quote 相關單位處理。

---

## 查過沒問題的部分(OK,不必再查一次)

| 檢查項 | 方法 | 結果 |
|---|---|---|
| **收盤點數逐位比對** | 獨立解析 vs PG 全表 join,769,751 列 | **0 筆不符**(含 NULL/非 NULL 的一致性) |
| **列數進出** | 同上 | 原始檔有而 DB 沒有 **0 筆**;DB 有而原始檔沒有 **0 筆** |
| **欄位錯位(fall-through)** | 抽 2009 / 2012 / 2016 / 2020 / 2024 / 2026 的 twse 檔 + 2016 / 2020 / 2026 的 tpex 檔逐行檢視,再全量解析 | TWSE 欄序 18 年恆定(6 欄 + 尾逗號),TPEx 4 欄恆定;`case 6 / case 7` 明確分派,**無錯位** |
| **單位** | 原始值 vs DB 值 | 指數點數原樣(千分位逗號被去掉);百分比原樣(`-0.78` 存 `-0.78`,不是 `-0.0078`);無元/千元、張/股問題 |
| **正負號** | 全史第 3 欄取值分佈 + 與 `pct` 符號交叉 | TWSE `+`/`-`/空 三值,組合後與第 5 欄百分比符號一致;TPEx 漲跌欄自帶符號、159,026 列全可解析 |
| **編碼(Big5/UTF-8)** | 以 Big5-HKSCS 解全部檔,掃替換字元 `�` | **0 筆亂碼**;中文指數名全數正確 |
| **日期(民國 vs 西元、檔名 vs 內容)** | 6,837 個非空檔逐檔比對檔頭民國日期 vs 檔名 | **0 筆不符**(twse `NNN年MM月DD日`、tpex `Data Date:NNN/MM/DD`) |
| **漏欄** | TWSE 第 6 欄「特殊處理註記」schema 未接 | 全史 610,757 列**皆為空字串**,全庫 grep `,"A",` / `,"B",` 命中 0 檔 → **實際零資訊損失** |
| **`filter(_.file.length > 1024)` 是否誤殺** | 列出全部 3,312 個 ≤1024B 的檔並看內容 | twse:1,845 個 0-byte + 227 個 116B(只有兩行表頭);tpex:1 個 0-byte + 1,239 個 120/122B(只有表頭)→ **沒有略過任何含資料的檔** |
| **unique index 靜默丟列** | 全史 (market, date, 預期落地名) 碰撞掃描 | **0 組碰撞** → `++=` 沒有因撞索引而整檔失敗或丟列 |
| **DuckDB cache 與 PG 一致** | `market_index` vs PG `index` 分市場列數 | twse 610,757 / tpex 158,994,**完全一致**(欄名 `change(%)` → `change_pct`) |
| **重複匯入(dedupe)語意** | `readIndex` 以 (market, 檔名) 去重 | 只要該 (market, date) 已有任一列就整檔跳過 → 半殘檔(如 2026-02-26)重抓後**不會被補齊**,需先 DELETE 再 read |

---

## 建議修法(不在本單位執行)

1. **BUG-1**:`IndexSetting.validate` 增加內容合理性檢查 ——
   下載後比對「本檔 `發行量加權股價指數` 的 `close − change`」是否等於 DB 中前一交易日的 close;
   對不上就判 `[deferred]` 刪檔重抓。並清掉現有 8 天 947 列後重抓(3 個週六直接刪)。
2. **BUG-2**:補抓 `tpex 2024-06-27 ~ 2024-08-12`、`2026-05-28`、`twse 2009-12-12`、`2026-03-12`;
   `2026-02-26`、`2026-03-11` 半殘檔先 `DELETE FROM index WHERE ...` 再重抓重讀
   (因為 dedupe 是整檔跳過,不刪就補不進去)。
3. **BUG-3**:`Index.scala` 的 `change` / `change(%)` 改成 `Option[Double]`,
   reader 改用 `toDoubleOption` 不 `getOrElse(0)`;既有 1,578 列
   (`close IS NULL AND change=0 AND "change(%)"=0`)回填 NULL。
4. **BUG-4**:改名器改成 `if (name.endsWith("報酬指數")) name else name.replace("指數","") + "報酬指數"`;
   既有 7,506 列改名。同時考慮不要 `.replace(" ","")` 掉指數名稱裡的空白。
5. **SUSPECT-1/2**:footer 切割改成 `line.replaceAll("^\"", "").startsWith("備註:")`;
   `match` 補 `case other => throw` 或明確記錄後跳過,別讓非預期值靜靜變成 0。
