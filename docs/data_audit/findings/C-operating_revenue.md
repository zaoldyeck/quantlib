# C-operating_revenue — cache 與 PostgreSQL 的一致性與缺漏

**結論:🔴 BUG。這張表的歷史(2013-2025)可以信,最近三個月(2026-04/05/06)不能信。**

白話講:同一句 `SELECT ... FROM operating_revenue`,你今天問 DuckDB cache 跟問
PostgreSQL 會拿到不一樣的答案,而且**每天早上跑一次資料更新,答案還會來回翻**。
差異全部集中在最近三個月:PostgreSQL 少了一整片金融保險業(富邦金 2881 的
2026-04/05/06 月營收在 PG 裡根本不存在),cache 少了三家下市公司與所有 TDR。
兩邊都不完整,合起來才是對的。

2013 年以後的解析本身**零錯誤**(27.7 萬列的內部算式逐列對得上),月份序列
**零缺口**,負營收與零營收經原始檔備註證實**都是真的**。所以問題不在「讀錯」,
在「同一張表有兩條會互相覆蓋的寫入路徑,而且兩條都會漏東西」。

---

## 一、事實盤點(數字都可重跑)

| 項目 | cache(`var/cache/cache.duckdb`,2026-07-21 08:16) | PostgreSQL(2026-07-22) |
|---|---|---|
| 總列數 | 481,582 | 481,529 |
| 欄位數 | 9 | 17 |
| twse/consolidated 2026-04 | 1,081(金融保險業 32) | 1,062(金融保險業 **17**) |
| twse/consolidated 2026-05 | 1,081(金融保險業 32) | 1,065(金融保險業 **16**) |
| twse/consolidated 2026-06 | 1,081(金融保險業 32) | 1,072(金融保險業 **24**) |
| 差異鍵 | cache 獨有 63 列 / PG 獨有 10 列 | |
| 共同鍵上值不同 | company_name 6、industry 14、monthly_revenue 4、yoy 4 | 全部落在 2026-04..06 |

2026-01..03 的金融保險業都是 32 家 —— 所以「17/16/24」不是市場變化,是資料漏了。

重跑方式:

```bash
psql -h localhost -p 5432 -d quantlib -c "
SELECT year, month, COUNT(*) FILTER (WHERE industry='金融保險業') fin, COUNT(*) all_
FROM operating_revenue WHERE market='twse' AND type='consolidated' AND year=2026
GROUP BY 1,2 ORDER BY 1,2;"
```

---

## 二、🔴 BUG 1:PG 永久漏掉「15 號之後才申報」的公司

**現象**:富邦金(2881)在 PostgreSQL 裡沒有 2026-04、05、06 的月營收——一列都沒有。
同樣消失的還有國泰金、華南金、凱基金、元大金、兆豐金、台新新光金、中信金、第一金、
合庫金、三商壽、新產、中再保、第一保、旺旺保、三商等 15~19 家。

**根因(兩處,同一個條件)**:

- `src/main/scala/Task.scala:189-217` `pullOperatingRevenue()`
  —— `.filterNot(existFiles)` 讓「檔案已存在的月份」永遠不再下載;只有
  `LocalDate.now.getDayOfMonth <= 15` 時才把「上個月」放進 `inWindow` 無條件重抓。
- `src/main/scala/reader/FinancialReader.scala:299-303` `readOperatingRevenue()`
  —— delete+insert 的重整視窗綁同一個 `dayOfMonth <= 15` 條件。

**為什麼會漏**:MOPS 的 `t21sc03_<民國年>_<月>.csv` 在 15 號之後**還會繼續長大**。
實測同一個 endpoint、同一個月份的檔案:

| 抓取時間 | twse 列數 | 金融保險業 |
|---|---|---|
| 2026-05-13(`data/operating_revenue/twse/2026/2026_4_c.csv`,mtime 05-13 12:04) | 1,062 | 17 |
| 2026-07 中下旬(Python 爬蟲寫進 cache) | 1,081 | 32 |

`wc -l data/operating_revenue/twse/2026/2026_4_c.csv` = 1063(含表頭)= PG 的 1,062 列
—— PG 忠實反映那個檔,檔本身就是殘缺的早期快照。

**為什麼 2026-01..03 沒事**:那三個月的檔案落地時間分別是 02-23、04-01、04-19,
都在申報潮結束之後才抓,自然是完整版。**這個 bug 的觸發條件是「剛好在 15 號前抓到」**,
所以它時有時無、極難察覺。

**影響**:Scala strategy 層與所有走 `research/db.py` pg-attach 模式的腳本,
在最近三個月看不到半個金控的營收。

---

## 三、🔴 BUG 2:cache 與 PG 是兩條會互相覆蓋的寫入路徑

- `research/crawl/sources/operating_revenue.py::refresh` 每次執行都重抓**最近 3 個月**
  (`_REFRESH_MONTHS = 3`),經 `research/crawl/sink.py::Sink.upsert`(刪整個
  `(market,type,year,month)` 再插)**直接寫進 `cache.duckdb`,完全不經過 PostgreSQL**。
- `research/cache_tables.py:20-23` 反過來:`os.remove(DB_PATH)` 之後
  `CREATE TABLE operating_revenue AS SELECT ... FROM pg.public.operating_revenue`
  —— **從 PG 全砍重建**。

兩者互蓋。目前 cache 領先(1,081 列);**只要跑一次 CLAUDE.md 規定的資料更新
Step 2(`research/cache_tables.py`),cache 就會退回 PG 的 1,062 列**,富邦金三個月的
營收從 cache 消失。而 `research/serenity/daily.py:242`(live 每日 loop)正是會呼叫它的地方。

**這代表回測不可重現**:同一份程式、同一個 cache 檔名,結果取決於今天早上先跑了哪一條。
`operating_revenue` 是 S / Serenity 進場訊號的原料(`research/serenity/daily.py:135`
讀 `monthly_revenue_yoy`、`research/apex/data.py:223`),屬 money path。

---

## 四、🔴 BUG 3:重抓舊月份會刪掉已下市公司的歷史(生存者偏誤)

| 代號 | 名稱 | 最後交易日(cache daily_quote) | 2026-04 月營收在 PG | 在 cache |
|---|---|---|---|---|
| 3426 | 台興 | 2026-06-01 | 54,906 ✓ | ✗ 整列消失 |
| 4987 | 科誠 | 2026-05-20 | 122,570 ✓ | ✗ |
| 6806 | 森崴能源 | 2026-06-22 | 1,052,473 ✓ | ✗ |

三家在 05-13 抓下的原始 CSV 裡都在(可 `grep '"3426"' data/operating_revenue/tpex/2026/2026_4_c.csv` 驗證)。

**機制**:MOPS 的彙總檔是「**當下上市清單**」的快照,公司下市後,連它過去月份的列
也會從檔案裡消失;`Sink.upsert` 又是「刪整月再插」,於是 cache 裡那幾個月的歷史
被一起抹掉。`_REFRESH_MONTHS=3` 表示**每家公司下市前最後 3 個月的營收都有被抹掉的風險**。
這正是會讓回測憑空變好看的那種偏誤。

---

## 五、🔴 BUG 4:6 碼 TDR 被 Python 爬蟲的正則吃掉

`research/crawl/sources/operating_revenue.py:36`

```python
_CODE = re.compile(r"^\d{4}[0-9A-Z]?$")   # 最多 5 碼
```

存託憑證是 6 碼:912000 晨訊科-DR、910069、912398 → `if not _CODE.match(code): continue`
直接跳過。證據:cache 內 912000 有 2026-01..03(那是從 PG 重建來的),
2026-04..06 消失(那三個月被 Python 爬蟲重寫過);PG 三個月都有。

---

## 六、🟡 我們只留「第一次公告版」,不追更正

同一筆營收事後被更正,我們不會知道:

| 代號 | 月份 | 原始檔(05-13)= PG | cache(7 月重抓) |
|---|---|---|---|
| 8942 森鉅 | 2026-04 | 250,438 | **213,316** |
| 1220 台榮 | 2026-04 | 300,795 | 299,352 |
| 3095 及成 | 2026-04 | 73,276 | 78,483 |
| 6426 統新 | 2026-05 | 67,251 | 67,213 |

更廣的量測(PG 自帶的「去年當月營收」欄 vs 我們自己一年前存的那一列,2014 年起):
254,381 對中 **7,206 對不一致(2.8%)**、**323 對差距 >50%**,逐年穩定(不是最近才壞)。
原始檔備註自己就寫明原因,例如 4950 金耘國際 2025-10:
「因113年12月適用組織重組,追溯113年9月,故去年同期申報數與去年公告金額不同」。

**這是來源特性不是解析錯**,但用的人要知道:我們的歷史營收是「首次公告值」。
好處是天然 PIT(當時看得到的就是這個),壞處是任何拿本表做「同比」的算式,
分母若取自我們自己一年前那列,會和 MOPS 現在的口徑不同。

---

## 七、🟢 已驗證正確的部分(負結果留檔,不要再查一次)

1. **解析欄位對齊零錯誤**(這是 A 維度的強力旁證)。IFRS 後(2013+)拿 PG 自帶的
   三組欄位互相驗算:
   - 去年同月增減% vs (當月/去年當月-1)×100 → 不合 **0 / 277,117**
   - 上月比較增減% vs (當月/上月-1)×100 → 不合 **0 / 276,270**
   - 累計前期比較增減% vs (累計/去年累計-1)×100 → 不合 **0 / 277,757**
   IFRS 前(HTML 來源)分別 18 / 2 / 20 例(約 0.01%),屬來源自身四捨五入。
2. **抽樣逐欄比對 15/15 全同**(2010-07、2020-10、2023-08 各 5 檔,9 欄全同)。
3. **月份序列零缺口**:twse/tpex `consolidated` 2005-01..2026-06 共 258 個月、零缺;
   `individual` 2001-06..2012-12 共 139 個月、零缺。最新月 = 2026-06 **正確**
   (7 月營收要 8/10 前才公告),資料不算過期。
4. **無重複鍵**(market,type,year,month,company_code 零重複)、**無未來月份**
   (max = 2026-06)、**無壞月份/壞年份**。
5. **負營收 800 筆都是真的**:集中金融保險業(250)、金融業(232)、金控控股口徑
   (73)、證券(33)。非金融的兩大戶經原始檔備註證實:
   - 2905 三商 2025-06 = -3,729,844,備註「主係子公司提列外匯價格變動準備淨變動」
   - 6901 鑽石投資 2025-03 = -646,593,備註「營業收入為投資標的之評價損益」
6. **零營收 1,435 筆是真的**:2013 年後集中生技醫療業(624)、建材營造(226)
   —— 與 CLAUDE.md 記載的已知真實邊界一致(生技無產品期、營建完工比例法)。
7. **型別無降級**:PG `double precision`→DuckDB `DOUBLE`、`varchar`→`VARCHAR`、
   `integer`→`INTEGER`,一一對應。
8. **`revenue_first_seen.parquet` 未被污染**:它用自己的 parquet 當記憶去比對新代號,
   cache 內容翻覆不會產生重複的 first_seen(已檢查 1,962 列、2026-06 全月)。

---

## 八、⚪ 不是 bug,但用的人要知道

1. **cache 只有 9 欄,PG 有 17 欄**。cache 刻意丟掉:上月營收、去年當月營收、
   上月比較增減%、累計營收、去年累計營收、累計前期比較增減%。要做累計營收或
   MoM 的研究,cache 不夠用,得回 PG。
2. **原始檔的「備註」欄從來沒進資料庫**(`FinancialReader.readOperatingRevenue`
   的 `.init` 把最後一欄丟掉)。而 CLAUDE.md 判斷「負營收是不是真的」的標準做法
   正是看備註 —— 現在只能回 `data/operating_revenue/<market>/<year>/*.csv` 查。
3. **2013 年前 `individual` 與 `consolidated` 併存**:27,917 組 (年,月,代號) 同時
   有兩種 type(全部 ≤ 2012)。不篩 `type` 的查詢在 2005-2012 會**重複計算**。
   `research/apex/data.py:218-228` 有正確去重(優先 consolidated);
   `research/serenity/daily.py:135` 沒篩,但只讀近月故無實害。
4. **檔案落地位置不一致**:2025-06..2025-12、2026-01..2026-03 的檔案躺在
   `data/operating_revenue/<market>/` 根目錄,2026-04..06 才在 `<year>/` 子目錄。
   不影響讀取(`getMarketFilesFromDirectory` 兩處都掃),但礙眼。

---

## 九、🔴 旁證:這張表唯一的稽核腳本是死的

```
$ uv run --project research python research/audits/05_revenue_audit.py
_duckdb.BinderException: Binder Error: Catalog "pg" does not exist!
```

`research/db.py::connect()` 預設 `use_cache=True`,cache 存在就回 cache 連線;
腳本卻寫死查 `pg.public.operating_revenue`。CLAUDE.md 仍把它列為可跑指令。
**沒人在稽核這張表 —— 這就是 2026-04 的洞能安靜躺三個月的原因。**

---

## 十、建議修法(由主流程決定是否執行)

1. **補抓**:重抓 2026-04、2026-05、2026-06 三個月的 `t21sc03`,twse 與 tpex 各一個檔
   (endpoint:`POST https://mopsov.twse.com.tw/server-java/FileDownLoad`,
   form `step=9&functionName=show_file&filePath=/home/html/nas/t21/sii/(或 otc/)&fileName=t21sc03_115_<月>.csv`),
   然後 `sbt "runMain Main read operating_revenue"`。
2. **改重整視窗為「以內容判定」而非「以日期判定」**:每次 `update` 都重抓最近 N 個月
   (N ≥ 3),比對列數 / 代號集合,**有變化才 delete+insert**;不要再用
   `dayOfMonth <= 15` 這個沒有量測出處的魔術數字。
3. **merge 而非 replace**:import 與 `Sink.upsert` 都改成 upsert-by-key,
   **只新增與更新,不刪除既有代號** —— 這樣下市公司的歷史不會被抹掉,
   TDR 也不會因為某一條路徑的正則而消失。
4. **`_CODE` 正則改成允許 6 碼**(`^\d{4,6}[0-9A-Z]?$` 或直接放寬到 `^\d{4,6}$`),
   並對「本次抓到的代號數 < 上次」fail-loud。
5. **單一寫入者**:要嘛 Python 爬蟲也回寫 PG,要嘛 `cache_tables.py` 對
   `operating_revenue` 改成 merge 不 drop。現在這樣「誰後跑誰贏」是不可重現的。
6. **修好 `research/audits/05_revenue_audit.py`**:改用 `connect(use_cache=False)`
   或改查 cache 的欄位,並加進每日 loop 的健康檢查。
7. **守護**:加一條測試 —— 對每個 `(market,type,year,month)`,cache 的列數必須
   ≥ PG 的列數且代號集合為超集(或明確列出可接受的差集原因),紅燈即擋。

---

*稽核時間 2026-07-22;cache 版本 `var/cache/cache.duckdb` mtime 2026-07-21 08:16;
本報告所有數字皆由上述指令現場量測,未引用任何既有結論。*
