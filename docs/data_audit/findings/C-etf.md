# C-etf:cache 表「etf」的一致性與缺漏

**結論(白話):cache 裡的 etf 表可以信,和 PostgreSQL 一模一樣。**這張表就是
一份「哪些代號是 ETF」的名單,cache 只搬了其中一欄(代號 company_code),而且搬得
分毫不差——兩邊都是 228 檔、228 個代號逐一比對零差異、位元完全相同。這張表沒有
每天一筆的時間序列(它是靜態名單,不是行情),所以「時間序列有沒有洞」對它不適用,
沒有交易日缺口要查。

**Verdict: OK**(cache 對 PG 忠實一致,cache 層不需要任何修補)

---

## 一、cache vs PostgreSQL 一致性(這是本單位的核心問題)

| 檢查 | PostgreSQL | DuckDB cache | 結果 |
|---|---|---|---|
| 列數 | 228 | 228 | 相等 |
| distinct 代號 | 228 | 228 | 相等 |
| 代號值(全量 diff) | 228 個 | 228 個 | **逐一相同,diff 全空** |
| company_code 型別 | `character varying` | `VARCHAR` | 等價,無降級 |
| null / 空字串 | 0 | 0 | 乾淨 |
| 前後/內嵌空白 | — | 0 | 乾淨,join 不會斷 |
| 能 join 到 daily_quote | — | 228 / 228 | 排除鍵命中真實代號 |

同步程式 `research/cache_tables.py:56` 對這張表是**純投影**:

```
CREATE TABLE etf AS SELECT company_code FROM pg.public.etf
```

沒有 `WHERE`、沒有去重、沒有轉型,所以 cache 必然逐列反映 PG。任務要求的
「3 個日期 × 5 檔逐欄抽樣」對這張表不適用(沒有日期維、cache 只有一欄),
因此改做**全表 228 個代號的逐一比對(100% 覆蓋,強於抽樣)**——結果位元完全一致。

`research/db.py:144` 的 pg-attach 對照 view 也只選 `company_code`,所以
cache-file 模式與 pg-attach 模式看到的 etf 完全相同,**兩條 Python 讀取路徑不會分岔**。

## 二、schema:cache 刻意只帶 1 欄(7 欄丟 6 欄)

PG etf 有 7 欄(id、listing_date、company_code、name、issuer、index、region),
cache 只帶 `company_code`,丟掉其餘 6 欄。**這是刻意的、且無害**:

- 全庫 9 個消費者(`v4.py`、`iter_13/20/24/32/37/98/99`)查 `FROM etf` 都**只取
  company_code 當排除名單**,沒有人讀 listing_date/name/issuer/region/index。
- `active_etf_ladder.py` 的 `etf.name` 是 Python 迴圈物件的屬性,不是 cache 表的欄,
  它不查 cache etf。
- 所以那 6 欄丟了也沒人受影響,型別也沒降級(唯一同步的 company_code 是等價 VARCHAR)。

## 三、時間序列缺口:不適用

etf 是**靜態 ETF 代號參考表**,沒有每日 date 欄,不存在「交易日序列」→ 沒有
「休市 vs 漏抓」可判。名單在**時間上不 stale**:listing_date 範圍
2003-06-30 ~ 2026-07-15(≤ 今日,無未來日期),最新一檔 00408A(主動第一金優股息,
2026-07-15 上市)已在表內,原始 JSON 於 2026-07-19 更新。

## 四、異常值掃描:乾淨

cache etf 只有代號字串一欄,沒有價格/成交量/本益比/日期數值欄可掃。代號本身:
228 檔**全部 0 開頭**(ETF 代號空間,無 1-9 開頭的一般股票混入)、**無重複**、
4 碼代號皆 005x/006x 經典 ETF。

## 五、缺漏(名單漏收 ETF)——真實但屬上游、對消費者零影響

etf 名單漏收約 **122 檔仍在交易的 ETF**(95 檔債券「B」、5 檔匯率避險「K」、
TPEx 富櫃50 006201 等;另有 69 檔已下市的漏收)。但要講清楚三件事:

1. **不是 cache 的錯。**上游原始 JSON 就沒有這些:`data/etf/all.json` 裡搜
   `00679B`、`006201`、`00687B` 全部找不到。缺口源自 ETF 名單來源本身(不含 TPEx
   上市 ETF 與多數債券 ETF),cache 只是忠實反映 PG。
2. **已被 A-etf 認領。**這條完整性議題(含雙幣別漏收、region 誤標、append-only
   凍結改名)已由 `docs/data_audit/_done/A-etf.json`(dim A 解析稽核,verdict SUSPECT)
   詳查,本單位不重複裁決。
3. **對現有策略零影響。**每個消費者都先套 `^[1-9][0-9]{3}$` 四碼正則。實測
   cache 裡 **0 個 etf 代號吻合這條正則**(全是 0 開頭或非四碼),等於正則早就把
   所有 ETF 排乾淨了——`NOT IN (SELECT company_code FROM etf)` 是多餘的
   belt-and-suspenders,漏收的 ETF 不會洩漏進任何策略池。

### 修法(cache 層不用動)

cache 忠實反映 PG,無需修補。若日後需要一份**完整** ETF 清單(例如未來有消費者
把 etf 當權威名單用),要修的是 dim A 的來源:`ETFSetting`/`application.conf` 的
`data.etf.file.*` 目前的名單不含 TPEx 上市與多數債券 ETF,需補一個涵蓋全市場
(TWSE+TPEx、含債券/槓桿反向/雙幣別)的 ETF 清單 endpoint,再由 `FinancialReader.readETF`
匯入(並把 append-only 改為 upsert 以反映改名/併購)。**補抓由主流程統一安排,
本稽核不自行下載。**

---

## 附:可重跑的證據指令

```bash
# 列數 + 代號全量 diff(應為 228=228、diff 全空)
psql -h localhost -p 5432 -d quantlib -tAc "SELECT company_code FROM etf ORDER BY 1" > /tmp/pg.txt
uv run --project research python -c "import duckdb; from research import paths; \
  con=duckdb.connect(str(paths.CACHE_DB),read_only=True); \
  open('/tmp/cache.txt','w').write('\n'.join(r[0] for r in con.sql('SELECT company_code FROM etf ORDER BY 1').fetchall())+'\n')"
diff /tmp/pg.txt /tmp/cache.txt   # 空 = 一致

# 排除多餘性:cache etf 有幾個代號吻合四碼正則(應為 0)
uv run --project research python -c "import duckdb; from research import paths; \
  con=duckdb.connect(str(paths.CACHE_DB),read_only=True); \
  print(con.sql(\"SELECT COUNT(*) FROM etf WHERE regexp_matches(company_code,'^[1-9][0-9]{3}\$')\").fetchone()[0])"

# 漏收清單:daily_quote 有交易但不在 etf 的 00-代號
psql -h localhost -p 5432 -d quantlib -c "SELECT COUNT(*) FROM (SELECT DISTINCT company_code FROM daily_quote \
  WHERE company_code LIKE '00%' AND company_code NOT IN (SELECT company_code FROM etf)) t"
```
