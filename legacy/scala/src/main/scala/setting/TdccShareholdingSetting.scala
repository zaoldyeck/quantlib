package setting

import java.time.LocalDate

/**
 * TDCC 集保戶股權分散表（週頻）
 * https://opendata.tdcc.com.tw/getOD.ashx?id=1-5
 *
 * Endpoint has NO date parameter — always returns the LATEST week's full CSV
 * (all tickers × 17 holding tiers). Historical data must be backfilled via
 * mirrors (神秘金字塔 / data.gov.tw archives) — see Task #20.
 *
 * Download filename uses the fetch date (YYYY_M_D.csv). Reader derives the
 * real data_date from the CSV's first data column.
 *
 * Columns of CSV: 資料日期,證券代號,持股分級(1-17),人數,股數,占集保庫存數比例%
 */
case class TdccShareholdingSetting(date: LocalDate = LocalDate.now) extends Setting {
  val weekly: Detail = new Detail(LocalDate.of(2008, 1, 1), None, date) {
    val file: String = conf.getString("data.tdcc.file")
    val dir: String = conf.getString("data.tdcc.dir")
    def url: String = file // static endpoint — no query params
  }

  val markets: Seq[Detail] = Seq(weekly)
}
