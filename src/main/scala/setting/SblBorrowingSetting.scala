package setting

import java.time.LocalDate

/**
 * 借券賣出餘額（機構結構性空頭，日頻）
 * twse https://www.twse.com.tw/exchangeReport/TWT93U?response=csv&date=YYYYMMDD from 2016-01-04 (CSV)
 * tpex https://www.tpex.org.tw/www/zh-tw/margin/sbl?date=YYY/MM/DD from ~2013 (JSON)
 *
 * 欄位：股票代號、股票名稱、借券賣出前日餘額、當日賣出、當日還券、當日調整、當日餘額、次一營業日限額
 * (融券部分在 TWT93U 同檔但已由 margin_transactions 涵蓋)
 */
case class SblBorrowingSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2016, 1, 4), None, date) {
    val file: String = conf.getString("data.sbl.twse.file")
    val dir: String = conf.getString("data.sbl.twse.dir")

    // TWT93U CSV: title row → group header (融券/借券賣出) → column header starting "代號".
    override def validate(downloaded: java.io.File): Option[String] =
      validateCSVSchema(
        downloaded,
        expectedHeaderKeywords = Seq("借券賣出", "代號"),
        minDataRows = 20,
        encoding = "Big5-HKSCS"
      )
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2013, 1, 2), None, date) {
    val file: String = conf.getString("data.sbl.tpex.file")
    val dir: String = conf.getString("data.sbl.tpex.dir")

    // TPEx returns JSON with totalCount=0 on non-trading days (before 2013 often empty).
    // Empty-data is treated by Crawler.isMarketHolidayResponse as holiday — good.
    // Genuine failures still land as HTML or empty file (< 50 bytes) → handled upstream.
    override def validate(downloaded: java.io.File): Option[String] = None
  }

  val markets: Seq[Detail] = Seq(twse, tpex)
}
