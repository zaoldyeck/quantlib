package setting

import java.time.LocalDate

case class IndexSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2009, 1, 5), None, date) {
    val file: String = conf.getString("data.index.twse.file")
    val dir: String = conf.getString("data.index.twse.dir")

    // TWSE index header: `"115年04月13日 價格指數(臺灣證券交易所)"` (Minguo date).
    // Rejects TWSE's silent-fallback response (e.g. returning 2018-02-18 header for a date
    // it has no data for) — a 116-byte response that passes isHtmlResponse/empty checks
    // but contains zero data rows and the wrong date.
    override def validate(downloaded: java.io.File): Option[String] = {
      val minguoDate = s"${date.getYear - 1911}年${"%02d".format(date.getMonthValue)}月${"%02d".format(date.getDayOfMonth)}日"
      validateCSVHeaderDate(downloaded, expectedDateMarker = minguoDate, minDataRows = 50)
    }
  }

  val tpex: TpexV2Detail = new TpexV2Detail(LocalDate.of(2016, 1, 4), None, date) {
    val file: String = conf.getString("data.index.tpex.file")
    val dir: String = conf.getString("data.index.tpex.dir")

    // TPEx index header has `Data Date:115/04/14` on the second line.
    override def validate(downloaded: java.io.File): Option[String] = {
      val minguoDate = s"${date.getYear - 1911}/${"%02d".format(date.getMonthValue)}/${"%02d".format(date.getDayOfMonth)}"
      validateCSVHeaderDate(downloaded, expectedDateMarker = s"Data Date:$minguoDate", minDataRows = 10)
    }
  }

  val markets = Seq(twse, tpex)
}