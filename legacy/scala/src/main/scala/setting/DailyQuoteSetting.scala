package setting

import java.time.LocalDate

case class DailyQuoteSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2004, 2, 11), None, date) {
    val file: String = conf.getString("data.dailyQuote.twse.file")
    val dir: String = conf.getString("data.dailyQuote.twse.dir")

    override def validate(downloaded: java.io.File): DownloadValidation = {
      val minguoDate = s"${date.getYear - 1911}年${"%02d".format(date.getMonthValue)}月${"%02d".format(date.getDayOfMonth)}日"
      validateCSVHeaderDate(downloaded, expectedDateMarker = minguoDate, minDataRows = 500) match {
        case DownloadValidation.Invalid(reason) if date.getDayOfWeek.getValue >= 6 =>
          DownloadValidation.NoData(reason)
        case other => other
      }
    }
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2007, 7, 2), None, date) {
    val file: String = conf.getString("data.dailyQuote.tpex.file")
    val dir: String = conf.getString("data.dailyQuote.tpex.dir")

    override def validate(downloaded: java.io.File): DownloadValidation = {
      val minguoDate = s"${date.getYear - 1911}/${"%02d".format(date.getMonthValue)}/${"%02d".format(date.getDayOfMonth)}"
      validateCSVHeaderDate(downloaded, expectedDateMarker = s"資料日期:$minguoDate", minDataRows = 100) match {
        case DownloadValidation.Invalid(reason) if date.getDayOfWeek.getValue >= 6 =>
          DownloadValidation.NoData(reason)
        case other => other
      }
    }
  }

  val markets = Seq(twse, tpex)
}
