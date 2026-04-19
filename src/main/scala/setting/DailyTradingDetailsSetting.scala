package setting

import java.time.LocalDate

case class DailyTradingDetailsSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2012, 5, 2), None, date) {
    val file: String = conf.getString("data.dailyTradingDetails.twse.file")
    val dir: String = conf.getString("data.dailyTradingDetails.twse.dir")

    // TWSE 三大法人買賣超日報 — modern format has 19 header fields (size=20 with trailing comma).
    // Rejects: empty files, HTML error pages, and the legacy 15-field format seen on 2026-03-16
    // that crashed the reader with IndexOutOfBoundsException.
    override def validate(downloaded: java.io.File): Option[String] =
      validateCSVSchema(
        downloaded,
        expectedHeaderKeywords = Seq("三大法人買賣超", "證券代號"),
        minDataRows = 100,
        minHeaderColumns = 19
      )
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2007, 4, 23), None, date) {
    val file: String = if (endDate.isBefore(LocalDate.of(2014, 12, 1))) conf.getString("data.dailyTradingDetails.tpex.file.before201412") else conf.getString("data.dailyTradingDetails.tpex.file.after201412")
    val dir: String = conf.getString("data.dailyTradingDetails.tpex.dir")

    // TPEx 三大法人買賣超日報 — post-2014/12 format. Size distribution observed in data/: 24 & 44.
    // The 44-column format is the newest; 24 is the previous version. Both use "代號" as header.
    override def validate(downloaded: java.io.File): Option[String] =
      validateCSVSchema(
        downloaded,
        expectedHeaderKeywords = Seq("代號"),
        minDataRows = 100,
        minHeaderColumns = 20
      )
  }

  val markets = Seq(twse, tpex)

}