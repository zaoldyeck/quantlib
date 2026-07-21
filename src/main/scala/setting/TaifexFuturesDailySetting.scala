package setting

import java.time.LocalDate

case class TaifexFuturesDailySetting(date: LocalDate = LocalDate.now) extends Setting {
  val taifex: Detail = new Detail(LocalDate.of(1998, 1, 1), None, date) {
    protected[this] val file: String = conf.getString("data.taifex.futuresDaily.file")
    val dir: String = conf.getString("data.taifex.futuresDaily.dir")

    override def url: String = file

    override def validate(downloaded: java.io.File): DownloadValidation =
      validateCSVSchema(
        downloaded,
        expectedHeaderKeywords = Seq("交易日期", "契約", "到期月份", "成交量", "結算價", "未沖銷契約數"),
        minDataRows = 1,
        minHeaderColumns = 18
      )
  }

  val markets: Seq[Detail] = Seq(taifex)
}
