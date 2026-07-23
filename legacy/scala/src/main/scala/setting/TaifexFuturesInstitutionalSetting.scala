package setting

import java.time.LocalDate

case class TaifexFuturesInstitutionalSetting(date: LocalDate = LocalDate.now) extends Setting {
  val taifex: Detail = new Detail(LocalDate.of(2008, 4, 7), None, date) {
    protected[this] val file: String = conf.getString("data.taifex.futuresInstitutional.file")
    val dir: String = conf.getString("data.taifex.futuresInstitutional.dir")

    override def url: String = file

    override def validate(downloaded: java.io.File): DownloadValidation =
      validateCSVSchema(
        downloaded,
        expectedHeaderKeywords = Seq("日期", "商品名稱", "身份別", "多方交易口數", "多空未平倉口數淨額"),
        minDataRows = 1,
        minHeaderColumns = 15
      )
  }

  val markets: Seq[Detail] = Seq(taifex)
}
