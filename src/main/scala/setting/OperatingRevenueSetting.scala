package setting

import java.time.LocalDate

case class OperatingRevenueSetting(year: Int = LocalDate.now.getYear, month: Int = LocalDate.now.getMonthValue) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2001, 6, 1), None, LocalDate.of(year, month, 1)) {
    private val y = super.endDate.getYear - 1911
    private val m = super.endDate.getMonthValue
    val file: String = if (y < 102) conf.getString("data.operatingRevenue.twse.file.beforeIFRSs") else conf.getString("data.operatingRevenue.twse.file.afterIFRSs")
    val dir: String = conf.getString("data.operatingRevenue.twse.dir")
    override val url: String = if (y < 102) (file + s"${y}_$m.html") else file
    override val formData = Map(
      "step" -> "9",
      "functionName" -> "show_file",
      "filePath" -> "/home/html/nas/t21/sii/",
      "fileName" -> s"t21sc03_${y}_$m.csv")
    override val fileName: String = s"${super.endDate.getYear}_$m." + (if (y < 102) "html" else "csv")
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2001, 6, 1), None, LocalDate.of(year, month, 1)) {
    private val y = super.endDate.getYear - 1911
    private val m = super.endDate.getMonthValue
    val file: String = if (y < 102) conf.getString("data.operatingRevenue.tpex.file.beforeIFRSs") else conf.getString("data.operatingRevenue.tpex.file.afterIFRSs")
    val dir: String = conf.getString("data.operatingRevenue.tpex.dir")
    override val url: String = if (y < 102) (file + s"${y}_$m.html") else file
    override val formData = Map(
      "step" -> "9",
      "functionName" -> "show_file",
      "filePath" -> "/home/html/nas/t21/otc/",
      "fileName" -> s"t21sc03_${y}_$m.csv")
    override val fileName: String = s"${super.endDate.getYear}_$m." + (if (y < 102) "html" else "csv")
  }

  val markets = Seq(twse, tpex)
}
