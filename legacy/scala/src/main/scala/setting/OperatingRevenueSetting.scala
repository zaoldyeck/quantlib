package setting

import java.time.LocalDate

case class OperatingRevenueSetting(year: Int = LocalDate.now.getYear, month: Int = LocalDate.now.getMonthValue) extends Setting {

  class TwseBeforeIFRSsIndividualDetail extends TwseDetail(LocalDate.of(2001, 6, 1), None, LocalDate.of(year, month, 1)) {
    private val y = super.endDate.getYear - 1911
    private val m = super.endDate.getMonthValue
    val file: String = conf.getString("data.operatingRevenue.file.beforeIFRSs.individual.twse")
    val dir: String = conf.getString("data.operatingRevenue.dir.twse")
    override val fileName: String = s"${super.endDate.getYear}_${m}_i.html"

    override def url: String = file + s"${y}_${m}.html"
  }

  class TwseBeforeIFRSsConsolidatedDetail extends TwseDetail(LocalDate.of(2005, 1, 1), None, LocalDate.of(year, month, 1)) {
    private val y = super.endDate.getYear - 1911
    private val m = super.endDate.getMonthValue
    val file: String = conf.getString("data.operatingRevenue.file.beforeIFRSs.consolidated")
    val dir: String = conf.getString("data.operatingRevenue.dir.twse")
    override val page: String = conf.getString("data.operatingRevenue.page")
    override val url: String = file
    override val fileName: String = s"${super.endDate.getYear}_${m}_c.csv"

    override def formData = Map(
      "encodeURIComponent" -> "1",
      "step" -> "1",
      "firstin" -> "1",
      "off" -> "1",
      "TYPEK" -> "sii",
      "year" -> y.toString,
      "month" -> "%02d".format(super.endDate.getMonthValue))
  }

  class TwseAfterIFRSsDetail extends TwseDetail(LocalDate.of(2013, 1, 1), None, LocalDate.of(year, month, 1)) {
    private val y = super.endDate.getYear - 1911
    private val m = super.endDate.getMonthValue
    val file: String = conf.getString("data.operatingRevenue.file.afterIFRSs")
    val dir: String = conf.getString("data.operatingRevenue.dir.twse")
    override val url: String = file
    override val fileName: String = s"${super.endDate.getYear}_${m}_c.csv"

    override def formData = Map(
      "step" -> "9",
      "functionName" -> "show_file",
      "filePath" -> "/home/html/nas/t21/sii/",
      "fileName" -> s"t21sc03_${y}_$m.csv")
  }

  class TpexBeforeIFRSsIndividualDetail extends TwseBeforeIFRSsIndividualDetail {
    override val file: String = conf.getString("data.operatingRevenue.file.beforeIFRSs.individual.tpex")
    override val dir: String = conf.getString("data.operatingRevenue.dir.tpex")
  }

  class TpexBeforeIFRSsConsolidatedDetail extends TwseBeforeIFRSsConsolidatedDetail {
    override val dir: String = conf.getString("data.operatingRevenue.dir.tpex")

    override def formData: Map[String, String] = super.formData + ("TYPEK" -> "otc")
  }

  class TpexAfterIFRSsDetail extends TwseAfterIFRSsDetail {
    override val dir: String = conf.getString("data.operatingRevenue.dir.tpex")

    override def formData: Map[String, String] = super.formData + ("filePath" -> "/home/html/nas/t21/otc/")
  }

  val twse = new TwseBeforeIFRSsIndividualDetail
  val tpex = new TpexBeforeIFRSsIndividualDetail
  val markets: Seq[Detail] = year match {
    case y if y < 2005 => Seq(new TwseBeforeIFRSsIndividualDetail, new TpexBeforeIFRSsIndividualDetail)
    case y if y < 2013 => Seq(new TwseBeforeIFRSsIndividualDetail, new TwseBeforeIFRSsConsolidatedDetail, new TpexBeforeIFRSsIndividualDetail, new TpexBeforeIFRSsConsolidatedDetail)
    case _ => Seq(new TwseAfterIFRSsDetail, new TpexAfterIFRSsDetail)
  }
}
