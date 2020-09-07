package setting

import java.time.LocalDate

case class FinancialAnalysisSetting(year: Int = LocalDate.now.getYear) extends Setting {

  class TwseBeforeIFRSsDetail extends TwseDetail(LocalDate.of(1989, 1, 1), None, LocalDate.of(year, 1, 1)) {
    private val y = super.endDate.getYear - 1911
    val file: String = conf.getString("data.financialAnalysis.file")
    val dir: String = conf.getString("data.financialAnalysis.dir.twse")
    override val page: String = conf.getString("data.financialAnalysis.page")
    override val url: String = file
    override val fileName = s"${super.endDate.getYear}_b.csv"

    override def formData = Map(
      "encodeURIComponent" -> "1",
      "step" -> "1",
      "firstin" -> "1",
      "off" -> "1",
      "TYPEK" -> "sii",
      "year" -> y.toString)
  }

  class TwseAfterIFRSsDetail extends TwseBeforeIFRSsDetail {
    override val fileName = s"${super.endDate.getYear}_a.csv"
    override val formData: Map[String, String] = super.formData + ("ifrs" -> "Y")
  }

  class TpexBeforeIFRSsDetail extends TwseBeforeIFRSsDetail {
    override val dir: String = conf.getString("data.financialAnalysis.dir.tpex")

    override def formData: Map[String, String] = super.formData + ("TYPEK" -> "otc")
  }

  class TpexAfterIFRSsDetail extends TpexBeforeIFRSsDetail {
    override val fileName = s"${super.endDate.getYear}_a.csv"
    override val formData: Map[String, String] = super.formData + ("ifrs" -> "Y")
  }

  val twse = new TwseAfterIFRSsDetail
  val tpex = new TpexAfterIFRSsDetail
  val markets: Seq[Detail] = year match {
    case y if y < 1993 => Seq(new TwseBeforeIFRSsDetail)
    case y if y < 2012 => Seq(new TwseBeforeIFRSsDetail, new TpexBeforeIFRSsDetail)
    case y if y > 2014 => Seq(new TwseAfterIFRSsDetail, new TpexAfterIFRSsDetail)
    case _ => Seq(new TwseBeforeIFRSsDetail, new TwseAfterIFRSsDetail, new TpexBeforeIFRSsDetail, new TpexAfterIFRSsDetail)
  }
}
