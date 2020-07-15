package setting

import java.time.LocalDate

case class IncomeStatementSetting(year: Int = LocalDate.now.getYear - 1, quarter: Int = if (LocalDate.now.getMonthValue < 4) 3 else 4) extends Setting {

  class TwseBeforeIFRSsDetail extends TwseDetail(LocalDate.of(1989, 1, 1), None, LocalDate.of(year, quarter, 1)) {
    private val y = super.endDate.getYear - 1911
    val file: String = conf.getString("data.incomeStatement.file")
    val dir: String = conf.getString("data.incomeStatement.dir.twse")
    override val page: String = conf.getString("data.incomeStatement.page.beforeIFRSs")
    override val url: String = file
    override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_b_"

    override def formData = Map(
      "encodeURIComponent" -> "1",
      "step" -> "1",
      "firstin" -> "1",
      "off" -> "1",
      "isQuery" -> "Y",
      "TYPEK" -> "sii",
      "year" -> y.toString,
      "season" -> s"0${super.endDate.getMonthValue}")
  }

  class TwseAfterIFRSsDetail extends TwseBeforeIFRSsDetail {
    override val page: String = conf.getString("data.incomeStatement.page.afterIFRSs")
    override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_a_"
  }

  class TpexBeforeIFRSsDetail extends TwseBeforeIFRSsDetail {
    override val dir: String = conf.getString("data.incomeStatement.dir.tpex")

    override def formData: Map[String, String] = super.formData + ("TYPEK" -> "otc")
  }

  class TpexAfterIFRSsDetail extends TpexBeforeIFRSsDetail {
    override val page: String = conf.getString("data.incomeStatement.page.afterIFRSs")
    override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_a_"
  }

  val twse: Detail = new TwseAfterIFRSsDetail
  val tpex: Detail = new TpexAfterIFRSsDetail
  val markets: Seq[Detail] = year match {
    case y if y < 1993 => Seq(new TwseBeforeIFRSsDetail)
    case y if y < 2013 => Seq(new TwseBeforeIFRSsDetail, new TpexBeforeIFRSsDetail)
    case _ => Seq(new TwseAfterIFRSsDetail, new TpexAfterIFRSsDetail)
  }
}
