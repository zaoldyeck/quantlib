package setting

import java.time.LocalDate

case class BalanceSheetSetting(year: Int = LocalDate.now.getYear - 1, quarter: Int = if (LocalDate.now.getMonthValue < 4) 3 else 4) extends Setting {

  class TwseBeforeIFRSsIndividualDetail extends TwseDetail(LocalDate.of(1989, 1, 1), None, LocalDate.of(year, quarter, 1)) {
    private val y = super.endDate.getYear - 1911
    val file: String = conf.getString("data.balanceSheet.file")
    val dir: String = conf.getString("data.balanceSheet.dir.twse")
    override val page: String = conf.getString("data.balanceSheet.page.beforeIFRSs.individual")
    override val url: String = file
    override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_b_i_"

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

  class TwseBeforeIFRSsConsolidatedDetail extends TwseBeforeIFRSsIndividualDetail {
    override val page: String = conf.getString("data.balanceSheet.page.beforeIFRSs.consolidated")
    override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_b_c_"
  }

  class TwseAfterIFRSsDetail extends TwseBeforeIFRSsIndividualDetail {
    override val page: String = conf.getString("data.balanceSheet.page.afterIFRSs")
    override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_a_c_"
  }

  class TpexBeforeIFRSsIndividualDetail extends TwseBeforeIFRSsIndividualDetail {
    override val dir: String = conf.getString("data.balanceSheet.dir.tpex")

    override def formData: Map[String, String] = super.formData + ("TYPEK" -> "otc")
  }

  class TpexBeforeIFRSsConsolidatedDetail extends TpexBeforeIFRSsIndividualDetail {
    override val page: String = conf.getString("data.balanceSheet.page.beforeIFRSs.consolidated")
    override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_b_c_"
  }

  class TpexAfterIFRSsDetail extends TpexBeforeIFRSsIndividualDetail {
    override val page: String = conf.getString("data.balanceSheet.page.afterIFRSs")
    override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_a_c_"
  }

  //twse b-i 1989-1 ~ 2014-4
  //tpex b-i 1993-1 ~ 2014-4

  //twse b-c 2004-4 ~ 2014-4
  //tpex b-c 2006-1 ~ 2014-4

  //a-c 2013-1 ~
  val twse = new TwseAfterIFRSsDetail
  val tpex = new TpexAfterIFRSsDetail
  val markets: Seq[Detail] = (year, quarter) match {
    case (y, _) if y < 1993 => Seq(new TwseBeforeIFRSsIndividualDetail)
    case (y, q) if y < 2004 || (y == 2004 && q < 4) => Seq(new TwseBeforeIFRSsIndividualDetail, new TpexBeforeIFRSsIndividualDetail)
    case (y, _) if y < 2006 => Seq(new TwseBeforeIFRSsIndividualDetail, new TwseBeforeIFRSsConsolidatedDetail, new TpexBeforeIFRSsIndividualDetail)
    case (y, _) if y < 2013 => Seq(new TwseBeforeIFRSsIndividualDetail, new TwseBeforeIFRSsConsolidatedDetail, new TpexBeforeIFRSsIndividualDetail, new TpexBeforeIFRSsConsolidatedDetail)
    case (y, _) if y < 2015 => Seq(new TwseBeforeIFRSsIndividualDetail, new TwseBeforeIFRSsConsolidatedDetail, new TpexBeforeIFRSsIndividualDetail, new TpexBeforeIFRSsConsolidatedDetail, new TwseAfterIFRSsDetail, new TpexAfterIFRSsDetail)
    case _ => Seq(new TwseAfterIFRSsDetail, new TpexAfterIFRSsDetail)
  }
}
