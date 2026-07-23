package setting

import java.time.LocalDate

case class FinancialStatementsSetting(year: Int = LocalDate.now.getYear - 1, quarter: Int = if (LocalDate.now.getMonthValue < 4) 3 else 4, companyCode: String = "") extends Setting {

  class IndividualDetail extends Detail(LocalDate.of(2009, 4, 1), None, LocalDate.of(year, quarter, 1)) {
    val file: String = year match {
      case y if y < 2013 => conf.getString("data.financial_statements.file.beforeIFRSs.individual")
      case _ => conf.getString("data.financial_statements.file.afterIFRSs.individual")
    }
    val dir: String = conf.getString("data.financial_statements.dir") + s"/${super.endDate.getYear}_${super.endDate.getMonthValue}"
    val url: String = file + (year match {
      case y if y < 2013 => s"${super.endDate.getYear}&SEASON1=${super.endDate.getMonthValue}&comp_id=$companyCode"
      case _ => s"${super.endDate.getYear}&SSEASON=${super.endDate.getMonthValue}&CO_ID=$companyCode"
    })
    override val fileName: String = s"$companyCode.html"
  }

  class ConsolidatedDetail extends Detail(LocalDate.of(2009, 4, 1), None, LocalDate.of(year, quarter, 1)) {
    val file: String = year match {
      case y if y < 2013 => conf.getString("data.financial_statements.file.beforeIFRSs.consolidated")
      case y if y < 2019 => conf.getString("data.financial_statements.file.afterIFRSs.consolidated")
      case _ => conf.getString("data.financial_statements.file.afterIFRSs.bulkInstanceDocuments")
    }
    val dir: String = year match {
      case y if y < 2019 => conf.getString("data.financial_statements.dir") + s"/${super.endDate.getYear}_${super.endDate.getMonthValue}"
      case _ => conf.getString("data.financial_statements.dir")
    }
    val url: String = file + (year match {
      case y if y < 2013 => s"${super.endDate.getYear}&SEASON1=${super.endDate.getMonthValue}&comp_id=$companyCode"
      case y if y < 2019 => s"${super.endDate.getYear}&SSEASON=${super.endDate.getMonthValue}&CO_ID=$companyCode"
      case _ => s"${super.endDate.getYear}/&fileName=tifrs-${super.endDate.getYear}Q${super.endDate.getMonthValue}.zip"
    })
    override val fileName: String = year match {
      case y if y < 2019 => s"$companyCode.html"
      case _ => s"${super.endDate.getYear}_${super.endDate.getMonthValue}.zip"
    }
  }

  val twse: Detail = new ConsolidatedDetail
  val tpex: Detail = new ConsolidatedDetail
  val markets: Seq[Detail] = year match {
    case y if y < 2019 => Seq(new ConsolidatedDetail, new IndividualDetail)
    case _ => Seq(new ConsolidatedDetail)
  }
}
