import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object Settings {
  private val conf: Config = ConfigFactory.load
  val ETFs = Set("0050", "0051", "0052", "0053", "0054", "0055", "0056", "0057", "0058", "0059", "006203", "006204", "006208", "00690", "00692", "00701", "00713", "00730", "00728", "00731", "00733", "00742", "006201")

  object financialAnalysis {
    val page: String = conf.getString("data.financialAnalysis.page")
    val file: String = conf.getString("data.financialAnalysis.file")
    val dir: String = conf.getString("data.financialAnalysis.dir")
  }

  object operatingRevenue {
    val dir: String = conf.getString("data.operatingRevenue.dir")

    object beforeIFRSs {
      val file: String = conf.getString("data.operatingRevenue.beforeIFRSs.file")
    }

    object afterIFRSs {
      val file: String = conf.getString("data.operatingRevenue.afterIFRSs.file")
    }

  }

  val quarterlyReportDir: String = conf.getString("data.quarterlyReport.dir")

  object dailyQuote {
    val file: String = conf.getString("data.dailyQuote.file")
    val dir: String = conf.getString("data.dailyQuote.dir")
  }

  object index {
    val file: String = conf.getString("data.index.file")
    val dir: String = conf.getString("data.index.dir")
  }

  object statementOfComprehensiveIncome {
    val page: String = conf.getString("data.statementOfComprehensiveIncome.page")
    val file: String = conf.getString("data.statementOfComprehensiveIncome.file")
    val dir: String = conf.getString("data.statementOfComprehensiveIncome.dir")
  }

  object exRightDividend {
    val file: String = conf.getString("data.exRightDividend.file")
    val dir: String = conf.getString("data.exRightDividend.dir")
  }

}
