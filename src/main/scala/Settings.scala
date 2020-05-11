import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object Settings {
  private val conf: Config = ConfigFactory.load

  object financialAnalysis {
    val page: String = conf.getString("data.financialAnalysis.page")
    val file: String = conf.getString("data.financialAnalysis.file")
    val dir: String = conf.getString("data.financialAnalysis.dir")
  }

  object operatingRevenue {
    val dir: String = conf.getString("data.operatingRevenue.dir")

    object beforeIFRSs {
      val page: String = conf.getString("data.financialAnalysis.beforeIFRSs.page")
      val file: String = conf.getString("data.financialAnalysis.beforeIFRSs.file")
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
