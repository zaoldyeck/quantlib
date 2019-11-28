import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object Settings {
  private val conf: Config = ConfigFactory.load

  object financialAnalysis {
    val dir: String = conf.getString("data.financialAnalysis.dir")
    val page: String = conf.getString("data.financialAnalysis.page")
    val file: String = conf.getString("data.financialAnalysis.file")
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
}
