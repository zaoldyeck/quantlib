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
    val file: String = conf.getString("data.operatingRevenue.file")
    val dir: String = conf.getString("data.operatingRevenue.dir")
  }

  val quarterlyReportDir: String = conf.getString("data.quarterlyReport.dir")
}
