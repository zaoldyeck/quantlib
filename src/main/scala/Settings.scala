import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object Settings {
  private val conf: Config = ConfigFactory.load
  val quarterlyReportDir: String = conf.getString("data.quarterlyReport.dir")
  val financialAnalysisDir: String = conf.getString("data.financialAnalysis.dir")
}
