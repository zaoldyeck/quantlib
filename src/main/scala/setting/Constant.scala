package setting

import com.typesafe.config.{Config, ConfigFactory}

object Constant {
  private val conf: Config = ConfigFactory.load
  val ETFs = Set("0050", "0051", "0052", "0053", "0054", "0055", "0056", "0057", "0058", "0059", "006203", "006204", "006208", "00690", "00692", "00701", "00713", "00730", "00728", "00731", "00733", "00742", "006201")
  val quarterlyReportDir: String = conf.getString("data.quarterlyReport.dir")
}
