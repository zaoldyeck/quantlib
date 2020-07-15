package setting

import java.time.LocalDate

case class StockPER_PBR_DividendYieldSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2005, 9, 2), None, date) {
    val file: String = conf.getString("data.stockPER_PBR_DividendYield.twse.file")
    val dir: String = conf.getString("data.stockPER_PBR_DividendYield.twse.dir")
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2007, 1, 2), None, date) {
    val file: String = conf.getString("data.stockPER_PBR_DividendYield.tpex.file")
    val dir: String = conf.getString("data.stockPER_PBR_DividendYield.tpex.dir")
  }

  val markets = Seq(twse, tpex)
}
