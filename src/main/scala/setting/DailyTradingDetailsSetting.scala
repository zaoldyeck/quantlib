package setting

import java.time.LocalDate

case class DailyTradingDetailsSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2012, 5, 2), None, date) {
    val file: String = conf.getString("data.dailyTradingDetails.twse.file")
    val dir: String = conf.getString("data.dailyTradingDetails.twse.dir")
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2007, 4, 23), None, date) {
    val file: String = if (endDate.isBefore(LocalDate.of(2014, 12, 1))) conf.getString("data.dailyTradingDetails.tpex.file.before201412") else conf.getString("data.dailyTradingDetails.tpex.file.after201412")
    val dir: String = conf.getString("data.dailyTradingDetails.tpex.dir")
  }

  val markets = Seq(twse, tpex)

}