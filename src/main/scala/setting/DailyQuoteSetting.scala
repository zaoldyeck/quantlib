package setting

import java.time.LocalDate

case class DailyQuoteSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2004, 2, 11), None, date) {
    val file: String = conf.getString("data.dailyQuote.twse.file")
    val dir: String = conf.getString("data.dailyQuote.twse.dir")
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2007, 7, 2), None, date) {
    val file: String = conf.getString("data.dailyQuote.tpex.file")
    val dir: String = conf.getString("data.dailyQuote.tpex.dir")
  }

  val markets = Seq(twse, tpex)
}