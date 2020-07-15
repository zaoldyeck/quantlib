package setting

import java.time.LocalDate

case class MarginTransactionsSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2001, 1, 2), None, date) {
    val file: String = conf.getString("data.marginTransactions.twse.file")
    val dir: String = conf.getString("data.marginTransactions.twse.dir")
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2007, 1, 2), None, date) {
    val file: String = conf.getString("data.marginTransactions.tpex.file")
    val dir: String = conf.getString("data.marginTransactions.tpex.dir")
  }

  val markets = Seq(twse, tpex)
}