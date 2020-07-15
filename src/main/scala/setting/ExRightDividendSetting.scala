package setting

import java.time.LocalDate

case class ExRightDividendSetting(strDate: LocalDate = LocalDate.now, endDate: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2003, 5, 5), Some(strDate), endDate) {
    val file: String = conf.getString("data.exRightDividend.twse.file")
    val dir: String = conf.getString("data.exRightDividend.twse.dir")
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2008, 1, 2), Some(strDate), endDate) {
    val file: String = conf.getString("data.exRightDividend.tpex.file")
    val dir: String = conf.getString("data.exRightDividend.tpex.dir")
  }

  val markets = Seq(twse, tpex)
}
