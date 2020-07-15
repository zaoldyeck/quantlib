package setting

import java.time.LocalDate

case class CapitalReductionSetting(strDate: LocalDate = LocalDate.now, endDate: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2011, 1, 1), Some(strDate), endDate) {
    val file: String = conf.getString("data.capitalReduction.twse.file")
    val dir: String = conf.getString("data.capitalReduction.twse.dir")
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2013, 1, 2), Some(strDate), endDate) {
    val file: String = conf.getString("data.capitalReduction.tpex.file")
    val dir: String = conf.getString("data.capitalReduction.tpex.dir")
  }

  val markets = Seq(twse, tpex)
}