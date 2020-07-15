package setting

import java.time.LocalDate

case class IndexSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2009, 1, 5), None, date) {
    val file: String = conf.getString("data.index.twse.file")
    val dir: String = conf.getString("data.index.twse.dir")
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2016, 1, 4), None, date) {
    val file: String = conf.getString("data.index.tpex.file")
    val dir: String = conf.getString("data.index.tpex.dir")
  }

  val markets = Seq(twse, tpex)
}