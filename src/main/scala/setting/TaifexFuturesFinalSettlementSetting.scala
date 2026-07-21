package setting

import java.time.LocalDate

case class TaifexFuturesFinalSettlementSetting(date: LocalDate = LocalDate.now) extends Setting {
  val taifex: Detail = new Detail(LocalDate.of(1998, 1, 1), None, date) {
    protected[this] val file: String = conf.getString("data.taifex.futuresFinalSettlement.file")
    val dir: String = conf.getString("data.taifex.futuresFinalSettlement.dir")

    override def url: String = file
  }

  val markets: Seq[Detail] = Seq(taifex)
}
