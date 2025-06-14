package setting

import java.time.LocalDate

case class ETFSetting() extends Setting {
  private val allDetail = new Detail(LocalDate.now(), None, LocalDate.now()) {
    override protected[this] val file: String = conf.getString("data.etf.file.all")
    override val fileName = "all.json"
    override val dir: String = conf.getString("data.etf.dir")

    override def url: String = file
  }

  private val domesticDetail = new Detail(LocalDate.now(), None, LocalDate.now()) {
    override protected[this] val file: String = conf.getString("data.etf.file.domestic")
    override val fileName = "domestic.json"
    override val dir: String = conf.getString("data.etf.dir")

    override def url: String = file
  }

  private val foreignDetail = new Detail(LocalDate.now(), None, LocalDate.now()) {
    override protected[this] val file: String = conf.getString("data.etf.file.foreign")
    override val fileName = "foreign.json"
    override val dir: String = conf.getString("data.etf.dir")

    override def url: String = file
  }

  override val markets: Seq[Detail] = Seq(allDetail, domesticDetail, foreignDetail)
}

