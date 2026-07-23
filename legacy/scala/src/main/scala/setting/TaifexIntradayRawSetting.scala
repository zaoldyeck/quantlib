package setting

import com.typesafe.config.ConfigFactory

final case class TaifexIntradayRawSource(
  key: String,
  page: String,
  dir: String,
  description: String
)

case class TaifexIntradayRawSetting() {
  private val conf = ConfigFactory.load()
  private val base = "data.taifex.intradayRaw"

  private def source(key: String): TaifexIntradayRawSource =
    TaifexIntradayRawSource(
      key = key,
      page = conf.getString(s"$base.$key.page"),
      dir = conf.getString(s"$base.$key.dir"),
      description = conf.getString(s"$base.$key.description")
    )

  val sources: Seq[TaifexIntradayRawSource] = Seq(
    source("futuresSales"),
    source("futuresSpreadSales"),
    source("futuresSpreadOrders"),
    source("optionsSales"),
    source("flexFuturesSales")
  )
}
