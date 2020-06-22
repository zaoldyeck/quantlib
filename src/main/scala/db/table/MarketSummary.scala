package db.table

import java.time.LocalDate

//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
import slick.jdbc.H2Profile.api._

/**
 * 大盤統計資訊
 * https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html
 * from 2004-2-11
 *
 * @param tag
 */
class MarketSummary(tag: Tag) extends Table[(Long, LocalDate, String, Long, Long, Int)](tag, "market_summary") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def date = column[LocalDate]("date")

  def summary = column[String]("summary")

  def tradeValue = column[Long]("trade_value(NT$)")

  def tradeVolume = column[Long]("trade_volume(share)")

  def transaction = column[Int]("transaction")

  def idx = index("idx_MarketSummary_date_summary", (date, summary), unique = true)

  def * = (id, date, summary, tradeValue, tradeVolume, transaction)
}
