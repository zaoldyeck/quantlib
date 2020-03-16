package db.table

import java.time.LocalDate

import slick.jdbc.H2Profile.api._

/**
 * https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html
 * 大盤統計資訊
 * @param tag
 */
class MarketSummary(tag: Tag) extends Table[(Long, LocalDate, String, Long, Long, Int)](tag, "market_summary") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def date = column[LocalDate]("date")

  def summary = column[String]("summary")

  def tradeValue = column[Long]("trade_value(NT$)")

  def tradeVolume = column[Long]("trade_volume(share)")

  def transaction = column[Int]("transaction")

  def idx = index("idx_marketSummary_date_summary", (date, summary), unique = true)

  def * = (id, date, summary, tradeValue, tradeVolume, transaction)
}
