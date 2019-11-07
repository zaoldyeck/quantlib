package db.table

import java.time.LocalDate

import slick.jdbc.H2Profile.api._

/**
 * https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html
 * 每日收盤行情
 *
 * @param tag
 */
class DailyQuote(tag: Tag) extends Table[(Long, LocalDate, String, String, Long, Int, Long, Option[Double], Option[Double], Option[Double], Option[Double], Int, Double, Option[Double], Int, Option[Double], Int, Double)](tag, "daily_quote") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def date = column[LocalDate]("date")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def tradeVolume = column[Long]("trade_volume")

  def transaction = column[Int]("transaction")

  def tradeValue = column[Long]("trade_value")

  def openingPrice = column[Option[Double]]("opening_price")

  def highestPrice = column[Option[Double]]("highest_price")

  def lowestPrice = column[Option[Double]]("lowest_price")

  def closingPrice = column[Option[Double]]("closing_price")

  def direction = column[Int]("direction") // -1, 0 , 1

  def change = column[Double]("change")

  def lastBestBidPrice = column[Option[Double]]("last_best_bid_price")

  def lastBestBidVolume = column[Int]("last_best_bid_volume")

  def lastBestAskPrice = column[Option[Double]]("last_best_ask_price")

  def lastBestAskVolume = column[Int]("last_best_ask_volume")

  def priceEarningRatio = column[Double]("price_earning_ratio")

  def idx = index("idx_a", (date, companyCode), unique = true)

  def * = (id, date, companyCode, companyName, tradeVolume, transaction, tradeValue, openingPrice, highestPrice, lowestPrice, closingPrice, direction, change, lastBestBidPrice, lastBestBidVolume, lastBestAskPrice, lastBestAskVolume, priceEarningRatio)
}
