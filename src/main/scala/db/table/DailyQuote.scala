package db.table

import java.time.LocalDate

import slick.jdbc.H2Profile.api._

class DailyQuote(tag: Tag) extends Table[(Long, Int, String, LocalDate, Int, Int, Long, Option[Double], Option[Double], Option[Double], Option[Double], Int, Double, Option[Double], Int, Option[Double], Int, Double)](tag, "daily_quote") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def stockId = column[Int]("stock_id")

  def stockName = column[String]("stock_name")

  def date = column[LocalDate]("date")

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

  def idx = index("idx_a", (stockId, date), unique = true)

  def * = (id, stockId, stockName, date, tradeVolume, transaction, tradeValue, openingPrice, highestPrice, lowestPrice, closingPrice, direction, change, lastBestBidPrice, lastBestBidVolume, lastBestAskPrice, lastBestAskVolume, priceEarningRatio)
}
