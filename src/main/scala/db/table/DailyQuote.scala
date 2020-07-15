package db.table

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._

/**
 * 每日收盤行情
 * twse https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html from 2004-2-11
 * tpex https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php from 2007-7-2
 *
 * @param tag
 */
class DailyQuote(tag: Tag) extends Table[DailyQuoteRow](tag, "daily_quote") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

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

  //def direction = column[Int]("direction") // -1, 0 , 1

  def change = column[Double]("change")

  def lastBestBidPrice = column[Option[Double]]("last_best_bid_price")

  def lastBestBidVolume = column[Option[Int]]("last_best_bid_volume")

  def lastBestAskPrice = column[Option[Double]]("last_best_ask_price")

  def lastBestAskVolume = column[Option[Int]]("last_best_ask_volume")

  def priceEarningRatio = column[Option[Double]]("price_earning_ratio")

  def idx = index("idx_DailyQuote_market_date_companyCode", (market, date, companyCode), unique = true)

  def * = (id, market, date, companyCode, companyName, tradeVolume, transaction, tradeValue, openingPrice, highestPrice, lowestPrice, closingPrice, change, lastBestBidPrice, lastBestBidVolume, lastBestAskPrice, lastBestAskVolume, priceEarningRatio) <> (DailyQuoteRow.tupled, DailyQuoteRow.unapply)
}

case class DailyQuoteRow(id: Long, market: String, date: LocalDate, companyCode: String, companyName: String, tradeVolume: Long, transaction: Int, tradeValue: Long, openingPrice: Option[Double], highestPrice: Option[Double], lowestPrice: Option[Double], closingPrice: Option[Double], change: Double, lastBestBidPrice: Option[Double], lastBestBidVolume: Option[Int], lastBestAskPrice: Option[Double], lastBestAskVolume: Option[Int], priceEarningRatio: Option[Double])