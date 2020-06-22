package db.table

//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._

import java.time.LocalDate

import slick.jdbc.H2Profile.api._

/**
 * 減資
 * twse https://www.twse.com.tw/zh/page/trading/exchange/TWTAUU.html from 2011-1-1
 * tpex https://www.tpex.org.tw/web/stock/exright/revivt/revivt.php from 2013-1-2
 *
 * @param tag
 */
class CapitalReduction(tag: Tag) extends Table[CapitalReductionRow](tag, "capital_reduction") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def date = column[LocalDate]("date")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def closingPriceOnTheLastTradingDate = column[Double]("closing_price_on_the_last_trading_date")

  def postReductionReferencePrice = column[Double]("post_reduction_reference_price")

  def limitUp = column[Double]("limit_up")

  def limitDown = column[Double]("limit_down")

  def openingReferencePrice = column[Double]("opening_reference_price")

  def exRightReferencePrice = column[Option[Double]]("ex_right_reference_price")

  def reasonForCapitalReduction = column[String]("reason_for_capital_reduction")

  def idx = index("idx_CapitalReduction_market_date_companyCode", (market, date, companyCode), unique = true)

  def * = (id, market, date, companyCode, companyName, closingPriceOnTheLastTradingDate, postReductionReferencePrice, limitUp, limitDown, openingReferencePrice, exRightReferencePrice, reasonForCapitalReduction) <> (CapitalReductionRow.tupled, CapitalReductionRow.unapply)
}

case class CapitalReductionRow(id: Long, market: String, date: LocalDate, companyCode: String, companyName: String, closingPriceOnTheLastTradingDate: Double, postReductionReferencePrice: Double, limitUp: Double, limitDown: Double, openingReferencePrice: Double, exRightReferencePrice: Option[Double], reasonForCapitalReduction: String)