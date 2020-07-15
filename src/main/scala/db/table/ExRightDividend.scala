package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._

/**
 * 除權除息計算結果表
 * twse https://www.twse.com.tw/zh/page/trading/exchange/TWT49U.html from 2003-5-5
 * tpex https://www.tpex.org.tw/web/stock/exright/dailyquo/exDailyQ.php from 2008-1-2
 *
 * @param tag
 */
class ExRightDividend(tag: Tag) extends Table[ExRightDividendRow](tag, "ex_right_dividend") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def date = column[LocalDate]("date")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def closingPriceBeforeExRightExDividend = column[Double]("closing_price_before_ex_right_ex_dividend")

  def exRightExDividendReferencePrice = column[Double]("ex_right_ex_dividend_reference_price")

  def cashDividend = column[Double]("cash_dividend")

  def rightOrDividend = column[String]("right_or_dividend")

  def limitUp = column[Double]("limit_up")

  def limitDown = column[Double]("limit_down")

  def openingReferencePrice = column[Double]("opening_reference_price")

  def exDividendReferencePrice = column[Double]("ex_dividend_reference_price")

  def idx = index("idx_ExRightDividend_market_date_companyCode", (market, date, companyCode), unique = true)

  def * = (id, market, date, companyCode, companyName, closingPriceBeforeExRightExDividend, exRightExDividendReferencePrice, cashDividend, rightOrDividend, limitUp, limitDown, openingReferencePrice, exDividendReferencePrice) <> (ExRightDividendRow.tupled, ExRightDividendRow.unapply)
}

case class ExRightDividendRow(id: Long, market: String, date: LocalDate, companyCode: String, companyName: String, closingPriceBeforeExRightExDividend: Double, exRightExDividendReferencePrice: Double, cashDividend: Double, rightOrDividend: String, limitUp: Double, limitDown: Double, openingReferencePrice: Double, exDividendReferencePrice: Double)