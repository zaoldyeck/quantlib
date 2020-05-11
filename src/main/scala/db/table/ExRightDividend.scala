package db.table

//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._

import java.time.LocalDate

import slick.jdbc.H2Profile.api._

/**
 * https://www.twse.com.tw/zh/page/trading/exchange/TWT49U.html
 * 除權息
 *
 * @param tag
 */
class ExRightDividend(tag: Tag) extends Table[(Long, LocalDate, String, String, Double, Double, Double, String, Double, Double, Double, Double)](tag, "ex_right_dividend") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

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

  def idx = index("idx_ExRightDividend_date_companyCode", (date, companyCode), unique = true)

  def * = (id, date, companyCode, companyName, closingPriceBeforeExRightExDividend, exRightExDividendReferencePrice, cashDividend, rightOrDividend, limitUp, limitDown, openingReferencePrice, exDividendReferencePrice)
}
