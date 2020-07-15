package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._

/**
 * 融資融券餘額
 * twse https://www.twse.com.tw/zh/page/trading/exchange/MI_MARGN.html from 2001-1-2
 * tpex https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal.php from 2007-1-2
 *
 * @param tag
 */
class MarginTransactions(tag: Tag) extends Table[(Long, String, LocalDate, String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tag, "margin_transactions") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def date = column[LocalDate]("date")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def marginPurchase = column[Int]("margin_purchase")

  def marginSales = column[Int]("margin_sales")

  def cashRedemption = column[Int]("cash_redemption")

  def marginBalanceOfPreviousDay = column[Int]("margin_balance_of_previous_day")

  def marginBalanceOfTheDay = column[Int]("margin_balance_of_the_day")

  def marginQuota = column[Int]("margin_quota")

  def shortCovering = column[Int]("short_covering")

  def shortSale = column[Int]("short_sale")

  def stockRedemption = column[Int]("stock_redemption")

  def shortBalanceOfPreviousDay = column[Int]("short_balance_of_previous_day")

  def shortBalanceOfTheDay = column[Int]("short_balance_of_the_day")

  def shortQuota = column[Int]("short_quota")

  def offsettingOfMarginPurchasesAndShortSales = column[Int]("offsetting_of_margin_purchases_and_short_sales")

  def idx = index("idx_MarginTransactions_market_date_companyCode", (market, date, companyCode), unique = true)

  def * = (id, market, date, companyCode, companyName, marginPurchase, marginSales, cashRedemption, marginBalanceOfPreviousDay, marginBalanceOfTheDay, marginQuota, shortCovering, shortSale, stockRedemption, shortBalanceOfPreviousDay, shortBalanceOfTheDay, shortQuota, offsettingOfMarginPurchasesAndShortSales)
}