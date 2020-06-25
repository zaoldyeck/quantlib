package db.table

import java.time.LocalDate

//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
import slick.jdbc.H2Profile.api._

/**
 * 個股本益比、殖利率、股價淨值比
 * twse https://www.twse.com.tw/zh/page/trading/exchange/BWIBBU_d.html from 2005-9-2
 * tpex https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera.php from 2007-1-2
 *
 * @param tag
 */
class StockPER_PBR_DividendYield(tag: Tag) extends Table[(Long, String, LocalDate, String, String, Option[Double], Option[Double], Double)](tag, "stock_per_pbr_dividend_yield") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def date = column[LocalDate]("date")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def priceToEarningRatio = column[Option[Double]]("price_to_earning_ratio")

  def priceBookRatio = column[Option[Double]]("price_book_ratio")

  def dividendYield = column[Double]("dividend_yield")

  def * = (id, market, date, companyCode, companyName, priceToEarningRatio, priceBookRatio, dividendYield)
}
