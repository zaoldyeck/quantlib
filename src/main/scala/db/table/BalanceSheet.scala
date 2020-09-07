package db.table

import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
/**
 * 資產負債表
 * https://mops.twse.com.tw/mops/web/t203sb01
 * https://mops.twse.com.tw/mops/web/t164sb03
 *
 * @param tag
 */
class BalanceSheet(tag: Tag) extends Table[(Long, String, Int, Int, String, String, Double)](tag, "balance_sheet") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def year = column[Int]("year")

  def quarter = column[Int]("quarter")

  def companyCode = column[String]("company_code")

  def title = column[String]("title")

  def value = column[Double]("value")

  def idx = index("idx_BalanceSheet_market_year_quarter_companyCode_title", (market, year, quarter, companyCode, title), unique = true)

  def * = (id, market, year, quarter, companyCode, title, value)
}

/**
 * 簡明資產負債表
 * https://mops.twse.com.tw/mops/web/t163sb05
 * https://emops.twse.com.tw/server-java/t58query#3
 * Schema https://emops.twse.com.tw/server-java/t163sb05_e?step=show&year=2019&season=3
 *
 * @param tag
 */
class ConciseBalanceSheet(tag: Tag) extends Table[(Long, String, String, Int, Int, String, String, String, Double)](tag, "concise_balance_sheet") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def `type` = column[String]("type")

  def year = column[Int]("year")

  def quarter = column[Int]("quarter")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def title = column[String]("title")

  def value = column[Double]("value")

  def idx = index("idx_ConciseBalanceSheet_market_type_year_quarter_companyCode_title", (market, `type`, year, quarter, companyCode, title), unique = true)

  def * = (id, market, `type`, year, quarter, companyCode, companyName, title, value)
}