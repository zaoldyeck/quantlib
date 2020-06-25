package db.table

//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._

import slick.jdbc.H2Profile.api._

/**
 * https://mops.twse.com.tw/mops/web/t163sb05
 * https://emops.twse.com.tw/server-java/t58query#3
 * Schema https://emops.twse.com.tw/server-java/t163sb05_e?step=show&year=2019&season=3
 * 資產負債表
 *
 * @param tag
 */
class BalanceSheet(tag: Tag) extends Table[(Long, String, Int, Int, String, String, String, Option[Double])](tag, "balance_sheet") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def year = column[Int]("year")

  def quarter = column[Int]("quarter")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def subject = column[String]("subject")

  def value = column[Option[Double]]("value")

  def idx = index("idx_BalanceSheet_market_year_quarter_companyCode_subject", (market, year, quarter, companyCode, subject), unique = true)

  def * = (id, market, year, quarter, companyCode, companyName, subject, value)
}
