package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._

/**
 * 綜合損益表
 * https://mops.twse.com.tw/mops/web/t163sb04
 * https://emops.twse.com.tw/server-java/t58query#3
 * Schema https://emops.twse.com.tw/server-java/t163sb04_e?step=show&year=2019&season=3
 *
 * @param tag
 * @param tableName
 */
protected[this] abstract class IncomeStatement(tag: Tag, tableName: String) extends Table[(Long, String, Int, Int, String, String, String, Double)](tag, tableName) {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def year = column[Int]("year")

  def quarter = column[Int]("quarter")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def subject = column[String]("subject")

  def value = column[Double]("value")

  def idx = index("idx_IncomeStatement_market_year_quarter_companyCode_subject", (market, year, quarter, companyCode, subject), unique = true)

  def * = (id, market, year, quarter, companyCode, companyName, subject, value)
}

class IncomeStatementProgressive(tag: Tag) extends IncomeStatement(tag, "income_statement_progressive")

class IncomeStatementIndividual(tag: Tag) extends IncomeStatement(tag, "income_statement_individual")
