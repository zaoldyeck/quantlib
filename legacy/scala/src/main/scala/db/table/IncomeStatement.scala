package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._

/**
 * 綜合損益表
 * https://mops.twse.com.tw/mops/web/t203sb01
 * https://mops.twse.com.tw/mops/web/t164sb04
 *
 * @param tag
 */
protected[this] abstract class IncomeStatement(tag: Tag, tableName: String) extends Table[(Long, String, Int, Int, String, String, Double)](tag, tableName) {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def year = column[Int]("year")

  def quarter = column[Int]("quarter")

  def companyCode = column[String]("company_code")

  def title = column[String]("title")

  def value = column[Double]("value")

  def idx = index("idx_IncomeStatement_market_year_quarter_companyCode_title", (market, year, quarter, companyCode, title), unique = true)

  def * = (id, market, year, quarter, companyCode, title, value)
}

class IncomeStatementProgressive(tag: Tag) extends IncomeStatement(tag, "income_statement_progressive")

class IncomeStatementIndividual(tag: Tag) extends IncomeStatement(tag, "income_statement_individual")

/**
 * 簡明綜合損益表
 * https://mops.twse.com.tw/mops/web/t163sb04
 * https://emops.twse.com.tw/server-java/t58query#3
 * Schema https://emops.twse.com.tw/server-java/t163sb04_e?step=show&year=2019&season=3
 *
 * @param tag
 * @param tableName
 */
protected[this] abstract class ConciseIncomeStatement(tag: Tag, tableName: String) extends Table[(Long, String, String, Int, Int, String, String, String, Double)](tag, tableName) {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def `type` = column[String]("type")

  def year = column[Int]("year")

  def quarter = column[Int]("quarter")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def title = column[String]("title")

  def value = column[Double]("value")

  def idx = index("idx_ConciseIncomeStatement_market_type_year_quarter_companyCode_title", (market, `type`, year, quarter, companyCode, title), unique = true)

  def * = (id, market, `type`, year, quarter, companyCode, companyName, title, value)
}

class ConciseIncomeStatementProgressive(tag: Tag) extends ConciseIncomeStatement(tag, "concise_income_statement_progressive")

class ConciseIncomeStatementIndividual(tag: Tag) extends ConciseIncomeStatement(tag, "concise_income_statement_individual")
