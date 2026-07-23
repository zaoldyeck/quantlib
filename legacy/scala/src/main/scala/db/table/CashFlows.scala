package db.table

import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
/**
 * 現金流量表
 * https://mops.twse.com.tw/mops/web/t203sb01
 * https://mops.twse.com.tw/mops/web/t164sb05
 *
 * @param tag
 */
protected[this] abstract class CashFlows(tag: Tag, tableName: String) extends Table[(Long, String, Int, Int, String, String, Double)](tag, tableName) {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def year = column[Int]("year")

  def quarter = column[Int]("quarter")

  def companyCode = column[String]("company_code")

  def title = column[String]("title")

  def value = column[Double]("value")

  def idx = index("idx_CashFlows_market_year_quarter_companyCode_title", (market, year, quarter, companyCode, title), unique = true)

  def * = (id, market, year, quarter, companyCode, title, value)
}

class CashFlowsProgressive(tag: Tag) extends CashFlows(tag, "cash_flows_progressive")

class CashFlowsIndividual(tag: Tag) extends CashFlows(tag, "cash_flows_individual")
