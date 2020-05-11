package db.table

//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._

import slick.jdbc.H2Profile.api._

/**
 * https://mops.twse.com.tw/mops/web/t203sb02
 * 財務報表
 *
 * @param tag
 */
class QuarterlyReport(tag: Tag) extends Table[(Long, Int, Int, String, String, Option[String], String, Double)](tag, "quarterly_report") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def year = column[Int]("year")

  def quarter = column[Int]("quarter")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def code = column[Option[String]]("code")

  def subject = column[String]("subject")

  def value = column[Double]("value")

  def idx = index("idx_QuarterlyReport_year_quarter_companyCode_subject", (year, quarter, companyCode, subject), unique = true)

  def * = (id, year, quarter, companyCode, companyName, code, subject, value)
}
