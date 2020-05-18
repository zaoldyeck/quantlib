package db.table

//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._

import slick.jdbc.H2Profile.api._

/**
 * https://mops.twse.com.tw/mops/web/t21sc04_ifrs
 * 營業收入統計表
 * Before IFRSs from 2001-6
 * After IFRSs from 2013-1
 * @param tag
 */
class OperatingRevenue(tag: Tag) extends Table[(Long, Int, Int, Option[String], String, String, Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])](tag, "operating_revenue") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def year = column[Int]("year")

  def month = column[Int]("month")

  def industry = column[Option[String]]("industry")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def monthlyRevenue = column[Option[Double]]("monthly_revenue")

  def lastMonthRevenue = column[Option[Double]]("last_month_revenue")

  def lastYearMonthlyRevenue = column[Option[Double]]("last_year_monthly_revenue")

  def monthlyRevenueComparedLastMonthPercentage = column[Option[Double]]("monthly_revenue_compared_last_month(%))")

  def monthlyRevenueComparedLastYearPercentage = column[Option[Double]]("monthly_revenue_compared_last_year(%))")

  def cumulativeRevenue = column[Option[Double]]("cumulative_revenue")

  def lastYearCumulativeRevenue = column[Option[Double]]("last_year_cumulative_revenue")

  def cumulativeRevenueComparedLastYearPercentage = column[Option[Double]]("cumulative_revenue_compared_last_year(%))")

  def idx = index("idx_OperatingRevenue_companyCode_year_month", (companyCode, year, month), unique = true)

  def * = (id, year, month, industry, companyCode, companyName, monthlyRevenue, lastMonthRevenue, lastYearMonthlyRevenue, monthlyRevenueComparedLastMonthPercentage, monthlyRevenueComparedLastYearPercentage, cumulativeRevenue, lastYearCumulativeRevenue, cumulativeRevenueComparedLastYearPercentage)
}
