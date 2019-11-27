package db.table

import slick.jdbc.H2Profile.api._

/**
 * https://mops.twse.com.tw/mops/web/t21sc04_ifrs
 * 營業收入統計表
 *
 * @param tag
 */
class OperatingRevenue(tag: Tag) extends Table[(Long, Int, Int, String, String, String, Double, Double, Double, Double, Double, Double, Double, Double)](tag, "operating_revenue") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def year = column[Int]("year")

  def month = column[Int]("month")

  def industry = column[String]("industry")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def monthlyRevenue = column[Double]("monthly_revenue")

  def lastMonthRevenue = column[Double]("last_month_revenue")

  def lastYearMonthlyRevenue = column[Double]("last_year_monthly_revenue")

  def monthlyRevenueComparedLastMonthPercentage = column[Double]("monthly_revenue_compared_last_month(%))")

  def monthlyRevenueComparedLastYearPercentage = column[Double]("monthly_revenue_compared_last_year(%))")

  def cumulativeRevenue = column[Double]("cumulative_revenue")

  def lastYearCumulativeRevenue = column[Double]("last_year_cumulative_revenue")

  def cumulativeRevenueComparedLastYearPercentage = column[Double]("cumulative_revenue_compared_last_year(%))")

  def idx = index("idx_a", (companyCode, year, month), unique = true)

  def * = (id, year, month, industry, companyCode, companyName, monthlyRevenue, lastMonthRevenue, lastYearMonthlyRevenue, monthlyRevenueComparedLastMonthPercentage, monthlyRevenueComparedLastYearPercentage, cumulativeRevenue, lastYearCumulativeRevenue, cumulativeRevenueComparedLastYearPercentage)
}
