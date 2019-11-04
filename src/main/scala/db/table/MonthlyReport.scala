package db.table

import slick.jdbc.H2Profile.api._

class MonthlyReport(tag: Tag) extends Table[(Long, String, Int, String, Int, Int, Double, Double, Double, Double, Double, Double, Double, Double)](tag, "monthly_report") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def industry = column[String]("industry")

  def stockId = column[Int]("stock_id")

  def stockName = column[String]("stock_name")

  def year = column[Int]("year")

  def month = column[Int]("month")

  def monthlyRevenue = column[Double]("monthly_revenue")

  def lastMonthRevenue = column[Double]("last_month_revenue")

  def lastYearMonthlyRevenue = column[Double]("last_year_monthly_revenue")

  def monthlyRevenueComparedLastMonthPercentage = column[Double]("monthly_revenue_compared_last_month_percentage")

  def monthlyRevenueComparedLastYearPercentage = column[Double]("monthly_revenue_compared_last_year_percentage")

  def cumulativeRevenue = column[Double]("cumulative_revenue")

  def lastYearCumulativeRevenue = column[Double]("last_year_cumulative_revenue")

  def cumulativeRevenueComparedLastYearPercentage = column[Double]("cumulative_revenue_compared_last_year_percentage")

  def idx = index("idx_a", (stockId, year, month), unique = true)

  def * = (id, industry, stockId, stockName, year, month, monthlyRevenue, lastMonthRevenue, lastYearMonthlyRevenue, monthlyRevenueComparedLastMonthPercentage, monthlyRevenueComparedLastYearPercentage, cumulativeRevenue, lastYearCumulativeRevenue, cumulativeRevenueComparedLastYearPercentage)
}
