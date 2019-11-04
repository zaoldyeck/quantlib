package db.table

import slick.jdbc.H2Profile.api._

class QuarterlyReport(tag: Tag) extends Table[(Long, Int, String, Int, Int, Option[String], String, Double)](tag, "quarterly_report") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def stockId = column[Int]("stock_id")

  def stockName = column[String]("stock_name")

  def year = column[Int]("year")

  def quarter = column[Int]("quarter")

  def code = column[Option[String]]("code")

  def subject = column[String]("subject")

  def value = column[Double]("value")

  def idx = index("idx_a", (stockId, year, quarter, subject), unique = true)

  def * = (id, stockId, stockName, year, quarter, code, subject, value)
}
