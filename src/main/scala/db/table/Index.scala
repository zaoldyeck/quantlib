package db.table

import java.time.LocalDate

import slick.jdbc.H2Profile.api._

/**
 * https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html
 * 指數
 * @param tag
 */
class Index(tag: Tag) extends Table[(Long, LocalDate, String, Double, Int, Double, Double)](tag, "index") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def date = column[LocalDate]("date")

  def index = column[String]("index")

  def closingIndex = column[Double]("closing_index")

  def direction = column[Int]("direction") // -1, 0 , 1

  def change = column[Double]("change")

  def changePercentage = column[Double]("change(%)")

  def idx = index("idx_index_date_index", (date, index), unique = true)

  def * = (id, date, index, closingIndex, direction, change, changePercentage)
}
