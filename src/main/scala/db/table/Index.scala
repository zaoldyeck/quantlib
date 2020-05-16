package db.table

import java.time.LocalDate

//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
import slick.jdbc.H2Profile.api._

/**
 * https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html
 * 指數 from 2004-2-11
 *
 * @param tag
 */
class Index(tag: Tag) extends Table[(Long, LocalDate, String, Option[Double], Double, Double)](tag, "index") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def date = column[LocalDate]("date")

  def name = column[String]("name")

  def closingIndex = column[Option[Double]]("closing_index")

  //def direction = column[Int]("direction") // -1, 0 , 1

  def change = column[Double]("change")

  def changePercentage = column[Double]("change(%)")

  def idx = index("idx_Index_date_index", (date, name), unique = true)

  def * = (id, date, name, closingIndex, change, changePercentage)
}
