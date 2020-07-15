package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._

/**
 * 價格指數
 * twse https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html from 2004-2-11
 * tpex https://www.tpex.org.tw/web/stock/aftertrading/index_summary/summary.php from 2016-1-4
 *
 * @param tag
 */
class Index(tag: Tag) extends Table[(Long, String, LocalDate, String, Option[Double], Double, Double)](tag, "index") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def date = column[LocalDate]("date")

  def name = column[String]("name")

  def close = column[Option[Double]]("close")

  //def direction = column[Int]("direction") // -1, 0 , 1

  def change = column[Double]("change")

  def changePercentage = column[Double]("change(%)")

  def idx = index("idx_Index_market_date_name", (market, date, name), unique = true)

  def * = (id, market, date, name, close, change, changePercentage)
}
