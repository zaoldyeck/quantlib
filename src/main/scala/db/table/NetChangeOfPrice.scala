package db.table

import java.time.LocalDate

import slick.jdbc.H2Profile.api._

/**
 * https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html
 * 漲跌證券數合計
 *
 * @param tag
 */
class NetChangeOfPrice(tag: Tag) extends Table[(Long, LocalDate, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tag, "net_change_of_price") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def date = column[LocalDate]("date")

  def upOverallMarket = column[Int]("up_overall_market")

  def upStocks = column[Int]("up_stocks")

  def limitUpOverallMarket = column[Int]("limit_up_overall_market")

  def limitUpStocks = column[Int]("limit_up_stocks")

  def downOverallMarket = column[Int]("down_overall_market")

  def downStocks = column[Int]("down_stocks")

  def limitDownOverallMarket = column[Int]("limit_down_overall_market")

  def limitDownStocks = column[Int]("limit_down_stocks")

  def unchangedOverallMarket = column[Int]("unchanged_overall_market")

  def unchangedStocks = column[Int]("unchanged_stocks")

  def unmatchedOverallMarket = column[Int]("unmatched_overall_market")

  def unmatchedStocks = column[Int]("unmatched_stocks")

  def naOverallMarket = column[Int]("n/a_overall_market")

  def naStocks = column[Int]("n/a_stocks")

  def idx = index("idx_netChangeOfPrice_date", date, unique = true)

  def * = (id, date, upOverallMarket, upStocks, limitUpOverallMarket, limitUpOverallMarket, downOverallMarket, downStocks, limitDownOverallMarket, limitDownStocks, unchangedOverallMarket, unchangedStocks, unmatchedOverallMarket, unmatchedStocks, naOverallMarket, naStocks)
}
