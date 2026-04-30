package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._

/**
 * 外資及陸資持股比率 (snapshot, daily).
 * twse https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS?response=csv from 2005
 * tpex https://www.tpex.org.tw/www/zh-tw/insti/qfii from ~2010
 *
 * `foreign_held_ratio` = 全體外資及陸資持股比率 (%). This is the key signal:
 * approaching 法令上限 (usually 50% or 100% for 僑外資共用) suggests topping-out.
 */
class ForeignHoldingRatio(tag: Tag)
  extends Table[(Long, String, LocalDate, String, String, Long, Long, Long, Double, Double, Double)](tag, "foreign_holding_ratio") {

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def date = column[LocalDate]("date")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def outstandingShares = column[Long]("outstanding_shares")

  def foreignRemainingShares = column[Long]("foreign_remaining_shares")

  def foreignHeldShares = column[Long]("foreign_held_shares")

  def foreignRemainingRatio = column[Double]("foreign_remaining_ratio")

  def foreignHeldRatio = column[Double]("foreign_held_ratio")

  def foreignLimitRatio = column[Double]("foreign_limit_ratio")

  def idx = index("idx_ForeignHoldingRatio_market_date_code", (market, date, companyCode), unique = true)

  def * = (id, market, date, companyCode, companyName, outstandingShares, foreignRemainingShares, foreignHeldShares, foreignRemainingRatio, foreignHeldRatio, foreignLimitRatio)
}
