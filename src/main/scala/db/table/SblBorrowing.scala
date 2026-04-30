package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._

/**
 * 借券賣出餘額（structural short by institutions）
 * twse https://www.twse.com.tw/exchangeReport/TWT93U?response=csv&date=YYYYMMDD from 2016-01-04
 *
 * Unlike 融券 (retail short) in margin_transactions, SBL tracks
 * institutional / foreign 借券 (securities borrowing & lending).
 * Squeeze signal: borrowed balance rises fast while price stays flat →
 * 軋空 risk; borrowed balance drops while price rises → short-cover rally.
 */
class SblBorrowing(tag: Tag)
  extends Table[(Long, String, LocalDate, String, String, Long, Long, Long, Long, Long, Long)](tag, "sbl_borrowing") {

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def date = column[LocalDate]("date")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def prevDayBalance = column[Long]("prev_day_balance")

  def dailySold = column[Long]("daily_sold")

  def dailyReturned = column[Long]("daily_returned")

  def dailyAdjustment = column[Long]("daily_adjustment")

  def dailyBalance = column[Long]("daily_balance")

  def nextDayLimit = column[Long]("next_day_limit")

  def idx = index("idx_SblBorrowing_market_date_code", (market, date, companyCode), unique = true)

  def * = (id, market, date, companyCode, companyName, prevDayBalance, dailySold, dailyReturned, dailyAdjustment, dailyBalance, nextDayLimit)
}
