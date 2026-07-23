package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._

/**
 * 除權除息計算結果表
 * twse https://www.twse.com.tw/zh/announcement/ex-right/twt49u.html from 2003-5-5
 * tpex https://www.tpex.org.tw/web/stock/exright/dailyquo/exDailyQ.php from 2008-1-2
 *
 * @param tag
 */
class ExRightDividend(tag: Tag) extends Table[ExRightDividendRow](tag, "ex_right_dividend") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def date = column[LocalDate]("date")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def closingPriceBeforeExRightExDividend = column[Double]("closing_price_before_ex_right_ex_dividend")

  def exRightExDividendReferencePrice = column[Double]("ex_right_ex_dividend_reference_price")

  // 語意警告(稽核 D-slick-schema-full BUG1):欄名 cash_dividend 但存的不是純現金
  //   股利,且語意跨資料源翻轉,直接當「現金股利」用會錯:
  //   ① legacy TWT49U(2024-06 前,readExRightDividend 取 values(5)):存「權值+息值」=
  //      除權息前收盤 − 除權息參考價 的總股價調整額(非純息值);純除權(配股無現金)列
  //      在此仍為正(PG 實測 avg ≈ 2.669),因它是總調整額而非現金股利。
  //   ② MOPS t108sb27(2024-07 起,parseMopsRows):改存 totalCash = 純現金股利(息值),
  //      且 closing_price_before / reference_price 皆為 0——同欄語意於此翻轉。
  //   還原因子官方真源是「參考價 / 除權息前收盤」;prices.py 已於 FC1 改優先採該兩欄,
  //   cash_dividend 僅在缺參考價時作純現金 fallback(gate cash_dividend > 0)。要純現金
  //   股利 / 殖利率請勿直接讀本欄。
  def cashDividend = column[Double]("cash_dividend")

  def rightOrDividend = column[String]("right_or_dividend")

  def limitUp = column[Double]("limit_up")

  def limitDown = column[Double]("limit_down")

  def openingReferencePrice = column[Double]("opening_reference_price")

  def exDividendReferencePrice = column[Double]("ex_dividend_reference_price")

  def idx = index("idx_ExRightDividend_market_date_companyCode", (market, date, companyCode), unique = true)

  def * = (id, market, date, companyCode, companyName, closingPriceBeforeExRightExDividend, exRightExDividendReferencePrice, cashDividend, rightOrDividend, limitUp, limitDown, openingReferencePrice, exDividendReferencePrice) <> (ExRightDividendRow.tupled, ExRightDividendRow.unapply)
}

case class ExRightDividendRow(id: Long, market: String, date: LocalDate, companyCode: String, companyName: String, closingPriceBeforeExRightExDividend: Double, exRightExDividendReferencePrice: Double, cashDividend: Double, rightOrDividend: String, limitUp: Double, limitDown: Double, openingReferencePrice: Double, exDividendReferencePrice: Double)