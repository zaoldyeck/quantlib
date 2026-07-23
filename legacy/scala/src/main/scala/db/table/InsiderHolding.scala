package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._

/**
 * 內部人持股轉讓「事前申報」日報表 — MOPS t56sb12 (上市櫃皆 bulk).
 *
 * Each row = one 內部人 declared an upcoming transfer. The same person on the
 * same day can declare multiple distinct (transfer_method, transferee) records.
 *
 * Forward signal:
 *   - 內部人申報轉讓 = 即將賣出 = forward 5-30d -2~-5% CAR (TW academic literature)
 *   - 大股東 (持股 ≥ 10%) > 董監事 > 經理人 信號強度
 *
 * `report_date` is the trading day the report was published (用 input year/month/day).
 * `declare_date` is the 內部人 actual declaration date (might lead by several days).
 *
 * Dedupe key: (market, report_date, company_code, reporter_name, transfer_method, transferee)
 */
class InsiderHolding(tag: Tag)
  extends Table[(Long, String, LocalDate, LocalDate, String, String, String, String, String, String, Long, Long, Long, Long, Long, Long)](tag, "insider_holding") {

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")                            // "twse" | "tpex"

  def reportDate = column[LocalDate]("report_date")                // 申報書發布日 (input day)
  def declareDate = column[LocalDate]("declare_date")              // 內部人申報日

  def companyCode = column[String]("company_code")
  def companyName = column[String]("company_name")

  def reporterTitle = column[String]("reporter_title")             // 申報人身分
  def reporterName = column[String]("reporter_name")               // 姓名

  def transferMethod = column[String]("transfer_method")           // 轉讓方式
  def transferee = column[String]("transferee")                    // 受讓人

  def transferShares = column[Long]("transfer_shares")             // 轉讓股數
  def maxIntradayShares = column[Long]("max_intraday_shares")      // 每日盤中最大轉讓
  def currentSharesOwn = column[Long]("current_shares_own")        // 目前持有 - 自有
  def currentSharesTrust = column[Long]("current_shares_trust")    // 目前持有 - 信託
  def plannedSharesOwn = column[Long]("planned_shares_own")        // 預定轉讓 - 自有
  def plannedSharesTrust = column[Long]("planned_shares_trust")    // 預定轉讓 - 信託

  def idx = index("idx_InsiderHolding_market_date_code_reporter_method_transferee",
    (market, reportDate, companyCode, reporterName, transferMethod, transferee), unique = true)

  def * = (id, market, reportDate, declareDate, companyCode, companyName,
    reporterTitle, reporterName, transferMethod, transferee,
    transferShares, maxIntradayShares,
    currentSharesOwn, currentSharesTrust,
    plannedSharesOwn, plannedSharesTrust)
}
