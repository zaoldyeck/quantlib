package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._

/**
 * 庫藏股執行情形 — 公司宣告買回自家股票.
 *
 * Source: MOPS t35sc09 (TWSE/TPEx 上市櫃皆同 endpoint, TYPEK=sii|otc).
 *
 * Floor signal:
 *   - 公告當日通常 +3-5%（Vermaelen 1981 / TW academic confirms）
 *   - 三個月後 mean revert 多數情形
 *   - 與 SBL 借券餘額同時擴大時，可能形成 squeeze setup
 *
 * Dedupe key: (market, announce_date, company_code) — same company can announce
 * multiple buyback rounds across years, but only one per (market, date, code).
 */
class TreasuryStockBuyback(tag: Tag)
  extends Table[(Long, String, LocalDate, String, String, Long, Double, Double, LocalDate, LocalDate, Long, Double)](tag, "treasury_stock_buyback") {

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")                            // "twse" | "tpex"

  def announceDate = column[LocalDate]("announce_date")            // 公告日

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def plannedShares = column[Long]("planned_shares")               // 預定買回張數 × 1000

  def priceLow = column[Double]("price_low")                       // 每股下限
  def priceHigh = column[Double]("price_high")                     // 每股上限

  def periodStart = column[LocalDate]("period_start")              // 執行起
  def periodEnd = column[LocalDate]("period_end")                  // 執行迄

  def executedShares = column[Long]("executed_shares")             // 已買回張數 × 1000
  def pctOfCapital = column[Double]("pct_of_capital")              // 已買回占資本比率(%)

  def idx = index("idx_TreasuryStockBuyback_market_date_code", (market, announceDate, companyCode), unique = true)

  def * = (id, market, announceDate, companyCode, companyName, plannedShares, priceLow, priceHigh, periodStart, periodEnd, executedShares, pctOfCapital)
}
