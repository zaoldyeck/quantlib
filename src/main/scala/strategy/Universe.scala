package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Point-in-time universe of eligible TWSE common stocks for the strategy.
 *
 * Filters applied as-of a rebalance date D:
 *   - market = 'twse', 4-digit code starting 1..9 (excludes ETFs 0xxx, preferred
 *     shares 2883A, TDRs 9xxxxx)
 *   - NOT in the `etf` table (secondary ETF guard for any 1..9 prefix ETF)
 *   - Rolling 30-calendar-day lookback window ending on D:
 *       - at least MinTradingDays actual trading days
 *       - median daily trade_value >= MinMedianTradeValue (NT$50M)
 *
 * Deferred (see plan "Open Decisions"):
 *   - Market-cap floor (>= NT$5bn): requires shares_outstanding, derivable from
 *     `balance_sheet.title='股本合計' / 10 NT$ par`. Current DB has sparse Q1/Q4
 *     coverage (e.g. 2023-Q1 only 89 companies), which would artificially cut
 *     the universe to a few dozen. ADV-based liquidity already correlates
 *     strongly with market cap in TWSE; defer until balance_sheet is backfilled.
 *   - Balance-sheet history depth (>= 6Q): same rationale.
 */
object Universe {
  /** Median daily trade value floor, in TWD. Plan §Universe default NT$50M. */
  val MinMedianTradeValue: Long = 50_000_000L

  /** Minimum number of trading days present within the lookback window. */
  val MinTradingDays: Int = 10

  /** Lookback window in calendar days (~20 trading days in normal weeks). */
  val LookbackDays: Int = 30

  /**
   * Returns the set of TWSE company_codes that satisfy all universe criteria
   * as of the given rebalance date.
   *
   * This query joins on daily_quote only (no financial-statement dependency),
   * so it is robust to sparse quarterly coverage and fast on indexed date ranges.
   */
  def eligible(asOf: LocalDate, db: Database): Set[String] = {
    val asOfStr = asOf.toString

    val query =
      sql"""
        WITH liquidity AS (
          SELECT company_code,
                 percentile_disc(0.5) WITHIN GROUP (ORDER BY trade_value) AS median_tv,
                 count(*) AS days
          FROM daily_quote
          WHERE market = 'twse'
            AND date <= #${"'" + asOfStr + "'"}::date
            AND date > #${"'" + asOfStr + "'"}::date - INTERVAL '#${LookbackDays} days'
            AND company_code ~ '^[1-9][0-9]{3}$$'
          GROUP BY company_code
        )
        SELECT company_code FROM liquidity
        WHERE days >= #${MinTradingDays}
          AND median_tv >= #${MinMedianTradeValue}
          AND company_code NOT IN (SELECT company_code FROM etf)
      """.as[String]

    Await.result(db.run(query), Duration.Inf).toSet
  }
}
