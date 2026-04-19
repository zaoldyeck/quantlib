package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Minimum-viable factor strategy — a baseline to prove the backtest spine works
 * end-to-end and to serve as the control against which richer strategies are
 * compared. Two factors combined cross-sectionally:
 *
 *   Relative Strength (weight 0.6):
 *     63-day total return ending 5 trading days before rebalance (skip-5),
 *     ranked into [0, 1] percentile. Momentum with a short skip avoids the
 *     well-documented 1-week reversal effect.
 *
 *   Value via P/B-band position (weight 0.4):
 *     Current P/B relative to the ticker's 3.5-year median P/B. Lower (cheaper
 *     vs own history) gets higher score. Ranked into [0, 1] percentile with
 *     low P/B scoring 1.0.
 *
 * Selection: top 10 by composite score, equal-weight (10% each).
 * Rebalance:  first trading day of each month (simplified vs plan's mid-month
 *            rule — acceptable for v1 since this strategy doesn't rely on
 *            quarterly fiscal data yet).
 *
 * The strategy intentionally omits the full Alpha Stack / Quality Filter from
 * the plan — those pieces require broader fundamental data (currently being
 * backfilled). v2 will layer them on once financial_statements is complete.
 */
class MomentumValueStrategy(topN: Int = 10) extends Strategy {
  override val name: String = s"momentum-value-top$topN"

  override def rebalanceDates(start: LocalDate, end: LocalDate, db: Database): Seq[LocalDate] = {
    val q = sql"""
      SELECT MIN(date) FROM daily_quote
      WHERE market = 'twse' AND company_code = '0050'
        AND date >= #${"'" + start + "'"}::date AND date <= #${"'" + end + "'"}::date
      GROUP BY date_trunc('month', date)
      ORDER BY MIN(date)
    """.as[java.sql.Date]
    Await.result(db.run(q), Duration.Inf).map(_.toLocalDate)
  }

  override def targetWeights(asOf: LocalDate, db: Database): Map[String, Double] = {
    val universe = Universe.eligible(asOf, db)
    if (universe.isEmpty) return Map.empty

    val rs = computeRelativeStrength(asOf, universe, db)
    val pb = computePBBandPosition(asOf, universe, db)

    val common = rs.keySet intersect pb.keySet
    if (common.isEmpty) return Map.empty

    val rsRank = percentileRank(common.iterator.map(c => c -> rs(c)).toMap, higherIsBetter = true)
    val pbRank = percentileRank(common.iterator.map(c => c -> pb(c)).toMap, higherIsBetter = false)

    val composite = common.iterator.map { c =>
      c -> (0.6 * rsRank.getOrElse(c, 0.0) + 0.4 * pbRank.getOrElse(c, 0.0))
    }.toMap

    val picks = composite.toSeq.sortBy(-_._2).take(topN).map(_._1)
    if (picks.isEmpty) Map.empty
    else {
      val w = 1.0 / picks.size
      picks.map(_ -> w).toMap
    }
  }

  /** 63-day skip-5 total return (dividend-unadjusted for v1; DRIP inside backtest
   *  will compensate on a portfolio-average basis). */
  private def computeRelativeStrength(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    val codesList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH recent AS (
        SELECT company_code, date, closing_price
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '120 days'
          AND company_code IN (#$codesList)
          AND closing_price > 0
      ),
      ranked AS (
        SELECT company_code, date, closing_price,
               ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date DESC) AS rn_from_latest
        FROM recent
      )
      SELECT ends.company_code, (ends.closing_price - starts.closing_price) / starts.closing_price AS ret
      FROM ranked ends
      JOIN ranked starts
        ON ends.company_code = starts.company_code
       AND ends.rn_from_latest = 5
       AND starts.rn_from_latest = 68
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** Current P/B relative to 3.5y median. Lower = cheaper vs own history. */
  private def computePBBandPosition(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    val codesList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH hist AS (
        SELECT company_code,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price_book_ratio) AS pb_median
        FROM stock_per_pbr_dividend_yield
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '3 years 6 months'
          AND company_code IN (#$codesList)
          AND price_book_ratio > 0
        GROUP BY company_code
      ),
      current_pb AS (
        SELECT DISTINCT ON (company_code) company_code, price_book_ratio AS pb_now
        FROM stock_per_pbr_dividend_yield
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '10 days'
          AND company_code IN (#$codesList)
          AND price_book_ratio > 0
        ORDER BY company_code, date DESC
      )
      SELECT h.company_code, c.pb_now / h.pb_median
      FROM hist h JOIN current_pb c USING (company_code)
      WHERE h.pb_median > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** Turn raw scores into [0, 1] cross-sectional percentile ranks. */
  private def percentileRank(scores: Map[String, Double], higherIsBetter: Boolean): Map[String, Double] = {
    if (scores.isEmpty) return Map.empty
    val sorted = scores.toSeq.sortBy(kv => if (higherIsBetter) kv._2 else -kv._2)
    val n = sorted.size
    sorted.zipWithIndex.map { case ((code, _), idx) =>
      code -> (idx.toDouble + 1) / n
    }.toMap
  }
}
