package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Regime-switching wrapper around ValueRevertStrategy.
 *
 * Motivation: v3 ValueRevert has statistically significant selection skill
 * (IC +0.0515, t=3.99 full-period), but during the 2023-2025 AI cycle 0050
 * returned +40% CAGR on TSMC concentration — a cap-weight tailwind that
 * long-only value cannot capture because TSMC is never cheap on PB. v3's
 * absolute CAGR trailed 0050 by -162pp OOS despite retaining IC > 0.
 *
 * This strategy preserves value alpha in mean-reverting regimes while riding
 * the index when large-cap momentum dominates.
 *
 * Regime signal: 0050 trailing 63-day return (split-adjusted).
 *   ≥ RegimeThreshold → 100% 0050 (trend regime)
 *   otherwise          → delegate to ValueRevertStrategy (mean-reversion regime)
 *
 * 63 days (~3 months) balances:
 *   - short enough to flip during regime transitions (not annual lag)
 *   - long enough to filter daily noise (daily 0050 returns are ±1-2%)
 *
 * RegimeThreshold = 0.10 means the trailing quarter annualizes to > 40%
 * absolute return — strong enough that stock-picking alpha is unlikely to
 * beat beta exposure net of turnover cost.
 */
class RegimeAwareStrategy(topN: Int = 10, regimeThreshold: Double = 0.05) extends Strategy {
  private val base = new ValueRevertStrategy(topN)
  override val name: String = s"regime-aware-top$topN-thr${(regimeThreshold * 100).toInt}"

  override def rebalanceDates(start: LocalDate, end: LocalDate, db: Database): Seq[LocalDate] =
    base.rebalanceDates(start, end, db)

  override def targetWeights(asOf: LocalDate, db: Database): Map[String, Double] = {
    val composite = computeComposite(asOf, db)
    if (composite.isEmpty) return Map.empty

    val top = composite.toSeq.sortBy(-_._2)
    // Sentinel: the regime branch pushes 0050 with score +Infinity so it
    // always dominates after sorting. composite contains only 0050 in trend
    // regime, only value picks otherwise.
    if (top.head._1 == "0050") Map("0050" -> 1.0)
    else {
      val picks = top.take(topN).map(_._1)
      if (picks.isEmpty) Map.empty else picks.map(_ -> (1.0 / picks.size)).toMap
    }
  }

  /** Exposed for IC measurement. In trend regime returns Map("0050" -> +Inf)
   *  so RankMetrics can still score the decision (IC will be trivially
   *  near-zero those months — correct, because we're not selecting). */
  def computeComposite(asOf: LocalDate, db: Database): Map[String, Double] = {
    val rs = trailing63dReturn("0050", asOf, db)
    rs match {
      case Some(r) if r >= regimeThreshold => Map("0050" -> Double.PositiveInfinity)
      case _                               => base.computeComposite(asOf, db)
    }
  }

  /** Split-adjusted 63-trading-day return ending at asOf. Returns None if
   *  insufficient data (e.g. early in series). */
  private def trailing63dReturn(code: String, asOf: LocalDate, db: Database): Option[Double] = {
    val q = sql"""
      SELECT date, closing_price
      FROM daily_quote
      WHERE market = 'twse' AND company_code = #${"'" + code + "'"}
        AND date <= #${"'" + asOf + "'"}::date
        AND date >= #${"'" + asOf + "'"}::date - INTERVAL '130 days'
        AND closing_price > 0
      ORDER BY date
    """.as[(java.sql.Date, Double)]
    val rows = Await.result(db.run(q), Duration.Inf).map { case (d, p) => d.toLocalDate -> p }
    if (rows.size < 64) return None

    // Detect and neutralize splits in window using same heuristic as Backtester:
    // 3-14 day gap + price ratio 2.5-15x + no ex_right row. Accumulate a
    // multiplicative adjustment applied to pre-split prices so all prices in
    // the window are on the post-split scale.
    val splits: Seq[(LocalDate, Double)] = rows.sliding(2).collect {
      case Seq((prevDate, prevClose), (date, close)) =>
        val gap = java.time.temporal.ChronoUnit.DAYS.between(prevDate, date)
        val ratio = prevClose / close
        if (gap >= 3 && gap <= 14 && ratio >= 2.5 && ratio <= 15 && !hasExRight(code, date, db))
          Some(date -> ratio)
        else None
    }.flatten.toSeq

    val endPrice = rows.last._2
    val startIdx = rows.size - 64
    val rawStartPrice = rows(startIdx)._2
    val startDate = rows(startIdx)._1

    // Any splits strictly after startDate and up to asOf shrink the pre-split
    // anchor accordingly.
    val adj = splits.filter(_._1.isAfter(startDate)).map(_._2).foldLeft(1.0)(_ * _)
    val adjustedStart = rawStartPrice / adj
    Some(endPrice / adjustedStart - 1.0)
  }

  private def hasExRight(code: String, date: LocalDate, db: Database): Boolean = {
    val q = sql"""
      SELECT 1 FROM ex_right_dividend
      WHERE company_code = #${"'" + code + "'"} AND date = #${"'" + date + "'"}::date
      LIMIT 1
    """.as[Int]
    Await.result(db.run(q), Duration.Inf).nonEmpty
  }
}
