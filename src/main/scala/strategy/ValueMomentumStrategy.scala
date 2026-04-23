package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * v4b — ValueRevertStrategy with a momentum-secondary overlay to filter out
 * "cheap and crashing" value traps.
 *
 * Rationale: pbBandPosition selects stocks that are statistically cheap vs
 * their own history (IC +0.044, t=+3.09). But a subset of "cheap" stocks are
 * cheap *because they're dying* — fundamentals deteriorating, price falling,
 * book value shrinking. The composite is symmetric: it doesn't distinguish
 * mean-reverters from free-falls.
 *
 * distFrom52wHigh (IC +0.031, t=+1.70, hit-rate 66.7%) is a price-position
 * factor: (px_now - max_252d) / max_252d, so 0 = at-high, -0.20 = 20% below.
 * Its alpha signal overlaps only minimally with pbBand (correlation < 0.5 per
 * FactorResearch), so combining them in a *two-stage* hierarchy (pool by
 * pbBand, rank by distance-from-high) preserves signal without the dilution
 * that killed MultiFactorStrategy's equal-weighted z-score composite.
 *
 * Selection:
 *   1. Universe.eligible ∩ (drop_score < 10)
 *   2. Top `poolSize` (default 20) by pbBandPosition (inverted: lower = better)
 *   3. Within pool, rank by distFrom52wHigh; take top `topN` (default 10)
 *
 * Equal-weighted allocation.
 */
class ValueMomentumStrategy(topN: Int = 10, poolSize: Int = 20) extends Strategy {
  override val name: String = s"value-momentum-top$topN-pool$poolSize"

  override def rebalanceDates(start: LocalDate, end: LocalDate, db: Database): Seq[LocalDate] =
    RebalanceCalendar.monthlyAfterDay(start, end, db)

  override def targetWeights(asOf: LocalDate, db: Database): Map[String, Double] = {
    val composite = computeComposite(asOf, db)
    if (composite.isEmpty) return Map.empty
    val picks = composite.toSeq.sortBy(-_._2).take(topN).map(_._1)
    if (picks.isEmpty) Map.empty else picks.map(_ -> (1.0 / picks.size)).toMap
  }

  def computeComposite(asOf: LocalDate, db: Database): Map[String, Double] = {
    val universe = Universe.eligible(asOf, db)
    if (universe.isEmpty) return Map.empty

    val safe = dropScoreFilter(asOf, universe, db)
    if (safe.isEmpty) return Map.empty

    // Stage 1: pool = top-poolSize by value (pbBand inverted).
    val pbRaw = Signals.pbBandPosition(asOf, safe, db)
    if (pbRaw.isEmpty) return Map.empty
    val valueRanked = pbRaw.toSeq.sortBy(_._2)  // ascending: lowest pbBand = cheapest = best
    val pool = valueRanked.take(poolSize).map(_._1).toSet
    if (pool.isEmpty) return Map.empty

    // Stage 2: score by distFrom52wHigh within pool. Higher (less negative) = better.
    Signals.distFrom52wHigh(asOf, pool, db)
  }

  private def dropScoreFilter(asOf: LocalDate, universe: Set[String], db: Database): Set[String] = {
    val (year, quarter) = PublicationLag.asOfQuarter(asOf)
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT DISTINCT ON (company_code) company_code
      FROM growth_analysis_ttm
      WHERE company_code IN (#$codeList)
        AND (year < #$year OR (year = #$year AND quarter <= #$quarter))
        AND COALESCE(drop_score, 0) < 10
      ORDER BY company_code, year DESC, quarter DESC
    """.as[String]
    Await.result(db.run(q), Duration.Inf).toSet
  }
}
