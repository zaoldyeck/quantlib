package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * v5 — data-driven 4-factor composite. Follows the same FactorResearch
 * pipeline as v3, but with (a) day-15+ rebalance timing (fresh monthly
 * revenue) and (b) an expanded 32-factor candidate pool.
 *
 * Survivors (|t-stat| >= 2, pairwise |ρ| < 0.5 to each other):
 *
 *   dividendYield       IC +0.055 (t=+3.35) — new king factor
 *   pbBandPosition      IC +0.044 (t=+3.09) — value vs own history, inverted
 *   fcfYield            IC +0.025 (t=+2.58) — new, cash-generation cheap
 *   revenueYoYLatest    IC +0.028 (t=+2.27) — timing-fix win; the 3M smoothed
 *                                               version was t=1.30 before
 *
 * Hard filter: drop_score < 10 (t=-2.93 as exclusion signal).
 *
 * Composite: equal-weighted z-score. IC-weighting tried but made no material
 * difference and adds one more over-fit knob (IC weights get recomputed
 * every rerun so their stability isn't measurable on 99 months).
 *
 * Selection: top N by composite, equal-weighted allocation.
 */
class MultiFactorStrategy(topN: Int = 10) extends Strategy {
  override val name: String = s"multi-factor-top$topN"

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

    // Raw factor values — sign applied so "higher = buy" uniformly.
    //  pbBandPosition: lower cheaper → invert
    val yield_   = Signals.dividendYield(asOf, safe, db)
    val pb       = Signals.pbBandPosition(asOf, safe, db).view.mapValues(-_).toMap
    val fcf      = Signals.fcfYield(asOf, safe, db)
    val revYoY   = Signals.revenueYoYLatest(asOf, safe, db)

    // Only include codes with all four factors (drop those with missing data).
    val common = yield_.keySet intersect pb.keySet intersect fcf.keySet intersect revYoY.keySet
    if (common.size < 20) return Map.empty

    // Z-score within the surviving cohort, winsorized at ±3σ.
    val zYield = zscore(common.map(c => c -> yield_(c)).toMap)
    val zPb    = zscore(common.map(c => c -> pb(c)).toMap)
    val zFcf   = zscore(common.map(c => c -> fcf(c)).toMap)
    val zRev   = zscore(common.map(c => c -> revYoY(c)).toMap)

    common.map { c =>
      c -> (zYield(c) + zPb(c) + zFcf(c) + zRev(c)) / 4.0
    }.toMap
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

  /** Winsorized z-score: clip to [-3, 3] to limit outlier damage. */
  private def zscore(raw: Map[String, Double]): Map[String, Double] = {
    val vals = raw.values.toSeq
    val n = vals.size
    if (n < 2) return raw.view.mapValues(_ => 0.0).toMap
    val mean = vals.sum / n
    val std = math.sqrt(vals.map(v => math.pow(v - mean, 2)).sum / (n - 1))
    if (std <= 0) return raw.view.mapValues(_ => 0.0).toMap
    raw.view.mapValues(v => math.max(-3.0, math.min(3.0, (v - mean) / std))).toMap
  }
}
