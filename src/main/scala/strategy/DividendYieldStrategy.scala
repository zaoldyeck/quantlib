package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * v5b — single-factor dividendYield strategy. FactorResearch found it is the
 * single strongest factor in the expanded 32-factor candidate pool:
 *   dividendYield IC +0.0553 (t=+3.35)
 *   pbBandPosition IC +0.0439 (t=+3.09)  (the v3/v4 base factor)
 *
 * dividendYield has low pairwise correlation (|ρ|<0.5) with any other
 * surviving factor, so it's not a duplicate of pbBand — it captures payers
 * with stable cash returns, a different slice than pure relative-cheapness.
 *
 * Filter: drop_score < 10 (same hygiene filter as v3). Top-N by yield,
 * equal-weighted.
 */
class DividendYieldStrategy(topN: Int = 10) extends Strategy {
  override val name: String = s"dividend-yield-top$topN"

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
    Signals.dividendYield(asOf, safe, db)
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
