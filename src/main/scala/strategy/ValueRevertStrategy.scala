package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * v3 strategy — data-driven minimal. Built from FactorResearch findings on
 * TW 2018-2026: of 16 candidate factors, only two survived t-stat ≥ 2.0:
 *
 *   pbBandPosition (IC +0.051, t = +3.95) — strong value signal
 *   dropScore      (IC -0.027, t = -2.82) — hard reject filter
 *
 * Every other factor (momentum, institutional flow, growth rates, quality
 * composites) was noise or wrong-sign in this window. The v2 AlphaStack
 * strategy's composite was *worse* than pbBand alone because of factor
 * dilution. v3 keeps only the evidence-backed factors and nothing else.
 *
 * Selection:
 *   Universe ∩ (drop_score < 10) → sort ascending by pbBandPosition → top N.
 *
 * Allocation: equal-weight (10% each with N=10). No momentum overlay, no
 * flow gating, no quality composite, no sector constraint — all rejected by
 * the IC measurement.
 */
class ValueRevertStrategy(topN: Int = 10) extends Strategy {
  override val name: String = s"value-revert-top$topN"

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
    val composite = computeComposite(asOf, db)
    if (composite.isEmpty) return Map.empty
    val picks = composite.toSeq.sortBy(-_._2).take(topN).map(_._1)
    if (picks.isEmpty) Map.empty
    else picks.map(_ -> (1.0 / picks.size)).toMap
  }

  /** Composite is just pbBandPosition (inverted — lower PB vs own history is
   *  better) restricted to the drop-score-safe pool. Exposed so RankMetrics
   *  can compute IC to verify the standalone signal carries over to a full
   *  backtest. */
  def computeComposite(asOf: LocalDate, db: Database): Map[String, Double] = {
    val universe = Universe.eligible(asOf, db)
    if (universe.isEmpty) return Map.empty

    // Hard filter: dropScore < 10 only (f_score filter rejected — it had IC
    // of just +0.008 and is NOT a useful avoidance signal either).
    val safe = dropScoreFilter(asOf, universe, db)
    if (safe.isEmpty) return Map.empty

    val pbRaw = Signals.pbBandPosition(asOf, safe, db)
    // pbBand lower = cheaper vs own history = better. Invert so higher score = buy.
    pbRaw.view.mapValues(-_).toMap
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
