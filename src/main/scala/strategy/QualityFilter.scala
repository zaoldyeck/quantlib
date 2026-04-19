package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Hard filters that exclude landmines before any scoring. Not alpha sources —
 * these just prevent picking up obviously deteriorating companies.
 *
 * Applied as veto: a stock must pass ALL criteria to remain in the scored pool.
 */
object QualityFilter {
  /** Max acceptable drop_score — higher = more multi-period deterioration signals. */
  val MaxDropScore: Int = 10

  /** Min Piotroski F-Score (8 binary factors; 5+ is a conventional "healthy" cut). */
  val MinFScore: Int = 5

  /** Return the subset of `universe` that passes all hard-filter rules as of `asOf`.
   *  Uses the latest growth_analysis_ttm quarterly snapshot available on D. */
  def eligible(asOf: LocalDate, universe: Set[String], db: Database): Set[String] = {
    if (universe.isEmpty) return Set.empty
    val (year, quarter) = PublicationLag.asOfQuarter(asOf)
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT company_code
      FROM growth_analysis_ttm
      WHERE company_code IN (#$codeList)
        AND (year < #$year OR (year = #$year AND quarter <= #$quarter))
        AND COALESCE(drop_score, 0) < #$MaxDropScore
        AND COALESCE(f_score, 0) >= #$MinFScore
      ORDER BY company_code, year DESC, quarter DESC
    """.as[String]
    // Each company may match several rows across quarters; distinct collapses to one.
    Await.result(db.run(q), Duration.Inf).toSet
  }
}
