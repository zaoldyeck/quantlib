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
      FROM (
        SELECT DISTINCT ON (company_code) company_code, f_score, drop_score
        FROM growth_analysis_ttm
        WHERE company_code IN (#$codeList)
          AND (year < #$year OR (year = #$year AND quarter <= #$quarter))
        ORDER BY company_code, year DESC, quarter DESC
      ) latest
      WHERE COALESCE(drop_score, 0) < #$MaxDropScore
        AND COALESCE(f_score, 0) >= #$MinFScore
    """.as[String]
    // DISTINCT ON collapses each company to its latest available quarter (the PIT
    // snapshot); the veto is then applied to THAT single row, so a stock passes iff its
    // *current* quarter is healthy — not "ever passed in some historical quarter".
    // Without the inner DISTINCT ON the f_score/drop_score predicates matched on ANY
    // quarter, making membership once-passed => always-passed (measured buggy 1033 vs
    // correct 624 names at PIT=2024Q1; 409 deteriorating leaks, e.g. 1301 latest drop=14
    // slipping through on a 2022Q3 drop=8). .toSet also converts the Vector result.
    Await.result(db.run(q), Duration.Inf).toSet
  }
}
