package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * v2 strategy implementing the plan's Alpha Stack design:
 *
 *   Hard filters (QualityFilter):
 *     F-Score >= 5, Drop Score < 10 on latest-available quarterly snapshot.
 *
 *   Alpha Stack (both must exceed threshold):
 *     Fundamental Acceleration  (30% weight) — monthly revenue 3m-YoY
 *     Institutional Flow 20d    (25% weight) — foreign + trust net buy / volume
 *
 *   Timing Overlay (additive):
 *     Relative Strength (15%)  — 63d skip-5 return vs peers
 *     Technical Confirmation (15%) — MA/volume gates
 *     P/B Band         (15%)   — mean-reversion vs own 3.5y
 *
 * Composite score = weighted sum of percentile ranks.
 * Entry rule: only score a stock if it passes Quality Filter AND both Alpha
 * Stack factors' rank exceeds threshold. Top-N selection at monthly rebalance.
 *
 * Rebalance timing: first trading day of each month (same as v1). Plan calls
 * for mid-month-plus after fiscal reports are available but here fundamentals
 * are monthly revenue + daily flow so earlier rebalance works.
 */
class AlphaStackStrategy(
  topN: Int = 10,
  alphaStackPercentileFund: Double = 0.50,
  alphaStackPercentileFlow: Double = 0.50
) extends Strategy {

  override val name: String = s"alpha-stack-top$topN"

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

  /** Publicly exposed composite (Alpha Stack gate + weighted percentile ranks)
   *  so RankMetrics can compute IC. */
  def computeComposite(asOf: LocalDate, db: Database): Map[String, Double] = {
    val baseUniverse = Universe.eligible(asOf, db)
    if (baseUniverse.isEmpty) return Map.empty

    // 1. Quality filter: drop landmines
    val qualityPool = QualityFilter.eligible(asOf, baseUniverse, db)
    if (qualityPool.isEmpty) return Map.empty

    // 2. Compute Alpha Stack factors
    val fund = Signals.revenueYoY3M(asOf, qualityPool, db)
    val flow = Signals.institutionalFlow20d(asOf, qualityPool, db)
    val common = fund.keySet intersect flow.keySet
    if (common.isEmpty) return Map.empty

    val fundRank = percentileRank(common.iterator.map(c => c -> fund(c)).toMap, higherIsBetter = true)
    val flowRank = percentileRank(common.iterator.map(c => c -> flow(c)).toMap, higherIsBetter = true)

    // 3. Alpha Stack gate
    val gated = common.filter(c =>
      fundRank.getOrElse(c, 0.0) >= alphaStackPercentileFund &&
        flowRank.getOrElse(c, 0.0) >= alphaStackPercentileFlow
    )
    if (gated.isEmpty) return Map.empty

    // 4. Timing overlay factors on gated pool
    val rs = relativeStrength(asOf, gated, db)
    val tech = Signals.technicalConfirmation(asOf, gated, db)
    val pb = pbBandPosition(asOf, gated, db)

    val rsRank = percentileRank(rs, higherIsBetter = true)
    val pbRank = percentileRank(pb, higherIsBetter = false) // low = cheap = good

    // 5. Composite: weighted sum over gated pool
    gated.iterator.map { c =>
      val score =
        0.30 * fundRank.getOrElse(c, 0.0) +
          0.25 * flowRank.getOrElse(c, 0.0) +
          0.15 * rsRank.getOrElse(c, 0.0) +
          0.15 * tech.getOrElse(c, 0.0) +
          0.15 * pbRank.getOrElse(c, 0.0)
      c -> score
    }.toMap
  }

  // --- factor subcomponents (kept as private methods for clarity) ---

  private def relativeStrength(asOf: LocalDate, codes: Set[String], db: Database): Map[String, Double] = {
    if (codes.isEmpty) return Map.empty
    val codeList = codes.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH recent AS (
        SELECT company_code, date, closing_price,
               ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date DESC) AS rn
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '120 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
      )
      SELECT a.company_code, (a.closing_price - b.closing_price) / b.closing_price
      FROM recent a JOIN recent b USING (company_code)
      WHERE a.rn = 5 AND b.rn = 68
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  private def pbBandPosition(asOf: LocalDate, codes: Set[String], db: Database): Map[String, Double] = {
    if (codes.isEmpty) return Map.empty
    val codeList = codes.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH hist AS (
        SELECT company_code,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price_book_ratio) AS pb_median
        FROM stock_per_pbr_dividend_yield
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '3 years 6 months'
          AND company_code IN (#$codeList)
          AND price_book_ratio > 0
        GROUP BY company_code
      ),
      current_pb AS (
        SELECT DISTINCT ON (company_code) company_code, price_book_ratio AS pb_now
        FROM stock_per_pbr_dividend_yield
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '10 days'
          AND company_code IN (#$codeList)
          AND price_book_ratio > 0
        ORDER BY company_code, date DESC
      )
      SELECT h.company_code, c.pb_now / h.pb_median
      FROM hist h JOIN current_pb c USING (company_code)
      WHERE h.pb_median > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  private def percentileRank(scores: Map[String, Double], higherIsBetter: Boolean): Map[String, Double] = {
    if (scores.isEmpty) return Map.empty
    val sorted = scores.toSeq.sortBy(kv => if (higherIsBetter) kv._2 else -kv._2)
    val n = sorted.size
    sorted.zipWithIndex.map { case ((code, _), idx) =>
      code -> (idx.toDouble + 1) / n
    }.toMap
  }
}
