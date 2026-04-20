package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

/**
 * Factor research pipeline. Instead of loading a set of factors by intuition,
 * this measures each factor's standalone skill (IC) and pairwise redundancy,
 * letting the final strategy be built from evidence.
 *
 * Workflow:
 *   1. Individual IC — run each candidate factor through the full period,
 *      compute Spearman correlation with forward-21-day return each month.
 *      Factors with t-stat ≥ ~1.5 and mean IC > 0 keep. Others reject.
 *   2. Pairwise correlation — for kept factors, compute the average
 *      cross-sectional rank correlation between every pair. Pairs with
 *      |ρ| > 0.7 are effectively the same signal; keep only the higher-IC one.
 *   3. Stacked-IC validation — build a composite of the surviving factors
 *      (equal-weight or IC-weighted) and verify combined IC exceeds the best
 *      single-factor IC (otherwise the composite is redundant).
 *
 * Intentionally stops short of threshold optimization: picking cutoffs on the
 * same dataset that measures IC is a classic overfitting trap. Do that with
 * train/test split only.
 */
object FactorResearch {
  type FactorFn = (LocalDate, Set[String], Database) => Map[String, Double]

  final case class FactorICResult(
    name: String,
    higherIsBetter: Boolean,
    summary: RankMetrics.ICSummary,
    monthlyScores: Seq[(LocalDate, Map[String, Double])]
  )

  /** For each factor, walk rebalance dates, compute score and its IC vs forward
   *  return. Returns series + summary per factor. */
  def individualICs(
    factors: Seq[(String, FactorFn, Boolean)],  // (name, fn, higherIsBetter)
    universeFn: (LocalDate, Database) => Set[String],
    rebalanceDates: Seq[LocalDate],
    horizonDays: Int,
    db: Database
  ): Seq[FactorICResult] = {
    factors.map { case (name, fn, higherIsBetter) =>
      println(s"[research] computing individual IC for factor: $name")
      val snapshots = rebalanceDates.flatMap { date =>
        val universe = universeFn(date, db)
        if (universe.isEmpty) None
        else {
          val rawScores = fn(date, universe, db)
          if (rawScores.isEmpty) None
          else {
            // Flip sign for lower-is-better factors so "higher score = buy" uniformly.
            val directionalScores = if (higherIsBetter) rawScores else rawScores.view.mapValues(-_).toMap
            Some(date -> directionalScores)
          }
        }
      }
      val icSnapshots = snapshots.flatMap { case (date, scores) =>
        val returns = RankMetricsEx.forwardReturns(date, horizonDays, scores.keySet, db)
        val pairs = scores.toSeq.flatMap { case (code, score) =>
          returns.get(code).map(score -> _)
        }
        if (pairs.size < 20) None
        else Some(RankMetrics.ICSnapshot(date, pairs.size, RankMetricsEx.spearman(pairs)))
      }
      val summary = RankMetrics.summarize(icSnapshots)
      FactorICResult(name, higherIsBetter, summary, snapshots)
    }
  }

  /** Pairwise average cross-sectional rank correlation. Uses already-computed
   *  monthly scores from individualICs to avoid re-querying. */
  def pairwiseCorrelations(factors: Seq[FactorICResult]): Map[(String, String), Double] = {
    val result = scala.collection.mutable.Map.empty[(String, String), Double]
    for {
      i <- factors.indices
      j <- (i + 1) until factors.size
    } {
      val a = factors(i)
      val b = factors(j)
      val perDate = a.monthlyScores.flatMap { case (date, aScores) =>
        b.monthlyScores.find(_._1 == date).map { case (_, bScores) =>
          val common = aScores.keySet intersect bScores.keySet
          if (common.size < 20) None
          else {
            val pairs = common.toSeq.map(c => aScores(c) -> bScores(c))
            Some(RankMetricsEx.spearman(pairs))
          }
        }.flatten
      }
      val meanCorr = if (perDate.isEmpty) 0.0 else perDate.sum / perDate.size
      result((a.name, b.name)) = meanCorr
    }
    result.toMap
  }

  /** Pretty-print the research report. */
  def report(results: Seq[FactorICResult], corr: Map[(String, String), Double]): String = {
    val sb = new StringBuilder
    sb.append("=== Individual IC (sorted by mean |IC|) ===\n")
    sb.append(f"${"factor"}%-32s  ${"IC"}%8s  ${"t-stat"}%8s  ${"pos%%"}%6s  ${"months"}%7s\n")
    results.sortBy(r => -math.abs(r.summary.mean)).foreach { r =>
      val s = r.summary
      sb.append(f"${r.name}%-32s  ${s.mean}%+.4f  ${s.tStat}%+7.2f  ${s.hitRate * 100}%5.1f%%  ${s.totalMonths}%7d\n")
    }
    sb.append("\n=== Pairwise correlation (|ρ| > 0.5 only) ===\n")
    corr.toSeq.sortBy(-_._2.abs).filter(_._2.abs > 0.5).foreach { case ((a, b), c) =>
      sb.append(f"  ${a}%-30s  ${b}%-30s  ρ=${c}%+.3f\n")
    }
    sb.toString
  }
}

/** Helpers borrowed from RankMetrics' private methods — exposed here for
 *  FactorResearch. Could be folded back into RankMetrics if both modules
 *  stabilize. */
private[strategy] object RankMetricsEx {
  def spearman(pairs: Seq[(Double, Double)]): Double = {
    val n = pairs.size
    if (n < 2) return 0.0
    val xRanks = averageRanks(pairs.map(_._1))
    val yRanks = averageRanks(pairs.map(_._2))
    val ranked = xRanks.zip(yRanks)
    val meanX = xRanks.sum / n
    val meanY = yRanks.sum / n
    val num = ranked.map { case (rx, ry) => (rx - meanX) * (ry - meanY) }.sum
    val denX = math.sqrt(xRanks.map(r => math.pow(r - meanX, 2)).sum)
    val denY = math.sqrt(yRanks.map(r => math.pow(r - meanY, 2)).sum)
    if (denX == 0 || denY == 0) 0.0 else num / (denX * denY)
  }

  private def averageRanks(xs: Seq[Double]): Seq[Double] = {
    val sorted = xs.zipWithIndex.sortBy(_._1)
    val ranks = Array.ofDim[Double](xs.size)
    var i = 0
    while (i < sorted.size) {
      var j = i
      while (j + 1 < sorted.size && sorted(j + 1)._1 == sorted(i)._1) j += 1
      val meanRank = (i + j) / 2.0 + 1
      (i to j).foreach(k => ranks(sorted(k)._2) = meanRank)
      i = j + 1
    }
    ranks.toSeq
  }

  def forwardReturns(asOf: LocalDate, horizonDays: Int, codes: Set[String],
                     db: Database): Map[String, Double] = {
    if (codes.isEmpty) return Map.empty
    val codeList = codes.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH candidates AS (
        SELECT company_code, closing_price,
               ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date) AS rn_asc
        FROM daily_quote
        WHERE market = 'twse'
          AND date > #${"'" + asOf + "'"}::date
          AND date <= #${"'" + asOf + "'"}::date + INTERVAL '#${horizonDays * 2} days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
      ),
      start_px AS (
        SELECT DISTINCT ON (company_code) company_code, closing_price AS p0
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date > #${"'" + asOf + "'"}::date - INTERVAL '10 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
        ORDER BY company_code, date DESC
      ),
      end_px AS (
        SELECT company_code, closing_price AS p1
        FROM candidates
        WHERE rn_asc = #${horizonDays}
      )
      SELECT s.company_code, (e.p1 - s.p0) / s.p0
      FROM start_px s JOIN end_px e USING (company_code)
      WHERE s.p0 > 0
    """.as[(String, Double)]
    import scala.concurrent.Await
    import scala.concurrent.duration.Duration
    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(db.run(q), Duration.Inf).toMap
  }
}
