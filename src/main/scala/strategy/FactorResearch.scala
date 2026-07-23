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
        val returns = RankMetrics.forwardReturns(date, horizonDays, scores.keySet, db)
        val pairs = scores.toSeq.flatMap { case (code, score) =>
          returns.get(code).map(score -> _)
        }
        if (pairs.size < 20) None
        else Some(RankMetrics.ICSnapshot(date, pairs.size, RankMetrics.spearman(pairs)))
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
            Some(RankMetrics.spearman(pairs))
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
