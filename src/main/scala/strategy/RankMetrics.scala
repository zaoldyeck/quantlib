package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Selection-skill metrics — the plan's "Primary KPIs". These are beta-immune:
 * a strategy can post +50% CAGR during a bull market and still have zero
 * information content if its score isn't correlated with relative outperformance.
 *
 * Information Coefficient (IC) — Spearman rank correlation between the strategy's
 * composite score and the forward N-trading-day return, measured at each rebalance
 * date. Industry yardstick (Grinold-Kahn):
 *   IC  0.02       — noise
 *   IC ~0.04-0.05  — modest skill; consistent with many MSCI factor indices
 *   IC  0.08+      — excellent
 *
 * A single-month IC is noisy (σ ≈ 0.10-0.20); judgement comes from the series
 * mean and t-statistic. Rule of thumb: t ≥ 2.0 = statistically non-zero.
 */
object RankMetrics {
  final case class ICSnapshot(rebalanceDate: LocalDate, universeSize: Int, ic: Double)

  final case class ICSummary(
    mean: Double,
    std: Double,
    tStat: Double,
    positiveMonths: Int,
    totalMonths: Int,
    hitRate: Double
  ) {
    def show: String =
      f"""|=== Information Coefficient ===
          |  mean IC:          ${mean}%+.4f
          |  std IC:           ${std}%.4f
          |  t-stat:           ${tStat}%+.2f  ${if (math.abs(tStat) >= 2.0) "(significant)" else "(not significant)"}
          |  positive months:  $positiveMonths / $totalMonths (${hitRate * 100}%.1f%%)
          |""".stripMargin
  }

  /** Compute IC at each rebalance date. Forward return measured from asOf close
   *  to horizonDays later. Spearman correlation of composite score vs return
   *  across all stocks present in both. */
  def computeICSeries(
    computeComposite: (LocalDate, Database) => Map[String, Double],
    rebalanceDates: Seq[LocalDate],
    horizonDays: Int,
    db: Database
  ): Seq[ICSnapshot] = {
    rebalanceDates.flatMap { date =>
      val scores = computeComposite(date, db)
      if (scores.isEmpty) None
      else {
        val returns = forwardReturns(date, horizonDays, scores.keySet, db)
        val pairs = scores.toSeq.flatMap { case (code, score) =>
          returns.get(code).map(score -> _)
        }
        if (pairs.size < 20) None
        else Some(ICSnapshot(date, pairs.size, spearman(pairs)))
      }
    }
  }

  def summarize(series: Seq[ICSnapshot]): ICSummary = {
    val ics = series.map(_.ic)
    val n = ics.size
    if (n == 0) return ICSummary(0, 0, 0, 0, 0, 0)
    val mean = ics.sum / n
    val std = if (n < 2) 0.0
              else math.sqrt(ics.map(x => math.pow(x - mean, 2)).sum / (n - 1))
    val tStat = if (std > 0) mean / (std / math.sqrt(n.toDouble)) else 0.0
    val positive = ics.count(_ > 0)
    ICSummary(mean, std, tStat, positive, n, positive.toDouble / n)
  }

  /** Spearman rank correlation on a sequence of (x, y) pairs. */
  private def spearman(pairs: Seq[(Double, Double)]): Double = {
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

  /** Fractional-rank conversion with tie-breaking via mean rank. */
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

  /** Forward N-trading-day return for each code. Skips codes with insufficient
   *  future price data (e.g. delisted before horizon). */
  private def forwardReturns(
    asOf: LocalDate,
    horizonDays: Int,
    codes: Set[String],
    db: Database
  ): Map[String, Double] = {
    if (codes.isEmpty) return Map.empty
    val codeList = codes.map(c => s"'$c'").mkString(",")
    // We need two closes per code: "at asOf" and "horizonDays trading days later".
    // Using a window of 2x horizonDays calendar days to cover weekends/holidays.
    val q = sql"""
      WITH candidates AS (
        SELECT company_code, date, closing_price,
               ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date) AS rn_asc,
               ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date DESC) AS rn_desc
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
    Await.result(db.run(q), Duration.Inf).toMap
  }
}
