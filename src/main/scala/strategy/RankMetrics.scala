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

  /** Compute IC at each rebalance date. Forward return is the total return from
   *  the T+1 close (first trading day after asOf) over horizonDays trading days
   *  (see forwardReturns — adjusted for dividends / capital reductions). Spearman
   *  correlation of composite score vs return across all stocks present in both. */
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

  /** Spearman rank correlation on a sequence of (x, y) pairs. Package-visible so
   *  FactorResearch shares this one canonical implementation (no twin copy). */
  private[strategy] def spearman(pairs: Seq[(Double, Double)]): Double = {
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

  /** Forward N-trading-day **total return** for each code, measured from the T+1
   *  close (first trading day strictly after `asOf`) to the close `horizonDays`
   *  trading days later. Two corrections vs. a naive raw-close price return:
   *
   *  1. **Total return, not price return.** The raw `(p1/p0 - 1)` is divided by
   *     the product of every corporate-action factor whose ex-date falls inside
   *     the holding window `(entry_date, exit_date]`:
   *       - dividend (cash and/or stock): factor = ex_reference_price / pre_ex_close
   *         (`ex_right_ex_dividend_reference_price / closing_price_before_ex_right_ex_dividend`;
   *          the exchange reference price already nets cash + stock components)
   *       - capital reduction: factor = post_reduction_ref / pre_reduction_close
   *     This is the two-point form of `research/prices.py::fetch_adjusted_panel`'s
   *     back-adjustment (adj_close[t1]/adj_close[t0] = (raw1/raw0) / ∏ factor).
   *     Without it, TWSE ex-dividend season (~1/3 of names ex in a single summer
   *     month, avg reference cut ≈ -4%) systematically understates the forward
   *     return of high-yield / value names and depresses their IC.
   *  2. **T+1 entry.** Signals use the T (=asOf) close, so the earliest fillable
   *     entry is the T+1 close — matching the backtest/live +1-day contract and
   *     `research/apex/factors.py` (`close.shift(-(1+k)) / close.shift(-1)`).
   *
   *  Factor guards (0.05<f<5 dividends, 0.05<f<100 reductions) and column
   *  semantics mirror `prices.py`. Pure splits absent from BOTH action tables
   *  (a post-2024 ETF-only TWSE data gap, not this common-stock universe) are out
   *  of scope. Codes without a full future window (delisted/long-suspended before
   *  the horizon) are skipped. */
  private[strategy] def forwardReturns(
    asOf: LocalDate,
    horizonDays: Int,
    codes: Set[String],
    db: Database
  ): Map[String, Double] = {
    if (codes.isEmpty) return Map.empty
    val codeList = codes.map(c => s"'$c'").mkString(",")
    // Trading-day counting: entry = 1st future trading day (rn=1 = T+1), exit =
    // (horizonDays+1)-th. A generous 3×(horizon+1) calendar window guarantees the
    // N-th trading day survives the ~9-day Lunar New Year close (the old fixed 2×
    // window silently dropped those months) while still excluding pathological
    // long suspensions rather than pricing an unintended multi-month return.
    val q = sql"""
      WITH fut AS (
        SELECT company_code, date, closing_price,
               ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date) AS rn
        FROM daily_quote
        WHERE market = 'twse'
          AND date > #${"'" + asOf + "'"}::date
          AND date <= #${"'" + asOf + "'"}::date + INTERVAL '#${(horizonDays + 1) * 3} days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
      ),
      span AS (
        SELECT en.company_code, en.date AS d0, en.closing_price AS p0,
                                ex.date AS d1, ex.closing_price AS p1
        FROM fut en JOIN fut ex USING (company_code)
        WHERE en.rn = 1 AND ex.rn = #${horizonDays + 1}
      ),
      events AS (
        SELECT company_code, date AS ex_date,
               ex_right_ex_dividend_reference_price
                 / closing_price_before_ex_right_ex_dividend AS factor
        FROM ex_right_dividend
        WHERE market = 'twse'
          AND company_code IN (#$codeList)
          AND closing_price_before_ex_right_ex_dividend > 0
          AND ex_right_ex_dividend_reference_price > 0
          AND ex_right_ex_dividend_reference_price
                / closing_price_before_ex_right_ex_dividend > 0.05
          AND ex_right_ex_dividend_reference_price
                / closing_price_before_ex_right_ex_dividend < 5.0
        UNION ALL
        SELECT company_code, date AS ex_date,
               post_reduction_reference_price
                 / closing_price_on_the_last_trading_date AS factor
        FROM capital_reduction
        WHERE market = 'twse'
          AND company_code IN (#$codeList)
          AND closing_price_on_the_last_trading_date > 0
          AND post_reduction_reference_price > 0
          AND post_reduction_reference_price
                / closing_price_on_the_last_trading_date > 0.05
          AND post_reduction_reference_price
                / closing_price_on_the_last_trading_date < 100.0
      ),
      corr AS (
        SELECT s.company_code,
               COALESCE(EXP(SUM(LN(ev.factor))), 1.0) AS c
        FROM span s
        LEFT JOIN events ev
          ON ev.company_code = s.company_code
         AND ev.ex_date > s.d0
         AND ev.ex_date <= s.d1
        GROUP BY s.company_code
      )
      SELECT s.company_code, ((s.p1 / s.p0) / corr.c) - 1
      FROM span s JOIN corr USING (company_code)
      WHERE s.p0 > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }
}
