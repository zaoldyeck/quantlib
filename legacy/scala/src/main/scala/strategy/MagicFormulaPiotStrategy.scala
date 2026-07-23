package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

/**
 * v8 — faithful port of finlab course MFPiot strategy
 * (github.com/zaoldyeck/finlab_course/MFPiot.ipynb, mf_piot_optimize_5stocks_rsv).
 *
 * Greenblatt Magic Formula + Piotroski F-Score + hard fundamental filters +
 * RSV momentum gate. Composite ranking on (ROIC_rank + EarningsYield_rank).
 *
 * The notebook's in-source comments claim 42.6% (total, not CAGR) over 2014-2019
 * for mf_piot_optimize_5stocks_rsv. This port runs 2018-2026 for apples-to-apples
 * comparison against v4 RegimeAware.
 *
 * Departures from the notebook (all addressing correctness issues I flagged):
 *   - capital_reduction_stocks: was a hard-coded 2014-2019 list, now replaced
 *     with a point-in-time query of the `capital_reduction` table. The original
 *     list is a classic look-ahead bias — it excludes future reducers at dates
 *     when the market couldn't have known.
 *   - F-Score source: notebook computes all 9 components; this port uses the
 *     pre-computed `growth_analysis_ttm.f_score` view. The view may use a
 *     slightly different formulation; a full Piotroski reimplementation is
 *     deferred unless this proxy fails.
 *   - FCF filter: the notebook's FCF = OCF + Investing CF (which is technically
 *     CF-from-ops-and-investing, not FCF). This port uses
 *     `financial_index_ttm.fcf_per_share > 0` as the equivalent health check.
 *   - RSV >= 0.9: kept as-is. High-RSV = near-recent-high momentum filter.
 *   - Universe: Universe.eligible (TWSE common stocks, ADV >= NT$50M) instead
 *     of finlab's larger implicit universe.
 *
 * Selection: top-5 by (ROIC_rank + EarningsYield_rank), equal-weighted.
 */
class MagicFormulaPiotStrategy(
  topN: Int = 5,
  minFScore: Int = 8,
  minRSV: Double = 0.9,
  useFundamentalFilters: Boolean = true
) extends Strategy {
  override val name: String = {
    val rsvTag = if (minRSV > 0) s"-rsv${(minRSV * 100).toInt}" else ""
    val filterTag = if (useFundamentalFilters) s"-f$minFScore" else "-raw"
    s"mf-piot-top$topN$filterTag$rsvTag"
  }

  override def rebalanceDates(start: LocalDate, end: LocalDate, db: Database): Seq[LocalDate] =
    RebalanceCalendar.monthlyAfterDay(start, end, db)

  override def targetWeights(asOf: LocalDate, db: Database): Map[String, Double] = {
    val composite = computeComposite(asOf, db)
    if (composite.isEmpty) return Map.empty
    val picks = composite.toSeq.sortBy(-_._2).take(topN).map(_._1)
    if (picks.isEmpty) Map.empty else picks.map(_ -> (1.0 / picks.size)).toMap
  }

  def computeComposite(asOf: LocalDate, db: Database): Map[String, Double] = {
    // 1. Universe ∩ not-reduced-recently (PIT blacklist)
    val universe0 = Universe.eligible(asOf, db)
    val blacklist = Signals.capitalReductionBlacklist(asOf, db)
    val universe = universe0 -- blacklist
    if (universe.isEmpty) return Map.empty

    // 2. Hard filters. Each filter can be disabled independently:
    //    minRSV = 0 skips RSV gate; useFundamentalFilters = false skips F-Score +
    //    revYoY + opGrowth + FCF (i.e. pure Magic Formula baseline).
    val survivors: Set[String] = if (!useFundamentalFilters && minRSV <= 0) universe else {
      val base = if (useFundamentalFilters) {
        val fscore = Signals.growthAnalysisField("f_score")(asOf, universe, db)
        val revYoY = Signals.revenueYoYLatest(asOf, universe, db)
        val opGrowth = Signals.opIncomeGrowthYoY(asOf, universe, db)
        val fcfPs = Signals.financialIndexField("fcf_per_share")(asOf, universe, db)
        universe.filter { c =>
          fscore.get(c).exists(_ >= minFScore) &&
          revYoY.get(c).exists(_ > 0) &&
          opGrowth.get(c).exists(_ > 0) &&
          fcfPs.get(c).exists(_ > 0)
        }
      } else universe
      if (minRSV > 0) {
        val rsv = Signals.rsv120d(asOf, base, db)
        base.filter(c => rsv.get(c).exists(_ >= minRSV))
      } else base
    }
    if (survivors.isEmpty) return Map.empty

    // 3. Magic Formula: rank by ROIC ascending + EarningsYield ascending,
    //    sum of ranks = composite score. Higher rank = better on that metric.
    val roic = Signals.greenblattROIC(asOf, survivors, db)
    val eyield = Signals.earningsYield(asOf, survivors, db)

    // Only score codes that have both factors positive (Greenblatt rules)
    val withBoth = survivors
      .filter(c => roic.get(c).exists(_ > 0) && eyield.get(c).exists(_ > 0))
      .toSeq

    if (withBoth.isEmpty) return Map.empty

    // Ascending rank: lowest value → rank 0, highest → rank N-1. So higher
    // ROIC / EarningsYield = higher rank = better.
    val roicRank = rankMap(withBoth.map(c => c -> roic(c)))
    val eyRank   = rankMap(withBoth.map(c => c -> eyield(c)))
    withBoth.map(c => c -> (roicRank(c) + eyRank(c)).toDouble).toMap
  }

  private def rankMap(pairs: Seq[(String, Double)]): Map[String, Int] = {
    pairs.sortBy(_._2).zipWithIndex.map { case ((c, _), i) => c -> i }.toMap
  }
}
