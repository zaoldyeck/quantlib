package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/** A strategy injects its own rebalance schedule and target weights. */
trait Strategy {
  def name: String

  /** Dates on which the strategy wants to recompute target weights. */
  def rebalanceDates(start: LocalDate, end: LocalDate, db: Database): Seq[LocalDate]

  /** Target portfolio weights as of a rebalance date. Weights should sum to <= 1.0. */
  def targetWeights(asOf: LocalDate, db: Database): Map[String, Double]

  /** Optional daily exit check: return codes to force-sell today. Default: no daily exits. */
  def dailyExits(today: LocalDate, holdings: Set[String], db: Database): Set[String] = Set.empty
}

/** A position held in a single company. */
final case class Position(shares: Double, avgCost: Double, peakPrice: Double)

/** Record of a single trade. Kind: "buy" | "sell" | "drip". */
final case class Trade(date: LocalDate, code: String, kind: String, shares: Double, price: Double, cost: Double)

/** Result of a backtest run. `dailyNav` includes every trading day; entries are (date, NAV in TWD). */
final case class BacktestResult(
  strategy: String,
  start: LocalDate,
  end: LocalDate,
  initialCapital: Double,
  dailyNav: Seq[(LocalDate, Double)],
  trades: Seq[Trade],
  finalHoldings: Map[String, Position]
) {
  def finalNav: Double = dailyNav.lastOption.map(_._2).getOrElse(initialCapital)
  def totalReturn: Double = finalNav / initialCapital - 1.0
}

/**
 * Event-driven backtester. Walks trading day by trading day:
 *   1. Apply cash dividends (DRIP: buy back same stock at same day's close)
 *   2. Run strategy's dailyExits (stop-loss, trailing stop, etc.)
 *   3. Update trailing peak for remaining holdings
 *   4. If today is a rebalance day: sell over-allocated, buy under-allocated
 *   5. Record NAV using today's close
 *
 * Cost model: TW commission 0.1425% buy, 0.1425% + 0.3% tax sell. Slippage
 * optional (default 0). Fractional shares assumed in v1 for simpler NAV math.
 */
object Backtester {
  /** TW e-broker commission at 2-折 discount (Sinotrade/Fubon < 1M/mo) —
   *  0.1425% × 0.20 = 0.0285%. Cathay offers 2.8-折 (0.04%) with immediate
   *  rebate; Fubon 1.8-折 (0.02565%) monthly rebate. Using 2-折 as realistic
   *  mid-estimate for retail e-trading. */
  val CommissionRate: Double = 0.000285
  val SellTaxRate: Double = 0.003

  def run(strategy: Strategy,
          start: LocalDate,
          end: LocalDate,
          initialCapital: Double,
          db: Database): BacktestResult = {

    // 1. Trading days (market-wide union of all twse trading days in [start, end])
    val tradingDays = loadTradingDays(start, end, db)
    require(tradingDays.nonEmpty, s"no trading days between $start and $end")

    // 2. Rebalance dates (bucket into a Set for O(1) lookup)
    val rebalSet: Set[LocalDate] = strategy.rebalanceDates(start, end, db).toSet

    // 3. State
    var cash: Double = initialCapital
    val holdings: mutable.Map[String, Position] = mutable.Map.empty
    val navHistory: mutable.ArrayBuffer[(LocalDate, Double)] = mutable.ArrayBuffer.empty
    val trades: mutable.ArrayBuffer[Trade] = mutable.ArrayBuffer.empty
    // Last observed close per code, for last-observation-carry-forward valuation on
    // data-gap days. avgCost (cost basis) is never used as a price proxy — that would
    // inject phantom drawdowns/rebounds and distort MDD/Sharpe. (audit SUSPECT 2)
    val lastPrice: mutable.Map[String, Double] = mutable.Map.empty

    // 4. Pre-load all ex_right_dividend rows in the period (small table; efficient)
    val divByDate: Map[(LocalDate, String), Double] = loadDividends(start, end, db)

    // Pre-load detected stock-split events. TWSE's ex_right endpoint has been
    // unreliable since mid-2024, so events like 0050's 4:1 split on 2025-06-18
    // aren't in ex_right_dividend. We detect them heuristically: a single-day
    // price ratio > 2.5x or < 0.4x across a 3-14 day trading suspension, with
    // no matching ex_right entry.
    val splitByDate: Map[(LocalDate, String), Double] = loadSplits(start, end, db)

    // Pre-load capital-reduction reference resets from the capital_reduction table
    // (precise closing_price_on_the_last_trading_date / post_reduction_reference_price).
    // Without this, holding a stock through a reduction books the accounting jump
    // (shares cut, price stepped up) as real return — e.g. 8103 on 2025-12-08 booked a
    // phantom +11.6% (74.7 → 83.4 with an unchanged share count). 91% of reductions
    // fall below the 2.5x split heuristic and were missed entirely. (audit BUG-1)
    val reductionByDate: Map[(LocalDate, String), Double] = loadReductions(start, end, db)

    for (today <- tradingDays) {
      // Execution convention: signals (targetWeights, dailyExits) are evaluated and
      // filled at *today's* close — same-bar decision + execution. This is a mildly
      // optimistic market-on-close idealization when a signal itself reads today's
      // close; the house Python engine instead shifts fills to t+1. Left as-is on
      // purpose: a t+1-fill restructuring would diverge from the live exit-replay
      // "sell at today's price" semantics, and the residual bias lives in the strategy
      // layer (whether targetWeights uses close_t), outside this engine. (audit SUSPECT 1)
      // 4a. Need prices for holdings + (if rebal day) target codes
      val targetCodes: Set[String] =
        if (rebalSet.contains(today)) strategy.targetWeights(today, db).keySet else Set.empty
      val codesToPrice: Set[String] = holdings.keySet.toSet ++ targetCodes
      val prices: Map[String, Double] =
        if (codesToPrice.isEmpty) Map.empty else loadClosingPrices(today, codesToPrice, db)

      // Carry today's observed closes forward for future gap-day valuation, and
      // define the effective price used for all valuation/execution below: today's
      // close, else last observed close (carry-forward), else the caller's fallback.
      // (audit SUSPECT 2 — never let avgCost stand in for a market price)
      prices.foreach { case (c, p) => if (p > 0) lastPrice(c) = p }
      def priceOf(code: String, fallback: Double): Double =
        prices.getOrElse(code, lastPrice.getOrElse(code, fallback))

      // 4b. DRIP: for each holding with ex-div today, compound shares
      holdings.keys.toSeq.foreach { code =>
        divByDate.get((today, code)).foreach { dividendPerShare =>
          val pos = holdings(code)
          val cashFromDiv = pos.shares * dividendPerShare
          val priceToday = priceOf(code, pos.avgCost)
          if (priceToday > 0) {
            val extraShares = cashFromDiv / priceToday
            holdings(code) = pos.copy(shares = pos.shares + extraShares)
            trades += Trade(today, code, "drip", extraShares, priceToday, 0.0)
          }
        }
      }

      // 4b'. Corporate-action reference resets — a single neutralization layer for
      // capital reductions (precise capital_reduction data) and stock splits
      // (close-ratio heuristic). Multiply shares by the factor, divide cost/peak by
      // it, keeping NAV continuous so the reset is a non-event for return math.
      // Precise reductions win over the heuristic, and loadSplits already excludes
      // capital_reduction days, so the two maps never both fire on one (day, code).
      // For 退還股款 (cash-return) reductions the returned cash is implicitly
      // reinvested into the stock at the reference price — the same total-return
      // convention the engine already uses for cash dividends (DRIP). (audit BUG-1, SUSPECT 4)
      holdings.keys.toSeq.foreach { code =>
        val action: Option[(Double, String)] =
          reductionByDate.get((today, code)).map(f => (f, "reduction"))
            .orElse(splitByDate.get((today, code)).map(f => (f, "split")))
        action.foreach { case (factor, kind) =>
          if (factor > 0) {
            val pos = holdings(code)
            val newShares = pos.shares * factor
            val newAvgCost = pos.avgCost / factor
            val newPeak = pos.peakPrice / factor
            holdings(code) = Position(newShares, newAvgCost, newPeak)
            val priceToday = priceOf(code, newAvgCost)
            trades += Trade(today, code, kind, newShares - pos.shares, priceToday, 0.0)
          }
        }
      }

      // 4c. Daily exits
      val exits = strategy.dailyExits(today, holdings.keySet.toSet, db)
      exits.foreach { code =>
        holdings.get(code).foreach { pos =>
          val price = priceOf(code, pos.avgCost)
          val proceeds = pos.shares * price
          val tax = proceeds * SellTaxRate
          val comm = proceeds * CommissionRate
          cash += proceeds - tax - comm
          trades += Trade(today, code, "sell", pos.shares, price, comm + tax)
          holdings.remove(code)
        }
      }

      // 4d. Update trailing peak
      holdings.keys.foreach { code =>
        val pos = holdings(code)
        val price = priceOf(code, pos.peakPrice)
        if (price > pos.peakPrice) holdings(code) = pos.copy(peakPrice = price)
      }

      // 4e. Rebalance
      if (rebalSet.contains(today)) {
        val targets = strategy.targetWeights(today, db)
        val nav = cash + holdings.map { case (c, p) => p.shares * priceOf(c, p.avgCost) }.sum
        val targetAmounts = targets.map { case (c, w) => c -> nav * w }

        // Sell over-allocated / remove dropped
        holdings.keys.toSeq.foreach { code =>
          val pos = holdings(code)
          val price = priceOf(code, pos.avgCost)
          val target = targetAmounts.getOrElse(code, 0.0)
          val currentVal = pos.shares * price
          if (currentVal > target && price > 0) {
            val sharesToSell = math.min(pos.shares, (currentVal - target) / price)
            val proceeds = sharesToSell * price
            val tax = proceeds * SellTaxRate
            val comm = proceeds * CommissionRate
            cash += proceeds - tax - comm
            trades += Trade(today, code, "sell", sharesToSell, price, comm + tax)
            val remaining = pos.shares - sharesToSell
            if (remaining <= 1e-8) holdings.remove(code)
            else holdings(code) = pos.copy(shares = remaining)
          }
        }

        // Buy under-allocated. Cap spend by available cash (after commission) so we
        // always execute even when targetWeights sum to 1.0 and cash is exactly NAV.
        targetAmounts.foreach { case (code, target) =>
          val price = prices.getOrElse(code, 0.0)
          if (price > 0) {
            val currentShares = holdings.get(code).map(_.shares).getOrElse(0.0)
            val currentVal = currentShares * price
            if (currentVal < target) {
              val desired = target - currentVal
              val maxAffordable = math.max(0.0, cash / (1.0 + CommissionRate))
              val spend = math.min(desired, maxAffordable)
              if (spend > 0) {
                val comm = spend * CommissionRate
                val newShares = spend / price
                cash -= (spend + comm)
                val oldCostBasis = currentShares * holdings.get(code).map(_.avgCost).getOrElse(price)
                val newTotalShares = currentShares + newShares
                val newAvgCost = (oldCostBasis + spend) / newTotalShares
                // Adding to a winner must not reset the trailing peak downward:
                // peak' = max(prior peak, fill price); a brand-new position uses the
                // fill price as its peak floor. (audit SUSPECT 3; exit-semantics contract)
                val newPeak = holdings.get(code).map(p => math.max(p.peakPrice, price)).getOrElse(price)
                holdings(code) = Position(newTotalShares, newAvgCost, newPeak)
                trades += Trade(today, code, "buy", newShares, price, comm)
              }
            }
          }
        }
      }

      // 4f. Record NAV
      val nav = cash + holdings.map { case (c, p) => p.shares * priceOf(c, p.avgCost) }.sum
      navHistory += today -> nav
    }

    BacktestResult(strategy.name, start, end, initialCapital, navHistory.toSeq, trades.toSeq, holdings.toMap)
  }

  /** Market-wide trading calendar: the union of every twse trading day in range,
   *  not a single ticker's. Binding the calendar to 0050 drops whole market days
   *  when 0050 itself is suspended (e.g. its 2025-06 4:1-split halt), silently
   *  freezing NAV for every other holding across that window. (audit SUSPECT 5) */
  private def loadTradingDays(start: LocalDate, end: LocalDate, db: Database): Seq[LocalDate] = {
    val q = sql"""
      SELECT DISTINCT date FROM daily_quote
      WHERE market = 'twse'
        AND date >= #${"'" + start + "'"}::date AND date <= #${"'" + end + "'"}::date
      ORDER BY date
    """.as[java.sql.Date]
    Await.result(db.run(q), Duration.Inf).map(_.toLocalDate)
  }

  private def loadClosingPrices(date: LocalDate, codes: Set[String], db: Database): Map[String, Double] = {
    if (codes.isEmpty) return Map.empty
    val codeList = codes.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT company_code, closing_price FROM daily_quote
      WHERE market = 'twse' AND date = #${"'" + date + "'"}::date
        AND closing_price IS NOT NULL
        AND company_code IN (#$codeList)
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  private def loadDividends(start: LocalDate, end: LocalDate, db: Database): Map[(LocalDate, String), Double] = {
    val q = sql"""
      SELECT date, company_code, cash_dividend FROM ex_right_dividend
      WHERE market = 'twse'
        AND date >= #${"'" + start + "'"}::date AND date <= #${"'" + end + "'"}::date
        AND cash_dividend > 0
    """.as[(java.sql.Date, String, Double)]
    Await.result(db.run(q), Duration.Inf)
      .map { case (d, c, v) => (d.toLocalDate, c) -> v }
      .toMap
  }

  /** Heuristic detection of stock-split (or reverse-split) events: a daily-close
   *  ratio ≥ 2.5x or ≤ 0.4x across a 3-14 day trading-suspension gap, with no
   *  ex_right_dividend AND no capital_reduction entry that day. Both of those are
   *  handled by precise-data paths (DRIP / loadReductions), so the heuristic is a
   *  fallback only for corporate actions with no record — this also stops the 36
   *  reductions whose jump exceeds 2.5x from being neutralized twice. Returns split
   *  factor = prev_close / today_close; shares × factor keeps NAV continuous. */
  private def loadSplits(start: LocalDate, end: LocalDate, db: Database): Map[(LocalDate, String), Double] = {
    val q = sql"""
      WITH seq AS (
        SELECT company_code, date, closing_price,
               LAG(date) OVER (PARTITION BY company_code ORDER BY date) AS prev_date,
               LAG(closing_price) OVER (PARTITION BY company_code ORDER BY date) AS prev_close
        FROM daily_quote
        WHERE market = 'twse'
          AND date >= #${"'" + start + "'"}::date
          AND date <= #${"'" + end + "'"}::date
      )
      SELECT date, company_code, prev_close / closing_price
      FROM seq
      WHERE prev_close IS NOT NULL AND closing_price > 0
        AND (prev_close / closing_price BETWEEN 2.5 AND 15
             OR prev_close / closing_price BETWEEN 0.067 AND 0.4)
        AND (date - prev_date) BETWEEN 3 AND 14
        AND NOT EXISTS (
          SELECT 1 FROM ex_right_dividend
          WHERE company_code = seq.company_code AND date = seq.date
        )
        AND NOT EXISTS (
          SELECT 1 FROM capital_reduction
          WHERE market = 'twse' AND company_code = seq.company_code AND date = seq.date
        )
    """.as[(java.sql.Date, String, Double)]
    Await.result(db.run(q), Duration.Inf)
      .map { case (d, c, f) => (d.toLocalDate, c) -> f }
      .toMap
  }

  /** Capital-reduction reference resets from the capital_reduction table. On the
   *  resumption date TWSE cancels shares and steps the reference price up (彌補虧損
   *  loss compensation) and/or returns cash (退還股款); daily_quote jumps to the
   *  reset price while the engine's share count is unchanged, so an untreated walk
   *  books the accounting step as economic return (8103 2025-12-08: phantom +11.6%).
   *  Returns factor = closing_price_on_the_last_trading_date /
   *  post_reduction_reference_price (< 1 for the usual share cut). Shares × factor
   *  (cost/peak ÷ factor) keeps NAV continuous across the reset, mirroring the split
   *  path; precise data here, so loadSplits excludes these days to avoid double
   *  adjustment. TWSE 減資恢復買賣參考價 calc rule; CRSP/Bloomberg TRI neutralize
   *  capital changes and splits alike. (audit BUG-1) */
  private def loadReductions(start: LocalDate, end: LocalDate, db: Database): Map[(LocalDate, String), Double] = {
    val q = sql"""
      SELECT date, company_code,
             closing_price_on_the_last_trading_date / post_reduction_reference_price
      FROM capital_reduction
      WHERE market = 'twse'
        AND date >= #${"'" + start + "'"}::date AND date <= #${"'" + end + "'"}::date
        AND post_reduction_reference_price > 0
        AND closing_price_on_the_last_trading_date > 0
    """.as[(java.sql.Date, String, Double)]
    Await.result(db.run(q), Duration.Inf)
      .map { case (d, c, f) => (d.toLocalDate, c) -> f }
      .toMap
  }
}
