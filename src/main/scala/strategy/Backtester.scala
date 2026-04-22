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

    // 1. Trading days (all twse trading days in [start, end], via any ticker's calendar — use 0050 as benchmark)
    val tradingDays = loadTradingDays(start, end, db)
    require(tradingDays.nonEmpty, s"no trading days between $start and $end")

    // 2. Rebalance dates (bucket into a Set for O(1) lookup)
    val rebalSet: Set[LocalDate] = strategy.rebalanceDates(start, end, db).toSet

    // 3. State
    var cash: Double = initialCapital
    val holdings: mutable.Map[String, Position] = mutable.Map.empty
    val navHistory: mutable.ArrayBuffer[(LocalDate, Double)] = mutable.ArrayBuffer.empty
    val trades: mutable.ArrayBuffer[Trade] = mutable.ArrayBuffer.empty

    // 4. Pre-load all ex_right_dividend rows in the period (small table; efficient)
    val divByDate: Map[(LocalDate, String), Double] = loadDividends(start, end, db)

    // Pre-load detected stock-split events. TWSE's ex_right endpoint has been
    // unreliable since mid-2024, so events like 0050's 4:1 split on 2025-06-18
    // aren't in ex_right_dividend. We detect them heuristically: a single-day
    // price ratio > 2.5x or < 0.4x across a 3-14 day trading suspension, with
    // no matching ex_right entry.
    val splitByDate: Map[(LocalDate, String), Double] = loadSplits(start, end, db)

    for (today <- tradingDays) {
      // 4a. Need prices for holdings + (if rebal day) target codes
      val targetCodes: Set[String] =
        if (rebalSet.contains(today)) strategy.targetWeights(today, db).keySet else Set.empty
      val codesToPrice: Set[String] = holdings.keySet.toSet ++ targetCodes
      val prices: Map[String, Double] =
        if (codesToPrice.isEmpty) Map.empty else loadClosingPrices(today, codesToPrice, db)

      // 4b. DRIP: for each holding with ex-div today, compound shares
      holdings.keys.toSeq.foreach { code =>
        divByDate.get((today, code)).foreach { dividendPerShare =>
          val pos = holdings(code)
          val cashFromDiv = pos.shares * dividendPerShare
          val priceToday = prices.getOrElse(code, pos.avgCost)
          if (priceToday > 0) {
            val extraShares = cashFromDiv / priceToday
            holdings(code) = pos.copy(shares = pos.shares + extraShares)
            trades += Trade(today, code, "drip", extraShares, priceToday, 0.0)
          }
        }
      }

      // 4b'. Stock splits: multiply share count by ratio, divide cost/peak by ratio.
      // Keeps NAV continuous across the split so the split is a non-event for return math.
      holdings.keys.toSeq.foreach { code =>
        splitByDate.get((today, code)).foreach { factor =>
          val pos = holdings(code)
          val newShares = pos.shares * factor
          val newAvgCost = if (factor > 0) pos.avgCost / factor else pos.avgCost
          val newPeak = if (factor > 0) pos.peakPrice / factor else pos.peakPrice
          holdings(code) = Position(newShares, newAvgCost, newPeak)
          val priceToday = prices.getOrElse(code, newAvgCost)
          trades += Trade(today, code, "split", newShares - pos.shares, priceToday, 0.0)
        }
      }

      // 4c. Daily exits
      val exits = strategy.dailyExits(today, holdings.keySet.toSet, db)
      exits.foreach { code =>
        holdings.get(code).foreach { pos =>
          val price = prices.getOrElse(code, pos.avgCost)
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
        val price = prices.getOrElse(code, pos.peakPrice)
        if (price > pos.peakPrice) holdings(code) = pos.copy(peakPrice = price)
      }

      // 4e. Rebalance
      if (rebalSet.contains(today)) {
        val targets = strategy.targetWeights(today, db)
        val nav = cash + holdings.map { case (c, p) => p.shares * prices.getOrElse(c, p.avgCost) }.sum
        val targetAmounts = targets.map { case (c, w) => c -> nav * w }

        // Sell over-allocated / remove dropped
        holdings.keys.toSeq.foreach { code =>
          val pos = holdings(code)
          val price = prices.getOrElse(code, pos.avgCost)
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
                holdings(code) = Position(newTotalShares, newAvgCost, price)
                trades += Trade(today, code, "buy", newShares, price, comm)
              }
            }
          }
        }
      }

      // 4f. Record NAV
      val nav = cash + holdings.map { case (c, p) => p.shares * prices.getOrElse(c, p.avgCost) }.sum
      navHistory += today -> nav
    }

    BacktestResult(strategy.name, start, end, initialCapital, navHistory.toSeq, trades.toSeq, holdings.toMap)
  }

  private def loadTradingDays(start: LocalDate, end: LocalDate, db: Database): Seq[LocalDate] = {
    val q = sql"""
      SELECT DISTINCT date FROM daily_quote
      WHERE market = 'twse' AND company_code = '0050'
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
   *  ex_right_dividend entry that day. Returns split factor = prev_close / today_close.
   *  Shares should be multiplied by this factor to keep NAV continuous. */
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
    """.as[(java.sql.Date, String, Double)]
    Await.result(db.run(q), Duration.Inf)
      .map { case (d, c, f) => (d.toLocalDate, c) -> f }
      .toMap
  }
}
