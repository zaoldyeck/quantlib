package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/** Sanity-check runner: hold 0050 DRIP vs manual total-return calc. */
object BacktesterTest {
  def main(args: Array[String]): Unit = {
    val db = Database.forConfig("db")
    try {
      val start = LocalDate.of(2020, 1, 2)
      val end = LocalDate.of(2024, 12, 30)
      val capital = 1_000_000.0

      println(s"=== Hold-0050 Backtester: $start → $end, initial NT$$$capital ===")
      val result = Backtester.run(new Hold0050Strategy, start, end, capital, db)

      val days = result.dailyNav.size
      val finalNav = result.finalNav
      val totalRet = result.totalReturn
      val years = (end.toEpochDay - start.toEpochDay).toDouble / 365.25
      val cagr = math.pow(finalNav / capital, 1.0 / years) - 1

      println(f"  trading days: $days%d")
      println(f"  final NAV:    $finalNav%,.0f")
      println(f"  total return: ${totalRet * 100}%+.2f%%")
      println(f"  CAGR:         ${cagr * 100}%+.2f%% (years=$years%.2f)")
      println(s"  trades (buy/sell/drip): ${result.trades.count(_.kind == "buy")}/${result.trades.count(_.kind == "sell")}/${result.trades.count(_.kind == "drip")}")

      // Manual DRIP calc for cross-check
      println("\n=== Manual DRIP comparison (same period, no transaction costs) ===")
      val manual = manualDripReturn(start, end, db)
      println(f"  manual final multiplier: $manual%.4f (x initial)")
      println(f"  manual total return:     ${(manual - 1) * 100}%+.2f%%")

      val backtestMultiplier = finalNav / capital
      val diffBps = (backtestMultiplier - manual) / manual * 10000
      println(f"  backtest multiplier:     $backtestMultiplier%.4f")
      println(f"  diff (backtest − manual): $diffBps%+.1f bps")
      println(if (math.abs(diffBps) < 100) "  ✓ within 100 bps (expected: commission drag)"
              else "  ✗ LARGER THAN EXPECTED — investigate")
    } finally {
      db.close()
    }
  }

  /** Manually compute 0050 DRIP total-return multiplier from start to end, no costs. */
  def manualDripReturn(start: LocalDate, end: LocalDate, db: Database): Double = {
    val qPrices = sql"""
      SELECT date, closing_price FROM daily_quote
      WHERE market='twse' AND company_code='0050'
        AND date >= #${"'" + start + "'"}::date AND date <= #${"'" + end + "'"}::date
        AND closing_price IS NOT NULL
      ORDER BY date
    """.as[(java.sql.Date, Double)]
    val prices = Await.result(db.run(qPrices), Duration.Inf).map { case (d, p) => d.toLocalDate -> p }
    val startPrice = prices.head._2
    val endPrice = prices.last._2
    val startDate = prices.head._1

    val qDivs = sql"""
      SELECT date, cash_dividend FROM ex_right_dividend
      WHERE market='twse' AND company_code='0050'
        AND date >= #${"'" + startDate + "'"}::date AND date <= #${"'" + end + "'"}::date
        AND cash_dividend > 0
      ORDER BY date
    """.as[(java.sql.Date, Double)]
    val divs = Await.result(db.run(qDivs), Duration.Inf)

    // Start with 1 share. On each ex-div date, compound: shares *= (1 + div / close_that_day)
    var shares: Double = 1.0
    val priceByDate = prices.toMap
    divs.foreach { case (d, div) =>
      val p = priceByDate.getOrElse(d.toLocalDate, startPrice)
      shares = shares * (1.0 + div / p)
    }
    // Final value in units of initial-price-normalized capital
    (shares * endPrice) / startPrice
  }
}
