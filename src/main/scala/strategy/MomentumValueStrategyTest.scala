package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

/**
 * Runs MomentumValueStrategy over 2018-01-01 → 2024-12-31 and compares to
 * a Hold-0050 baseline. Reports:
 *   - Final NAV & CAGR for strategy vs baseline
 *   - Annualized vol, Sharpe, MDD (quick metrics for sanity)
 *   - Rebalance count and trade breakdown
 */
object MomentumValueStrategyTest {
  def main(args: Array[String]): Unit = {
    val db = Database.forConfig("db")
    try {
      val start = LocalDate.of(2018, 1, 2)
      val end = LocalDate.of(2026, 4, 17)
      val capital = 1_000_000.0

      println(s"=== Backtest period: $start → $end, initial NT$$$capital ===\n")

      println("▶ Running MomentumValueStrategy …")
      val stratResult = Backtester.run(new MomentumValueStrategy(10), start, end, capital, db)

      println("▶ Running Hold-0050 baseline …")
      val baseResult = Backtester.run(new Hold0050Strategy, start, end, capital, db)

      println(Metrics.summarize(stratResult).show)
      println(Metrics.summarize(baseResult).show)

      println("=== Comparison ===")
      val excess = stratResult.totalReturn - baseResult.totalReturn
      println(f"  Excess vs 0050: ${excess * 100}%+.2f pp\n")

      val outBase = Output.writeAll(stratResult, baseResult)
      println(s"Output written: ${outBase}.html + _trades.csv + _monthly.csv")
    } finally {
      db.close()
    }
  }

}
