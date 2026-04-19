import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._
import reader.{FinancialReader, TradingReader}
import scopt.OParser

/**
 * Single entry point for the quantlib toolchain. Subcommands:
 *
 *   update                 — full crawler + reader (default)
 *   pull    <target>       — crawler only; target = daily_quote | daily_trading_details |
 *                            index | margin | stock_per_pbr | capital_reduction |
 *                            ex_right_dividend | operating_revenue | balance_sheet |
 *                            income_statement | financial_analysis | financial_statements |
 *                            etf | all
 *   read    <target>       — reader only; same targets as pull
 *   strategy               — run MomentumValueStrategy backtest vs 0050
 *     --start YYYY-MM-DD
 *     --end   YYYY-MM-DD
 *     --capital N
 *
 * Invocation:
 *   sbt 'runMain Main'
 *   sbt 'runMain Main pull daily_trading_details'
 *   sbt 'runMain Main strategy --start 2018-01-02 --end 2026-04-17'
 */
object Main {
  final case class Config(
    command: String = "update",
    target: String = "",
    since: Option[LocalDate] = None,
    start: LocalDate = LocalDate.of(2018, 1, 2),
    end: LocalDate = LocalDate.now,
    capital: Double = 1_000_000.0
  )

  private val builder = OParser.builder[Config]
  private val parser = {
    import builder._
    implicit val localDateRead: scopt.Read[LocalDate] =
      scopt.Read.reads(LocalDate.parse)

    OParser.sequence(
      programName("quantlib"),
      head("quantlib"),
      cmd("update")
        .action((_, c) => c.copy(command = "update"))
        .text("Full pull + read (default)"),
      cmd("pull")
        .action((_, c) => c.copy(command = "pull"))
        .text("Crawler only")
        .children(
          arg[String]("<target>").action((x, c) => c.copy(target = x)),
          opt[LocalDate]("since").action((x, c) => c.copy(since = Some(x)))
            .text("Start date (YYYY-MM-DD). Applies to daily_trading_details; other targets ignore it.")
        ),
      cmd("read")
        .action((_, c) => c.copy(command = "read"))
        .text("Reader only")
        .children(
          arg[String]("<target>").action((x, c) => c.copy(target = x))
        ),
      cmd("strategy")
        .action((_, c) => c.copy(command = "strategy"))
        .text("Run MomentumValueStrategy backtest vs Hold-0050")
        .children(
          opt[LocalDate]("start").action((x, c) => c.copy(start = x)),
          opt[LocalDate]("end").action((x, c) => c.copy(end = x)),
          opt[Double]("capital").action((x, c) => c.copy(capital = x))
        )
    )
  }

  def main(args: Array[String]): Unit = {
    OParser.parse(parser, args, Config()) match {
      case Some(cfg) => run(cfg)
      case None      => sys.exit(1)
    }
  }

  private def run(cfg: Config): Unit = {
    val task = new Task
    val tradingReader = new TradingReader
    val financialReader = new FinancialReader
    val job = new Job

    try cfg.command match {
      case "update" =>
        job.updateData()

      case "pull" =>
        cfg.target match {
          case "daily_quote"            => task.pullDailyQuote()
          case "daily_trading_details"  => task.pullDailyTradingDetails(cfg.since)
          case "index"                  => task.pullIndex()
          case "margin" | "margin_transactions" => task.pullMarginTransactions()
          case "stock_per_pbr"          => task.pullStockPER_PBR_DividendYield()
          case "capital_reduction"      => task.pullCapitalReduction()
          case "ex_right_dividend"      => task.pullExRightDividend()
          case "operating_revenue"      => task.pullOperatingRevenue()
          case "balance_sheet"          => task.pullBalanceSheet()
          case "income_statement"       => task.pullIncomeStatement()
          case "financial_analysis"     => task.pullFinancialAnalysis()
          case "financial_statements"   => task.pullFinancialStatements()
          case "etf"                    => task.pullETF()
          case "all"                    => job.pullAllData()
          case t                        => sys.error(s"unknown pull target: $t")
        }

      case "read" =>
        cfg.target match {
          case "daily_quote"            => tradingReader.readDailyQuote()
          case "daily_trading_details"  => tradingReader.readDailyTradingDetails()
          case "index"                  => tradingReader.readIndex()
          case "margin" | "margin_transactions" => tradingReader.readMarginTransactions()
          case "stock_per_pbr"          => tradingReader.readStockPER_PBR_DividendYield()
          case "capital_reduction"      => tradingReader.readCapitalReduction()
          case "ex_right_dividend"      => tradingReader.readExRightDividend()
          case "operating_revenue"      => financialReader.readOperatingRevenue()
          case "balance_sheet"          => financialReader.readBalanceSheet()
          case "income_statement"       => financialReader.readIncomeStatement()
          case "financial_analysis"     => financialReader.readFinancialAnalysis()
          case "financial_statements"   => financialReader.readFinancialStatements()
          case "etf"                    => financialReader.readETF()
          case "all"                    => job.readAllData()
          case t                        => sys.error(s"unknown read target: $t")
        }

      case "strategy" =>
        val db = Database.forConfig("db")
        try {
          val strat = new strategy.MomentumValueStrategy(10)
          val primary = strategy.Backtester.run(strat, cfg.start, cfg.end, cfg.capital, db)
          val bench = strategy.Backtester.run(
            new strategy.Hold0050Strategy, cfg.start, cfg.end, cfg.capital, db)

          // IC comes first — a strategy with IC < 0.04 has no selection skill,
          // so any CAGR outperformance is likely beta or luck.
          val rebalDates = strat.rebalanceDates(cfg.start, cfg.end, db)
          val icSeries = strategy.RankMetrics.computeICSeries(strat, rebalDates, horizonDays = 21, db)
          val icSummary = strategy.RankMetrics.summarize(icSeries)
          println(icSummary.show)

          println(strategy.Metrics.summarize(primary).show)
          println(strategy.Metrics.summarize(bench).show)
          println(f"Excess vs 0050: ${(primary.totalReturn - bench.totalReturn) * 100}%+.2f pp\n")

          val out = strategy.Output.writeAll(primary, bench)
          println(s"Output: ${out}.html + _trades.csv + _monthly.csv")
        } finally db.close()

      case c => sys.error(s"unknown command: $c")
    }
    finally {
      job.complete()
      // Reader.forkJoinPool holds non-daemon workers and Akka shutdown is async;
      // without this the JVM idles for 30-60s after work is done. sbt reports
      // `[success] Total time: Ns` only after the JVM cleanly exits.
      sys.exit(0)
    }
  }
}
