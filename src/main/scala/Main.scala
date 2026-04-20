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
      cmd("research")
        .action((_, c) => c.copy(command = "research"))
        .text("Measure each candidate factor's standalone IC + pairwise redundancy")
        .children(
          opt[LocalDate]("start").action((x, c) => c.copy(start = x)),
          opt[LocalDate]("end").action((x, c) => c.copy(end = x))
        ),
      cmd("strategy")
        .action((_, c) => c.copy(command = "strategy"))
        .text("Run strategy backtest vs Hold-0050; default momentum_value, pass alpha_stack for v2")
        .children(
          arg[String]("<variant>").optional()
            .action((x, c) => c.copy(target = x))
            .text("momentum_value (default) | alpha_stack"),
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
          val strategyName = cfg.target  // reuse --target via positional arg within "strategy"
          val (stratName, rawComposite, stratRun): (String, (LocalDate, Database) => Map[String, Double], strategy.Strategy) =
            strategyName match {
              case "alpha_stack" =>
                val s = new strategy.AlphaStackStrategy(10)
                ("alpha_stack", s.computeComposite _, s)
              case "value_revert" =>
                val s = new strategy.ValueRevertStrategy(10)
                ("value_revert", s.computeComposite _, s)
              case "regime_aware" =>
                val s = new strategy.RegimeAwareStrategy(10)
                ("regime_aware", s.computeComposite _, s)
              case "multi_factor" =>
                val s = new strategy.MultiFactorStrategy(10)
                ("multi_factor", s.computeComposite _, s)
              case "regime_multi" =>
                val mf = new strategy.MultiFactorStrategy(10)
                val s = new strategy.RegimeAwareStrategy(10, 0.05, mf.computeComposite _, "multi-factor")
                ("regime_multi", s.computeComposite _, s)
              case "dividend_yield" =>
                val s = new strategy.DividendYieldStrategy(10)
                ("dividend_yield", s.computeComposite _, s)
              case "regime_yield" =>
                val dy = new strategy.DividendYieldStrategy(10)
                val s = new strategy.RegimeAwareStrategy(10, 0.05, dy.computeComposite _, "dividend-yield")
                ("regime_yield", s.computeComposite _, s)
              case _ =>
                val s = new strategy.MomentumValueStrategy(10)
                ("momentum_value", s.computeComposite _, s)
            }
          println(s"[strategy] $stratName, ${cfg.start} → ${cfg.end}, capital=NT$$${cfg.capital}")

          // Memoize composite scores per rebalance date — Backtester and
          // RankMetrics both need them, and each computation is expensive
          // (5+ SQL queries across Universe/Quality/Signals).
          val cache = scala.collection.mutable.Map.empty[LocalDate, Map[String, Double]]
          val composite: (LocalDate, Database) => Map[String, Double] = { (d, dbArg) =>
            cache.getOrElseUpdate(d, rawComposite(d, dbArg))
          }
          // Rebuild a StrategyProxy that routes targetWeights through the cache
          // so Backtester's calls populate the cache too.
          val cachedStrat = new strategy.Strategy {
            override val name = stratRun.name
            override def rebalanceDates(start: LocalDate, end: LocalDate, d: Database) =
              stratRun.rebalanceDates(start, end, d)
            override def targetWeights(asOf: LocalDate, d: Database) = {
              val comp = composite(asOf, d)
              if (comp.isEmpty) Map.empty
              else {
                val picks = comp.toSeq.sortBy(-_._2).take(10).map(_._1)
                if (picks.isEmpty) Map.empty else picks.map(_ -> (1.0 / picks.size)).toMap
              }
            }
          }

          val primary = strategy.Backtester.run(cachedStrat, cfg.start, cfg.end, cfg.capital, db)
          val bench = strategy.Backtester.run(
            new strategy.Hold0050Strategy, cfg.start, cfg.end, cfg.capital, db)

          // IC comes first — a strategy with IC < 0.04 has no selection skill,
          // so any CAGR outperformance is likely beta or luck.
          val rebalDates = stratRun.rebalanceDates(cfg.start, cfg.end, db)
          val icSeries = strategy.RankMetrics.computeICSeries(composite, rebalDates, horizonDays = 21, db)
          val icSummary = strategy.RankMetrics.summarize(icSeries)
          println(icSummary.show)

          println(strategy.Metrics.summarize(primary).show)
          println(strategy.Metrics.summarize(bench).show)
          println(f"Excess vs 0050: ${(primary.totalReturn - bench.totalReturn) * 100}%+.2f pp\n")

          val out = strategy.Output.writeAll(primary, bench)
          println(s"Output: ${out}.html + _trades.csv + _monthly.csv")
        } finally db.close()

      case "research" =>
        val db = Database.forConfig("db")
        try {
          println(s"[research] ${cfg.start} → ${cfg.end}")
          // Day-15+ monthly cadence to capture fresh monthly revenue (day-10 release)
          // and same-month Q1/Q2/Q3 reports (5/15, 8/14, 11/14 deadlines).
          val rebalDates = strategy.RebalanceCalendar.monthlyAfterDay(cfg.start, cfg.end, db)
          println(s"[research] ${rebalDates.size} rebalance dates")

          val universeFn: (LocalDate, Database) => Set[String] =
            (d, dbArg) => strategy.Universe.eligible(d, dbArg)

          // Factors: (name, signal_fn, higherIsBetter)
          val factors: Seq[(String, strategy.FactorResearch.FactorFn, Boolean)] = Seq(
            // --- Fundamental acceleration (fresh-publication exploit) ---
            ("revenueYoYLatest",        strategy.Signals.revenueYoYLatest,      true),
            ("revenueAccel",            strategy.Signals.revenueAccel,          true),
            ("revenueYoY3M",            strategy.Signals.revenueYoY3M,          true),

            // --- Institutional flow breakdown ---
            ("institutionalFlow20d",    strategy.Signals.institutionalFlow20d,  true),
            ("foreignNetBuy20d",        strategy.Signals.foreignNetBuy20d,      true),
            ("dealerNetBuy20d",         strategy.Signals.dealerNetBuy20d,       false), // contrarian hypothesis

            // --- Multi-horizon price/momentum ---
            ("relativeStrength63d",     strategy.Signals.relativeStrength,      true),
            ("momentum12m1m",           strategy.Signals.momentum12m1m,         true),
            ("shortTermReversal5d",     strategy.Signals.shortTermReversal5d,   false),

            // --- Technical indicators ---
            ("distFrom52wHigh",         strategy.Signals.distFrom52wHigh,       true), // near-high = strong
            ("rsi14",                   strategy.Signals.rsi14,                 false), // low RSI = oversold
            ("bollingerPosition",       strategy.Signals.bollingerPosition,     false), // below MA = rebound
            ("lowVolatility60d",        strategy.Signals.lowVolatility60d,      false), // low vol anomaly
            ("technicalConfirmation",   strategy.Signals.technicalConfirmation, true),

            // --- Valuation ---
            ("pbBandPosition",          strategy.Signals.pbBandPosition,        false), // lower = cheaper
            ("peBandPosition",          strategy.Signals.peBandPosition,        false),
            ("dividendYield",           strategy.Signals.dividendYield,         true),

            // --- Margin / short-interest ---
            ("marginCrowding20d",       strategy.Signals.marginCrowding20d,     false), // crowded = overheated
            ("shortToMarginRatio",      strategy.Signals.shortToMarginRatio,    true),  // squeeze potential

            // --- Cash-flow quality ---
            ("fcfYield",                strategy.Signals.fcfYield,              true),
            ("ocfToNetIncome",          strategy.Signals.ocfToNetIncome,        true),

            // --- Growth & quality (growth_analysis_ttm) ---
            ("fScore",                  strategy.Signals.growthAnalysisField("f_score"),            true),
            ("dropScore",               strategy.Signals.growthAnalysisField("drop_score"),         false),
            ("growthScore",             strategy.Signals.growthAnalysisField("growth_score"),       true),
            ("roicGrowth",              strategy.Signals.growthAnalysisField("roic_growth_rate"),   true),
            ("epsGrowth",               strategy.Signals.growthAnalysisField("eps_growth_rate"),    true),
            ("revenueGrowth",           strategy.Signals.growthAnalysisField("revenue_growth_rate"), true),
            ("fcfGrowth",               strategy.Signals.growthAnalysisField("fcf_per_share_growth_rate"), true),

            // --- Quality levels (financial_index_ttm) ---
            ("cbs",                     strategy.Signals.financialIndexField("cbs"),                true),
            ("roic",                    strategy.Signals.financialIndexField("roic"),               true),
            ("grossMargin",             strategy.Signals.financialIndexField("gross_margin"),       true),
            ("operatingMargin",         strategy.Signals.financialIndexField("operating_margin"),   true)
          )

          val results = strategy.FactorResearch.individualICs(
            factors, universeFn, rebalDates, horizonDays = 21, db)
          val corr = strategy.FactorResearch.pairwiseCorrelations(results)
          print(strategy.FactorResearch.report(results, corr))
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
