package strategy

import java.time.LocalDate

/**
 * Return-based performance metrics for a backtest. These are the "secondary
 * KPIs" in the plan — useful for understanding the strategy's risk profile,
 * but not sufficient to prove selection skill (see RankMetrics for that).
 */
object Metrics {
  /** Risk-free rate used in Sharpe. Taiwan 10Y bond yields ~1%. */
  val RiskFreeRate: Double = 0.01

  /** Trading days per year (TWSE averages ~242). */
  val TradingDaysPerYear: Double = 242.0

  final case class Summary(
    strategy: String,
    start: LocalDate,
    end: LocalDate,
    initialCapital: Double,
    finalNav: Double,
    totalReturn: Double,
    cagr: Double,
    annualVol: Double,
    sharpe: Double,
    sortino: Double,
    maxDrawdown: Double,
    calmar: Double,
    hitRate: Double,
    turnover: Double,
    numRebalances: Int,
    numBuys: Int,
    numSells: Int,
    numDrips: Int
  ) {
    def show: String = {
      f"""|=== ${strategy} (${start} → ${end}) ===
          |  initial capital:  $$${initialCapital}%,.0f
          |  final NAV:        $$${finalNav}%,.0f
          |  total return:     ${totalReturn * 100}%+.2f%%
          |  CAGR:             ${cagr * 100}%+.2f%%
          |  annual vol:       ${annualVol * 100}%.2f%%
          |  Sharpe:           ${sharpe}%.3f
          |  Sortino:          ${sortino}%.3f
          |  max drawdown:     ${maxDrawdown * 100}%.2f%%
          |  Calmar:           ${calmar}%.3f
          |  monthly hit rate: ${hitRate * 100}%.2f%%
          |  turnover (ann.):  ${turnover * 100}%.2f%%
          |  rebalances:       $numRebalances
          |  trades:           buy=$numBuys sell=$numSells drip=$numDrips
          |""".stripMargin
    }
  }

  def summarize(result: BacktestResult): Summary = {
    val navs = result.dailyNav.map(_._2)
    val dates = result.dailyNav.map(_._1)
    val years = math.max((result.end.toEpochDay - result.start.toEpochDay).toDouble / 365.25, 1e-9)

    val cagr = if (result.initialCapital > 0) math.pow(result.finalNav / result.initialCapital, 1.0 / years) - 1 else 0.0

    val dailyRets = navs.sliding(2).collect { case Seq(a, b) if a > 0 => b / a - 1 }.toSeq
    val meanRet = if (dailyRets.isEmpty) 0.0 else dailyRets.sum / dailyRets.size
    val vol = if (dailyRets.size < 2) 0.0
              else math.sqrt(dailyRets.map(r => math.pow(r - meanRet, 2)).sum / (dailyRets.size - 1)) * math.sqrt(TradingDaysPerYear)
    val sharpe = if (vol > 0) (cagr - RiskFreeRate) / vol else 0.0

    val downside = dailyRets.filter(_ < 0)
    val downVol = if (downside.size < 2) 0.0
                  else math.sqrt(downside.map(r => r * r).sum / downside.size) * math.sqrt(TradingDaysPerYear)
    val sortino = if (downVol > 0) (cagr - RiskFreeRate) / downVol else 0.0

    val mdd = maxDrawdown(navs)
    val calmar = if (mdd < 0) cagr / math.abs(mdd) else 0.0

    // Monthly hit rate: % of calendar months where NAV at month-end > NAV at prior month-end
    val monthEnds = dates.zip(navs).groupBy { case (d, _) => (d.getYear, d.getMonthValue) }
      .map { case (_, entries) => entries.maxBy(_._1) }
      .toSeq.sortBy(_._1)
    val monthRets = monthEnds.sliding(2).collect {
      case Seq((_, a), (_, b)) if a > 0 => b / a - 1
    }.toSeq
    val hitRate = if (monthRets.isEmpty) 0.0 else monthRets.count(_ > 0).toDouble / monthRets.size

    // Turnover: total buy+sell notional / (years * initial capital)
    val tradedNotional = result.trades.filter(t => t.kind == "buy" || t.kind == "sell")
      .map(t => math.abs(t.shares * t.price)).sum
    val turnover = tradedNotional / (years * math.max(result.initialCapital, 1.0))

    Summary(
      strategy = result.strategy,
      start = result.start,
      end = result.end,
      initialCapital = result.initialCapital,
      finalNav = result.finalNav,
      totalReturn = result.totalReturn,
      cagr = cagr,
      annualVol = vol,
      sharpe = sharpe,
      sortino = sortino,
      maxDrawdown = mdd,
      calmar = calmar,
      hitRate = hitRate,
      turnover = turnover,
      numRebalances = result.trades.groupBy(_.date).size,
      numBuys = result.trades.count(_.kind == "buy"),
      numSells = result.trades.count(_.kind == "sell"),
      numDrips = result.trades.count(_.kind == "drip")
    )
  }

  private def maxDrawdown(navs: Seq[Double]): Double = {
    if (navs.isEmpty) return 0.0
    var peak = navs.head
    var worst = 0.0
    navs.foreach { n =>
      if (n > peak) peak = n
      val dd = if (peak > 0) (n - peak) / peak else 0.0
      if (dd < worst) worst = dd
    }
    worst
  }
}
