package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Factor signal computations — each method returns raw per-code values over a
 * universe, ready to be z-scored/percentile-ranked by the strategy layer.
 *
 * All queries are point-in-time aware: no field is read from beyond the
 * rebalance date, and fiscal data is filtered through PublicationLag.
 */
object Signals {

  // ====== Fundamental Acceleration ======

  /** Average monthly revenue YoY over the most recent 3 reported months, as of
   *  the rebalance date. Monthly revenue has a day-10-of-next-month publication
   *  lag enforced via PublicationLag.asOfMonthlyRevenue. */
  def revenueYoY3M(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val (latestYear, latestMonth) = PublicationLag.asOfMonthlyRevenue(asOf)
    val cutoffEpoch = latestYear * 12 + latestMonth
    val startEpoch = cutoffEpoch - 2 // include latestMonth + 2 prior
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT company_code,
             AVG(CASE WHEN last_year_monthly_revenue > 0
                      THEN (monthly_revenue - last_year_monthly_revenue) / last_year_monthly_revenue
                      ELSE NULL END) AS avg_yoy_3m
      FROM operating_revenue
      WHERE company_code IN (#$codeList)
        AND (year * 12 + month) BETWEEN #$startEpoch AND #$cutoffEpoch
        AND monthly_revenue > 0
      GROUP BY company_code
      HAVING COUNT(*) >= 2
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  // ====== Institutional Flow Persistence ======

  /** Net buy of foreign investors + securities investment trust companies over
   *  the trailing 20 trading days, normalized by 20-day total trade volume.
   *  Positive = accumulated institutional accumulation. */
  def institutionalFlow20d(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH flow AS (
        SELECT d.company_code,
               SUM(d.foreign_investors_difference +
                   d.securities_investment_trust_companies_difference) AS net_buy,
               SUM(q.trade_volume)::double precision AS total_vol
        FROM daily_trading_details d
        JOIN daily_quote q USING (market, date, company_code)
        WHERE d.market = 'twse'
          AND d.date <= #${"'" + asOf + "'"}::date
          AND d.date > #${"'" + asOf + "'"}::date - INTERVAL '30 days'
          AND d.company_code IN (#$codeList)
        GROUP BY d.company_code
        HAVING SUM(q.trade_volume) > 0
      )
      SELECT company_code, net_buy::double precision / total_vol
      FROM flow
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  // ====== Technical Confirmation ======

  // ====== Momentum & Valuation ======

  /** 63-day skip-5 total return. Used both as standalone signal and as
   *  Relative Strength input. Positive = recently up. */
  def relativeStrength(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH recent AS (
        SELECT company_code, date, closing_price,
               ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date DESC) AS rn
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '120 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
      )
      SELECT a.company_code, (a.closing_price - b.closing_price) / b.closing_price
      FROM recent a JOIN recent b USING (company_code)
      WHERE a.rn = 5 AND b.rn = 68
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** Current P/B / 3.5y median P/B. **Lower is better (cheaper vs own history).** */
  def pbBandPosition(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH hist AS (
        SELECT company_code,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price_book_ratio) AS pb_median
        FROM stock_per_pbr_dividend_yield
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '3 years 6 months'
          AND company_code IN (#$codeList)
          AND price_book_ratio > 0
        GROUP BY company_code
      ),
      current_pb AS (
        SELECT DISTINCT ON (company_code) company_code, price_book_ratio AS pb_now
        FROM stock_per_pbr_dividend_yield
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '10 days'
          AND company_code IN (#$codeList)
          AND price_book_ratio > 0
        ORDER BY company_code, date DESC
      )
      SELECT h.company_code, c.pb_now / h.pb_median
      FROM hist h JOIN current_pb c USING (company_code)
      WHERE h.pb_median > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  // ====== Fundamental quality (from growth_analysis_ttm / financial_index_ttm) ======

  /** Generic single-column loader from growth_analysis_ttm with latest-quarter
   *  semantics (via PublicationLag). Higher raw value is assumed better. */
  def growthAnalysisField(col: String)(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] =
    latestQuarterField("growth_analysis_ttm", col, asOf, universe, db)

  def financialIndexField(col: String)(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] =
    latestQuarterField("financial_index_ttm", col, asOf, universe, db)

  private def latestQuarterField(table: String, col: String, asOf: LocalDate,
                                  universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val (year, quarter) = PublicationLag.asOfQuarter(asOf)
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT DISTINCT ON (company_code) company_code, #$col::double precision
      FROM #$table
      WHERE company_code IN (#$codeList)
        AND (year < #$year OR (year = #$year AND quarter <= #$quarter))
        AND #$col IS NOT NULL
      ORDER BY company_code, year DESC, quarter DESC
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** Boolean score in {0, 0.5, 1}: current close > 200D MA (1/3),
   *  50D MA > 200D MA (1/3), max 20-day volume / 20-day avg volume > 1.5 (1/3).
   *  Summed to [0, 1]. Captures "trend + volume confirmation". */
  def technicalConfirmation(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH px AS (
        SELECT company_code, date, closing_price, trade_volume,
               ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date DESC) AS rn
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date > #${"'" + asOf + "'"}::date - INTERVAL '1 year'
          AND company_code IN (#$codeList)
          AND closing_price > 0
      )
      SELECT company_code,
             -- current close
             MAX(CASE WHEN rn = 1 THEN closing_price END) AS px_now,
             -- 50d and 200d MAs
             AVG(CASE WHEN rn <= 50 THEN closing_price END) AS ma50,
             AVG(CASE WHEN rn <= 200 THEN closing_price END) AS ma200,
             -- 20d avg vol vs max vol in last 20
             AVG(CASE WHEN rn <= 20 THEN trade_volume END) AS avg_vol20,
             MAX(CASE WHEN rn <= 20 THEN trade_volume END) AS max_vol20
      FROM px
      GROUP BY company_code
      HAVING COUNT(*) >= 200
    """.as[(String, Double, Double, Double, Double, Double)]
    val rows = Await.result(db.run(q), Duration.Inf)
    rows.map { case (code, pxNow, ma50, ma200, avgVol20, maxVol20) =>
      val aboveMA200 = if (pxNow > ma200) 1.0 / 3 else 0.0
      val goldenCross = if (ma50 > ma200) 1.0 / 3 else 0.0
      val volSurge = if (avgVol20 > 0 && maxVol20 / avgVol20 > 1.5) 1.0 / 3 else 0.0
      code -> (aboveMA200 + goldenCross + volSurge)
    }.toMap
  }

  // ====== Multi-horizon momentum & reversal ======

  /** Simple price return over `lookbackDays` calendar days. Positive = up. */
  def priceReturn(lookbackDays: Int)(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH now_px AS (
        SELECT DISTINCT ON (company_code) company_code, closing_price AS p_now
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '10 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
        ORDER BY company_code, date DESC
      ),
      then_px AS (
        SELECT DISTINCT ON (company_code) company_code, closing_price AS p_then
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date - INTERVAL '#$lookbackDays days'
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '#${lookbackDays + 10} days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
        ORDER BY company_code, date DESC
      )
      SELECT n.company_code, (n.p_now - t.p_then) / t.p_then
      FROM now_px n JOIN then_px t USING (company_code)
      WHERE t.p_then > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** Short-term reversal: 5-day return. Academic evidence: short-term reversals
   *  are a known TW effect (liquidity-provision premium). Lower = better. */
  def shortTermReversal5d(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] =
    priceReturn(7)(asOf, universe, db)

  /** 12-month momentum skip-1-month (standard Jegadeesh-Titman). */
  def momentum12m1m(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH skip_px AS (
        SELECT DISTINCT ON (company_code) company_code, closing_price AS p1
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date - INTERVAL '21 days'
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '31 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
        ORDER BY company_code, date DESC
      ),
      base_px AS (
        SELECT DISTINCT ON (company_code) company_code, closing_price AS p0
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date - INTERVAL '365 days'
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '380 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
        ORDER BY company_code, date DESC
      )
      SELECT s.company_code, (s.p1 - b.p0) / b.p0
      FROM skip_px s JOIN base_px b USING (company_code)
      WHERE b.p0 > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  // ====== Technical indicators ======

  /** Distance from 52-week high: (px_now - max_52w) / max_52w.
   *  Negative = below high; values near 0 = at high. */
  def distFrom52wHigh(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH stats AS (
        SELECT company_code,
               MAX(closing_price) AS max_px,
               MAX(closing_price) FILTER (WHERE date = (
                 SELECT MAX(date) FROM daily_quote dq2
                 WHERE dq2.market='twse' AND dq2.company_code=dq.company_code
                   AND dq2.date <= #${"'" + asOf + "'"}::date AND dq2.closing_price > 0
               )) AS px_now
        FROM daily_quote dq
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '252 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
        GROUP BY company_code
      )
      SELECT company_code, (px_now - max_px) / max_px
      FROM stats WHERE max_px > 0 AND px_now IS NOT NULL
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** RSI-14 normalized to [-1, 1]: (RSI - 50)/50. Overbought > +0.4, oversold < -0.4. */
  def rsi14(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH px AS (
        SELECT company_code, date, closing_price,
               closing_price - LAG(closing_price) OVER (PARTITION BY company_code ORDER BY date) AS diff,
               ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date DESC) AS rn
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date > #${"'" + asOf + "'"}::date - INTERVAL '30 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
      )
      SELECT company_code,
             AVG(CASE WHEN rn <= 14 AND diff > 0 THEN diff ELSE 0 END) AS gain,
             AVG(CASE WHEN rn <= 14 AND diff < 0 THEN -diff ELSE 0 END) AS loss
      FROM px GROUP BY company_code
      HAVING COUNT(*) >= 14
    """.as[(String, Double, Double)]
    Await.result(db.run(q), Duration.Inf).collect {
      case (code, gain, loss) if (gain + loss) > 0 =>
        val rs = gain / math.max(loss, 1e-9)
        val rsi = 100 - 100.0 / (1 + rs)
        code -> ((rsi - 50) / 50)
    }.toMap
  }

  /** Bollinger-band position: (px - MA20) / (2 * σ20). 0 = at MA, -1 = lower band, +1 = upper.
   *  Reversal hypothesis: lower = better (rebound candidate). */
  def bollingerPosition(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH px AS (
        SELECT company_code, closing_price,
               ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date DESC) AS rn
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date > #${"'" + asOf + "'"}::date - INTERVAL '40 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
      )
      SELECT company_code,
             MAX(CASE WHEN rn = 1 THEN closing_price END) AS px_now,
             AVG(CASE WHEN rn <= 20 THEN closing_price END) AS ma20,
             STDDEV_POP(CASE WHEN rn <= 20 THEN closing_price END) AS sd20
      FROM px GROUP BY company_code
      HAVING COUNT(*) >= 20
    """.as[(String, Double, Double, Double)]
    Await.result(db.run(q), Duration.Inf).collect {
      case (code, pxNow, ma20, sd20) if sd20 > 0 =>
        code -> ((pxNow - ma20) / (2 * sd20))
    }.toMap
  }

  /** 60-day realized volatility of daily returns (annualized). Lower = better (low-vol anomaly). */
  def lowVolatility60d(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH px AS (
        SELECT company_code, date, closing_price,
               LN(closing_price / LAG(closing_price) OVER (PARTITION BY company_code ORDER BY date)) AS r
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date > #${"'" + asOf + "'"}::date - INTERVAL '90 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
      )
      SELECT company_code, STDDEV_POP(r) * SQRT(252) AS vol
      FROM px WHERE r IS NOT NULL AND ABS(r) < 0.5  -- exclude split days
      GROUP BY company_code
      HAVING COUNT(r) >= 40
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  // ====== Margin / short interest ======

  /** Margin balance ratio = margin_balance_of_the_day / margin_quota. High = crowded. */
  def marginCrowding20d(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT company_code,
             AVG(CASE WHEN margin_quota > 0
                      THEN margin_balance_of_the_day::double precision / margin_quota
                      ELSE NULL END)
      FROM margin_transactions
      WHERE market = 'twse'
        AND date <= #${"'" + asOf + "'"}::date
        AND date > #${"'" + asOf + "'"}::date - INTERVAL '30 days'
        AND company_code IN (#$codeList)
      GROUP BY company_code
      HAVING COUNT(*) >= 10
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** Short-to-margin ratio: short_balance / margin_balance. High = squeeze potential. */
  def shortToMarginRatio(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT company_code,
             AVG(CASE WHEN margin_balance_of_the_day > 0
                      THEN short_balance_of_the_day::double precision / margin_balance_of_the_day
                      ELSE NULL END)
      FROM margin_transactions
      WHERE market = 'twse'
        AND date <= #${"'" + asOf + "'"}::date
        AND date > #${"'" + asOf + "'"}::date - INTERVAL '30 days'
        AND company_code IN (#$codeList)
      GROUP BY company_code
      HAVING COUNT(*) >= 10
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  // ====== Institutional breakdown (finer than combined flow) ======

  /** Foreign-investor-only 20d net buy / total volume. Foreign alone may have
   *  different signal than foreign + trust combined. */
  def foreignNetBuy20d(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT d.company_code,
             SUM(d.foreign_investors_difference)::double precision /
               NULLIF(SUM(q.trade_volume), 0)
      FROM daily_trading_details d
      JOIN daily_quote q USING (market, date, company_code)
      WHERE d.market = 'twse'
        AND d.date <= #${"'" + asOf + "'"}::date
        AND d.date > #${"'" + asOf + "'"}::date - INTERVAL '30 days'
        AND d.company_code IN (#$codeList)
      GROUP BY d.company_code
      HAVING SUM(q.trade_volume) > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** Dealer (proprietary) self-trading 20d net. Dealer flow is considered a
   *  short-term contrarian signal in TW literature. */
  def dealerNetBuy20d(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT d.company_code,
             SUM(COALESCE(d.dealers_proprietary_difference, 0))::double precision /
               NULLIF(SUM(q.trade_volume), 0)
      FROM daily_trading_details d
      JOIN daily_quote q USING (market, date, company_code)
      WHERE d.market = 'twse'
        AND d.date <= #${"'" + asOf + "'"}::date
        AND d.date > #${"'" + asOf + "'"}::date - INTERVAL '30 days'
        AND d.company_code IN (#$codeList)
      GROUP BY d.company_code
      HAVING SUM(q.trade_volume) > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  // ====== Valuation ratios (beyond pbBandPosition) ======

  /** Current P/E / trailing-3.5y median P/E. Lower = cheaper. */
  def peBandPosition(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH hist AS (
        SELECT company_code,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price_to_earning_ratio) AS pe_median
        FROM stock_per_pbr_dividend_yield
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '3 years 6 months'
          AND company_code IN (#$codeList)
          AND price_to_earning_ratio > 0
        GROUP BY company_code
      ),
      cur AS (
        SELECT DISTINCT ON (company_code) company_code, price_to_earning_ratio AS pe_now
        FROM stock_per_pbr_dividend_yield
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '10 days'
          AND company_code IN (#$codeList)
          AND price_to_earning_ratio > 0
        ORDER BY company_code, date DESC
      )
      SELECT h.company_code, c.pe_now / h.pe_median
      FROM hist h JOIN cur c USING (company_code)
      WHERE h.pe_median > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** Current dividend yield (higher = better for income-oriented value). */
  def dividendYield(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT DISTINCT ON (company_code) company_code, dividend_yield
      FROM stock_per_pbr_dividend_yield
      WHERE market = 'twse'
        AND date <= #${"'" + asOf + "'"}::date
        AND date >= #${"'" + asOf + "'"}::date - INTERVAL '10 days'
        AND company_code IN (#$codeList)
        AND dividend_yield IS NOT NULL
      ORDER BY company_code, date DESC
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  // ====== Cash-flow factors (from cash_flows_individual) ======

  /** OCF / Net Income ratio (earnings quality). ratio > 1 = conservative accounting.
   *  Uses TTM-latest OCF and NI (profit). */
  def ocfToNetIncome(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val (yr, qtr) = PublicationLag.asOfQuarter(asOf)
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT DISTINCT ON (company_code) company_code,
             CASE WHEN ABS(profit) > 0 THEN ocf::double precision / profit ELSE NULL END
      FROM financial_index_ttm
      WHERE company_code IN (#$codeList)
        AND (year < #$yr OR (year = #$yr AND quarter <= #$qtr))
        AND ocf IS NOT NULL AND profit IS NOT NULL
      ORDER BY company_code, year DESC, quarter DESC
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** Free cash flow yield: fcf_per_share / closing_price.
   *  Higher = cheaper FCF-wise (value signal). */
  def fcfYield(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val (yr, qtr) = PublicationLag.asOfQuarter(asOf)
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH fcf AS (
        SELECT DISTINCT ON (company_code) company_code, fcf_per_share
        FROM financial_index_ttm
        WHERE company_code IN (#$codeList)
          AND (year < #$yr OR (year = #$yr AND quarter <= #$qtr))
          AND fcf_per_share IS NOT NULL
        ORDER BY company_code, year DESC, quarter DESC
      ),
      px AS (
        SELECT DISTINCT ON (company_code) company_code, closing_price
        FROM daily_quote
        WHERE market = 'twse'
          AND date <= #${"'" + asOf + "'"}::date
          AND date >= #${"'" + asOf + "'"}::date - INTERVAL '10 days'
          AND company_code IN (#$codeList)
          AND closing_price > 0
        ORDER BY company_code, date DESC
      )
      SELECT f.company_code, f.fcf_per_share / p.closing_price
      FROM fcf f JOIN px p USING (company_code)
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  // ====== Fresh-publication signals (leverage day-15+ rebalance) ======

  /** Revenue YoY of the **single latest** month (not 3-month average).
   *  Rebalancing on day 15+ captures the freshly-released month (published day 10). */
  def revenueYoYLatest(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val (yr, mo) = PublicationLag.asOfMonthlyRevenue(asOf)
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      SELECT company_code,
             CASE WHEN last_year_monthly_revenue > 0
                  THEN (monthly_revenue - last_year_monthly_revenue)::double precision / last_year_monthly_revenue
                  ELSE NULL END
      FROM operating_revenue
      WHERE company_code IN (#$codeList)
        AND year = #$yr AND month = #$mo
        AND monthly_revenue > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }

  /** Revenue MoM acceleration: latest month revenue / avg of prior 3 months.
   *  Uses the freshly-published month as the numerator. */
  def revenueAccel(asOf: LocalDate, universe: Set[String], db: Database): Map[String, Double] = {
    if (universe.isEmpty) return Map.empty
    val (yr, mo) = PublicationLag.asOfMonthlyRevenue(asOf)
    val cutoffEpoch = yr * 12 + mo
    val priorStart = cutoffEpoch - 3
    val priorEnd = cutoffEpoch - 1
    val codeList = universe.map(c => s"'$c'").mkString(",")
    val q = sql"""
      WITH latest AS (
        SELECT company_code, monthly_revenue AS rev_latest
        FROM operating_revenue
        WHERE company_code IN (#$codeList)
          AND year = #$yr AND month = #$mo
          AND monthly_revenue > 0
      ),
      prior AS (
        SELECT company_code, AVG(monthly_revenue) AS rev_prior
        FROM operating_revenue
        WHERE company_code IN (#$codeList)
          AND (year * 12 + month) BETWEEN #$priorStart AND #$priorEnd
          AND monthly_revenue > 0
        GROUP BY company_code
        HAVING COUNT(*) >= 2
      )
      SELECT l.company_code, l.rev_latest::double precision / p.rev_prior
      FROM latest l JOIN prior p USING (company_code)
      WHERE p.rev_prior > 0
    """.as[(String, Double)]
    Await.result(db.run(q), Duration.Inf).toMap
  }
}
