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
}
