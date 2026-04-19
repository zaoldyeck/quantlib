package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Sanity-check strategy: put 100% of capital into 0050 on the first trading
 * day and never rebalance. With DRIP handled by the Backtester, the resulting
 * daily NAV series should closely match 0050's total return.
 *
 * Used to validate the Backtester engine — if hold-0050 doesn't match the
 * benchmark, NAV math is wrong and any factor-strategy result is noise.
 */
class Hold0050Strategy extends Strategy {
  val name: String = "hold-0050"

  override def rebalanceDates(start: LocalDate, end: LocalDate, db: Database): Seq[LocalDate] = {
    val q = sql"""
      SELECT MIN(date) FROM daily_quote
      WHERE market = 'twse' AND company_code = '0050'
        AND date >= #${"'" + start + "'"}::date AND date <= #${"'" + end + "'"}::date
    """.as[java.sql.Date].head
    Seq(Await.result(db.run(q), Duration.Inf).toLocalDate)
  }

  override def targetWeights(asOf: LocalDate, db: Database): Map[String, Double] = Map("0050" -> 1.0)
}
