package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Shared rebalance-date calendar. Uses 0050's trading days as the canonical
 * TWSE calendar.
 *
 * Timing rationale (addresses "information-freshness" critique):
 *   - Monthly revenue publication deadline: day 10 (+3-day buffer in PublicationLag)
 *   - Q1 report deadline: 5/15, Q2: 8/14, Q3: 11/14 (each +7-day buffer)
 *   - Annual report: 3/31 (+7-day buffer)
 *
 * A month-start rebalance (day 1) uses monthly revenue that was published
 * 22-40 days ago — most post-announcement drift (70% of abnormal return
 * within T+5, per TW PEAD research) has already been captured by other
 * traders. Rebalancing at day-15+ catches the freshly-released monthly
 * revenue and any Q1/Q2/Q3 reports released that same month.
 */
object RebalanceCalendar {
  /** First trading day of each month on/after day-of-month `minDay`, within
   *  [start, end], as reflected in 0050's trading calendar. Default 15 = month-mid. */
  def monthlyAfterDay(start: LocalDate, end: LocalDate, db: Database, minDay: Int = 15): Seq[LocalDate] = {
    val q = sql"""
      SELECT MIN(date) FROM daily_quote
      WHERE market = 'twse' AND company_code = '0050'
        AND date >= #${"'" + start + "'"}::date
        AND date <= #${"'" + end + "'"}::date
        AND EXTRACT(DAY FROM date) >= #$minDay
      GROUP BY date_trunc('month', date)
      ORDER BY MIN(date)
    """.as[java.sql.Date]
    Await.result(db.run(q), Duration.Inf).map(_.toLocalDate)
  }
}
