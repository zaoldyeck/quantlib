package strategy

import java.time.LocalDate

/**
 * Earliest dates at which Taiwanese financial reports are safely usable by a
 * point-in-time backtest, accounting for the statutory publication window and
 * a defensive buffer for companies that apply to extend their filing deadline.
 *
 * Quarterly report deadlines (公開資訊觀測站):
 *   Q1 period-end 3/31 → file by 5/15
 *   Q2 period-end 6/30 → file by 8/14
 *   Q3 period-end 9/30 → file by 11/14
 *   Q4/annual period-end 12/31 → file by 3/31 of the following year
 *
 * Monthly revenue deadline: day 10 of the month following the report month.
 *
 * A buffer is added to each to absorb delayed filings; without it the backtest
 * would leak future data on every company that filed even a day late.
 */
object PublicationLag {
  /** Days added to quarterly filing deadlines to cover extensions. */
  val QuarterlyBufferDays: Int = 7

  /** Days added to the monthly revenue deadline. */
  val MonthlyRevenueBufferDays: Int = 3

  /**
   * Earliest date on which a (year, quarter) financial report is safely usable.
   * Throws IllegalArgumentException if quarter is not in 1..4.
   */
  def quarterlyDeadline(year: Int, quarter: Int): LocalDate = quarter match {
    case 1 => LocalDate.of(year, 5, 15).plusDays(QuarterlyBufferDays)
    case 2 => LocalDate.of(year, 8, 14).plusDays(QuarterlyBufferDays)
    case 3 => LocalDate.of(year, 11, 14).plusDays(QuarterlyBufferDays)
    case 4 => LocalDate.of(year + 1, 3, 31).plusDays(QuarterlyBufferDays)
    case _ => throw new IllegalArgumentException(s"quarter must be 1..4, got $quarter")
  }

  /**
   * Latest (year, quarter) whose publication deadline (with buffer) is on or
   * before the given rebalance date. Scans the prior two years plus the
   * current year to cover edge cases where only a previous Q4 is available.
   */
  def asOfQuarter(d: LocalDate): (Int, Int) = {
    val candidates = for {
      y <- (d.getYear - 2) to d.getYear
      q <- 1 to 4
      deadline = quarterlyDeadline(y, q)
      if !deadline.isAfter(d)
    } yield (y, q, deadline)
    require(candidates.nonEmpty, s"no quarterly report is available on $d (too early in history)")
    val chosen = candidates.maxBy { case (_, _, dl) => dl.toEpochDay }
    (chosen._1, chosen._2)
  }

  /** Earliest date on which the (year, month) monthly revenue is safely usable. */
  def monthlyRevenueDeadline(year: Int, month: Int): LocalDate = {
    val base = LocalDate.of(year, month, 1).plusMonths(1).withDayOfMonth(10)
    base.plusDays(MonthlyRevenueBufferDays)
  }

  /** Latest (year, month) of monthly revenue safely usable on date d. */
  def asOfMonthlyRevenue(d: LocalDate): (Int, Int) = {
    val candidates = for {
      y <- (d.getYear - 1) to d.getYear
      m <- 1 to 12
      deadline = monthlyRevenueDeadline(y, m)
      if !deadline.isAfter(d)
    } yield (y, m, deadline)
    require(candidates.nonEmpty, s"no monthly revenue is available on $d (too early in history)")
    val chosen = candidates.maxBy { case (_, _, dl) => dl.toEpochDay }
    (chosen._1, chosen._2)
  }
}
