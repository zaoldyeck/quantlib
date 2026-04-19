package strategy

import java.time.LocalDate

/** Temporary runnable test for PublicationLag — runs via `sbt "runMain strategy.PublicationLagTest"`.
 *  Remove once a proper test framework is wired for this project. */
object PublicationLagTest {
  private var failures = 0

  private def check[A](label: String, actual: A, expected: A): Unit = {
    if (actual == expected) {
      println(s"  ✓ $label = $actual")
    } else {
      println(s"  ✗ $label = $actual (expected $expected)")
      failures += 1
    }
  }

  def main(args: Array[String]): Unit = {
    println("=== quarterlyDeadline ===")
    check("Q1 2023", PublicationLag.quarterlyDeadline(2023, 1), LocalDate.of(2023, 5, 22))
    check("Q2 2023", PublicationLag.quarterlyDeadline(2023, 2), LocalDate.of(2023, 8, 21))
    check("Q3 2023", PublicationLag.quarterlyDeadline(2023, 3), LocalDate.of(2023, 11, 21))
    check("Q4 2023", PublicationLag.quarterlyDeadline(2023, 4), LocalDate.of(2024, 4, 7))

    println("=== asOfQuarter ===")
    check("2023-06-01",  PublicationLag.asOfQuarter(LocalDate.of(2023, 6, 1)),  (2023, 1))
    check("2023-05-22",  PublicationLag.asOfQuarter(LocalDate.of(2023, 5, 22)), (2023, 1))
    check("2023-05-21",  PublicationLag.asOfQuarter(LocalDate.of(2023, 5, 21)), (2022, 4))
    check("2023-08-20",  PublicationLag.asOfQuarter(LocalDate.of(2023, 8, 20)), (2023, 1))
    check("2023-08-22",  PublicationLag.asOfQuarter(LocalDate.of(2023, 8, 22)), (2023, 2))
    check("2024-04-07",  PublicationLag.asOfQuarter(LocalDate.of(2024, 4, 7)),  (2023, 4))
    check("2024-04-06",  PublicationLag.asOfQuarter(LocalDate.of(2024, 4, 6)),  (2023, 3))

    println("=== monthlyRevenueDeadline ===")
    check("2023-04 revenue", PublicationLag.monthlyRevenueDeadline(2023, 4),  LocalDate.of(2023, 5, 13))
    check("2023-05 revenue", PublicationLag.monthlyRevenueDeadline(2023, 5),  LocalDate.of(2023, 6, 13))
    check("2023-12 revenue", PublicationLag.monthlyRevenueDeadline(2023, 12), LocalDate.of(2024, 1, 13))

    println("=== asOfMonthlyRevenue ===")
    check("2023-06-01", PublicationLag.asOfMonthlyRevenue(LocalDate.of(2023, 6, 1)),  (2023, 4))
    check("2023-06-13", PublicationLag.asOfMonthlyRevenue(LocalDate.of(2023, 6, 13)), (2023, 5))
    check("2023-06-12", PublicationLag.asOfMonthlyRevenue(LocalDate.of(2023, 6, 12)), (2023, 4))

    println()
    if (failures == 0) println("All tests passed.")
    else {
      println(s"$failures test(s) failed.")
      sys.exit(1)
    }
  }
}
