package strategy

import java.time.LocalDate
import slick.jdbc.PostgresProfile.api._

/** Temporary runnable test for Universe — sanity-check size across years. */
object UniverseTest {
  def main(args: Array[String]): Unit = {
    val db = Database.forConfig("db")
    try {
      val dates = Seq(
        LocalDate.of(2018, 6, 15),
        LocalDate.of(2020, 6, 15),
        LocalDate.of(2022, 6, 15),
        LocalDate.of(2023, 6, 1),
        LocalDate.of(2024, 6, 15),
        LocalDate.of(2026, 4, 17)
      )
      println("=== Universe size by date ===")
      dates.foreach { d =>
        val u = Universe.eligible(d, db)
        println(f"  $d%s → ${u.size}%4d tickers")
      }
      // Spot check for 2023-06-01: TSMC (2330), Hon Hai (2317) should be in
      val u2023 = Universe.eligible(LocalDate.of(2023, 6, 1), db)
      val musts = Seq("2330", "2317", "2454", "2412")
      println("\n=== Spot checks 2023-06-01 ===")
      musts.foreach { code =>
        val present = u2023.contains(code)
        println(s"  $code ${if (present) "✓ in" else "✗ MISSING"}")
      }
      // Should exclude ETFs
      val etfs = Seq("0050", "0056", "00878")
      println("\n=== ETF exclusion 2023-06-01 ===")
      etfs.foreach { code =>
        val present = u2023.contains(code)
        println(s"  $code ${if (present) "✗ LEAKED" else "✓ excluded"}")
      }
    } finally {
      db.close()
    }
  }
}
