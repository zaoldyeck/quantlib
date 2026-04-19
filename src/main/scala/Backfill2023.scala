import java.io.{File => JFile}
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.io.Path._

/**
 * Backfills 2023 Q1 + Q4 financial_statements bulk zips. These quarters were
 * incompletely downloaded originally (Q1: 89/1800 files, Q4: 703/2300 files)
 * because the crawler ran before TWSE had finalized the bulk zip. Today both
 * quarters are long past their publication deadlines, so a fresh bulk download
 * should yield complete data.
 *
 * Strategy:
 *   1. Wipe the partial directory so Helpers.unzip doesn't conflict
 *   2. Re-download the bulk tifrs-{year}Q{quarter}.zip
 *   3. Re-import via FinancialReader.readFinancialStatements() — its existing
 *      dataAlreadyInDB filter will skip the 89/703 rows already imported.
 */
object Backfill2023 {
  def main(args: Array[String]): Unit = {
    val finReader = new reader.FinancialReader
    try {
      val q1Count = Option(new JFile(s"data/financial_statements/2023_1").listFiles).getOrElse(Array.empty[JFile]).length
      val q4Count = Option(new JFile(s"data/financial_statements/2023_4").listFiles).getOrElse(Array.empty[JFile]).length
      println(s"[check] 2023_1 has $q1Count files, 2023_4 has $q4Count files")
      println("[reader] running readFinancialStatements() to import …")
      finReader.readFinancialStatements()
      println("[reader] done.")
    } finally {
      Http.terminate()
    }
  }
}
