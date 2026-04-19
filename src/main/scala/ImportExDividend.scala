import reader.TradingReader

/** Runs just the readExRightDividend step to import newly backfilled MOPS monthly
 *  CSVs (2024-07 through current) into ex_right_dividend. */
object ImportExDividend {
  def main(args: Array[String]): Unit = {
    val tr = new TradingReader
    try {
      println("[import] running readExRightDividend() — will pick up new MOPS monthly files")
      tr.readExRightDividend()
      println("[import] done.")
    } finally {
      Http.terminate()
    }
  }
}
