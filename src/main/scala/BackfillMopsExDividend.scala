import java.io.{File => JFile, FileOutputStream, PrintWriter}
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import scala.util.matching.Regex

/**
 * Backfills ex-dividend data via MOPS t108sb27 for months 2024-07 through current,
 * covering both TWSE (TYPEK=sii) and TPEx (TYPEK=otc). The legacy TWSE TWT49U
 * endpoint silently stopped returning data in mid-2024 and filled disk with 2-byte
 * empty sentinel files, leaving the ex_right_dividend table starved of TWSE dividends
 * after 2024-06-21.
 *
 * MOPS t108sb27 two-step flow (same pattern as balance_sheet):
 *   Step 1: POST /mops/web/ajax_t108sb27 with market+year+month → HTML containing
 *           <input name="filename" value="t108sb27_YYYYMMDD_HHMMSSmmm.csv">
 *   Step 2: POST /server-java/t105sb02 with that filename → CSV body
 *
 * Saves to data/ex_right_dividend_mops/{market}/{year}_{month}.csv. Uses plain
 * HttpURLConnection to avoid pulling in the project's Akka/Play WS stack for a
 * one-shot backfill.
 */
object BackfillMopsExDividend {
  private val PageUrl = "https://mopsov.twse.com.tw/mops/web/ajax_t108sb27"
  private val DownloadUrl = "https://mopsov.twse.com.tw/server-java/t105sb02"
  private val FilenameRegex: Regex = """filename' value='(t108sb27_[^']+\.csv)'""".r
  private val UserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"

  def main(args: Array[String]): Unit = {
    val today = LocalDate.now
    val startYear = 2024
    val startMonth = 7

    val markets = Seq("sii" -> "twse", "otc" -> "tpex")

    val months: Seq[(Int, Int)] = (for {
      y <- startYear to today.getYear
      m <- 1 to 12
      if (y > startYear || m >= startMonth) && (y < today.getYear || m <= today.getMonthValue)
    } yield (y, m))

    println(s"[backfill] range: ${months.head} to ${months.last} (${months.size} months × ${markets.size} markets = ${months.size * markets.size} downloads)")

    var ok, skipped, failed = 0
    for {
      (year, month) <- months
      (typek, marketDir) <- markets
    } {
      val minguoYear = year - 1911
      val file = new JFile(s"data/ex_right_dividend_mops/$marketDir/${year}_${month}.csv")
      if (file.exists() && file.length() > 200) {
        skipped += 1
      } else {
        file.getParentFile.mkdirs()
        try {
          Thread.sleep(2000) // light rate limit
          val step1Body = fetchStep1(typek, minguoYear, month)
          FilenameRegex.findFirstMatchIn(step1Body).map(_.group(1)) match {
            case None =>
              println(s"[backfill] $typek ${year}-${month}: no filename in step1 (empty month?)")
              failed += 1
            case Some(filename) =>
              val csvBytes = fetchStep2(filename)
              val out = new FileOutputStream(file)
              try out.write(csvBytes) finally out.close()
              println(f"[backfill] $typek%s ${year}%d-${month}%02d ok (${csvBytes.length}%d bytes, filename=$filename)")
              ok += 1
          }
        } catch {
          case e: Throwable =>
            println(s"[backfill] $typek ${year}-${month}: FAILED — ${e.getClass.getSimpleName}: ${e.getMessage}")
            failed += 1
        }
      }
    }
    println(s"[backfill] done. ok=$ok skipped=$skipped failed=$failed")
  }

  private def fetchStep1(typek: String, minguoYear: Int, month: Int): String = {
    val form =
      s"step=1&firstin=ture&off=1&TYPEK=${url(typek)}" +
        s"&year=${minguoYear}&month=$month&b_date=1&e_date=31&type=0"
    post(PageUrl, form)
  }

  private def fetchStep2(filename: String): Array[Byte] = {
    val form = s"firstin=true&step=10&filename=${url(filename)}"
    postBytes(DownloadUrl, form)
  }

  private def post(urlStr: String, formBody: String): String = {
    new String(postBytes(urlStr, formBody), "Big5")
  }

  private def postBytes(urlStr: String, formBody: String): Array[Byte] = {
    val conn = new URL(urlStr).openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setConnectTimeout(30000)
    conn.setReadTimeout(120000)
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
    conn.setRequestProperty("User-Agent", UserAgent)
    val os = conn.getOutputStream
    try os.write(formBody.getBytes(StandardCharsets.UTF_8)) finally os.close()
    val is = conn.getInputStream
    try {
      val buf = new java.io.ByteArrayOutputStream()
      val tmp = new Array[Byte](8192)
      var n = is.read(tmp)
      while (n >= 0) { buf.write(tmp, 0, n); n = is.read(tmp) }
      buf.toByteArray
    } finally is.close()
  }

  private def url(s: String): String = URLEncoder.encode(s, StandardCharsets.UTF_8)
}
