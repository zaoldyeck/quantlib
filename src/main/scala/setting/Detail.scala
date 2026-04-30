package setting

import java.time.LocalDate

import scala.reflect.io.File
import scala.reflect.io.Path._
import scala.util.Try

abstract class Detail(val firstDate: LocalDate, _strDateOption: Option[LocalDate], _endDate: LocalDate) {
  protected[this] val file: String
  val page: String = ""
  val dir: String
  val fileName = s"${endDate.getYear}_${endDate.getMonthValue}_${endDate.getDayOfMonth}.csv"

  /**
   * Validate a freshly downloaded file against the expected schema.
   * Returns None if the file is valid; Some(errorMessage) to trigger a retry + delete.
   * Default: no validation (preserves prior behavior for Settings that have not opted in).
   * Individual Settings should override this for data types whose schema is important.
   */
  def validate(downloaded: java.io.File): Option[String] = None

  /**
   * Generic CSV schema validator: ensures the file contains all expected header keywords
   * and at least `minDataRows` lines that look like data rows (start with a digit, optionally
   * prefixed by `="` which TWSE uses for text-formatted stock codes). Intended to catch the
   * common failure modes: HTML error pages, truncated responses, and legacy-format snapshots.
   */
  /**
   * Validate a CSV by scanning the first few lines for a header-date marker. Used to detect
   * silent fallback responses like TWSE returning 2018-02-18's header when asked for a date
   * it has no data for. Empty files still pass (market-holiday convention).
   */
  protected def validateCSVHeaderDate(downloaded: java.io.File,
                                       expectedDateMarker: String,
                                       minDataRows: Int,
                                       searchLines: Int = 5,
                                       encoding: String = "Big5-HKSCS"): Option[String] = {
    if (!downloaded.exists()) return Some("file is missing")
    if (downloaded.length() == 0L) return None
    val source = scala.io.Source.fromFile(downloaded, encoding)
    try {
      val lines = source.getLines().toList
      if (lines.isEmpty) return Some("file has no lines")
      if (lines.headOption.exists(_.toLowerCase.contains("<html>"))) return Some("HTML error page")
      val headerMatch = lines.take(searchLines).exists(_.contains(expectedDateMarker))
      if (!headerMatch) {
        val headPreview = lines.headOption.getOrElse("").take(80)
        return Some(s"header date mismatch: expected marker '$expectedDateMarker' in first $searchLines lines; got head='$headPreview'")
      }
      val dataRows = lines.drop(2).count(l => l.trim.nonEmpty)
      if (dataRows < minDataRows) return Some(s"only $dataRows data rows (expected >= $minDataRows)")
      None
    } finally {
      source.close()
    }
  }

  protected def validateCSVSchema(downloaded: java.io.File,
                                   expectedHeaderKeywords: Seq[String],
                                   minDataRows: Int,
                                   minHeaderColumns: Int = 0,
                                   encoding: String = "Big5-HKSCS"): Option[String] = {
    if (!downloaded.exists()) return Some("file is missing")
    // Empty files are intentionally treated as valid — the system uses size=0 to mark
    // market holidays (weekends, TW public holidays). See Task.pullDailyFiles.
    if (downloaded.length() == 0L) return None
    val source = scala.io.Source.fromFile(downloaded, encoding)
    try {
      val lines = source.getLines().toList
      if (lines.isEmpty) return Some("file has no lines")
      if (lines.headOption.exists(_.toLowerCase.contains("<html>"))) return Some("HTML error page")
      val missingKeywords = expectedHeaderKeywords.filterNot(kw => lines.exists(_.contains(kw)))
      if (missingKeywords.nonEmpty) return Some(s"missing header keywords: ${missingKeywords.mkString(", ")}")
      if (minHeaderColumns > 0) {
        // Locate the true header row by the LAST keyword in the list (convention: first keyword may
        // match a single-column title line like "115年04月08日 三大法人買賣超日報"; the last keyword
        // should only appear in the actual header row such as "證券代號,證券名稱,...").
        val headerMarker = expectedHeaderKeywords.last
        lines.find(_.contains(headerMarker)) match {
          case Some(headerLine) =>
            val columns = headerLine.split(",", -1).length
            if (columns < minHeaderColumns)
              return Some(s"header has only $columns columns (expected >= $minHeaderColumns) — likely legacy/partial format")
          case None =>
        }
      }
      val dataRowPattern = """^(="?\d|"?\d).*""".r
      val dataRows = lines.count(l => dataRowPattern.findFirstIn(l).isDefined)
      if (dataRows < minDataRows) return Some(s"only $dataRows data rows (expected >= $minDataRows)")
      None
    } finally {
      source.close()
    }
  }

  protected[this] def strDateOption: Option[LocalDate] = _strDateOption.map(strDate => if (strDate.isBefore(firstDate)) firstDate else strDate)

  protected[this] def endDate: LocalDate = if (_endDate.isBefore(firstDate)) firstDate else _endDate

  def url: String

  def formData: Map[String, String] = Map()

  private def files: Iterator[File] = if (dir.toDirectory.exists) dir.toDirectory.deepFiles else Iterator.empty

  def getYearsOfExistFiles: Set[Int] = files.map {
    file =>
      val fileNamePattern = """(\d+)_.*.csv""".r
      val fileNamePattern(year) = file.name
      year.toInt
  }.toSet

  def getTuplesOfExistFiles: Set[(Int, Int)] = files.map {
    file =>
      val fileNamePattern = """(\d+)_(\d+).*""".r
      val fileNamePattern(year, month) = file.name
      (year.toInt, month.toInt)
  }.toSet

  def getDatesOfExistFiles: Set[LocalDate] = files.flatMap {
    file =>
      val fileNamePattern = """(\d+)_(\d+)_(\d+).*""".r
      Try {
        fileNamePattern.findFirstMatchIn(file.name).flatMap { m =>
          val year = m.group(1).toInt
          val month = m.group(2).toInt
          val day = m.group(3).toInt
          val date = LocalDate.of(year, month, day)

          // Quick HTML-error sniff using ISO-8859-1 (per-byte, never throws on
          // UTF-8 / Big5-HKSCS / mixed content). Previously used Big5-HKSCS which
          // threw MalformedInputException on UTF-8 JSON files (TPEx SBL/QFII)
          // and silently dropped real-data files from existFiles, causing
          // O(thousands of dates) of redundant re-fetching after restart.
          val source = scala.io.Source.fromFile(file.jfile, "ISO-8859-1")
          try {
            val lines = source.getLines().buffered
            if (!lines.isEmpty && lines.head.toLowerCase.contains("<html>")) {
              None
            } else {
              Some(date)
            }
          } finally {
            source.close()
          }
        }
      }.getOrElse {
        // Fallback: even if read fails (corrupt encoding, IO error), keep the date —
        // a stat-time file is strong-enough evidence the date is "tried/saved".
        // This avoids the regression mode where transient read errors trigger
        // unnecessary re-fetches.
        val fileNamePattern = """(\d+)_(\d+)_(\d+).*""".r
        fileNamePattern.findFirstMatchIn(file.name).flatMap { m =>
          Try(LocalDate.of(m.group(1).toInt, m.group(2).toInt, m.group(3).toInt)).toOption
        }
      }
  }.toSet
}
