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

          val source = scala.io.Source.fromFile(file.jfile, "Big5-HKSCS")
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
      }.getOrElse(None)
  }.toSet
}
