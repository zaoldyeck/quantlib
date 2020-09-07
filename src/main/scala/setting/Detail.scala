package setting

import java.time.LocalDate

import scala.reflect.io.File
import scala.reflect.io.Path._

abstract class Detail(val firstDate: LocalDate, _strDateOption: Option[LocalDate], _endDate: LocalDate) {
  protected[this] val file: String
  val page: String = ""
  val dir: String
  val fileName = s"${endDate.getYear}_${endDate.getMonthValue}_${endDate.getDayOfMonth}.csv"

  protected[this] def strDateOption: Option[LocalDate] = _strDateOption.map(strDate => if (strDate.isBefore(firstDate)) firstDate else strDate)

  protected[this] def endDate: LocalDate = if (_endDate.isBefore(firstDate)) firstDate else _endDate

  def url: String

  def formData: Map[String, String] = Map()

  private def files: Iterator[File] = dir.toDirectory.files

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
      val fileNamePattern(year, month, day) = file.name
      val date = LocalDate.of(year.toInt, month.toInt, day.toInt)
      val lines = file.lines("Big5-HKSCS")
      val firstLineOption = lines.nextOption
      //if ((firstLineOption.isEmpty && date.getDayOfWeek.getValue < 6) || (firstLineOption == Option("<html>"))) None else Some(date)
      if (firstLineOption == Option("<html>")) None else Some(date)
  }.toSet
}
