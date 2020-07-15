package setting

import java.time.LocalDate

abstract class Detail(val firstDate: LocalDate, _strDateOption: Option[LocalDate], _endDate: LocalDate) {
  protected[this] val file: String
  val page: String = ""
  val dir: String
  val fileName = s"${endDate.getYear}_${endDate.getMonthValue}_${endDate.getDayOfMonth}.csv"

  protected[this] def strDateOption: Option[LocalDate] = _strDateOption.map(strDate => if (strDate.isBefore(firstDate)) firstDate else strDate)

  protected[this] def endDate: LocalDate = if (_endDate.isBefore(firstDate)) firstDate else _endDate

  def url: String

  def formData: Map[String, String] = Map()
}
