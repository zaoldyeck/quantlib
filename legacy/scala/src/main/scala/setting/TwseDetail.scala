package setting

import java.time.LocalDate
import java.time.format.DateTimeFormatter

protected[this] abstract class TwseDetail(firstDate: LocalDate, strDate: Option[LocalDate], endDate: LocalDate) extends Detail(firstDate, strDate, endDate) {
  private val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def url: String = {
    val endDateString = super.endDate.format(dateFormatter)
    val queryString = super.strDateOption match {
      case Some(strDate) => s"${strDate.format(dateFormatter)}&endDate=$endDateString"
      case None => endDateString
    }
    this.file + queryString
  }
}
