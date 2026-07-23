package setting

import java.time.LocalDate
import java.time.chrono.MinguoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

protected[this] abstract class TpexDetail(firstDate: LocalDate, strDate: Option[LocalDate], endDate: LocalDate) extends Detail(firstDate, strDate, endDate) {
  private val dateFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseLenient
    .appendPattern("y/MM/dd")
    .toFormatter
    .withChronology(MinguoChronology.INSTANCE)

  def url: String = {
    val endDateString = super.endDate.format(dateFormatter)
    val queryString = super.strDateOption match {
      case Some(strDate) => s"${strDate.format(dateFormatter)}&ed=$endDateString"
      case None => endDateString
    }
    this.file + queryString
  }
}