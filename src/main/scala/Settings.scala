import java.time.LocalDate
import java.time.chrono.MinguoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import com.typesafe.config.{Config, ConfigFactory}

import scala.reflect.io.File
import scala.reflect.io.Path._

case class MarketFile(market: String, file: File)

trait Setting {
  val twse: Detail
  val tpex: Detail
  val markets: Seq[Detail]

  def getMarketFiles: Seq[MarketFile] = markets.map {
    detail =>
      val directory = detail.dir.toDirectory
      val files = directory.files
      files.map(file => MarketFile(directory.name, file))
  }.reduce(_ ++ _).toSeq
}

abstract class Detail(firstDate: LocalDate, _strDateOption: Option[LocalDate], _endDate: LocalDate) {
  protected[this] val file: String
  val page: String = ""
  val dir: String
  val fileName = s"${endDate.getYear}_${endDate.getMonthValue}_${endDate.getDayOfMonth}.csv"

  protected[this] def strDateOption: Option[LocalDate] = _strDateOption.map(strDate => if (strDate.isBefore(firstDate)) firstDate else strDate)

  protected[this] def endDate: LocalDate = if (_endDate.isBefore(firstDate)) firstDate else _endDate

  def url: String

  def formData: Map[String, String] = Map()
}

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

object Settings {
  private val conf: Config = ConfigFactory.load
  val ETFs = Set("0050", "0051", "0052", "0053", "0054", "0055", "0056", "0057", "0058", "0059", "006203", "006204", "006208", "00690", "00692", "00701", "00713", "00730", "00728", "00731", "00733", "00742", "006201")

  case class FinancialAnalysisSetting(year: Int = LocalDate.now.getYear) extends Setting {

    class TwseBeforeIFRSsDetail extends TwseDetail(LocalDate.of(1989, 1, 1), None, LocalDate.of(year, 1, 1)) {
      private val y = super.endDate.getYear - 1911
      val file: String = conf.getString("data.financialAnalysis.file")
      val dir: String = conf.getString("data.financialAnalysis.dir.twse")
      override val page: String = conf.getString("data.financialAnalysis.page")
      override val url: String = file
      override val fileName = s"${super.endDate.getYear}_b.csv"

      override def formData = Map(
        "encodeURIComponent" -> "1",
        "step" -> "1",
        "firstin" -> "1",
        "off" -> "1",
        "TYPEK" -> "sii",
        "year" -> y.toString)
    }

    class TwseAfterIFRSsDetail extends TwseBeforeIFRSsDetail {
      override val fileName = s"${super.endDate.getYear}_a.csv"
      override val formData: Map[String, String] = super.formData + ("ifrs" -> "Y")
    }

    class TpexBeforeIFRSsDetail extends TwseBeforeIFRSsDetail {
      override val dir: String = conf.getString("data.financialAnalysis.dir.tpex")

      override def formData: Map[String, String] = super.formData + ("TYPEK" -> "otc")
    }

    class TpexAfterIFRSsDetail extends TpexBeforeIFRSsDetail {
      override val fileName = s"${super.endDate.getYear}_a.csv"
      override val formData: Map[String, String] = super.formData + ("ifrs" -> "Y")
    }

    val twse: Detail = new TwseAfterIFRSsDetail
    val tpex: Detail = new TpexAfterIFRSsDetail
    val markets: Seq[Detail] = year match {
      case y if y < 1993 => Seq(new TwseBeforeIFRSsDetail)
      case y if y < 2012 => Seq(new TwseBeforeIFRSsDetail, new TpexBeforeIFRSsDetail)
      case y if y > 2014 => Seq(new TwseAfterIFRSsDetail, new TpexAfterIFRSsDetail)
      case _ => Seq(new TwseBeforeIFRSsDetail, new TwseAfterIFRSsDetail, new TpexBeforeIFRSsDetail, new TpexAfterIFRSsDetail)
    }
  }

  case class OperatingRevenueSetting(year: Int = LocalDate.now.getYear, month: Int = LocalDate.now.getMonthValue) extends Setting {
    val twse: TwseDetail = new TwseDetail(LocalDate.of(2001, 6, 1), None, LocalDate.of(year, month, 1)) {
      private val y = super.endDate.getYear - 1911
      private val m = super.endDate.getMonthValue
      val file: String = if (y < 102) conf.getString("data.operatingRevenue.twse.file.beforeIFRSs") else conf.getString("data.operatingRevenue.twse.file.afterIFRSs")
      val dir: String = conf.getString("data.operatingRevenue.twse.dir")
      override val url: String = if (y < 102) (file + s"${y}_$m.html") else file
      override val formData = Map(
        "step" -> "9",
        "functionName" -> "show_file",
        "filePath" -> "/home/html/nas/t21/sii/",
        "fileName" -> s"t21sc03_${y}_$m.csv")
      override val fileName: String = s"${super.endDate.getYear}_$m." + (if (y < 102) "html" else "csv")
    }

    val tpex: TpexDetail = new TpexDetail(LocalDate.of(2001, 6, 1), None, LocalDate.of(year, month, 1)) {
      private val y = super.endDate.getYear - 1911
      private val m = super.endDate.getMonthValue
      val file: String = if (y < 102) conf.getString("data.operatingRevenue.tpex.file.beforeIFRSs") else conf.getString("data.operatingRevenue.tpex.file.afterIFRSs")
      val dir: String = conf.getString("data.operatingRevenue.tpex.dir")
      override val url: String = if (y < 102) (file + s"${y}_$m.html") else file
      override val formData = Map(
        "step" -> "9",
        "functionName" -> "show_file",
        "filePath" -> "/home/html/nas/t21/otc/",
        "fileName" -> s"t21sc03_${y}_$m.csv")
      override val fileName: String = s"${super.endDate.getYear}_$m." + (if (y < 102) "html" else "csv")
    }

    val markets = Seq(twse, tpex)
  }

  val quarterlyReportDir: String = conf.getString("data.quarterlyReport.dir")

  case class BalanceSheetSetting(year: Int, season: Int) extends Setting {

    class TwseBeforeIFRSsDetail extends TwseDetail(LocalDate.of(1989, 1, 1), None, LocalDate.of(year, season, 1)) {
      private val y = super.endDate.getYear - 1911
      val file: String = conf.getString("data.balanceSheet.file")
      val dir: String = conf.getString("data.balanceSheet.dir.twse")
      override val page: String = conf.getString("data.balanceSheet.page.beforeIFRSs")
      override val url: String = file
      override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_b_"

      override def formData = Map(
        "encodeURIComponent" -> "1",
        "step" -> "1",
        "firstin" -> "1",
        "off" -> "1",
        "isQuery" -> "Y",
        "TYPEK" -> "sii",
        "year" -> y.toString,
        "season" -> s"0${super.endDate.getMonthValue}")
    }

    class TwseAfterIFRSsDetail extends TwseBeforeIFRSsDetail {
      override val page: String = conf.getString("data.balanceSheet.page.afterIFRSs")
      override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_a_"
    }

    class TpexBeforeIFRSsDetail extends TwseBeforeIFRSsDetail {
      override val dir: String = conf.getString("data.balanceSheet.dir.tpex")

      override def formData: Map[String, String] = super.formData + ("TYPEK" -> "otc")
    }

    class TpexAfterIFRSsDetail extends TpexBeforeIFRSsDetail {
      override val page: String = conf.getString("data.balanceSheet.page.afterIFRSs")
      override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_a_"
    }

    val twse: Detail = new TwseAfterIFRSsDetail
    val tpex: Detail = new TpexAfterIFRSsDetail
    val markets: Seq[Detail] = year match {
      case y if y < 1993 => Seq(new TwseBeforeIFRSsDetail)
      case y if y < 2013 => Seq(new TwseBeforeIFRSsDetail, new TpexBeforeIFRSsDetail)
      case _ => Seq(new TwseAfterIFRSsDetail, new TpexAfterIFRSsDetail)
    }
  }

  case class IncomeStatementSetting(year: Int, quarter: Int) extends Setting {

    class TwseBeforeIFRSsDetail extends TwseDetail(LocalDate.of(1989, 1, 1), None, LocalDate.of(year, quarter, 1)) {
      private val y = super.endDate.getYear - 1911
      val file: String = conf.getString("data.incomeStatement.file")
      val dir: String = conf.getString("data.incomeStatement.dir.twse")
      override val page: String = conf.getString("data.incomeStatement.page.beforeIFRSs")
      override val url: String = file
      override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_b_"

      override def formData = Map(
        "encodeURIComponent" -> "1",
        "step" -> "1",
        "firstin" -> "1",
        "off" -> "1",
        "isQuery" -> "Y",
        "TYPEK" -> "sii",
        "year" -> y.toString,
        "season" -> s"0${super.endDate.getMonthValue}")
    }

    class TwseAfterIFRSsDetail extends TwseBeforeIFRSsDetail {
      override val page: String = conf.getString("data.incomeStatement.page.afterIFRSs")
      override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_a_"
    }

    class TpexBeforeIFRSsDetail extends TwseBeforeIFRSsDetail {
      override val dir: String = conf.getString("data.incomeStatement.dir.tpex")

      override def formData: Map[String, String] = super.formData + ("TYPEK" -> "otc")
    }

    class TpexAfterIFRSsDetail extends TpexBeforeIFRSsDetail {
      override val page: String = conf.getString("data.incomeStatement.page.afterIFRSs")
      override val fileName = s"${super.endDate.getYear}_${super.endDate.getMonthValue}_a_"
    }

    val twse: Detail = new TwseAfterIFRSsDetail
    val tpex: Detail = new TpexAfterIFRSsDetail
    val markets: Seq[Detail] = year match {
      case y if y < 1993 => Seq(new TwseBeforeIFRSsDetail)
      case y if y < 2013 => Seq(new TwseBeforeIFRSsDetail, new TpexBeforeIFRSsDetail)
      case _ => Seq(new TwseAfterIFRSsDetail, new TpexAfterIFRSsDetail)
    }
  }

  case class ExRightDividendSetting(strDate: LocalDate = LocalDate.now, endDate: LocalDate = LocalDate.now) extends Setting {
    val twse: TwseDetail = new TwseDetail(LocalDate.of(2003, 5, 5), Some(strDate), endDate) {
      val file: String = conf.getString("data.exRightDividend.twse.file")
      val dir: String = conf.getString("data.exRightDividend.twse.dir")
    }

    val tpex: TpexDetail = new TpexDetail(LocalDate.of(2008, 1, 2), Some(strDate), endDate) {
      val file: String = conf.getString("data.exRightDividend.tpex.file")
      val dir: String = conf.getString("data.exRightDividend.tpex.dir")
    }

    val markets = Seq(twse, tpex)
  }

  case class CapitalReductionSetting(strDate: LocalDate = LocalDate.now, endDate: LocalDate = LocalDate.now) extends Setting {
    val twse: TwseDetail = new TwseDetail(LocalDate.of(2011, 1, 1), Some(strDate), endDate) {
      val file: String = conf.getString("data.capitalReduction.twse.file")
      val dir: String = conf.getString("data.capitalReduction.twse.dir")
    }

    val tpex: TpexDetail = new TpexDetail(LocalDate.of(2013, 1, 2), Some(strDate), endDate) {
      val file: String = conf.getString("data.capitalReduction.tpex.file")
      val dir: String = conf.getString("data.capitalReduction.tpex.dir")
    }

    val markets = Seq(twse, tpex)
  }

  case class DailyQuoteSetting(date: LocalDate = LocalDate.now) extends Setting {
    val twse: TwseDetail = new TwseDetail(LocalDate.of(2004, 2, 11), None, date) {
      val file: String = conf.getString("data.dailyQuote.twse.file")
      val dir: String = conf.getString("data.dailyQuote.twse.dir")
    }

    val tpex: TpexDetail = new TpexDetail(LocalDate.of(2007, 7, 2), None, date) {
      val file: String = conf.getString("data.dailyQuote.tpex.file")
      val dir: String = conf.getString("data.dailyQuote.tpex.dir")
    }

    //val markets = Seq(twse, tpex)
    val markets = Seq(tpex)
  }

  case class IndexSetting(date: LocalDate = LocalDate.now) extends Setting {
    val twse: TwseDetail = new TwseDetail(LocalDate.of(2004, 2, 11), None, date) {
      val file: String = conf.getString("data.index.twse.file")
      val dir: String = conf.getString("data.index.twse.dir")
    }

    val tpex: TpexDetail = new TpexDetail(LocalDate.of(2016, 1, 4), None, date) {
      val file: String = conf.getString("data.index.tpex.file")
      val dir: String = conf.getString("data.index.tpex.dir")
    }

    val markets = Seq(twse, tpex)
  }

  case class MarginTransactionsSetting(date: LocalDate = LocalDate.now) extends Setting {
    val twse: TwseDetail = new TwseDetail(LocalDate.of(2001, 1, 2), None, date) {
      val file: String = conf.getString("data.marginTransactions.twse.file")
      val dir: String = conf.getString("data.marginTransactions.twse.dir")
    }

    val tpex: TpexDetail = new TpexDetail(LocalDate.of(2007, 1, 2), None, date) {
      val file: String = conf.getString("data.marginTransactions.tpex.file")
      val dir: String = conf.getString("data.marginTransactions.tpex.dir")
    }

    val markets = Seq(twse, tpex)
  }

  case class DailyTradingDetailsSetting(date: LocalDate = LocalDate.now) extends Setting {
    val twse: TwseDetail = new TwseDetail(LocalDate.of(2012, 5, 2), None, date) {
      val file: String = conf.getString("data.dailyTradingDetails.twse.file")
      val dir: String = conf.getString("data.dailyTradingDetails.twse.dir")
    }

    val tpex: TpexDetail = new TpexDetail(LocalDate.of(2007, 4, 23), None, date) {
      val file: String = if (endDate.isBefore(LocalDate.of(2014, 12, 1))) conf.getString("data.dailyTradingDetails.tpex.file.before201412") else conf.getString("data.dailyTradingDetails.tpex.file.after201412")
      val dir: String = conf.getString("data.dailyTradingDetails.tpex.dir")
    }

    val markets = Seq(twse, tpex)

  }

  case class StockPER_PBR_DividendYieldSetting(date: LocalDate = LocalDate.now) extends Setting {
    val twse: TwseDetail = new TwseDetail(LocalDate.of(2005, 9, 2), None, date) {
      val file: String = conf.getString("data.stockPER_PBR_DividendYield.twse.file")
      val dir: String = conf.getString("data.stockPER_PBR_DividendYield.twse.dir")
    }

    val tpex: TpexDetail = new TpexDetail(LocalDate.of(2007, 1, 2), None, date) {
      val file: String = conf.getString("data.stockPER_PBR_DividendYield.tpex.file")
      val dir: String = conf.getString("data.stockPER_PBR_DividendYield.tpex.dir")
    }

    val markets = Seq(twse, tpex)
  }

}
