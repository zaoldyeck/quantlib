import java.io.File
import java.time.LocalDate
import java.util.concurrent.Executors

import Http.materializer
import Settings._
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import play.api.libs.ws.DefaultBodyWritables._
import play.api.libs.ws.StandaloneWSResponse

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Press following code in the dev console can easily track url when click button open a new tab
 * [].forEach.call(document.querySelectorAll('a'),function(link){if(link.attributes.target) {link.attributes.target.value = '_self';}});window.open = function(url) {location.href = url;};
 */
class Crawler {
  implicit val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  def getFinancialAnalysis(year: Int): Future[Seq[File]] = {
    Thread.sleep(40000)
    println(s"Get financial analysis of $year")
    Future.sequence(FinancialAnalysisSetting(year).markets.map {
      detail =>
        Http.client.url(detail.page)
          .post(detail.formData)
          .flatMap {
            res =>
              val browser = JsoupBrowser()
              val doc = browser.parseString(res.body)
              val fileName = doc >> element("input[name=filename]") >> attr("value")
              val formData = Map(
                "firstin" -> "true",
                "step" -> "10",
                "filename" -> fileName)
              Http.client.url(detail.url)
                .withMethod("POST")
                .withBody(formData)
                .withRequestTimeout(5.minutes)
                .stream()
                .flatMap(downloadFile(detail.dir, Some(detail.fileName)))
          }
    })
  }

  def getOperatingRevenue(year: Int, month: Int): Future[Seq[File]] = {
    Thread.sleep(40000)
    println(s"Get operating revenue of $year-$month")
    Future.sequence(OperatingRevenueSetting(year, month).markets.map {
      detail =>
        year match {
          case y if y < 2013 =>
            Http.client.url(detail.url).get.flatMap(downloadFile(detail.dir, Some(detail.fileName)))
          case y if y > 2012 =>
            Http.client.url(detail.url).post(detail.formData).flatMap(downloadFile(detail.dir, Some(detail.fileName)))
        }
    })
  }

  def getBalanceSheet(year: Int, season: Int): Future[Seq[File]] = {
    Thread.sleep(40000)
    println(s"Get balance sheet of $year-Q$season")
    Future.sequence(Settings.BalanceSheetSetting(year, season).markets.map {
      detail =>
        Http.client.url(detail.page).post(detail.formData).flatMap {
          res =>
            val browser = JsoupBrowser()
            val doc = browser.parseString(res.body)
            val fileNames = (doc >> elements("input[name=filename]")).map(_ >> attr("value")).toSeq.distinct.sorted
            Future.sequence(fileNames.zipWithIndex.map {
              case (fileName, index) =>
                val formData = Map(
                  "firstin" -> "true",
                  "step" -> "10",
                  "filename" -> fileName)
                Http.client.url(detail.url)
                  .withMethod("POST")
                  .withBody(formData)
                  .withRequestTimeout(5.minutes)
                  .stream()
                  .flatMap(downloadFile(detail.dir, Some(detail.fileName + s"$index.csv")))
            })
        }
    }).map(_.reduce(_ ++ _))
  }

  def getIncomeStatement(year: Int, quarter: Int): Future[Unit] = {
    Thread.sleep(40000)
    println(s"Get income statement of $year-Q$quarter")
    Future.sequence(Settings.IncomeStatementSetting(year, quarter).markets.map {
      detail =>
        Http.client.url(detail.page).post(detail.formData).flatMap {
          res =>
            val browser = JsoupBrowser()
            val doc = browser.parseString(res.body)
            val fileNames = (doc >> elements("input[name=filename]")).map(_ >> attr("value")).toSeq.distinct.sorted
            Future.sequence(fileNames.zipWithIndex.map {
              case (fileName, index) =>
                val formData = Map(
                  "firstin" -> "true",
                  "step" -> "10",
                  "filename" -> fileName)
                Http.client.url(detail.url)
                  .withMethod("POST")
                  .withBody(formData)
                  .withRequestTimeout(5.minutes)
                  .stream()
                  .flatMap(downloadFile(detail.dir, Some(detail.fileName + s"$index.csv")))
            })
        }
    }).map(_.reduce(_ ++ _))
  }

  def getQuarterlyReport(year: Int, season: Int): Future[File] = {
    //2014 後開始有 ifrs
    //沒有 ifrs 的到 2014
    ///server-java/FileDownLoad?step=9&fileName=tw-gaap-2014Q4.zip&filePath=/home/html/nas/xbrl/2014/
    Http.client.url(s"https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-${year}Q$season.zip&filePath=/home/html/nas/ifrs/$year/")
      .withMethod("GET")
      .withRequestTimeout(5.minutes)
      .stream()
      .flatMap(downloadFile(quarterlyReportDir))
  }

  def getExRightDividend(strDate: LocalDate, endDate: LocalDate): Future[Seq[File]] = {
    println(s"Get ex-right/dividend from ${strDate.toString} to ${endDate.toString}")
    getCSV(ExRightDividendSetting(strDate, endDate))
  }

  def getCapitalReduction(strDate: LocalDate, endDate: LocalDate): Future[Seq[File]] = {
    println(s"Get capital reduction from ${strDate.toString} to ${endDate.toString}")
    getCSV(CapitalReductionSetting(strDate, endDate))
  }

  def getDailyQuote(date: LocalDate): Future[Seq[File]] = {
    println(s"Get daily quote of ${date.toString}")
    getCSV(DailyQuoteSetting(date))
  }

  def getIndex(date: LocalDate): Future[Seq[File]] = {
    println(s"Get index of ${date.toString}")
    getCSV(IndexSetting(date))
  }

  def getMarginTransactions(date: LocalDate): Future[Seq[File]] = {
    println(s"Get margin transactions of ${date.toString}")
    getCSV(MarginTransactionsSetting(date))
  }

  def getDailyTradingDetails(date: LocalDate): Future[Seq[File]] = {
    println(s"Get daily trading details of ${date.toString}")
    getCSV(DailyTradingDetailsSetting(date))
  }

  def getStockPER_PBR_DividendYield(date: LocalDate): Future[Seq[File]] = {
    println(s"Get stock PER, PBR and dividend yield of ${date.toString}")
    getCSV(StockPER_PBR_DividendYieldSetting(date))
  }

  private def getCSV(setting: Setting): Future[Seq[File]] = {
    Thread.sleep(20000)
    Future.sequence(setting.markets.map {
      detail =>
        Http.client.url(detail.url)
          .withMethod("GET")
          .withRequestTimeout(5.minutes)
          .stream()
          .flatMap(downloadFile(detail.dir, Some(detail.fileName)))
    })
  }

  private def downloadFile(filePath: String, fileName: Option[String] = None): StandaloneWSResponse => Future[File] = (res: StandaloneWSResponse) => {
    val fn = fileName.getOrElse(res.header("Content-disposition").get.split("filename=")(1).replace("\"", ""))
    val file = new File(s"$filePath/$fn")
    file.getParentFile.mkdirs()
    val outputStream = java.nio.file.Files.newOutputStream(file.toPath)

    // The sink that writes to the output stream
    val sink = Sink.foreach[ByteString] { bytes =>
      outputStream.write(bytes.toArray)
    }

    // materialize and run the stream
    res.bodyAsSource
      .runWith(sink)
      .andThen {
        case result =>
          // Close the output stream whether there was an error or not
          outputStream.close()
          // Get the result or rethrow the error
          result.get
      } map (_ => file)
  }

  //https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-2019Q4.zip&filePath=/home/html/nas/ifrs/2019/
}
