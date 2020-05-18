import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
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

class Crawler {
  implicit val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  def getFinancialAnalysis(year: Int): Future[Unit] = {
    Thread.sleep(20000)
    println(s"Get financial analysis of $year")

    def request(formData: Map[String, String], fileName: String): Future[File] = {
      Http.client.url(financialAnalysis.page)
        .post(formData)
        .flatMap {
          res =>
            val browser = JsoupBrowser()
            val doc = browser.parseString(res.body)
            val fileName = doc >> element("input[name=filename]") >> attr("value")
            val fd = Map(
              "firstin" -> "true",
              "step" -> "10",
              "filename" -> fileName)
            Http.client.url(financialAnalysis.file)
              .withMethod("POST")
              .withBody(fd)
              .withRequestTimeout(5.minutes)
              .stream()
        } flatMap (downloadFile(financialAnalysis.dir, Some(fileName)))
    }

    val y = year - 1911
    val taskBeforeIFRS = if (y < 104) {
      // Before IFRS
      val formData = Map(
        "encodeURIComponent" -> "1",
        "step" -> "1",
        "firstin" -> "1",
        "off" -> "1",
        "TYPEK" -> "sii",
        "year" -> y.toString)
      request(formData, s"${year}_b.csv")
    } else Future.successful()

    val taskAfterIFRS = if (y > 100) {
      // After IFRS
      val formData = Map(
        "encodeURIComponent" -> "1",
        //"run" -> "Y",
        "step" -> "1",
        "TYPEK" -> "sii",
        "year" -> y.toString,
        "firstin" -> "1",
        "off" -> "1",
        "ifrs" -> "Y")
      request(formData, s"${year}_a.csv")
    } else Future.successful()

    for {
      _ <- taskAfterIFRS
      _ <- taskBeforeIFRS
    } yield ()
  }

  def getOperatingRevenue(year: Int, month: Int): Future[File] = {
    Thread.sleep(20000)
    println(s"Get operating revenue of $year-$month")
    year - 1911 match {
      case y if y < 102 =>
        Http.client.url(operatingRevenue.beforeIFRSs.file + s"${y}_$month.html").get.flatMap(downloadFile(operatingRevenue.dir, Some(s"${year}_$month.html")))
      case y if y > 101 =>
        // After IFRS
        val formData = Map(
          "step" -> "9",
          "functionName" -> "show_file",
          "filePath" -> "/home/html/nas/t21/sii/",
          "fileName" -> s"t21sc03_${y}_$month.csv")
        Http.client.url(operatingRevenue.afterIFRSs.file).post(formData).flatMap(downloadFile(operatingRevenue.dir, Some(s"${year}_$month.csv")))
    }
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

  def getDailyQuote(date: LocalDate): Future[File] = {
    Thread.sleep(20000)
    println(s"Get daily quote of ${date.toString}")
    val dateString = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    Http.client.url(dailyQuote.file + dateString)
      .withMethod("GET")
      .withRequestTimeout(5.minutes)
      .stream()
      .flatMap(downloadFile(dailyQuote.dir, Some(s"${date.getYear}_${date.getMonthValue}_${date.getDayOfMonth}.csv")))
  }

  def getIndex(date: LocalDate): Future[File] = {
    Thread.sleep(20000)
    val dateString = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    Http.client.url(index.file + dateString)
      .withMethod("GET")
      .withRequestTimeout(5.minutes)
      .stream()
      .flatMap(downloadFile(index.dir, Some(s"${date.getYear}_${date.getMonthValue}_${date.getDayOfMonth}.csv")))
  }

  def getStatementOfComprehensiveIncome(year: Int, season: Int): Future[Unit] = {
    println("Here it is")
    Http.client.url(statementOfComprehensiveIncome.page)
      .post(Map(
        "encodeURIComponent" -> "1",
        "step" -> "1",
        "firstin" -> "1",
        "off" -> "1",
        "isQuery" -> "Y",
        "TYPEK" -> "sii",
        "year" -> (year - 1911).toString,
        "season" -> season.toString))
      .map {
        res =>
          println(res.body)
          val browser = JsoupBrowser()
          val doc = browser.parseString(res.body)
          val fileNames = (doc >> elements("input[name=filename]")).map(_ >> attr("value")).toSeq.distinct.sorted
          //val fileName = doc >> elements("input[name=filename]") >> attr("value")
          println(fileNames.size)
          fileNames.foreach(println)


        /*
          val fd = Map(
            "firstin" -> "true",
            "step" -> "10",
            "filename" -> fileName)
          Http.client.url(financialAnalysis.file)
            .withMethod("POST")
            .withBody(fd)
            .withRequestTimeout(5.minutes)
            .stream()
      } flatMap (downloadFile(financialAnalysis.dir, Some(fileName)))

           */
      }
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
