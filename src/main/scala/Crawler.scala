import java.io.File

import Http.materializer
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import play.api.libs.ws.DefaultBodyWritables._
import play.api.libs.ws.{StandaloneWSRequest, StandaloneWSResponse}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL.Parse._
import Settings._

class Crawler {
  def getFinancialAnalysis(year: Int)= {
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
        } flatMap (downloadFile(financialAnalysis.dir, Some(s"$fileName.csv")))
    }

    val y = year - 1911
    val task1 = if (y > 100) {
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
      request(formData, s"${y}_a")
    }



    if (y > 100) {
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
      request(formData, s"${y}_a")
    }

    if (y < 104) {
      // Before IFRS
      val formData = Map(
        "encodeURIComponent" -> "1",
        "step" -> "1",
        "firstin" -> "1",
        "off" -> "1",
        "TYPEK" -> "sii",
        "year" -> y.toString)
      request(formData, s"${y}_b")
    }
  }

  def getOperatingRevenue(year: Int, month: Int): Future[File] = {
    Thread.sleep(10000)
    year - 1911 match {
      case y if y < 102 =>
        // Before IFRS
        val formData = Map(
          "encodeURIComponent" -> "1",
          "step" -> "9",
          "firstin" -> "1",
          "off" -> "1",
          "TYPEK" -> "sii",
          "year" -> "",
          "month" -> "")
        Http.client.url(operatingRevenue.beforeIFRSs.page).post(formData).flatMap(downloadFile(operatingRevenue.dir, Some(s"${y}_$month.csv")))
      case y if y > 101 =>
        // After IFRS
        val formData = Map(
          "step" -> "9",
          "functionName" -> "show_file",
          "filePath" -> "/home/html/nas/t21/sii/",
          "fileName" -> s"t21sc03_${y}_$month.csv")
        Http.client.url(operatingRevenue.afterIFRSs.file).post(formData).flatMap(downloadFile(operatingRevenue.dir, Some(s"${y}_$month.csv")))
    }
  }

  def getQuarterlyReport(year: Int, quarter: Int): Future[File] = {
    //2014 後開始有 ifrs
    //沒有 ifrs 的到 2014
    ///server-java/FileDownLoad?step=9&fileName=tw-gaap-2014Q4.zip&filePath=/home/html/nas/xbrl/2014/
    Http.client.url(s"https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-${year}Q$quarter.zip&filePath=/home/html/nas/ifrs/$year/")
      .withMethod("GET")
      .withRequestTimeout(5.minutes)
      .stream()
      .flatMap(downloadFile(quarterlyReportDir))
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
