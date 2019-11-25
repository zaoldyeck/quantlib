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

class Crawler {
  def getFinancialAnalysis(year: Int): Future[File] = {
    year - 1911 match {
      /*
    case y if y > 100 =>
    // After IFRS

    case y if y > 100 && y < 104 =>
    // Both
       */
      case y if y < 104 =>
        // Before IFRS
        val formData = Map(
          "encodeURIComponent" -> "1",
          "step" -> "1",
          "firstin" -> "1",
          "off" -> "1",
          "TYPEK" -> "sii",
          "year" -> y.toString)
        Http.client.url("https://mops.twse.com.tw/mops/web/ajax_t51sb02")
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
              Http.client.url("https://mops.twse.com.tw/server-java/t105sb02")
                .withMethod("POST")
                .withBody(fd)
                .withRequestTimeout(5.minutes)
                .stream()
          } flatMap (downloadFile("./data/financial_analysis", Some(s"${y}_b.csv")))
    }
  }

  def getMonthlyReport = {

  }

  def getQuarterlyReport(year: Int, quarter: Int): Future[File] = {
    //2014 後開始有 ifrs
    //沒有 ifrs 的到 2014
    ///server-java/FileDownLoad?step=9&fileName=tw-gaap-2014Q4.zip&filePath=/home/html/nas/xbrl/2014/
    Http.client.url(s"https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-${year}Q$quarter.zip&filePath=/home/html/nas/ifrs/$year/")
      .withMethod("GET")
      .withRequestTimeout(5.minutes)
      .stream()
      .flatMap(downloadFile("./data/quarterly_report"))
  }

  def downloadFile(filePath: String, fileName: Option[String] = None): StandaloneWSResponse => Future[File] = (res: StandaloneWSResponse) => {
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
