import java.io.File

import Http.materializer
import akka.stream.scaladsl.Sink
import akka.util.ByteString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class Crawler {
  def getMonthlyReport = {

  }

  def getQuarterlyReport(year: Int, quarter: Int): Future[File] = {
    //2014 後開始有 ifrs
    //沒有 ifrs 的到 2014
    ///server-java/FileDownLoad?step=9&fileName=tw-gaap-2014Q4.zip&filePath=/home/html/nas/xbrl/2014/
    Http.client.url(s"https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-${year}Q$quarter.zip&filePath=/home/html/nas/ifrs/$year/").withMethod("GET").withRequestTimeout(5.minutes).stream().flatMap {
      res =>
        val fileName = res.header("Content-disposition").get.split("filename=")(1).replace("\"", "")
        val file = new File(s"../data/quarterly_report/$fileName")
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
          }
          .map(_ => file)
    }
  }

  //https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-2019Q4.zip&filePath=/home/html/nas/ifrs/2019/
}
