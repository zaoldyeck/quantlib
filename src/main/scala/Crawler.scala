import java.io.{File, FileInputStream, FileOutputStream}
import java.time.LocalDate
import java.util.concurrent.Executors
import java.util.zip.ZipInputStream

import Http.{materializer, scheduler}
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import play.api.libs.ws.DefaultBodyWritables._
import play.api.libs.ws.StandaloneWSResponse
import setting._
import util.Helpers
import util.Helpers.SeqExtension

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.io.Path._

/**
 * Press following code in the dev console can easily track url when click button open a new tab
 * [].forEach.call(document.querySelectorAll('a'),function(link){if(link.attributes.target) {link.attributes.target.value = '_self';}});window.open = function(url) {location.href = url;};
 */
class Crawler {
  implicit val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  def getFinancialAnalysis(year: Int): Future[Seq[File]] = {
    println(s"Get financial analysis of $year")
    FinancialAnalysisSetting(year).markets.mapInSeries {
      detail =>
        Thread.sleep(20000)
        Helpers.retry {
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
        }
    }
  }

  def getOperatingRevenue(year: Int, month: Int): Future[Seq[File]] = {
    println(s"Get operating revenue of $year-$month")
    OperatingRevenueSetting(year, month).markets.mapInSeries {
      detail =>
        Thread.sleep(20000)
        Helpers.retry {
          year match {
            case y if y < 2013 =>
              detail.page match {
                case "" => Http.client.url(detail.url).get.flatMap(downloadFile(detail.dir, Some(detail.fileName)))
                case _ =>
                  Http.client.url(detail.page)
                    .post(detail.formData)
                    .flatMap {
                      res =>
                        val browser = JsoupBrowser()
                        val doc = browser.parseString(res.body)
                        val fileName = doc >> element("input[name=filename]") >> attr("value")
                        val formData = Map(
                          "firstin" -> "true",
                          "step" -> "9",
                          "filename" -> fileName)
                        Http.client.url(detail.url)
                          .withMethod("POST")
                          .withBody(formData)
                          .stream()
                          .flatMap(downloadFile(detail.dir, Some(detail.fileName)))
                    }
              }
            case y if y > 2012 =>
              Http.client.url(detail.url).post(detail.formData).flatMap(downloadFile(detail.dir, Some(detail.fileName)))
          }
        }
    }
  }

  def getBalanceSheet(year: Int, quarter: Int): Future[Seq[File]] = {
    println(s"Get balance sheet of $year-Q$quarter")
    BalanceSheetSetting(year, quarter).markets.mapInSeries {
      detail =>
        Thread.sleep(20000)
        Helpers.retry {
          Http.client.url(detail.page).post(detail.formData).flatMap {
            res =>
              val browser = JsoupBrowser()
              val doc = browser.parseString(res.body)
              val fileNames = (doc >> elements("input[name=filename]")).map(_ >> attr("value")).toSeq.distinct.sorted
              fileNames.zipWithIndex.mapInSeries {
                case (fileName, index) =>
                  Thread.sleep(10000)
                  val formData = Map(
                    "firstin" -> "true",
                    "step" -> "10",
                    "filename" -> fileName)
                  Http.client.url(detail.url)
                    .withMethod("POST")
                    .withBody(formData)
                    .stream()
                    .flatMap(downloadFile(detail.dir, Some(detail.fileName + s"$index.csv")))
              }
          }
        }
    }.map(_.reduce(_ ++ _))
  }

  def getIncomeStatement(year: Int, quarter: Int): Future[Seq[File]] = {
    println(s"Get income statement of $year-Q$quarter")
    IncomeStatementSetting(year, quarter).markets.mapInSeries {
      detail =>
        Thread.sleep(20000)
        Helpers.retry {
          Http.client.url(detail.page).post(detail.formData).flatMap {
            res =>
              val browser = JsoupBrowser()
              val doc = browser.parseString(res.body)
              val fileNames = (doc >> elements("input[name=filename]")).map(_ >> attr("value")).toSeq.distinct.sorted
              fileNames.zipWithIndex.mapInSeries {
                case (fileName, index) =>
                  Thread.sleep(10000)
                  val formData = Map(
                    "firstin" -> "true",
                    "step" -> "10",
                    "filename" -> fileName)
                  Http.client.url(detail.url)
                    .withMethod("POST")
                    .withBody(formData)
                    .stream()
                    .flatMap(downloadFile(detail.dir, Some(detail.fileName + s"$index.csv")))
              }
          }
        }
    }.map(_.reduce(_ ++ _))
  }

  def getFinancialStatements(year: Int, quarter: Int, companyCode: String): Future[Seq[File]] = {
    println(s"Get financial statements of $year-Q$quarter-$companyCode")
    FinancialStatementsSetting(year, quarter, companyCode).markets.mapInSeries {
      detail =>
        val file = s"${detail.dir}/${detail.fileName}".toFile
        (file.length match {
          case l if l > 10000 => Future(file.jfile)
          case _ =>
            println(detail.url)
            Thread.sleep(20000)
            Helpers.retry {
              Http.client.url(detail.url)
                .withMethod("GET")
                .stream()
                .flatMap(downloadFile(detail.dir, Some(detail.fileName)))
            }
        }).map {
          file =>
            if (file.extension == "zip") Helpers.unzip(file, delete = true)
            file
        }
    }
  }

  def getExRightDividend(year: Int, month: Int): Future[Seq[File]] = {
    println(s"Get ex-right/dividend of $year-$month (MOPS t108sb27)")
    ExRightDividendSetting(year, month).markets.mapInSeries { detail =>
      Thread.sleep(10000)
      Helpers.retry {
        Http.client.url(detail.page).post(detail.formData).flatMap { res =>
          val browser = JsoupBrowser()
          val doc = browser.parseString(res.body)
          val filename: String = doc >> element("input[name=filename]") >> attr("value")
          if (filename.endsWith(".csv")) {
            val downloadForm: Map[String, String] =
              Map("firstin" -> "true", "step" -> "10", "filename" -> filename)
            Http.client.url(detail.fileUrl)
              .withMethod("POST")
              .withBody(downloadForm)
              .withRequestTimeout(2.minutes)
              .stream()
              .flatMap(downloadFile(detail.dir + s"/$year", Some(detail.fileName)))
          } else {
            Future.failed(new RuntimeException(
              s"MOPS step-1 returned no filename for $year-$month/${detail.formData("TYPEK")}"))
          }
        }
      }
    }
  }

  def getCapitalReduction(strDate: LocalDate, endDate: LocalDate): Future[Seq[File]] = {
    println(s"Get capital reduction from ${strDate.toString} to ${endDate.toString}")
    getFile(CapitalReductionSetting(strDate, endDate))
  }

  def getDailyQuote(date: LocalDate): Future[Seq[File]] = {
    println(s"Get daily quote of ${date.toString}")
    getFile(DailyQuoteSetting(date))
  }

  def getIndex(date: LocalDate): Future[Seq[File]] = {
    println(s"Get index of ${date.toString}")
    getFile(IndexSetting(date))
  }

  def getMarginTransactions(date: LocalDate): Future[Seq[File]] = {
    println(s"Get margin transactions of ${date.toString}")
    getFile(MarginTransactionsSetting(date))
  }

  def getDailyTradingDetails(date: LocalDate): Future[Seq[File]] = {
    println(s"Get daily trading details of ${date.toString}")
    getFile(DailyTradingDetailsSetting(date))
  }

  def getStockPER_PBR_DividendYield(date: LocalDate): Future[Seq[File]] = {
    println(s"Get stock PER, PBR and dividend yield of ${date.toString}")
    getFile(StockPER_PBR_DividendYieldSetting(date))
  }

  def getETF: Future[Seq[File]] = {
    println(s"Get ETF")
    getFile(ETFSetting())
  }

  private def getFile(setting: Setting): Future[Seq[File]] = {
    Thread.sleep(20000)
    Future.sequence(setting.markets.map {
      detail =>
        println(detail.url)
        Helpers.retry {
          Http.client.url(detail.url)
            .withMethod("GET")
            .withFollowRedirects(false)
            .stream()
            .flatMap(downloadFile(detail.dir, Some(detail.fileName), detail.validate))
        }.recover {
          case e =>
            // Stateless recovery: after bounded retries still fail, DELETE any partial download
            // so Detail.getDatesOfExistFiles reports this date as "not downloaded" and a future
            // pullAllData run will retry. Do NOT write a 0-byte sentinel — that would permanently
            // mask the failure as a market holiday and lose data when the endpoint recovers.
            Console.err.println(s"[giveup] will retry next run: ${detail.url}: ${e.getClass.getSimpleName}: ${e.getMessage}")
            val fn = detail.fileName
            val yearPath = if (fn.matches("""^\d{4}_\d+_\d+\.csv$""")) s"${detail.dir}/${fn.substring(0, 4)}" else detail.dir
            val f = new File(s"$yearPath/$fn")
            if (f.exists()) f.delete()
            f // non-existent File — satisfies Future[File] type; getDatesOfExistFiles treats as missing
        }
    })
  }

  private def downloadFile(filePath: String,
                           fileName: Option[String] = None,
                           validate: File => Option[String] = _ => None): StandaloneWSResponse => Future[File] = (res: StandaloneWSResponse) => {
    val fn = fileName.getOrElse(res.header("Content-disposition").get.split("filename=")(1).replace("\"", ""))

    // Extract year from any file whose name starts with YYYY_...  This covers
    // both daily (YYYY_M_D.csv) and quarterly (YYYY_Q_a_c_idx.csv) patterns.
    // Before the widening: only 3-pure-digit (daily) filenames matched, leaving
    // quarterly CSVs dumped in the market root dir — reader deep-scan still read
    // them, but `ls data/balance_sheet/twse/{year}/` wouldn't show them, which
    // caused spurious "crawler failed" diagnoses during debugging.
    val finalPath = fn match {
      case s if s.matches("""^\d{4}_.*\.csv$""") => s"$filePath/${s.substring(0, 4)}"
      case _ => filePath
    }

    val file = new File(s"$finalPath/$fn")
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
      } map { _ =>
      // HTML response (307 redirect body, Cloudflare challenge, or "頁面無法執行" anti-scraping page)
      // always indicates a failed fetch — never market holiday. Delete and throw so Helpers.retry
      // can re-attempt; if all retries fail, the outer .recover will clean up and let the next
      // pullAllData run try again.
      if (isHtmlResponse(file)) {
        val deleted = file.delete()
        throw new RuntimeException(s"HTML error response for ${file.getAbsolutePath} (deleted=$deleted)")
      } else if (isMarketHolidayResponse(file)) {
        // Non-trading day fallback (e.g. TWSE "很抱歉，沒有符合條件的資料" / JSON total=0).
        // Truncate to 0 bytes so Detail.getDatesOfExistFiles considers the date "done"
        // and future pullAllData runs don't retry this weekend forever.
        new FileOutputStream(file).close()
        file
      } else {
        validate(file) match {
          case None => file
          case Some(err) =>
            val deleted = file.delete()
            throw new RuntimeException(s"Schema validation failed for ${file.getAbsolutePath} (deleted=$deleted): $err")
        }
      }
    }
  }

  /** Detect "no data for this date" responses from TWSE/TPEx, used to distinguish
   *  non-trading days from genuine errors. Known patterns:
   *    TWSE CSV weekend   — 2-byte near-empty body
   *    TWSE JSON no-data  — {"stat":"很抱歉...","total":0}
   *    TPEx CSV weekend   — ~1-2KB JSON {"csvName":"BIGD_...","totalCount":0}
   *  We use a size cap + content sniff. "Real" CSVs are always much larger (30KB+). */
  private def isMarketHolidayResponse(file: File): Boolean = {
    if (!file.exists() || file.length() == 0L) return false
    val size = file.length()
    if (size < 50) return true                    // effectively-empty CSV body
    if (size > 4096) return false                 // real data
    val buf = new Array[Byte](math.min(1024, size).toInt)
    val in = new FileInputStream(file)
    try in.read(buf) finally in.close()
    val head = new String(buf)
    head.contains("很抱歉") || head.contains("沒有符合") ||
      head.contains("\"total\":0") || head.contains("\"totalCount\":0") ||
      head.startsWith("{\"csvName\"")  // TPEx JSON wrapper (empty on non-trading days)
  }

  private def isHtmlResponse(file: File): Boolean = {
    if (!file.exists() || file.length() == 0L) return false
    val buf = new Array[Byte](math.min(64, file.length()).toInt)
    val in = new FileInputStream(file)
    try in.read(buf) finally in.close()
    val head = new String(buf).trim.toLowerCase
    head.startsWith("<html") || head.startsWith("<!doctype")
  }

  //https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-2019Q4.zip&filePath=/home/html/nas/ifrs/2019/
}
