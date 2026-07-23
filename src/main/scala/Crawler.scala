import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.StandardCopyOption
import java.net.{URI, URLDecoder}
import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
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
import util.Log
import util.Helpers.SeqExtension

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.io.Path._
import scala.util.Try

/**
 * Press following code in the dev console can easily track url when click button open a new tab
 * [].forEach.call(document.querySelectorAll('a'),function(link){if(link.attributes.target) {link.attributes.target.value = '_self';}});window.open = function(url) {location.href = url;};
 */
class Crawler {
  private val executor = Executors.newSingleThreadExecutor()
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

  def close(): Unit = executor.shutdown()

  def getFinancialAnalysis(year: Int): Future[Seq[File]] = {
    Log.debug(s"Get financial analysis of $year")
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
    Log.debug(s"Get operating revenue of $year-$month")
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
    Log.debug(s"Get balance sheet of $year-Q$quarter")
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
    Log.debug(s"Get income statement of $year-Q$quarter")
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
    Log.debug(s"Get financial statements of $year-Q$quarter-$companyCode")
    FinancialStatementsSetting(year, quarter, companyCode).markets.mapInSeries {
      detail =>
        val file = s"${detail.dir}/${detail.fileName}".toFile
        (file.length match {
          case l if l > 10000 => Future(file.jfile)
          case _ =>
            Log.debug(detail.url)
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
    Log.debug(s"Get ex-right/dividend of $year-$month (MOPS t108sb27)")
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
            // NOTE: pass detail.dir (NOT detail.dir/$year). downloadFile already
            // routes YYYY_*.csv into a year subdir; passing the year here too put
            // files in data/.../2026/2026/ where monthDone never found them →
            // re-downloaded every run (2026-07-16 double-year bug).
            Http.client.url(detail.fileUrl)
              .withMethod("POST")
              .withBody(downloadForm)
              .withRequestTimeout(2.minutes)
              .stream()
              .flatMap(downloadFile(detail.dir, Some(detail.fileName)))
          } else {
            // No filename = the month has no ex-right/dividend events yet
            // (current/future months). Persist a 0-byte sentinel in the same
            // year subdir downloadFile would use, so pullExRightDividend sees
            // it as "done" and stops re-fetching every run.
            val dir = new File(s"${detail.dir}/$year")
            dir.mkdirs()
            val sentinel = new File(dir, detail.fileName)
            new FileOutputStream(sentinel).close()
            Future.successful(sentinel)
          }
        }
      }
    }
  }

  def getCapitalReduction(strDate: LocalDate, endDate: LocalDate): Future[Seq[File]] = {
    Log.debug(s"Get capital reduction from ${strDate.toString} to ${endDate.toString}")
    getFile(CapitalReductionSetting(strDate, endDate))
  }

  def getDailyQuote(date: LocalDate): Future[Seq[File]] = {
    Log.debug(s"Get daily quote of ${date.toString}")
    getFile(DailyQuoteSetting(date), perTradingDay = true)
  }

  def getIndex(date: LocalDate): Future[Seq[File]] = {
    Log.debug(s"Get index of ${date.toString}")
    getFile(IndexSetting(date), perTradingDay = true)
  }

  def getTaifexFuturesDailyYear(year: Int): Future[Seq[File]] = {
    val detail = TaifexFuturesDailySetting().taifex
    val fileName = s"${year}_fut.zip"
    Log.debug(s"Get TAIFEX futures daily annual archive of $year")
    Thread.sleep(1000)
    Helpers.retry {
      Http.client.url(detail.url)
        .withMethod("POST")
        .withBody(Map("down_type" -> "2", "his_year" -> year.toString))
        .withRequestTimeout(5.minutes)
        .stream()
        .flatMap(downloadFile(detail.dir, Some(fileName)))
        .map(zipFile => unzipSingleTaifexCsv(zipFile, detail.dir, delete = true))
        .map(Seq(_))
    }
  }

  def getTaifexFuturesDailyMonth(year: Int, month: Int): Future[Seq[File]] = {
    val detail = TaifexFuturesDailySetting().taifex
    val start = LocalDate.of(year, month, 1)
    val monthEnd = start.plusMonths(1).minusDays(1)
    val end = if (monthEnd.isAfter(LocalDate.now())) LocalDate.now() else monthEnd
    val fileName = s"${year}_${month}.csv"
    Log.debug(s"Get TAIFEX futures daily month $year-$month")
    Thread.sleep(1000)
    Helpers.retry {
      Http.client.url(detail.url)
        .withMethod("POST")
        .withBody(Map(
          "down_type" -> "1",
          "queryStartDate" -> start.format(java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd")),
          "queryEndDate" -> end.format(java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd")),
          "commodity_id" -> "all",
          "commodity_id2" -> ""
        ))
        .withRequestTimeout(3.minutes)
        .stream()
        .flatMap(downloadFile(detail.dir, Some(fileName), detail.validate))
        .map(Seq(_))
    }
  }

  def getTaifexFuturesInstitutionalMonth(year: Int, month: Int): Future[Seq[File]] = {
    val detail = TaifexFuturesInstitutionalSetting().taifex
    val rawStart = LocalDate.of(year, month, 1)
    val freeStart = LocalDate.now().minusYears(3)
    val start = if (rawStart.isBefore(freeStart)) freeStart else rawStart
    val monthEnd = start.plusMonths(1).minusDays(1)
    val end = if (monthEnd.isAfter(LocalDate.now())) LocalDate.now() else monthEnd
    val fileName = s"${year}_${month}.csv"
    Log.debug(s"Get TAIFEX futures institutional month $year-$month")
    Thread.sleep(1000)
    Helpers.retry {
      Http.client.url(detail.url)
        .withMethod("POST")
        .withBody(Map(
          "queryStartDate" -> start.format(java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd")),
          "queryEndDate" -> end.format(java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd")),
          "commodityId" -> ""
        ))
        .withRequestTimeout(3.minutes)
        .stream()
        .flatMap(downloadFile(detail.dir, Some(fileName), detail.validate))
        .map(Seq(_))
    }
  }

  def getTaifexFuturesFinalSettlementYear(year: Int): Future[Seq[File]] = {
    val detail = TaifexFuturesFinalSettlementSetting().taifex
    val fileName = s"$year.html"
    val file = new File(s"${detail.dir}/$year/$fileName")
    val commodityIds = Seq("1", "3", "4")
    val params = Seq(
      "start_year" -> year.toString,
      "start_month" -> "01",
      "end_year" -> year.toString,
      "end_month" -> "12"
    ) ++ commodityIds.map("commodityIds" -> _)
    Log.debug(s"Get TAIFEX futures final settlement year $year")
    Thread.sleep(1000)
    Helpers.retry {
      Http.client.url(detail.url)
        .withQueryStringParameters(params: _*)
        .withRequestTimeout(2.minutes)
        .get()
        .map { res =>
          val body = res.body
          if (!body.contains("最後") || !body.contains("TX/MTX/TMF")) {
            throw new RuntimeException(s"Unexpected TAIFEX final-settlement response for $year")
          }
          file.getParentFile.mkdirs()
          java.nio.file.Files.write(file.toPath, body.getBytes(StandardCharsets.UTF_8))
          Seq(file)
      }
    }
  }

  def getTaifexIntradayRawFiles(source: TaifexIntradayRawSource): Future[Seq[File]] = {
    Log.debug(s"Get TAIFEX intraday raw files: ${source.description}")
    Helpers.retry {
      Http.client.url(source.page)
        .withHttpHeaders(browserHeaders: _*)
        .withRequestTimeout(2.minutes)
        .get()
    }.flatMap { res =>
      if (res.status >= 400) {
        Future.failed(new RuntimeException(s"TAIFEX intraday page failed: ${source.page} status=${res.status}"))
      } else {
        val urls = extractTaifexArchiveUrls(res.body)
        if (urls.isEmpty) {
          Future.failed(new RuntimeException(s"No downloadable TAIFEX archive links found on ${source.page}"))
        } else {
          val force = sys.env.get("QL_TAIFEX_INTRADAY_FORCE").exists(_.equalsIgnoreCase("true"))
          val allowToday = sys.env.get("QL_TAIFEX_INTRADAY_ALLOW_TODAY").exists(_.equalsIgnoreCase("true"))
          val refreshDays = Try(sys.env.getOrElse("QL_TAIFEX_INTRADAY_REFRESH_DAYS", "2").toInt).getOrElse(2).max(0)
          val taipei = ZoneId.of("Asia/Taipei")
          val today = LocalDate.now(taipei)
          val safeAfter = Try(LocalTime.parse(sys.env.getOrElse("QL_TAIFEX_INTRADAY_SAFE_AFTER", "16:00:00"))).getOrElse(LocalTime.of(16, 0))
          val latestSafeDate =
            sys.env.get("QL_TAIFEX_INTRADAY_MAX_DATE").flatMap(s => Try(LocalDate.parse(s)).toOption).getOrElse {
              if (allowToday || !LocalTime.now(taipei).isBefore(safeAfter)) today else today.minusDays(1)
            }
          val refreshFrom = latestSafeDate.minusDays(math.max(refreshDays - 1, 0).toLong)
          val eligibleUrls = urls.filter { url =>
            taifexArchiveDate(url).forall(!_.isAfter(latestSafeDate))
          }
          val skippedUnsafe = urls.size - eligibleUrls.size
          if (skippedUnsafe > 0) {
            println(s"  skip $skippedUnsafe current/future partial intraday archives; latest_safe_date=$latestSafeDate safe_after=$safeAfter Asia/Taipei")
          }
          eligibleUrls.mapInSeries { url =>
            val outFile = taifexIntradayOutputFile(source, url)
            val fileDate = taifexArchiveDate(url)
            val shouldRefreshRecent = fileDate.exists(!_.isBefore(refreshFrom))
            if (outFile.exists() && outFile.length() > 0 && !force && !shouldRefreshRecent) {
              Future.successful(TaifexRawManifestRecord(source.key, url, outFile, fileDate, downloaded = false))
            } else {
              Thread.sleep(300)
              Helpers.retry(downloadRawUrlToFile(url, outFile), delay = 500.millis, retries = 3)
                .map(file => TaifexRawManifestRecord(source.key, url, file, fileDate, downloaded = true))
            }
          }.map { records =>
            writeTaifexIntradayManifest(source, records)
            val downloaded = records.count(_.downloaded)
            val skipped = records.size - downloaded
            val dates = records.flatMap(_.date)
            val range = if (dates.nonEmpty) s"${dates.min} to ${dates.max}" else "unknown"
            val bytes = records.map(_.file.length()).sum
            println(s"  ${source.key}: files=${records.size}, downloaded=$downloaded, skipped=$skipped, date_range=$range, bytes=$bytes")
            records.map(_.file)
          }
        }
      }
    }
  }

  def getMarginTransactions(date: LocalDate): Future[Seq[File]] = {
    Log.debug(s"Get margin transactions of ${date.toString}")
    getFile(MarginTransactionsSetting(date), perTradingDay = true)
  }

  def getDailyTradingDetails(date: LocalDate): Future[Seq[File]] = {
    Log.debug(s"Get daily trading details of ${date.toString}")
    getFile(DailyTradingDetailsSetting(date), perTradingDay = true)
  }

  def getStockPER_PBR_DividendYield(date: LocalDate): Future[Seq[File]] = {
    Log.debug(s"Get stock PER, PBR and dividend yield of ${date.toString}")
    getFile(StockPER_PBR_DividendYieldSetting(date), perTradingDay = true)
  }

  def getETF: Future[Seq[File]] = {
    Log.debug(s"Get ETF")
    getFile(ETFSetting())
  }

  def getTdccShareholding(): Future[Seq[File]] = {
    Log.debug(s"Get TDCC shareholding (current week snapshot)")
    getFile(TdccShareholdingSetting())
  }

  def getSblBorrowing(date: LocalDate): Future[Seq[File]] = {
    Log.debug(s"Get SBL borrowing of ${date.toString}")
    getFile(SblBorrowingSetting(date), perTradingDay = true)
  }

  def getForeignHoldingRatio(date: LocalDate): Future[Seq[File]] = {
    Log.debug(s"Get foreign holding ratio of ${date.toString}")
    getFile(ForeignHoldingRatioSetting(date), perTradingDay = true)
  }

  def getTreasuryStockBuyback(year: Int, month: Int): Future[Seq[File]] = {
    Log.debug(s"Get treasury stock buyback of $year-$month (MOPS t35sc09)")
    // t35sc09 is friendly to bare POST (verified 2026-04-29).
    postMopsDirect(TreasuryStockBuybackSetting(year, month).markets, parentPage = None)
  }

  /**
   * 內部人持股轉讓事前申報日報 (t56sb12_q1 / q2).
   *
   * Two-step ajax chain (verified Playwright network capture 2026-04-29):
   *   Step 1: POST /mops/web/ajax_t56sb12_q1 (or q2) — emits intermediate auto-form
   *           Body: encodeURIComponent=1&step=0&firstin=1&off=1&year+month+day
   *   Step 2: POST /mops/web/ajax_t56sb12 — returns actual data table
   *           Body: encodeURIComponent=1&run=&step=2&year+month+day&report=SY|OY&firstin=true
   *
   * report=SY = 上市 (q1), OY = 上櫃 (q2). Detail carries the right code.
   *
   * Saves step 2 HTML to `{detail.dir}/{year}/YYYY_M_D.html`. Empty body
   * (< 1KB) = no-data day → truncate to 0-byte sentinel (skip on next run).
   */
  def getInsiderHolding(date: LocalDate): Future[Seq[File]] = {
    Log.debug(s"Get insider holding of $date (MOPS t56sb12_q1/q2)")
    Future.sequence(InsiderHoldingSetting(date).markets.map { detail =>
      Thread.sleep(20000)  // MOPS rate-limit between markets
      Log.debug(s"  step1 POST ${detail.page} → step2 ajax_t56sb12 report=${detail.reportCode}")
      Helpers.retry {
        // Step 1 — emits intermediate form (we don't parse it; we already know
        // step 2 form data because reportCode is fixed per endpoint).
        Http.client.url(detail.page)
          .withRequestTimeout(2.minutes)
          .post(detail.formData)
          .flatMap { _ =>
            // Step 2 — actual data fetch, stream Big5 bytes to disk.
            Http.client.url(detail.dataUrl)
              .withMethod("POST")
              .withBody(detail.step2FormData)
              .withRequestTimeout(2.minutes)
              .stream()
              .flatMap { res =>
                val outDir = s"${detail.dir}/${date.getYear}"
                new File(outDir).mkdirs()
                val outFile = new File(s"$outDir/${detail.fileName}")
                val outputStream = java.nio.file.Files.newOutputStream(outFile.toPath)
                val sink = Sink.foreach[ByteString] { bytes => outputStream.write(bytes.toArray) }
                res.bodyAsSource.runWith(sink).andThen { case _ => outputStream.close() }.map { _ =>
                  Log.debug(s"    saved ${outFile.getAbsolutePath} (${outFile.length()} bytes)")
                  val sameDay = isBeforeCompleteTime(date)
                  if (isHtmlResponse(outFile) && outFile.length() < 2000) {
                    // could be MOPS error page; truncate so getDatesOfExistFiles
                    // still treats date as "tried" and moves on. Same-day: delete
                    // instead — MOPS may simply not have published yet.
                    if (sameDay) deferSameDayNoData(outFile)
                    else new FileOutputStream(outFile).close()
                  } else if (outFile.length() > 0 && outFile.length() < 1024) {
                    if (sameDay) deferSameDayNoData(outFile)
                    else new FileOutputStream(outFile).close()
                  }
                  outFile
                }
              }
          }
      }.recover {
        case e =>
          Console.err.println(s"[giveup] insider holding ${detail.market} $date: ${e.getClass.getSimpleName}: ${e.getMessage}")
          val f = new File(s"${detail.dir}/${date.getYear}/${detail.fileName}")
          if (f.exists()) f.delete()
          f
      }
    })
  }

  // Browser-like headers to bypass MOPS anti-scraping. Without these the
  // server completes TLS handshake then closes the connection ("Empty reply").
  private val browserHeaders: Seq[(String, String)] = Seq(
    "User-Agent" -> "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept" -> "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language" -> "zh-TW,zh;q=0.9,en;q=0.8",
    "Cache-Control" -> "no-cache"
  )

  /**
   * MOPS single-step POST helper: bare POST to ajax_* endpoint, stream raw
   * Big5-HKSCS HTML body to `{detail.dir}/{year}/{detail.fileName}` byte-for-byte.
   * Used by buyback. (Insider / CB explored but rejected — see FUTURE_CRAWLERS_SPEC.md)
   *
   * Streaming is required (vs `.post().body`) because `body: String` would force
   * play-ws to decode using its default charset (UTF-8) and mangle Big5 bytes.
   *
   * Empty bodies (< 1KB) are normalized to 0-byte sentinel files — same convention
   * as `isMarketHolidayResponse` so `getDatesOfExistFiles` counts the date as
   * "tried" and pull won't loop on no-data months forever.
   */
  /**
   * MOPS single-step POST with optional session priming.
   *
   * If `parentPage` is provided, performs GET parent → extract Set-Cookie
   * (JSESSIONID) → POST ajax with cookies + browser headers + Referer.
   * If None, bare POST (works for t35sc09 buyback).
   *
   * For session-required endpoints (t56sb01 / t24sb03), MOPS rejects
   * connections without browser-like UA + cookies (closes TLS without HTTP
   * response). The session priming + UA combo is the minimum to bypass it
   * without resorting to Playwright.
   */
  private def postMopsDirect(markets: Seq[setting.MopsDirectDetail],
                             parentPage: Option[String]): Future[Seq[File]] = {
    Future.sequence(markets.map { detail =>
      val outYear = detail.minguoYear + 1911
      val typek = detail.formData.getOrElse("TYPEK", "?")
      Thread.sleep(20000)  // MOPS rate-limit
      Log.debug(s"  POST ${detail.page} TYPEK=$typek minguoYear=${detail.minguoYear} → outYear=$outYear")

      val cookieFuture: Future[Seq[(String, String)]] = parentPage match {
        case Some(parent) =>
          Http.client.url(parent)
            .withHttpHeaders(browserHeaders: _*)
            .withRequestTimeout(30.seconds)
            .get()
            .map { res =>
              res.cookies.map(c => "Cookie" -> s"${c.name}=${c.value}").toSeq
            }
            .recover { case _ => Seq.empty }
        case None => Future.successful(Seq.empty)
      }

      cookieFuture.flatMap { sessionCookies =>
        Helpers.retry {
          val refererHeader: Seq[(String, String)] =
            parentPage.map(p => Seq("Referer" -> p, "Origin" -> "https://mopsov.twse.com.tw")).getOrElse(Seq.empty)
          val headers = browserHeaders ++ refererHeader ++ sessionCookies
          Http.client.url(detail.page)
            .withHttpHeaders(headers: _*)
            .withMethod("POST")
            .withBody(detail.formData)
            .withRequestTimeout(2.minutes)
            .stream()
            .flatMap { res =>
              val outDir = s"${detail.dir}/$outYear"
              new File(outDir).mkdirs()
              val outFile = new File(s"$outDir/${detail.fileName}")
              val outputStream = java.nio.file.Files.newOutputStream(outFile.toPath)
              val sink = Sink.foreach[ByteString] { bytes => outputStream.write(bytes.toArray) }
              res.bodyAsSource.runWith(sink).andThen { case _ => outputStream.close() }.map { _ =>
                Log.debug(s"    saved ${outFile.getAbsolutePath} (${outFile.length()} bytes)")
                if (isHtmlResponse(outFile) && outFile.length() < 5000) {
                  outFile.delete()
                  throw new RuntimeException(s"HTML error response for ${outFile.getAbsolutePath}")
                }
                if (outFile.length() > 0 && outFile.length() < 1024) {
                  new FileOutputStream(outFile).close()  // sentinel for no-data month
                }
                outFile
              }
            }
        }.recover {
          case e =>
            Console.err.println(s"[giveup] MOPS POST will retry next run: ${detail.page} minguoYear=${detail.minguoYear}: ${e.getClass.getSimpleName}: ${e.getMessage}")
            val f = new File(s"${detail.dir}/$outYear/${detail.fileName}")
            if (f.exists()) f.delete()
            f
        }
      }
    })
  }

  private def getFile(setting: Setting, perTradingDay: Boolean = false): Future[Seq[File]] = {
    Thread.sleep(20000)
    Future.sequence(setting.markets.map {
      detail =>
        Log.debug(detail.url)
        Helpers.retry {
          Http.client.url(detail.url)
            .withMethod("GET")
            .withFollowRedirects(false)
            .stream()
            .flatMap(downloadFile(detail.dir, Some(detail.fileName), detail.validate, perTradingDay))
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
                           validate: File => DownloadValidation = _ => DownloadValidation.Valid,
                           perTradingDay: Boolean = false): StandaloneWSResponse => Future[File] = (res: StandaloneWSResponse) => {
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
        // A near-empty / no-data response looks the same whether the exchange was
        // closed or the server just hiccupped. Writing a 0-byte "holiday" sentinel
        // is only safe with POSITIVE evidence the date was a non-trading day (see
        // sentinelOrDefer); otherwise this permanently drops a real trading day's
        // TWSE margin/PER/T86/index rows because getDatesOfExistFiles counts the
        // sentinel as "done" and never retries (audit D-crawler-scala: 27 lost days).
        sentinelOrDefer(file, fn, perTradingDay)
      } else {
        validate(file) match {
          case DownloadValidation.Valid => file
          case DownloadValidation.NoData(reason) =>
            Console.err.println(s"[nodata] ${file.getAbsolutePath}: $reason")
            sentinelOrDefer(file, fn, perTradingDay)
          case DownloadValidation.Invalid(err) =>
            val deleted = file.delete()
            throw new RuntimeException(s"Schema validation failed for ${file.getAbsolutePath} (deleted=$deleted): $err")
        }
      }
    }
  }

  private val dailyCsvName = """^(\d{4})_(\d{1,2})_(\d{1,2})\.csv$""".r

  /** D 日的資料自 D+1 00:30 起視為齊備——在那之前收到的「無資料」一律不可
   *  當成休市(可能只是還沒發布/暫時性故障),必須刪檔重抓;之後收到的乾淨
   *  「無資料」才是交易所親口說的「該日無交易」,寫 0-byte sentinel。
   *
   *  這條規則是 sentinel 的唯一閘門,依據見 docs/data_ops/twse_publish_times.md:
   *  融資融券官方只保證「次一營業日開市前公告」、借券 22:30 才是最終值。
   *  sentinel 同時是我們的休市日曆(颱風假無法從星期幾推得——2026-07-10 即是),
   *  故寧可晚一點寫、也不能寫錯。 */
  private val dataCompleteAfter = LocalTime.of(0, 30)

  private def isSentinelUnsafe(fileName: String): Boolean = fileName match {
    case dailyCsvName(y, m, d) =>
      Try(LocalDate.of(y.toInt, m.toInt, d.toInt)).toOption.exists(isBeforeCompleteTime)
    case _ => false
  }

  private def isBeforeCompleteTime(fileDate: LocalDate): Boolean = {
    val taipei = ZoneId.of("Asia/Taipei")
    ZonedDateTime.now(taipei).isBefore(fileDate.plusDays(1).atTime(dataCompleteAfter).atZone(taipei))
  }

  private def deferSameDayNoData(file: File): Unit = {
    file.delete()
    println(s"[deferred] ${file.getName}: no data yet and the date is not past its completeness time (D+1 00:30) — deleting instead of sentinelling; a later run will retry")
  }

  /** Decide what to do with a "no-data" response: write a 0-byte market-holiday
   *  sentinel, or delete + defer (retry on a later run).
   *
   *  Policy (docs/data_ops/twse_publish_times.md, CLAUDE.md "sentinel 規則"): a
   *  sentinel is a POSITIVE claim "the exchange was closed that day" and doubles
   *  as our holiday calendar — so it must not be written without positive evidence.
   *  When uncertain we defer (寧晚勿錯), because a false sentinel is permanent
   *  (getDatesOfExistFiles treats it as "done" and never retries).
   *
   *  For per-trading-day market sources the evidence is daily_quote, our trading-
   *  day ground truth. pullAllData fetches daily_quote FIRST, so it is already on
   *  disk when every derived daily source (margin / PER / T86 / index) is fetched:
   *    - daily_quote has real data (> 1 KB)  → real trading day  → DEFER (transient).
   *    - daily_quote is a ≤ 1 KB sentinel    → confirmed holiday → write sentinel.
   *    - daily_quote file absent             → calendar unknown  → DEFER.
   *  Non-per-day sources (capital_reduction is a date RANGE whose empty result is
   *  legitimate; etf / tdcc are snapshots) pass perTradingDay=false and keep the
   *  plain time-gated behaviour. Before D+1 00:30 (isSentinelUnsafe) we always defer. */
  private def sentinelOrDefer(file: File, fn: String, perTradingDay: Boolean): File = {
    val confirmedHoliday =
      !perTradingDay || dailyFileDate(fn).exists(d => twseTradingDayEvidence(d).contains(false))
    if (isSentinelUnsafe(fn) || !confirmedHoliday) deferSameDayNoData(file)
    else new FileOutputStream(file).close() // genuine holiday → sentinel so pull stops retrying
    file
  }

  private lazy val dailyQuoteTwseDir: String = DailyQuoteSetting().twse.dir

  /** Parse a daily YYYY_M_D.csv filename into its date (None for non-daily names). */
  private def dailyFileDate(fileName: String): Option[LocalDate] = fileName match {
    case dailyCsvName(y, m, d) => Try(LocalDate.of(y.toInt, m.toInt, d.toInt)).toOption
    case _ => None
  }

  /** Trading-day ground truth from the LOCAL daily_quote TWSE archive (DB-free —
   *  the crawl layer must not read PostgreSQL). Taiwan TWSE + TPEx share one
   *  trading calendar, so TWSE daily_quote answers for both markets.
   *    Some(true)  = real data (> 1 KB)  → trading day
   *    Some(false) = ≤ 1 KB sentinel     → non-trading day
   *    None        = file absent         → unknown
   *  The > 1 KB threshold matches Task.loadLocalTwseDailyQuoteTradingDays; a
   *  wrong-date daily_quote body can never persist here because DailyQuoteSetting's
   *  validateCSVHeaderDate deletes header-date mismatches at download time. */
  private def twseTradingDayEvidence(date: LocalDate): Option[Boolean] = {
    val f = new File(s"$dailyQuoteTwseDir/${date.getYear}/${date.getYear}_${date.getMonthValue}_${date.getDayOfMonth}.csv")
    if (!f.exists()) None else Some(f.length() > 1024)
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
    val big5Head = Try(new String(buf, "Big5-HKSCS")).getOrElse(head)
    head.contains("很抱歉") || head.contains("沒有符合") ||
      head.contains("\"total\":0") || head.contains("\"totalCount\":0") ||
      head.startsWith("{\"csvName\"") || // TPEx JSON wrapper (empty on non-trading days)
      (size <= 512 && (head.contains("Data Date:") || big5Head.contains("價格指數")))
  }

  private def isHtmlResponse(file: File): Boolean = {
    if (!file.exists() || file.length() == 0L) return false
    val buf = new Array[Byte](math.min(64, file.length()).toInt)
    val in = new FileInputStream(file)
    try in.read(buf) finally in.close()
    val head = new String(buf).trim.toLowerCase
    head.startsWith("<html") || head.startsWith("<!doctype")
  }

  private case class TaifexRawManifestRecord(
    sourceKey: String,
    url: String,
    file: File,
    date: Option[LocalDate],
    downloaded: Boolean
  )

  private val WindowOpenUrl = """window\.open\(['"]([^'"]+)['"]\)""".r
  private val DailyArchiveDate = """.*Daily_(\d{4})_(\d{2})_(\d{2}).*\.zip$""".r
  private val ManifestTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  private def extractTaifexArchiveUrls(html: String): Seq[String] =
    WindowOpenUrl.findAllMatchIn(html)
      .map(_.group(1).replace("&amp;", "&"))
      .filter { url =>
        val lower = url.toLowerCase
        lower.startsWith("https://www.taifex.com.tw/file/taifex/") &&
          lower.endsWith(".zip") &&
          !lower.contains("/help/")
      }
      .toSeq
      .distinct
      .sorted

  private def taifexArchiveDate(url: String): Option[LocalDate] =
    url match {
      case DailyArchiveDate(y, m, d) => Try(LocalDate.of(y.toInt, m.toInt, d.toInt)).toOption
      case _ => None
    }

  private def taifexIntradayOutputFile(source: TaifexIntradayRawSource, url: String): File = {
    val path = URI.create(url).getPath.split("/").filter(_.nonEmpty).toSeq
    val parent = path.dropRight(1).lastOption.getOrElse("unknown")
    val fileName = URLDecoder.decode(path.lastOption.getOrElse("download.zip"), StandardCharsets.UTF_8.name())
    new File(s"${source.dir}/$parent/$fileName")
  }

  private def downloadRawUrlToFile(url: String, outFile: File): Future[File] = {
    Log.debug(s"  download $url")
    outFile.getParentFile.mkdirs()
    val partFile = new File(outFile.getAbsolutePath + ".part")
    if (partFile.exists()) partFile.delete()

    Http.client.url(url)
      .withHttpHeaders(browserHeaders: _*)
      .withRequestTimeout(10.minutes)
      .stream()
      .flatMap { res =>
        if (res.status >= 400) {
          Future.failed(new RuntimeException(s"HTTP ${res.status} for $url"))
        } else {
          val outputStream = java.nio.file.Files.newOutputStream(partFile.toPath)
          val sink = Sink.foreach[ByteString] { bytes => outputStream.write(bytes.toArray) }
          res.bodyAsSource
            .runWith(sink)
            .andThen { case _ => outputStream.close() }
            .map { _ =>
              if (partFile.length() <= 0) {
                partFile.delete()
                throw new RuntimeException(s"Empty TAIFEX archive: $url")
              }
              if (isHtmlResponse(partFile)) {
                partFile.delete()
                throw new RuntimeException(s"HTML response while downloading TAIFEX archive: $url")
              }
              java.nio.file.Files.move(
                partFile.toPath,
                outFile.toPath,
                StandardCopyOption.REPLACE_EXISTING
              )
              outFile
            }
        }
      }
  }

  private def writeTaifexIntradayManifest(source: TaifexIntradayRawSource, records: Seq[TaifexRawManifestRecord]): Unit = {
    val manifest = new File(s"${source.dir}/manifest.csv")
    manifest.getParentFile.mkdirs()
    val now = java.time.LocalDateTime.now().format(ManifestTimeFormatter)
    val header = "fetched_at,source_key,date,downloaded,bytes,local_path,url"
    val lines = records.sortBy(_.file.getPath).map { r =>
      Seq(
        now,
        r.sourceKey,
        r.date.map(_.toString).getOrElse(""),
        r.downloaded.toString,
        r.file.length().toString,
        r.file.getPath,
        r.url
      ).map(csvEscape).mkString(",")
    }
    java.nio.file.Files.write(
      manifest.toPath,
      (header +: lines).mkString("\n").appended('\n').getBytes(StandardCharsets.UTF_8)
    )
  }

  private def csvEscape(value: String): String =
    if (value.iterator.exists(ch => ch == ',' || ch == '"' || ch == '\n' || ch == '\r'))
      "\"" + value.replace("\"", "\"\"") + "\""
    else value

  private def unzipSingleTaifexCsv(zipFile: File, outputDir: String, delete: Boolean): File = {
    val zipInput = new ZipInputStream(new FileInputStream(zipFile))
    try {
      val entry = zipInput.getNextEntry
      if (entry == null || entry.isDirectory) {
        throw new RuntimeException(s"TAIFEX archive has no CSV entry: ${zipFile.getAbsolutePath}")
      }
      val outputFile = new File(s"$outputDir/${entry.getName}")
      outputFile.getParentFile.mkdirs()
      val output = new FileOutputStream(outputFile)
      try {
        val buffer = new Array[Byte](8192)
        var read = zipInput.read(buffer)
        while (read != -1) {
          output.write(buffer, 0, read)
          read = zipInput.read(buffer)
        }
      } finally {
        output.close()
        zipInput.closeEntry()
      }
      outputFile
    } finally {
      zipInput.close()
      if (delete) zipFile.delete()
    }
  }

  //https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-2019Q4.zip&filePath=/home/html/nas/ifrs/2019/
}
