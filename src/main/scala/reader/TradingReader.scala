package reader

import java.time.LocalDate
import java.time.chrono.MinguoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import com.github.tototoshi.csv._
import db.table._
import me.tongfei.progressbar.ProgressBar
import play.api.libs.json._
import slick.collection.heterogeneous.HNil
import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
import setting._
import slick.lifted.TableQuery
// `_root_` qualifier needed: `import play.api.libs.json._` leaks a `util` package
// into scope, which would otherwise shadow the project's top-level `util.*`.
import _root_.util.QuantlibCSVReader

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

class TradingReader extends Reader {
  // Detect JSON (TPEx) vs CSV (TWSE) by first non-whitespace byte.
  private def isJsonFile(file: java.io.File): Boolean = {
    if (file.length() == 0L) return false
    val in = new java.io.FileInputStream(file)
    try {
      var b = in.read()
      while (b == ' '.toInt || b == '\t'.toInt || b == '\n'.toInt || b == '\r'.toInt) b = in.read()
      b == '{'.toInt
    } finally in.close()
  }

  // Normalize "1,234" / "87.8%" / " 100 " → "1234" / "87.8" / "100".
  private def cleanCell(s: String): String =
    s.replace(",", "").replace("%", "").replace(" ", "").trim
  def readDailyQuote(): Unit = {
    val dailyQuote = TableQuery[DailyQuote]
    val query = dailyQuote.map(d => (d.market, d.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }

    val files = DailyQuoteSetting().getMarketFilesFromDirectory.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
    val pb = new ProgressBar("Read daily quote -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read daily quote of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)

        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val data = marketFile.market match {
          case "twse" =>
            val rows = reader.all().dropWhile(_.head != "證券代號")
            if (rows.isEmpty) Seq.empty else
              rows.tail.filter(_.size >= 17).map(_.map(_.replace(" ", "").replace(",", ""))).map {
                values =>
                  val splitValues = values.splitAt(2)
                  val transferValues: Seq[Option[Double]] = splitValues._2.init.map {
                    case v if v == "--" => None
                    case v if v.isEmpty || v == " " || v == "X" => Some(0)
                    case v if v == "+" => Some(1)
                    case v if v == "-" => Some(-1)
                    case value => Some(value.toDouble)
                  }
                  val companyCode = values.head
                  val direction = transferValues(7).get
                  val change = direction match {
                    case -1 => -transferValues(8).get
                    case _ => transferValues(8).get
                  }
                  (marketFile.market, date, companyCode, values(1), transferValues.head.get.toLong, transferValues(1).get.toInt, transferValues(2).get.toLong, transferValues(3), transferValues(4), transferValues(5), transferValues(6), change, transferValues(9), transferValues(10).map(_.toInt), transferValues(11), transferValues(12).map(_.toInt), transferValues(13))
              }
          case "tpex" =>
            val rows = reader.all().dropWhile(_.head != "代號")
            if (rows.isEmpty) Seq.empty else
              rows.init.tail.filter(_.size >= 15).map(_.map(_.replace(" ", "").replace(",", ""))).map {
                values =>
                  val splitValues = values.splitAt(2)
                  val transferValues: Seq[Option[Double]] = splitValues._2.init.map {
                    case v if v == "---" => None
                    case v if v == "----" => None
                    case v if v == "除權息" || v == "除權" || v == "除息" => Some(0)
                    case value => Some(value.toDouble)
                  }
                  val companyCode = values.head
                  val noneInt: Option[Int] = None
                  val noneDouble: Option[Double] = None
                  values.size match {
                    case 15 => (marketFile.market, date, companyCode, values(1), transferValues(5).get.toLong, transferValues(7).get.toInt, transferValues(6).get.toLong, transferValues(2), transferValues(3), transferValues(4), transferValues.head, transferValues(1).getOrElse(0D), transferValues(8), noneInt, transferValues(9), noneInt, noneDouble)
                    case _ => (marketFile.market, date, companyCode, values(1), transferValues(5).get.toLong, transferValues(7).get.toInt, transferValues(6).get.toLong, transferValues(2), transferValues(3), transferValues(4), transferValues.head, transferValues(1).getOrElse(0D), transferValues(8), transferValues(9).map(_.toInt), transferValues(10), transferValues(11).map(_.toInt), noneDouble)
                  }
              }
        }

        val dbIO = dailyQuote.map(d => (d.market, d.date, d.companyCode, d.companyName, d.tradeVolume, d.transaction, d.tradeValue, d.openingPrice, d.highestPrice, d.lowestPrice, d.closingPrice, d.change, d.lastBestBidPrice, d.lastBestBidVolume, d.lastBestAskPrice, d.lastBestAskVolume, d.priceEarningRatio)) ++= data
        dbRun(dbIO)
        reader.close()
        pb.step()
    }
    pb.close()
  }

  def readDailyTradingDetails(): Unit = {
    val dailyTradingDetails = TableQuery[DailyTradingDetails]
    val query = dailyTradingDetails.map(d => (d.market, d.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }

    val files = DailyTradingDetailsSetting().getMarketFilesFromDirectory.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
    val pb = new ProgressBar("Read daily trading details -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read daily trading details of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)

        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val data = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size >= 13 && row.head != "證券代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val splitValues = values.splitAt(2)
                val transferValues = splitValues._2.init.map(value => Try(value.toInt).getOrElse(0))
                val companyCode = values.head
                val noneInt: Option[Int] = None
                values.size match {
                  case 13 =>
                    marketFile.market :: date :: companyCode :: values(1) :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues(6) :: transferValues(7) :: transferValues(8) :: transferValues(9) :: HNil
                  case 17 =>
                    marketFile.market :: date :: companyCode :: values(1) ::
                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                      Option(transferValues(7)) :: Option(transferValues(8)) :: Option(transferValues(9)) :: Option(transferValues(10)) :: Option(transferValues(11)) :: Option(transferValues(12)) ::
                      transferValues(7) + transferValues(10) :: transferValues(8) + transferValues(11) :: transferValues(6) :: transferValues(13) :: HNil
                  case _ =>
                    marketFile.market :: date :: companyCode :: values(1) ::
                      Option(transferValues.head) :: Option(transferValues(1)) :: Option(transferValues(2)) :: Option(transferValues(3)) :: Option(transferValues(4)) :: Option(transferValues(5)) ::
                      transferValues.head + transferValues(3) :: transferValues(1) + transferValues(4) :: transferValues(2) + transferValues(5) :: transferValues(6) :: transferValues(7) :: transferValues(8) ::
                      Option(transferValues(10)) :: Option(transferValues(11)) :: Option(transferValues(12)) :: Option(transferValues(13)) :: Option(transferValues(14)) :: Option(transferValues(15)) ::
                      transferValues(10) + transferValues(13) :: transferValues(11) + transferValues(14) :: transferValues(9) :: transferValues(16) :: HNil
                }
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size >= 12 && row.head != "代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val splitValues = values.splitAt(2)
                val transferValues = splitValues._2.map(value => Try(value.toInt).getOrElse(0))
                val companyCode = values.head
                val noneInt: Option[Int] = None
                values.size match {
                  case 12 =>
                    marketFile.market :: date :: companyCode :: values(1) ::
                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues(6) :: transferValues(7) :: transferValues(6) :: transferValues(9) :: HNil
                  case 16 =>
                    marketFile.market :: date :: companyCode :: values(1) ::
                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                      Option(transferValues(7)) :: Option(transferValues(8)) :: Option(transferValues(9)) :: Option(transferValues(10)) :: Option(transferValues(11)) :: Option(transferValues(12)) ::
                      transferValues(7) + transferValues(10) :: transferValues(8) + transferValues(11) :: transferValues(6) :: transferValues(13) :: HNil
                  case _ =>
                    marketFile.market :: date :: companyCode :: values(1) ::
                      Option(transferValues.head) :: Option(transferValues(1)) :: Option(transferValues(2)) :: Option(transferValues(3)) :: Option(transferValues(4)) :: Option(transferValues(5)) ::
                      transferValues(6) :: transferValues(7) :: transferValues(8) :: transferValues(9) :: transferValues(10) :: transferValues(11) ::
                      Option(transferValues(12)) :: Option(transferValues(13)) :: Option(transferValues(14)) :: Option(transferValues(15)) :: Option(transferValues(16)) :: Option(transferValues(17)) ::
                      transferValues(18) :: transferValues(19) :: transferValues(20) :: transferValues(21) :: HNil
                }
            }
        }

        val dbIO = dailyTradingDetails.map(d => d.market :: d.date :: d.companyCode :: d.companyName :: d.foreignInvestorsExcludeDealersTotalBuy :: d.foreignInvestorsExcludeDealersTotalSell :: d.foreignInvestorsExcludeDealersDifference :: d.foreignDealersTotalBuy :: d.foreignDealersTotalSell :: d.foreignDealersDifference :: d.foreignInvestorsTotalBuy :: d.foreignInvestorsTotalSell :: d.foreignInvestorsDifference :: d.securitiesInvestmentTrustCompaniesTotalBuy :: d.securitiesInvestmentTrustCompaniesTotalSell :: d.securitiesInvestmentTrustCompaniesDifference :: d.dealersProprietaryTotalBuy :: d.dealersProprietaryTotalSell :: d.dealersProprietaryDifference :: d.dealersHedgeTotalBuy :: d.dealersHedgeTotalSell :: d.dealersHedgeDifference :: d.dealersTotalBuy :: d.dealersTotalSell :: d.dealersDifference :: d.totalDifference :: HNil) ++= data
        dbRun(dbIO)
        reader.close()
        pb.step()
    }
    pb.close()
  }

  def readCapitalReduction(): Unit = {
    val capitalReduction = TableQuery[CapitalReduction]
    val query = capitalReduction.map(c => (c.market, c.date, c.companyCode)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf)

    val files = CapitalReductionSetting().getMarketFilesFromDirectory.par
    val pb = new ProgressBar("Read capital reduction -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read capital reduction of ${marketFile.market}-${marketFile.file.name}")
        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val data = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size == 12 && row.head != "恢復買賣日期").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val datePattern = """(\d+)/(\d+)/(\d+)""".r
                val datePattern(y, m, d) = values.head
                val year = y.toInt + 1911
                val month = m.toInt
                val day = d.toInt
                val date = LocalDate.of(year, month, day)
                val companyCode = values(1)
                (marketFile.market, date, companyCode, values(2), values(3).toDouble, values(4).toDouble, values(5).toDouble, values(6).toDouble, values(7).toDouble,
                  if (values(8) == "--") None else values(8).toDoubleOption, values(9))
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size == 10 && !row.head.trim.startsWith("恢復買賣日期") && row.head.matches("\\d{7}")).map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val dateFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
                  .parseLenient
                  .appendPattern("yyyMMdd")
                  .toFormatter
                  .withChronology(MinguoChronology.INSTANCE)
                val date = LocalDate.parse(values.head, dateFormatter)
                val companyCode = values(1)
                (marketFile.market, date, companyCode, values(2), values(3).toDouble, values(4).toDouble, values(5).toDouble, values(6).toDouble, values(7).toDouble, values(8).toDoubleOption, values(9))
            }
        }

        val filterData = data.distinctBy(d => (d._1, d._2, d._3)).filterNot(d => dataAlreadyInDB.contains((d._1, d._2, d._3)))
        val dbIO = capitalReduction.map(c => (c.market, c.date, c.companyCode, c.companyName, c.closingPriceOnTheLastTradingDate, c.postReductionReferencePrice, c.limitUp, c.limitDown, c.openingReferencePrice, c.exRightReferencePrice, c.reasonForCapitalReduction)) ++= filterData
        dbRun(dbIO)
        reader.close()
        pb.step()
    }
    pb.close()
  }

  def readExRightDividend(): Unit = {
    val exRightDividend = TableQuery[ExRightDividend]
    val query = exRightDividend.map(e => (e.market, e.date, e.companyCode)).distinct.result
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf)

    val files = ExRightDividendSetting().getMarketFilesFromDirectory.par
    val pb = new ProgressBar("Read ex-right dividend -", files.size)
    files.tasksupport = taskSupport
    val monthlyFilePattern = """(\d+)_(\d+)\.csv""".r
    files.foreach {
      marketFile =>
        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        // New monthly files (YYYY_M.csv) come from MOPS t108sb27 since the legacy
        // TWT49U endpoint silently stopped returning data in 2024-06. Old legacy
        // day-range files (YYYY_M_D.csv) still use the previous parsers below.
        val isMopsMonthly = marketFile.file.name match {
          case monthlyFilePattern(_, _) => true
          case _ => false
        }
        val data: Seq[(String, LocalDate, String, String, Double, Double, Double, String, Double, Double, Double, Double)] =
          if (isMopsMonthly) parseMopsRows(reader.all(), marketFile.market)
          else marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size == 16 && row.head != "資料日期").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val datePattern = """(\d+)年(\d+)月(\d+)日""".r
                val datePattern(y, m, d) = values.head
                val year = y.toInt + 1911
                val month = m.toInt
                val day = d.toInt
                val date = LocalDate.of(year, month, day)
                val companyCode = values(1)
                (marketFile.market, date, companyCode, values(2), values(3).toDouble, values(4).toDouble, values(5).toDouble, values(6), values(7).toDouble, values(8).toDouble, values(9).toDouble, values(10).toDouble)
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size > 20 && row.head != "除權息日期").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val date = LocalDate.parse(values.head, minguoDateTimeFormatter)
                val companyCode = values(1)
                (marketFile.market, date, companyCode, values(2), values(3).toDouble, values(4).toDouble, values(7).toDouble,
                  values(8) match {
                    case "除權" => "權"
                    case "除息" => "息"
                    case "除權息" => "權息"
                  },
                  values(9).toDouble, values(10).toDouble, values(11).toDouble, values(12).toDouble)
            }
        }

        val filterData = data.distinctBy(d => (d._1, d._2, d._3)).filterNot(d => dataAlreadyInDB.contains((d._1, d._2, d._3)))
        val dbIO = exRightDividend.map(e => (e.market, e.date, e.companyCode, e.companyName, e.closingPriceBeforeExRightExDividend, e.exRightExDividendReferencePrice, e.cashDividend, e.rightOrDividend, e.limitUp, e.limitDown, e.openingReferencePrice, e.exDividendReferencePrice)) ++= filterData
        dbRun(dbIO)
        reader.close()
        pb.step()
    }
    pb.close()
  }

  /** Parse MOPS t108sb27 monthly CSV rows into the ex_right_dividend schema.
   *  One company-period can emit up to two rows: one for 除息日 (if cash > 0) and
   *  one for 除權日 (if stock dividend > 0). Price-calc columns (closing price
   *  before, reference price, limits) are unavailable in MOPS and default to 0. */
  private def parseMopsRows(allRows: Seq[Seq[String]], market: String)
    : Seq[(String, LocalDate, String, String, Double, Double, Double, String, Double, Double, Double, Double)] = {
    val slashDate = java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd")
    def d(s: String): Double = Try(s.replaceAll("[,\\s]", "").toDouble).getOrElse(0.0)
    allRows.filter(r => r.size >= 17 && r.head != "公司代號" && r.head.nonEmpty)
      .flatMap { r =>
        val code = r(0).trim
        val name = r(1).trim
        val stockSurplus = d(r(4))
        val stockCapital = d(r(5))
        val exRightDateStr = r(6).trim
        val cashSurplus = d(r(7))
        val cashStatutory = d(r(8))
        val cashPreferred = d(r(9))
        val exDividendDateStr = r(10).trim
        val totalCash = cashSurplus + cashStatutory + cashPreferred
        val totalStock = stockSurplus + stockCapital

        val dividendRow = if (totalCash > 0 && exDividendDateStr.nonEmpty)
          Try(LocalDate.parse(exDividendDateStr, slashDate)).toOption.map { date =>
            (market, date, code, name, 0.0, 0.0, totalCash, "息", 0.0, 0.0, 0.0, 0.0)
          }
        else None

        val rightRow = if (totalStock > 0 && exRightDateStr.nonEmpty)
          Try(LocalDate.parse(exRightDateStr, slashDate)).toOption.map { date =>
            (market, date, code, name, 0.0, 0.0, 0.0, "權", 0.0, 0.0, 0.0, 0.0)
          }
        else None

        dividendRow.toSeq ++ rightRow.toSeq
      }
  }

  def readIndex(): Unit = {
    val index = TableQuery[Index]
    val query = index.map(i => (i.market, i.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }

    val files = IndexSetting().getMarketFilesFromDirectory.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
    val pb = new ProgressBar("Read index -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        println(s"Read index of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)

        val data = marketFile.market match {
          case "twse" =>
            import scala.io.Source
            val source = Source.fromFile(marketFile.file.jfile, "Big5-HKSCS")
            // The file has a footer that is not in CSV format, starting with "備註:".
            // We read all lines and take only those before this footer.
            val csvData = source.getLines().takeWhile(! _.startsWith("備註:")).mkString("\n")
            source.close()

            val reader = QuantlibCSVReader.open(new java.io.StringReader(csvData))
            val rows = reader.all().filter(row => (row.size == 6 || row.size == 7) && row.head != "指數" && row.head != "報酬指數").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val name = values.head
                val close = values(1).toDoubleOption
                val change = values(2) match {
                  case "-" => Try(-values(3).toDouble).getOrElse(0D)
                  case "" => 0
                  case "+" => Try(values(3).toDouble).getOrElse(0D)
                }
                val changePercentage: Double = values(4).toDoubleOption.getOrElse(0)
                (marketFile.market, date, name, close, change, changePercentage)
            }
          case "tpex" =>
            val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
            val rows = reader.all().filter(_.size == 4).map(_.map(_.replace(" ", "").replace(",", "")))
            reader.close()
            val spanRows = rows.span(_.head != "報酬指數")
            val indexes = spanRows._1.tail
            val returnIndexes = spanRows._2.tail.map(values => (values.head.replace("指數", "") + "報酬指數") +: values.tail)
            (indexes :++ returnIndexes).map {
              values =>
                val name = values.head
                val close = values(1).toDoubleOption
                val change = values(2).toDouble
                val changePercentage: Double = values(3).toDoubleOption.getOrElse(0)
                (marketFile.market, date, name, close, change, changePercentage)
            }
        }

        val dbIO = index.map(i => (i.market, i.date, i.name, i.close, i.change, i.changePercentage)) ++= data.filterNot(_._3 == "null")
        dbRun(dbIO)
        pb.step()
    }
    pb.close()
  }

  def readMarginTransactions(): Unit = {
    val marginTransactions = TableQuery[MarginTransactions]
    val query = marginTransactions.map(m => (m.market, m.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }

    val files = MarginTransactionsSetting().getMarketFilesFromDirectory.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
    val pb = new ProgressBar("Read margin transactions -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        println(s"Read margin transactions of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)

        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val StockCode = """[0-9][0-9A-Z]*"""
        val data = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size == 17 && row.head.matches(StockCode)).map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                (marketFile.market, date, companyCode, values(1), values(2).toInt, values(3).toInt, values(4).toInt, values(5).toInt, values(6).toInt, values(7).toInt, values(8).toInt, values(9).toInt, values(10).toInt, values(11).toInt, values(12).toInt, values(13).toInt, values(14).toInt)
            }
          case "tpex" =>
            val rows = reader.all().filter(row => (row.size == 20 || row.size >= 22) && row.head.matches(StockCode)).map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                (marketFile.market, date, companyCode, values(1), 
                 values(3).toInt, values(4).toInt, values(5).toInt, values(2).toInt, values(6).toInt, 
                 Try(values(9).toInt).getOrElse(values(8).toInt), 
                 values(12).toInt, values(11).toInt, values(13).toInt, values(10).toInt, values(14).toInt, 
                 values(17).toInt, 
                 if (values(18).nonEmpty && values(18) != "\"\"") Try(values(18).toInt).getOrElse(0) else 0)
            }
        }

        val dbIO = marginTransactions.map(m => (m.market, m.date, m.companyCode, m.companyName, m.marginPurchase, m.marginSales, m.cashRedemption, m.marginBalanceOfPreviousDay, m.marginBalanceOfTheDay, m.marginQuota, m.shortCovering, m.shortSale, m.stockRedemption, m.shortBalanceOfPreviousDay, m.shortBalanceOfTheDay, m.shortQuota, m.offsettingOfMarginPurchasesAndShortSales)) ++= data
        dbRun(dbIO)
        reader.close()
        pb.step()
    }
    pb.close()
  }

  def readStockPER_PBR_DividendYield(): Unit = {
    val stockPER_PBR_DividendYield = TableQuery[StockPER_PBR_DividendYield]
    val query = stockPER_PBR_DividendYield.map(s => (s.market, s.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }

    val files = StockPER_PBR_DividendYieldSetting().getMarketFilesFromDirectory.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
    val pb = new ProgressBar("Read stock PER, PBR, dividend yield -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read stock PER, PBR, dividend yield of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)

        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val data = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size >= 5 && row.head != "證券代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                // TWSE schema variations (observed):
                //   6-col (very legacy): code, name, pe, dy, div_year, pb
                //   7-col (pre-2024-07): code, name, dy, div_year, pe, pb, fiscal
                //   8-col (2024-07 onwards — added 收盤價 as col 2):
                //          code, name, close, dy, div_year, pe, pb, fiscal
                // If we fall back to the 7-col offset on an 8-col CSV, pe/pb/dy
                // all land on the wrong field (div_year / pe / close), polluting
                // ~23% of rows from 2024-07 through present.
                values.size match {
                  case 6 => (marketFile.market, date, companyCode, values(1), values(2).toDoubleOption, values(5).toDoubleOption, values(3).toDoubleOption)
                  case 7 => (marketFile.market, date, companyCode, values(1), values(4).toDoubleOption, values(5).toDoubleOption, values(2).toDoubleOption)
                  case _ => (marketFile.market, date, companyCode, values(1), values(5).toDoubleOption, values(6).toDoubleOption, values(3).toDoubleOption)
                }
            }
          case "tpex" =>
            // TPEx added an 8th column (財報年/季, e.g. "114Q1") mid-2024. The old
            // `row.size == 7` filter silently dropped every row from 2025 onwards,
            // starving the DB of ~16 months of TPEx PER/PBR/yield history.
            val rows = reader.all()
              .filter(row => (row.size == 7 || row.size == 8) && row.head != "股票代號")
              .map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                (marketFile.market, date, companyCode, values(1),
                 values(2).toDoubleOption, values(6).toDoubleOption, values(5).toDoubleOption)
            }
        }

        val dbIO = stockPER_PBR_DividendYield.map(s => (s.market, s.date, s.companyCode, s.companyName, s.priceToEarningRatio, s.priceBookRatio, s.dividendYield)) ++= data.distinctBy(d => (d._1, d._3))
        dbRun(dbIO)
        reader.close()
        pb.step()
    }
    pb.close()
  }

  def readTdccShareholding(): Unit = {
    val tdccShareholding = TableQuery[TdccShareholding]
    // Dedupe by data_date (not filename) — CSV's 資料日期 column is the source of truth.
    // Multiple downloads within the same week land in different files but encode the
    // same snapshot; the unique(data_date, company_code, holding_tier) constraint on
    // insert would fail without this pre-filter.
    val existingDatesFuture = db.run(tdccShareholding.map(_.dataDate).distinct.result)
    val existingDates = Await.result(existingDatesFuture, Duration.Inf).toSet

    val files = TdccShareholdingSetting().getMarketFilesFromDirectory.par
    val pb = new ProgressBar("Read TDCC shareholding -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        // TDCC opendata endpoint serves UTF-8 with a BOM on the header row.
        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "UTF-8")
        val rows = reader.all().dropWhile(row => row.isEmpty || !row.head.matches("""\d{8}"""))
        if (rows.nonEmpty) {
          val dateStr = rows.head.head
          val dataDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"))
          if (!existingDates.contains(dataDate)) {
            val data = rows.flatMap { values =>
              Try {
                val companyCode = values(1).trim
                val tier = values(2).toShort
                val holders = values(3).replace(",", "").toInt
                val shares = values(4).replace(",", "").toLong
                val pct = values(5).toDouble
                (dataDate, companyCode, tier, holders, shares, pct)
              }.toOption
            }.toSeq.distinctBy(t => (t._1, t._2, t._3))
            val dbIO = tdccShareholding.map(t =>
              (t.dataDate, t.companyCode, t.holdingTier, t.numHolders, t.numShares, t.pctOfOutstanding)) ++= data
            dbRun(dbIO)
            println(s"[tdcc] inserted ${data.size} rows for data_date=${dataDate} (file=${marketFile.file.name})")
          } else {
            println(s"[tdcc] data_date=${dataDate} already in DB — skipping ${marketFile.file.name}")
          }
        }
        reader.close()
        pb.step()
    }
    pb.close()
  }

  def readSblBorrowing(): Unit = {
    val sblBorrowing = TableQuery[SblBorrowing]
    val query = sblBorrowing.map(s => (s.market, s.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) =>
      (market, date.format(dateTimeFormatter) + ".csv")
    }

    val files = SblBorrowingSetting().getMarketFilesFromDirectory.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
    val pb = new ProgressBar("Read SBL borrowing -", files.size)
    files.tasksupport = taskSupport
    val stockCode = """[0-9][0-9A-Z]*"""
    files.foreach { marketFile =>
      val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
      val fileNamePattern(y, m, d) = marketFile.file.name
      val date = LocalDate.of(y.toInt, m.toInt, d.toInt)

      // Both TWSE CSV and TPEx JSON use SAME column layout after extraction:
      //   0 code | 1 name
      //   2..7 融券 (already in margin_transactions)
      //   8 借券 前日餘額 | 9 當日賣出 | 10 當日還券 | 11 當日調整
      //   12 當日餘額    | 13 次一營業日可限額         | 14 備註
      val rows: Seq[Seq[String]] = if (isJsonFile(marketFile.file.jfile)) {
        // TPEx JSON
        val raw = new String(java.nio.file.Files.readAllBytes(marketFile.file.jfile.toPath), "UTF-8")
        Try {
          val json = Json.parse(raw)
          val arr = (json \ "tables")(0) \ "data"
          arr.as[Seq[Seq[String]]]
        }.getOrElse(Seq.empty)
      } else {
        // TWSE CSV (Big5)
        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        try {
          reader.all()
            .filter(row => row.size >= 14 && row.head.matches(stockCode))
            .map(_.toSeq)
        } finally reader.close()
      }

      val cleaned = rows
        .filter(r => r.size >= 14 && r.head.matches(stockCode))
        .map(r => r.map(cleanCell))
      val data = cleaned.flatMap { values =>
        Try {
          val companyCode = values.head
          val companyName = values(1)
          val prev   = values(8).toLong
          val sold   = values(9).toLong
          val ret    = values(10).toLong
          val adj    = values(11).toLong
          val bal    = values(12).toLong
          val limit  = Try(values(13).toLong).getOrElse(0L)
          (marketFile.market, date, companyCode, companyName, prev, sold, ret, adj, bal, limit)
        }.toOption
      }.toSeq.distinctBy(d => (d._1, d._2, d._3))

      val dbIO = sblBorrowing.map(s =>
        (s.market, s.date, s.companyCode, s.companyName, s.prevDayBalance, s.dailySold,
         s.dailyReturned, s.dailyAdjustment, s.dailyBalance, s.nextDayLimit)) ++= data
      dbRun(dbIO)
      pb.step()
    }
    pb.close()
  }

  def readForeignHoldingRatio(): Unit = {
    val foreignHoldingRatio = TableQuery[ForeignHoldingRatio]
    val query = foreignHoldingRatio.map(f => (f.market, f.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) =>
      (market, date.format(dateTimeFormatter) + ".csv")
    }

    val files = ForeignHoldingRatioSetting().getMarketFilesFromDirectory.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
    val pb = new ProgressBar("Read foreign holding ratio -", files.size)
    files.tasksupport = taskSupport
    val stockCode = """[0-9][0-9A-Z]*"""
    files.foreach { marketFile =>
      val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
      val fileNamePattern(y, m, d) = marketFile.file.name
      val date = LocalDate.of(y.toInt, m.toInt, d.toInt)

      // TWSE CSV cols (after QuantlibCSVReader strips = and quotes):
      //   0 code | 1 name | 2 ISIN | 3 發行股數 | 4 尚可投資股數 | 5 持有股數
      //   6 尚可投資比率 | 7 持股比率 | 8 共用法令上限 | 9 陸資上限 | 10 異動原因 | 11 最近申報日
      // TPEx JSON cols (insti/qfii):
      //   0 排行 | 1 代號 | 2 名稱 | 3 發行股數 | 4 尚可投資股數 | 5 持有股數
      //   6 尚可投資比率 "X.XX%" | 7 持股比率 "X.XX%" | 8 法令上限 "X%" | 9 備註
      val data: Seq[(String, LocalDate, String, String, Long, Long, Long, Double, Double, Double)] =
        if (isJsonFile(marketFile.file.jfile)) {
          val raw = new String(java.nio.file.Files.readAllBytes(marketFile.file.jfile.toPath), "UTF-8")
          Try {
            val json = Json.parse(raw)
            val arr = (json \ "tables")(0) \ "data"
            arr.as[Seq[Seq[String]]]
          }.getOrElse(Seq.empty).flatMap { row =>
            if (row.size < 9) None else Try {
              val vals = row.map(cleanCell)
              val code = vals(1)
              if (!code.matches(stockCode)) None
              else Some((
                marketFile.market, date, code, vals(2),
                vals(3).toLong, vals(4).toLong, vals(5).toLong,
                vals(6).toDouble, vals(7).toDouble, vals(8).toDouble
              ))
            }.toOption.flatten
          }
        } else {
          val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
          try {
            reader.all()
              .filter(row => row.size >= 10 && row.head.matches(stockCode))
              .flatMap { row =>
                Try {
                  val v = row.map(cleanCell)
                  (
                    marketFile.market, date, v.head, v(1),
                    v(3).toLong, v(4).toLong, v(5).toLong,
                    v(6).toDouble, v(7).toDouble, v(8).toDouble
                  )
                }.toOption
              }
          } finally reader.close()
        }

      val dbIO = foreignHoldingRatio.map(f =>
        (f.market, f.date, f.companyCode, f.companyName, f.outstandingShares,
         f.foreignRemainingShares, f.foreignHeldShares, f.foreignRemainingRatio,
         f.foreignHeldRatio, f.foreignLimitRatio)) ++= data.distinctBy(d => (d._1, d._2, d._3))
      dbRun(dbIO)
      pb.step()
    }
    pb.close()
  }

  // ============================================================
  // Sprint B (MOPS structured filings): buyback (working) / insider (pending 2-step ajax)
  //
  // Common parser pattern:
  //   1. Read .html file as Big5-HKSCS bytes (MOPS encoding)
  //   2. JsoupBrowser parse → extract rows from <table> elements
  //   3. Map per-endpoint schema to typed tuple
  //   4. Filter + dedupe + bulk insert
  //
  // 0-byte sentinel files (no-data months) are skipped silently.
  // Multi-table responses: take all <tr> rows where the first <td> matches a
  // stock-code regex, ignoring header / summary rows.
  // ============================================================

  private val stockCodeRegex = """[0-9][0-9A-Z]{3,}""".r

  private def parseMopsHtml(file: java.io.File): Seq[Seq[String]] = {
    if (!file.exists() || file.length() < 1024) return Seq.empty
    // MOPS endpoints serve mixed encodings: t35sc09 (買回) = Big5-HKSCS, t56sb12 (內部人) = UTF-8.
    // Sniff: UTF-8 valid bytes shouldn't have 0x80-0xBF in non-multibyte position.
    // Practical heuristic: if file has BOM or contains non-ASCII Chinese decoded valid as UTF-8 → UTF-8.
    val bytes = java.nio.file.Files.readAllBytes(file.toPath)
    val raw = Try(new String(bytes, "UTF-8")) match {
      case scala.util.Success(s) if !s.contains("�") => s  // valid UTF-8
      case _ => new String(bytes, "Big5-HKSCS")
    }
    val browser = net.ruippeixotog.scalascraper.browser.JsoupBrowser()
    val doc = browser.parseString(raw)
    import net.ruippeixotog.scalascraper.dsl.DSL._
    import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
    val rows = doc >> elementList("table tr")
    rows.map(tr => (tr >> elementList("td")).map(_.text.trim))
      .filter(_.nonEmpty)
  }

  // 民國 yyy/MM/dd → LocalDate; falls back to None if unparseable.
  private val minguoSlashFormatter = new DateTimeFormatterBuilder()
    .appendPattern("yyy/MM/dd")
    .toFormatter()
    .withChronology(MinguoChronology.INSTANCE)

  private def parseMinguoSlashDate(s: String): Option[LocalDate] = {
    val cleaned = s.trim
    if (cleaned.isEmpty) return None
    Try(LocalDate.from(minguoSlashFormatter.parse(cleaned))).toOption
  }

  private def parseLong(s: String): Long = Try(cleanCell(s).toLong).getOrElse(0L)
  private def parseDouble(s: String): Double = Try(cleanCell(s).toDouble).getOrElse(0.0)

  /** 庫藏股 t35sc09 — 18-column main data table (verified 2026-04-29 against TWSE Apr 2026):
   *   [0] 序號 / [1] 公司代號 / [2] 公司名稱 / [3] 公告日 (民國 yyy/MM/dd) /
   *   [4] 買回次別 / [5] 買回前股本 / [6] 預定買回股數 / [7] 買回價格區間下限 / [8] 買回價格區間上限 /
   *   [9] 執行起 / [10] 執行迄 / [11] 是否已執行(Y/N) /
   *   [12] 已買回股數 / [13] 已買回占已發行 % / [14] 平均每股 NTD / [15] 已買回成本(NT$) /
   *   [16] 占公司資本 % / [17] 變更原因
   *
   * Endpoint serves SNAPSHOT of all historical buybacks (4.4MB全套)，每月跑一次刷新即可.
   * Dedupe key (market, announce_date, company_code) — Slick unique index.
   */
  def readTreasuryStockBuyback(): Unit = {
    val table = TableQuery[TreasuryStockBuyback]
    val existing = Await.result(
      db.run(table.map(t => (t.market, t.announceDate, t.companyCode)).distinct.result),
      Duration.Inf).toSet

    val files = TreasuryStockBuybackSetting().getMarketFilesFromDirectory.par
    val pb = new ProgressBar("Read treasury stock buyback -", files.size)
    files.tasksupport = taskSupport
    files.foreach { mf =>
      val rows = parseMopsHtml(mf.file.jfile)
      val data: Seq[(String, LocalDate, String, String, Long, Double, Double, LocalDate, LocalDate, Long, Double)] =
        rows.flatMap { cols =>
          if (cols.size < 17) None
          else Try {
            val code = cols(1)
            if (!stockCodeRegex.pattern.matcher(code).matches()) throw new RuntimeException("not stock code")
            val name = cols(2)
            val announceDate = parseMinguoSlashDate(cols(3)).getOrElse(throw new RuntimeException("bad announce date"))
            val plannedShares = parseLong(cols(6))           // 預定買回股數 (already in 股, no ×1000)
            val priceLow = parseDouble(cols(7))
            val priceHigh = parseDouble(cols(8))
            val periodStart = parseMinguoSlashDate(cols(9)).getOrElse(announceDate)
            val periodEnd = parseMinguoSlashDate(cols(10)).getOrElse(announceDate)
            val executedShares = parseLong(cols(12))         // 已買回股數
            val pctOfCapital = parseDouble(cols(16))          // 占公司資本 %
            (mf.market, announceDate, code, name, plannedShares, priceLow, priceHigh, periodStart, periodEnd, executedShares, pctOfCapital)
          }.toOption
        }

      val filtered = data.distinctBy(d => (d._1, d._2, d._3)).filterNot(d => existing.contains((d._1, d._2, d._3)))
      if (filtered.nonEmpty) {
        val dbIO = table.map(t =>
          (t.market, t.announceDate, t.companyCode, t.companyName, t.plannedShares,
           t.priceLow, t.priceHigh, t.periodStart, t.periodEnd, t.executedShares, t.pctOfCapital)) ++= filtered
        dbRun(dbIO)
        println(s"[buyback] ${mf.market}/${mf.file.name}: inserted ${filtered.size} rows (parsed ${data.size}/${rows.size} rows from HTML)")
      } else {
        println(s"[buyback] ${mf.market}/${mf.file.name}: 0 new (parsed ${data.size}/${rows.size} rows)")
      }
      pb.step()
    }
    pb.close()
  }

  /** 內部人持股轉讓「事前申報」daily report (t56sb12_q1/q2).
   *
   * Verified schema (2026-04-29 against TWSE 2026-04-24 data):
   * Each data row in HTML = ONE 內部人 transfer declaration. 17-18 cells per row.
   *
   *   [ 0] 異動情形 sub-cell placeholder (empty)
   *   [ 1] 申報日期 (民國 yyy/MM/dd)
   *   [ 2] 公司代號
   *   [ 3] 公司名稱
   *   [ 4] 申報人身分（董事本人 / 大股東本人 / 經理人本人 / 監察人本人 / 配偶等）
   *   [ 5] 姓名
   *   [ 6] 預定轉讓方式（鉅額逐筆 / 一般交易 / 贈與 / 信託 / 拍賣 等）
   *   [ 7] 預定轉讓股數
   *   [ 8] 每日盤中最大轉讓股數（只有「一般交易」有，其他空白）
   *   [ 9] 受讓人（只有「贈與 / 私下交易 / 信託」有）
   *   [10] 目前持有股數 - 自有持股
   *   [11] 目前持有股數 - 保留運用決定權信託股數
   *   [12] 預定轉讓總股數 - 自有
   *   [13] 預定轉讓總股數 - 信託
   *   [14] 預定轉讓後持股 - 自有 (skip, derivable)
   *   [15] 預定轉讓後持股 - 信託 (skip)
   *   [16] 有效轉讓期間 (skip, free text)
   *   [17] 是否申報未完成轉讓 (skip, optional)
   *
   * Files saved as `{dir}/{year}/YYYY_M_D.html`. `report_date` = filename date
   * (data publication day). `declare_date` = parsed from cell [0].
   *
   * Forward signal: 內部人 declared upcoming transfer → -2~-5% CAR over 5-30d.
   */
  def readInsiderHolding(): Unit = {
    val table = TableQuery[InsiderHolding]
    val existing = Await.result(
      db.run(table.map(t => (t.market, t.reportDate, t.companyCode, t.reporterName, t.transferMethod, t.transferee)).distinct.result),
      Duration.Inf).toSet

    val dateFromName = """(\d+)_(\d+)_(\d+)\.html""".r
    val files = InsiderHoldingSetting().getMarketFilesFromDirectory.par
    val pb = new ProgressBar("Read insider holding -", files.size)
    files.tasksupport = taskSupport
    files.foreach { mf =>
      val reportDateOpt = mf.file.name match {
        case dateFromName(y, m, d) => Try(LocalDate.of(y.toInt, m.toInt, d.toInt)).toOption
        case _ => None
      }
      reportDateOpt.foreach { reportDate =>
        val rows = parseMopsHtml(mf.file.jfile)
        // Skip header (rows 0-1) and footer rows; data rows have stock code at index 2.
        val data: Seq[(String, LocalDate, LocalDate, String, String, String, String, String, String, Long, Long, Long, Long, Long, Long)] =
          rows.flatMap { cols =>
            if (cols.size < 14) None
            else Try {
              val code = cols(2).trim
              if (!stockCodeRegex.pattern.matcher(code).matches()) throw new RuntimeException("not stock code")
              val declareDate = parseMinguoSlashDate(cols(1).trim).getOrElse(reportDate)
              val name = cols(3).trim
              val reporterTitle = cols(4).trim
              val reporterName = cols(5).trim.replace("\n", "")
              val transferMethod = cols(6).trim
              val transferShares = parseLong(cols(7))
              val maxIntraday = parseLong(cols(8))
              val transferee = cols(9).trim.replace("\n", "")
              val currentOwn = parseLong(cols(10))
              val currentTrust = parseLong(cols(11))
              val plannedOwn = parseLong(cols(12))
              val plannedTrust = parseLong(cols(13))
              (mf.market, reportDate, declareDate, code, name, reporterTitle, reporterName,
                transferMethod, transferee,
                transferShares, maxIntraday,
                currentOwn, currentTrust,
                plannedOwn, plannedTrust)
            }.toOption
          }

        val filtered = data
          .distinctBy(d => (d._1, d._2, d._4, d._7, d._8, d._9))  // (market, report_date, code, reporter_name, method, transferee)
          .filterNot(d => existing.contains((d._1, d._2, d._4, d._7, d._8, d._9)))
        if (filtered.nonEmpty) {
          val dbIO = table.map(t =>
            (t.market, t.reportDate, t.declareDate, t.companyCode, t.companyName,
             t.reporterTitle, t.reporterName, t.transferMethod, t.transferee,
             t.transferShares, t.maxIntradayShares,
             t.currentSharesOwn, t.currentSharesTrust,
             t.plannedSharesOwn, t.plannedSharesTrust)) ++= filtered
          dbRun(dbIO)
          println(s"[insider] ${mf.market}/${mf.file.name}: inserted ${filtered.size} rows (parsed ${data.size}/${rows.size})")
        }
      }
      pb.step()
    }
    pb.close()
  }

}
