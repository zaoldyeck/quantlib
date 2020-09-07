package reader

import java.time.LocalDate
import java.time.chrono.MinguoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import com.github.tototoshi.csv._
import db.table._
import me.tongfei.progressbar.ProgressBar
import slick.collection.heterogeneous.HNil
import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
import setting._
import slick.lifted.TableQuery
import util.QuantlibCSVReader

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

class TradingReader extends Reader {
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
              rows.tail.map(_.map(_.replace(" ", "").replace(",", ""))).map {
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
              rows.init.tail.map(_.map(_.replace(" ", "").replace(",", ""))).map {
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
                  if (values(8) == "--") None else Some(values(8).toDouble), values(9))
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size == 10 && row.head != "恢復買賣日期 ").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val dateFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
                  .parseLenient
                  .appendPattern("yyyMMdd")
                  .toFormatter
                  .withChronology(MinguoChronology.INSTANCE)
                val date = LocalDate.parse(values.head, dateFormatter)
                val companyCode = values(1)
                (marketFile.market, date, companyCode, values(2), values(3).toDouble, values(4).toDouble, values(5).toDouble, values(6).toDouble, values(7).toDouble, Option(values(8).toDouble), values(9))
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
    files.foreach {
      marketFile =>
        //println(s"Read ex-right dividend of ${marketFile.market}-${marketFile.file.name}")
        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val data = marketFile.market match {
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
        //println(s"Read index of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)

        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val data = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size == 7 && row.head != "指數" && row.head != "報酬指數").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val name = values.head
                val close = values(1) match {
                  case "--" => None
                  case value => Some(value.toDouble)
                }
                val change = values(2) match {
                  case "-" => Try(-values(3).toDouble).getOrElse(0D)
                  case "" => 0
                  case "+" => Try(values(3).toDouble).getOrElse(0D)
                }
                val changePercentage = values(4) match {
                  case v if v == "--" || v == "---" => 0
                  case value => value.toDouble
                }
                (marketFile.market, date, name, close, change, changePercentage)
            }
          case "tpex" =>
            val rows = reader.all().filter(_.size == 4).map(_.map(_.replace(" ", "").replace(",", "")))
            val spanRows = rows.span(_.head != "報酬指數")
            val indexes = spanRows._1.tail
            val returnIndexes = spanRows._2.tail.map(values => (values.head.replace("指數", "") + "報酬指數") +: values.tail)
            (indexes :++ returnIndexes).map {
              values =>
                val name = values.head
                val close = values(1) match {
                  case "--" => None
                  case value => Some(value.toDouble)
                }
                val change = values(2).toDouble
                val changePercentage = values(3) match {
                  case "--" => 0
                  case value => value.toDouble
                }
                (marketFile.market, date, name, close, change, changePercentage)
            }
        }

        val dbIO = index.map(i => (i.market, i.date, i.name, i.close, i.change, i.changePercentage)) ++= data
        dbRun(dbIO)
        reader.close()
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
        //println(s"Read margin transactions of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)

        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val data = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size == 17 && row.head != "" && row.head != "股票代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                (marketFile.market, date, companyCode, values(1), values(2).toInt, values(3).toInt, values(4).toInt, values(5).toInt, values(6).toInt, values(7).toInt, values(8).toInt, values(9).toInt, values(10).toInt, values(11).toInt, values(12).toInt, values(13).toInt, values(14).toInt)
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size == 20 && row.head != "代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                (marketFile.market, date, companyCode, values(1), values(3).toInt, values(4).toInt, values(5).toInt, values(2).toInt, values(6).toInt, Try(values(9).toInt).getOrElse(values(8).toInt), values(12).toInt, values(11).toInt, values(13).toInt, values(10).toInt, values(14).toInt, values(17).toInt, Try(values(18).toInt).orElse(Try(values(19).toInt)).getOrElse(0))
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
                values.size match {
                  case 6 =>
                    (marketFile.market, date, companyCode, values(1),
                      values(2) match {
                        case "-" => None
                        case _ => Some(values(2).toDouble)
                      },
                      values(4) match {
                        case "-" => None
                        case _ => Some(values(4).toDouble)
                      },
                      Some(values(3).toDouble))
                  case _ =>
                    (marketFile.market, date, companyCode, values(1),
                      values(4) match {
                        case "-" => None
                        case _ => Some(values(4).toDouble)
                      },
                      values(5) match {
                        case "-" => None
                        case _ => Some(values(5).toDouble)
                      },
                      Some(values(2).toDouble))
                }
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size == 7 && row.head != "股票代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                (marketFile.market, date, companyCode, values(1),
                  values(2) match {
                    case "N/A" => None
                    case _ => Some(values(2).toDouble)
                  },
                  values(6) match {
                    case "N/A" => None
                    case _ => Some(values(6).toDouble)
                  },
                  values(5) match {
                    case "" => None
                    case "N/A" => None
                    case _ => Some(values(5).toDouble)
                  })
            }
        }

        val dbIO = stockPER_PBR_DividendYield.map(s => (s.market, s.date, s.companyCode, s.companyName, s.priceToEarningRatio, s.priceBookRatio, s.dividendYield)) ++= data.distinctBy(d => (d._1, d._3))
        dbRun(dbIO)
        reader.close()
        pb.step()
    }
    pb.close()
  }
}
