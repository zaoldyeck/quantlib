import java.time.LocalDate
import java.time.chrono.MinguoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.concurrent.ForkJoinPool

import Settings._
import com.github.tototoshi.csv._
import db.table.{CapitalReduction, DailyQuote, ExRightDividend, IncomeStatement, OperatingRevenue, _}
import me.tongfei.progressbar.ProgressBar
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import slick.collection.heterogeneous.HNil
import slick.dbio.Effect
import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
import slick.lifted.TableQuery
import slick.sql.FixedSqlAction
import util.QuantlibCSVReader

import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Try

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class Reader {
  private val forkJoinPool = new ForkJoinPool(20)
  private val taskSupport = new ForkJoinTaskSupport(forkJoinPool)
  private val minguoDateTimeFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseLenient
    .appendPattern("y/MM/dd")
    .toFormatter
    .withChronology(MinguoChronology.INSTANCE)

  def readFinancialAnalysis(): Unit = {
    val db = Database.forConfig("db")
    val financialAnalysis = TableQuery[FinancialAnalysis]
    val query = financialAnalysis.map(f => (f.market, f.year)).distinct.result
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf)
    db.close()

    val files = FinancialAnalysisSetting().getMarketFiles.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name.split('_')(0).toInt))).par
    val pb = new ProgressBar("Read financial analysis -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read financial analysis of ${marketFile.market}-${marketFile.file.name}")
        val year = marketFile.file.name.split('_').head.toInt
        val reader = CSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val rows = reader.all().tail
        val dbIOActions = rows.map {
          values =>
            val splitValues = values.splitAt(2)
            val transferValues = splitValues._2.map {
              case v if v == "NA" => None
              case v if v.contains("*") => None
              case value => Some(value.toDouble)
            }
            val companyCode = values.head
            val query = Query(marketFile.market :: year :: companyCode :: values(1) :: transferValues.head :: transferValues(1) ::
              transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) :: transferValues(6) :: transferValues(7) ::
              transferValues(8) :: transferValues(9) :: transferValues(10) :: transferValues(11) :: transferValues(12) :: transferValues(13) ::
              transferValues(14) :: transferValues(15) :: transferValues(16) :: transferValues(17) :: transferValues(18) :: HNil)
            //val exists = financialAnalysis.filter(f => f.market === marketFile.market && f.year === year && f.companyCode === companyCode).exists
            //val selectExpression = query.filterNot(_ => exists)
            //financialAnalysis.map(f => (f.market :: f.year :: f.companyCode :: f.companyName :: f.liabilitiesOfAssetsRatioPercentage :: f.longTermFundsToPropertyAndPlantAndEquipmentPercentage :: f.currentRatioPercentage :: f.quickRatioPercentage :: f.timesInterestEarnedRatioPercentage :: f.averageCollectionTurnoverTimes :: f.averageCollectionDays :: f.averageInventoryTurnoverTimes :: f.averageInventoryDays :: f.propertyAndPlantAndEquipmentTurnoverTimes :: f.totalAssetsTurnoverTimes :: f.returnOnTotalAssetsPercentage :: f.returnOnEquityPercentage :: f.profitBeforeTaxToCapitalPercentage :: f.profitToSalesPercentage :: f.earningsPerShareNTD :: f.cashFlowRatioPercentage :: f.cashFlowAdequacyRatioPercentage :: f.cashFlowReinvestmentRatioPercentage :: HNil)).forceInsertQuery(selectExpression)
            financialAnalysis.map(f => (f.market :: f.year :: f.companyCode :: f.companyName :: f.liabilitiesOfAssetsRatioPercentage :: f.longTermFundsToPropertyAndPlantAndEquipmentPercentage :: f.currentRatioPercentage :: f.quickRatioPercentage :: f.timesInterestEarnedRatioPercentage :: f.averageCollectionTurnoverTimes :: f.averageCollectionDays :: f.averageInventoryTurnoverTimes :: f.averageInventoryDays :: f.propertyAndPlantAndEquipmentTurnoverTimes :: f.totalAssetsTurnoverTimes :: f.returnOnTotalAssetsPercentage :: f.returnOnEquityPercentage :: f.profitBeforeTaxToCapitalPercentage :: f.profitToSalesPercentage :: f.earningsPerShareNTD :: f.cashFlowRatioPercentage :: f.cashFlowAdequacyRatioPercentage :: f.cashFlowReinvestmentRatioPercentage :: HNil)).forceInsertQuery(query)
        }

        dbRun(dbIOActions)
        reader.close()
        pb.step()
    }
    pb.close()
  }

  def readBalanceSheet(): Unit = {
    val db = Database.forConfig("db")
    val balanceSheet = TableQuery[BalanceSheet]
    val query = balanceSheet.map(b => (b.market, b.year, b.quarter)).distinct.result
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf)
    db.close()

    val files = BalanceSheetSetting().getMarketFiles.filterNot(m => {
      val strings = m.file.name.split('_')
      dataAlreadyInDB.contains((m.market, strings(0).toInt, strings(1).toInt))
    }).par
    val pb = new ProgressBar("Read balance sheet -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read balance sheet of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+).*""".r
        val fileNamePattern(y, q) = marketFile.file.name
        val year = y.toInt
        val quarter = q.toInt

        val reader = CSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val rows = reader.allWithHeaders()
        val dbIOActions = rows.flatMap {
          values =>
            val companyCode = values("公司代號")
            val companyName = values("公司名稱")
            val date: Option[LocalDate] = values.get("出表日期").map(LocalDate.parse(_, minguoDateTimeFormatter))

            values
              .filterNot { case (k, v) => k == "公司代號" || k == "公司名稱" || k == "出表日期" || k == "年度" || k == "季別" }
              .map { case (k, v) => k.replace(" ", "") -> v.replace(" ", "").replace(",", "") }
              .filter(v => Try(v._2.toDouble).isSuccess)
              .map {
                case (k, v) =>
                  val query = Query(marketFile.market,
                    year,
                    quarter,
                    date,
                    companyCode,
                    companyName,
                    k,
                    v.toDouble)
                  //val exists = balanceSheet.filter(b => b.market === marketFile.market && b.year === year && b.quarter === quarter && b.companyCode === companyCode && b.subject === k).exists
                  //val selectExpression = query.filterNot(_ => exists)
                  //balanceSheet.map(b => (b.market, b.year, b.quarter, b.date, b.companyCode, b.companyName, b.subject, b.value)).forceInsertQuery(selectExpression)
                  balanceSheet.map(b => (b.market, b.year, b.quarter, b.date, b.companyCode, b.companyName, b.subject, b.value)).forceInsertQuery(query)
              }
        }

        dbRun(dbIOActions)
        reader.close()
        pb.step()
    }
    pb.close()
  }

  def readIncomeStatement(): Unit = {
    val db = Database.forConfig("db")
    val incomeStatement = TableQuery[IncomeStatement]
    val query = incomeStatement.map(i => (i.market, i.year, i.quarter)).distinct.result
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf)
    db.close()

    val files = IncomeStatementSetting().getMarketFiles.filterNot(m => {
      val strings = m.file.name.split('_')
      dataAlreadyInDB.contains((m.market, strings(0).toInt, strings(1).toInt))
    }).par
    val pb = new ProgressBar("Read income statement -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read income statement of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+).*""".r
        val fileNamePattern(y, q) = marketFile.file.name
        val year = y.toInt
        val quarter = q.toInt

        val reader = CSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val rows = reader.allWithHeaders()
        val dbIOActions = rows.flatMap {
          values =>
            val companyCode = values.get("公司代號")
            val companyName = values.get("公司名稱")
            val date: Option[LocalDate] = values.get("出表日期").map(LocalDate.parse(_, minguoDateTimeFormatter))

            values
              .filterNot { case (k, v) => k == "公司代號" || k == "公司名稱" || k == "出表日期" || k == "年度" || k == "季別" }
              .map { case (k, v) => k.replace(" ", "") -> v.replace(" ", "").replace(",", "") }
              .filter(v => Try(v._2.toDouble).isSuccess)
              .map {
                case (k, v) =>
                  val query = Query(marketFile.market,
                    year,
                    quarter,
                    date,
                    companyCode.get,
                    companyName.get,
                    k,
                    v.toDouble)
                  //val exists = incomeStatement.filter(i => i.market === marketFile.market && i.year === year && i.quarter === quarter && i.companyCode === companyCode.get && i.subject === k).exists
                  //val selectExpression = query.filterNot(_ => exists)
                  //incomeStatement.map(i => (i.market, i.year, i.quarter, i.date, i.companyCode, i.companyName, i.subject, i.value)).forceInsertQuery(selectExpression)
                  incomeStatement.map(i => (i.market, i.year, i.quarter, i.date, i.companyCode, i.companyName, i.subject, i.value)).forceInsertQuery(query)
              }
        }

        dbRun(dbIOActions)
        reader.close()
        pb.step()
    }
    pb.close()
  }

  def readOperatingRevenue(): Unit = {
    val db = Database.forConfig("db")
    val operatingRevenue = TableQuery[OperatingRevenue]
    val query = operatingRevenue.map(o => (o.market, o.year, o.month)).distinct.result
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf)
    db.close()

    val browser = JsoupBrowser()
    val files = OperatingRevenueSetting().getMarketFiles.filterNot(m => {
      val strings = m.file.name.split('.')(0).split('_')
      dataAlreadyInDB.contains((m.market, strings(0).toInt, strings(1).toInt))
    }).par
    val pb = new ProgressBar("Read operating revenue -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read operating revenue of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+).*""".r
        val fileNamePattern(y, m) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt

        val data = marketFile.file.extension match {
          case "html" =>
            val doc = browser.parseFile(marketFile.file.jfile, "Big5-HKSCS")
            val rows = (doc >> elements("tr")).map {
              tr =>
                val thOption = tr >?> element("th")
                if (thOption.isDefined) {
                  (tr >> elements("th")).map(_.text)
                } else {
                  (tr >> elements("td")).map(_.text)
                }
            }.iterator

            val industryPattern = """產業別：(.*)""".r

            @scala.annotation.tailrec
            def getData(rowOption: Option[Iterable[String]], industry: String = "", data: Seq[(String, Int, Int, String, String, Option[String], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])] = Seq()): Seq[(String, Int, Int, String, String, Option[String], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])] = {
              rowOption match {
                case Some(v) =>
                  val values = v.toSeq
                  if (values.size == 10 && values.head != "公司 代號") {
                    val splitValues = values.splitAt(2)
                    val transferValues = splitValues._2.map(_.replace(",", "")).map {
                      case v if v == "" => None
                      case value: String => Some(value.toDouble)
                    }
                    val d = (marketFile.market, year, month, values.head, values(1), Option(industry), transferValues.head, transferValues(1), transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7))
                    getData(rows.nextOption(), industry, data.appended(d))
                  } else {
                    val id = values.head match {
                      case industryPattern(v) => v
                      case _ => industry
                    }
                    getData(rows.nextOption(), id, data)
                  }
                case None => data
              }
            }

            getData(rows.nextOption)
          case "csv" =>
            val reader = CSVReader.open(marketFile.file.jfile)
            val rows = reader.all().tail
            rows.map {
              values =>
                val splitValues = values.splitAt(5)
                val transferValues = splitValues._2.init.map {
                  case v if v == "" => None
                  case value => Some(value.toDouble)
                }
                val d = (marketFile.market, year, month, values(2), values(3), Option(values(4)), transferValues.head, transferValues(1), transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7))
                reader.close()
                d
            }
        }

        val dbIOActions = data.map {
          d =>
            val query = Query(d)
            //val exists = operatingRevenues.filter(o => o.market === marketFile.market && o.year === year && o.month === month && o.companyCode === d._4).exists
            //val selectExpression = query.filterNot(_ => exists)
            //operatingRevenue.map(o => (o.market, o.year, o.month, o.companyCode, o.companyName, o.industry, o.monthlyRevenue, o.lastMonthRevenue, o.lastYearMonthlyRevenue, o.monthlyRevenueComparedLastMonthPercentage, o.monthlyRevenueComparedLastYearPercentage, o.cumulativeRevenue, o.lastYearCumulativeRevenue, o.cumulativeRevenueComparedLastYearPercentage)).forceInsertQuery(selectExpression)
            operatingRevenue.map(o => (o.market, o.year, o.month, o.companyCode, o.companyName, o.industry, o.monthlyRevenue, o.lastMonthRevenue, o.lastYearMonthlyRevenue, o.monthlyRevenueComparedLastMonthPercentage, o.monthlyRevenueComparedLastYearPercentage, o.cumulativeRevenue, o.lastYearCumulativeRevenue, o.cumulativeRevenueComparedLastYearPercentage)).forceInsertQuery(query)
        }

        dbRun(dbIOActions)
        pb.step()
    }
    pb.stop()
  }

  def readDailyQuote(): Unit = {
    val db = Database.forConfig("db")
    val dailyQuote = TableQuery[DailyQuote]
    val query = dailyQuote.map(d => (d.market, d.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }
    db.close()

    val files = DailyQuoteSetting().getMarketFiles.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
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
        val dbIOActions = marketFile.market match {
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

                  val query = Query((marketFile.market,
                    date,
                    companyCode,
                    values(1),
                    transferValues.head.get.toLong,
                    transferValues(1).get.toInt,
                    transferValues(2).get.toLong,
                    transferValues(3),
                    transferValues(4),
                    transferValues(5),
                    transferValues(6),
                    change,
                    transferValues(9),
                    transferValues(10).map(_.toInt),
                    transferValues(11),
                    transferValues(12).map(_.toInt),
                    transferValues(13)))
                  //val exists = dailyQuote.filter(d => d.market === marketFile.market && d.date === date && d.companyCode === companyCode).exists
                  //val selectExpression = query.filterNot(_ => exists)
                  //dailyQuote.map(d => (d.market, d.date, d.companyCode, d.companyName, d.tradeVolume, d.transaction, d.tradeValue, d.openingPrice, d.highestPrice, d.lowestPrice, d.closingPrice, d.change, d.lastBestBidPrice, d.lastBestBidVolume, d.lastBestAskPrice, d.lastBestAskVolume, d.priceEarningRatio)).forceInsertQuery(selectExpression)
                  dailyQuote.map(d => (d.market, d.date, d.companyCode, d.companyName, d.tradeVolume, d.transaction, d.tradeValue, d.openingPrice, d.highestPrice, d.lowestPrice, d.closingPrice, d.change, d.lastBestBidPrice, d.lastBestBidVolume, d.lastBestAskPrice, d.lastBestAskVolume, d.priceEarningRatio)).forceInsertQuery(query)
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
                    //case v if v.isEmpty || v == " " || v == "X" => Some(0)
                    case v if v == "除權息" || v == "除權" || v == "除息" => Some(0)
                    case value => Some(value.toDouble)
                  }
                  val companyCode = values.head
                  //"成交股數","成交筆數","成交金額","開盤價","最高價","最低價","收盤價","漲跌(+/-)","漲跌價差","最後揭示買價","最後揭示買量","最後揭示賣價","最後揭示賣量","本益比",
                  //收盤 ,漲跌,開盤 ,最高 ,最低,成交股數  , 成交金額(元), 成交筆數 ,最後買價,最後買量(千股),最後賣價,最後賣量(千股),發行股數 ,次日漲停價 ,次日跌停價
                  //收盤 ,漲跌,開盤 ,最高 ,最低,成交股數  , 成交金額(元), 成交筆數 ,最後買價,最後賣價,發行股數 ,次日漲停價 ,次日跌停價
                  val noneInt: Option[Int] = None
                  val noneDouble: Option[Double] = None
                  val query = values.size match {
                    case 15 =>
                      Query((marketFile.market,
                        date,
                        companyCode,
                        values(1),
                        transferValues(5).get.toLong,
                        transferValues(7).get.toInt,
                        transferValues(6).get.toLong,
                        transferValues(2),
                        transferValues(3),
                        transferValues(4),
                        transferValues.head,
                        transferValues(1).getOrElse(0D),
                        transferValues(8),
                        noneInt,
                        transferValues(9),
                        noneInt,
                        noneDouble))
                    case _ =>
                      Query((marketFile.market,
                        date,
                        companyCode,
                        values(1),
                        transferValues(5).get.toLong,
                        transferValues(7).get.toInt,
                        transferValues(6).get.toLong,
                        transferValues(2),
                        transferValues(3),
                        transferValues(4),
                        transferValues.head,
                        transferValues(1).getOrElse(0D),
                        transferValues(8),
                        transferValues(9).map(_.toInt),
                        transferValues(10),
                        transferValues(11).map(_.toInt),
                        noneDouble))
                  }
                  //val exists = dailyQuotes.filter(d => d.market === marketFile.market && d.date === date && d.companyCode === companyCode).exists
                  //val selectExpression = query.filterNot(_ => exists)
                  //dailyQuote.map(d => (d.market, d.date, d.companyCode, d.companyName, d.tradeVolume, d.transaction, d.tradeValue, d.openingPrice, d.highestPrice, d.lowestPrice, d.closingPrice, d.change, d.lastBestBidPrice, d.lastBestBidVolume, d.lastBestAskPrice, d.lastBestAskVolume, d.priceEarningRatio)).forceInsertQuery(selectExpression)
                  dailyQuote.map(d => (d.market, d.date, d.companyCode, d.companyName, d.tradeVolume, d.transaction, d.tradeValue, d.openingPrice, d.highestPrice, d.lowestPrice, d.closingPrice, d.change, d.lastBestBidPrice, d.lastBestBidVolume, d.lastBestAskPrice, d.lastBestAskVolume, d.priceEarningRatio)).forceInsertQuery(query)
              }
        }

        dbRun(dbIOActions)
        reader.close()
        pb.step()
    }
    pb.stop()
  }

  def readIndex(): Unit = {
    val db = Database.forConfig("db")
    val index = TableQuery[Index]
    val query = index.map(i => (i.market, i.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }
    db.close()

    val files = IndexSetting().getMarketFiles.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
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
        val dbIOActions = marketFile.market match {
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

                val query = Query((marketFile.market,
                  date,
                  name,
                  close,
                  change,
                  changePercentage))
                //val exists = index.filter(i => i.market === marketFile.market && i.date === date && i.name === name).exists
                //val selectExpression = query.filterNot(_ => exists)
                //index.map(i => (i.market, i.date, i.name, i.close, i.change, i.changePercentage)).forceInsertQuery(selectExpression)
                index.map(i => (i.market, i.date, i.name, i.close, i.change, i.changePercentage)).forceInsertQuery(query)
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size == 4 && row.head != "指數" && row.head != "報酬指數").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
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

                val query = Query((marketFile.market,
                  date,
                  name,
                  close,
                  change,
                  changePercentage))
                //val exists = index.filter(i => i.market === marketFile.market && i.date === date && i.name === name).exists
                //val selectExpression = query.filterNot(_ => exists)
                //index.map(i => (i.market, i.date, i.name, i.close, i.change, i.changePercentage)).forceInsertQuery(selectExpression)
                index.map(i => (i.market, i.date, i.name, i.close, i.change, i.changePercentage)).forceInsertQuery(query)
            }
        }

        dbRun(dbIOActions)
        reader.close()
        pb.step()
    }
    pb.stop()
  }

  def readExRightDividend(): Unit = {
    val db = Database.forConfig("db")
    val exRightDividend = TableQuery[ExRightDividend]
    val query = exRightDividend.map(e => (e.market, e.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }
    db.close()

    val files = ExRightDividendSetting().getMarketFiles.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
    val pb = new ProgressBar("Read ex-right dividend -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read ex-right dividend of ${marketFile.market}-${marketFile.file.name}")
        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val dbIOActions = marketFile.market match {
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
                val query = Query(marketFile.market,
                  date,
                  companyCode,
                  values(2),
                  values(3).toDouble,
                  values(4).toDouble,
                  values(5).toDouble,
                  values(6),
                  values(7).toDouble,
                  values(8).toDouble,
                  values(9).toDouble,
                  values(10).toDouble)
                //val exists = exRightDividend.filter(e => e.market === marketFile.market && e.date === date && e.companyCode === companyCode).exists
                //val selectExpression = query.filterNot(_ => exists)
                //exRightDividend.map(e => (e.market, e.date, e.companyCode, e.companyName, e.closingPriceBeforeExRightExDividend, e.exRightExDividendReferencePrice, e.cashDividend, e.rightOrDividend, e.limitUp, e.limitDown, e.openingReferencePrice, e.exDividendReferencePrice)).forceInsertQuery(selectExpression)
                exRightDividend.map(e => (e.market, e.date, e.companyCode, e.companyName, e.closingPriceBeforeExRightExDividend, e.exRightExDividendReferencePrice, e.cashDividend, e.rightOrDividend, e.limitUp, e.limitDown, e.openingReferencePrice, e.exDividendReferencePrice)).forceInsertQuery(query)
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size == 22 && row.head != "除權息日期").init.map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val date = LocalDate.parse(values.head, minguoDateTimeFormatter)
                val companyCode = values(1)
                //"資料日期","股票代號","股票名稱","除權息前收盤價","除權息參考價","權值+息值","權/息","漲停價格","跌停價格","開盤競價基準","減除股利參考價","詳細資料","最近一次申報資料 季別/日期","最近一次申報每股 (單位)淨值","最近一次申報每股 (單位)盈餘",
                //除權息日期,代號,名稱, 除權息前收盤價, 除權息參考價,權值,息值,權值+息值,權/息,漲停價,跌停價,開始交易基準價,減除股利參考價,現金股利,每仟股無償配股,員工紅利轉增資,現金增資股數,現金增資認購價,公開承銷股數,員工認購股數,原股東認購股數,按持股比例仟股認購
                val query = Query(marketFile.market,
                  date,
                  companyCode,
                  values(2),
                  values(3).toDouble,
                  values(4).toDouble,
                  values(7).toDouble,
                  values(8) match {
                    case "除權" => "權"
                    case "除息" => "息"
                    case "除權息" => "權息"
                  },
                  values(9).toDouble,
                  values(10).toDouble,
                  values(11).toDouble,
                  values(12).toDouble)
                //val exists = exRightDividend.filter(e => e.market === marketFile.market && e.date === date && e.companyCode === companyCode).exists
                //val selectExpression = query.filterNot(_ => exists)
                //exRightDividend.map(e => (e.market, e.date, e.companyCode, e.companyName, e.closingPriceBeforeExRightExDividend, e.exRightExDividendReferencePrice, e.cashDividend, e.rightOrDividend, e.limitUp, e.limitDown, e.openingReferencePrice, e.exDividendReferencePrice)).forceInsertQuery(selectExpression)
                exRightDividend.map(e => (e.market, e.date, e.companyCode, e.companyName, e.closingPriceBeforeExRightExDividend, e.exRightExDividendReferencePrice, e.cashDividend, e.rightOrDividend, e.limitUp, e.limitDown, e.openingReferencePrice, e.exDividendReferencePrice)).forceInsertQuery(query)
            }
        }

        dbRun(dbIOActions)
        reader.close()
        pb.step()
    }
    pb.stop()
  }

  def readCapitalReduction(): Unit = {
    val db = Database.forConfig("db")
    val capitalReduction = TableQuery[CapitalReduction]
    val query = capitalReduction.map(c => (c.market, c.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }
    db.close()

    val files = CapitalReductionSetting().getMarketFiles.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
    val pb = new ProgressBar("Read capital reduction -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read capital reduction of ${marketFile.market}-${marketFile.file.name}")
        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val dbIOActions = marketFile.market match {
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
                val query = Query(marketFile.market,
                  date,
                  companyCode,
                  values(2),
                  values(3).toDouble,
                  values(4).toDouble,
                  values(5).toDouble,
                  values(6).toDouble,
                  values(7).toDouble,
                  if (values(8) == "--") None else Some(values(8).toDouble),
                  values(9))
                //val exists = capitalReduction.filter(c => c.market === marketFile.market && c.date === date && c.companyCode === companyCode).exists
                //val selectExpression = query.filterNot(_ => exists)
                //capitalReduction.map(c => (c.market, c.date, c.companyCode, c.companyName, c.closingPriceOnTheLastTradingDate, c.postReductionReferencePrice, c.limitUp, c.limitDown, c.openingReferencePrice, c.exRightReferencePrice, c.reasonForCapitalReduction)).forceInsertQuery(selectExpression)
                capitalReduction.map(c => (c.market, c.date, c.companyCode, c.companyName, c.closingPriceOnTheLastTradingDate, c.postReductionReferencePrice, c.limitUp, c.limitDown, c.openingReferencePrice, c.exRightReferencePrice, c.reasonForCapitalReduction)).forceInsertQuery(query)
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
                //"恢復買賣日期","股票代號","名稱","停止買賣前收盤價格","恢復買賣參考價","漲停價格","跌停價格","開盤競價基準","除權參考價","減資原因","詳細資料",
                //恢復買賣日期 ,股票代號,名稱,最後交易日之收盤價格,減資恢復買賣開始日參考價格,漲停價格,跌停價格,開始交易基準價,除權參考價,減資原因
                val query = Query(marketFile.market,
                  date,
                  companyCode,
                  values(2),
                  values(3).toDouble,
                  values(4).toDouble,
                  values(5).toDouble,
                  values(6).toDouble,
                  values(7).toDouble,
                  Option(values(8).toDouble),
                  values(9))
                //val exists = capitalReduction.filter(c => c.market === marketFile.market && c.date === date && c.companyCode === companyCode).exists
                //val selectExpression = query.filterNot(_ => exists)
                //capitalReduction.map(c => (c.market, c.date, c.companyCode, c.companyName, c.closingPriceOnTheLastTradingDate, c.postReductionReferencePrice, c.limitUp, c.limitDown, c.openingReferencePrice, c.exRightReferencePrice, c.reasonForCapitalReduction)).forceInsertQuery(selectExpression)
                capitalReduction.map(c => (c.market, c.date, c.companyCode, c.companyName, c.closingPriceOnTheLastTradingDate, c.postReductionReferencePrice, c.limitUp, c.limitDown, c.openingReferencePrice, c.exRightReferencePrice, c.reasonForCapitalReduction)).forceInsertQuery(query)
            }
        }

        dbRun(dbIOActions)
        reader.close()
        pb.step()
    }
    pb.stop()
  }

  def readDailyTradingDetails(): Unit = {
    val db = Database.forConfig("db")
    val dailyTradingDetails = TableQuery[DailyTradingDetails]
    val query = dailyTradingDetails.map(d => (d.market, d.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }
    db.close()

    val files = DailyTradingDetailsSetting().getMarketFiles.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
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
        val dbIOActions = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size >= 13 && row.head != "證券代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val splitValues = values.splitAt(2)
                val transferValues = splitValues._2.init.map(value => Try(value.toInt).getOrElse(0))
                val companyCode = values.head
                val noneInt: Option[Int] = None
                val query = values.size match {
                  case 13 =>
                    //                    Query(marketFile.market :: date :: companyCode :: values(1) ::
                    //                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                    //                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                    //                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                    //                      transferValues(6) :: transferValues(7) :: transferValues(8) :: transferValues(9) :: HNil)
                    marketFile.market :: date :: companyCode :: values(1) :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues(6) :: transferValues(7) :: transferValues(8) :: transferValues(9) :: HNil
                  case 17 =>
                    //                    Query(marketFile.market :: date :: companyCode :: values(1) ::
                    //                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                    //                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                    //                      Option(transferValues(7)) :: Option(transferValues(8)) :: Option(transferValues(9)) :: Option(transferValues(10)) :: Option(transferValues(11)) :: Option(transferValues(12)) ::
                    //                      transferValues(7) + transferValues(10) :: transferValues(8) + transferValues(11) :: transferValues(6) :: transferValues(13) :: HNil)
                    marketFile.market :: date :: companyCode :: values(1) ::
                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                      Option(transferValues(7)) :: Option(transferValues(8)) :: Option(transferValues(9)) :: Option(transferValues(10)) :: Option(transferValues(11)) :: Option(transferValues(12)) ::
                      transferValues(7) + transferValues(10) :: transferValues(8) + transferValues(11) :: transferValues(6) :: transferValues(13) :: HNil
                  case _ =>
                    //                    Query(marketFile.market :: date :: companyCode :: values(1) ::
                    //                      Option(transferValues.head) :: Option(transferValues(1)) :: Option(transferValues(2)) :: Option(transferValues(3)) :: Option(transferValues(4)) :: Option(transferValues(5)) ::
                    //                      transferValues.head + transferValues(3) :: transferValues(1) + transferValues(4) :: transferValues(2) + transferValues(5) :: transferValues(6) :: transferValues(7) :: transferValues(8) ::
                    //                      Option(transferValues(10)) :: Option(transferValues(11)) :: Option(transferValues(12)) :: Option(transferValues(13)) :: Option(transferValues(14)) :: Option(transferValues(15)) ::
                    //                      transferValues(10) + transferValues(13) :: transferValues(11) + transferValues(14) :: transferValues(9) :: transferValues(16) :: HNil)
                    marketFile.market :: date :: companyCode :: values(1) ::
                      Option(transferValues.head) :: Option(transferValues(1)) :: Option(transferValues(2)) :: Option(transferValues(3)) :: Option(transferValues(4)) :: Option(transferValues(5)) ::
                      transferValues.head + transferValues(3) :: transferValues(1) + transferValues(4) :: transferValues(2) + transferValues(5) :: transferValues(6) :: transferValues(7) :: transferValues(8) ::
                      Option(transferValues(10)) :: Option(transferValues(11)) :: Option(transferValues(12)) :: Option(transferValues(13)) :: Option(transferValues(14)) :: Option(transferValues(15)) ::
                      transferValues(10) + transferValues(13) :: transferValues(11) + transferValues(14) :: transferValues(9) :: transferValues(16) :: HNil
                }
                //val exists = dailyTradingDetails.filter(d => d.market === marketFile.market && d.date === date && d.companyCode === companyCode).exists
                //val selectExpression = query.filterNot(_ => exists)
                //dailyTradingDetails.map(d => d.market :: d.date :: d.companyCode :: d.companyName :: d.foreignInvestorsExcludeDealersTotalBuy :: d.foreignInvestorsExcludeDealersTotalSell :: d.foreignInvestorsExcludeDealersDifference :: d.foreignDealersTotalBuy :: d.foreignDealersTotalSell :: d.foreignDealersDifference :: d.foreignInvestorsTotalBuy :: d.foreignInvestorsTotalSell :: d.foreignInvestorsDifference :: d.securitiesInvestmentTrustCompaniesTotalBuy :: d.securitiesInvestmentTrustCompaniesTotalSell :: d.securitiesInvestmentTrustCompaniesDifference :: d.dealersProprietaryTotalBuy :: d.dealersProprietaryTotalSell :: d.dealersProprietaryDifference :: d.dealersHedgeTotalBuy :: d.dealersHedgeTotalSell :: d.dealersHedgeDifference :: d.dealersTotalBuy :: d.dealersTotalSell :: d.dealersDifference :: d.totalDifference :: HNil).forceInsertQuery(selectExpression)
                //dailyTradingDetails.map(d => d.market :: d.date :: d.companyCode :: d.companyName :: d.foreignInvestorsExcludeDealersTotalBuy :: d.foreignInvestorsExcludeDealersTotalSell :: d.foreignInvestorsExcludeDealersDifference :: d.foreignDealersTotalBuy :: d.foreignDealersTotalSell :: d.foreignDealersDifference :: d.foreignInvestorsTotalBuy :: d.foreignInvestorsTotalSell :: d.foreignInvestorsDifference :: d.securitiesInvestmentTrustCompaniesTotalBuy :: d.securitiesInvestmentTrustCompaniesTotalSell :: d.securitiesInvestmentTrustCompaniesDifference :: d.dealersProprietaryTotalBuy :: d.dealersProprietaryTotalSell :: d.dealersProprietaryDifference :: d.dealersHedgeTotalBuy :: d.dealersHedgeTotalSell :: d.dealersHedgeDifference :: d.dealersTotalBuy :: d.dealersTotalSell :: d.dealersDifference :: d.totalDifference :: HNil).forceInsertQuery(query)
                query
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size >= 12 && row.head != "代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val splitValues = values.splitAt(2)
                val transferValues = splitValues._2.map(value => Try(value.toInt).getOrElse(0))
                val companyCode = values.head
                val noneInt: Option[Int] = None
                val query = values.size match {
                  case 12 =>
                    //                    Query(marketFile.market :: date :: companyCode :: values(1) ::
                    //                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                    //                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                    //                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                    //                      transferValues(6) :: transferValues(7) :: transferValues(6) :: transferValues(9) :: HNil)
                    marketFile.market :: date :: companyCode :: values(1) ::
                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues(6) :: transferValues(7) :: transferValues(6) :: transferValues(9) :: HNil
                  case 16 =>
                    //                    Query(marketFile.market :: date :: companyCode :: values(1) ::
                    //                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                    //                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                    //                      Option(transferValues(7)) :: Option(transferValues(8)) :: Option(transferValues(9)) :: Option(transferValues(10)) :: Option(transferValues(11)) :: Option(transferValues(12)) ::
                    //                      transferValues(7) + transferValues(10) :: transferValues(8) + transferValues(11) :: transferValues(6) :: transferValues(13) :: HNil)
                    marketFile.market :: date :: companyCode :: values(1) ::
                      noneInt :: noneInt :: noneInt :: noneInt :: noneInt :: noneInt ::
                      transferValues.head :: transferValues(1) :: transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) ::
                      Option(transferValues(7)) :: Option(transferValues(8)) :: Option(transferValues(9)) :: Option(transferValues(10)) :: Option(transferValues(11)) :: Option(transferValues(12)) ::
                      transferValues(7) + transferValues(10) :: transferValues(8) + transferValues(11) :: transferValues(6) :: transferValues(13) :: HNil
                  case _ =>
                    //                    Query(marketFile.market :: date :: companyCode :: values(1) ::
                    //                      Option(transferValues.head) :: Option(transferValues(1)) :: Option(transferValues(2)) :: Option(transferValues(3)) :: Option(transferValues(4)) :: Option(transferValues(5)) ::
                    //                      transferValues(6) :: transferValues(7) :: transferValues(8) :: transferValues(9) :: transferValues(10) :: transferValues(11) ::
                    //                      Option(transferValues(12)) :: Option(transferValues(13)) :: Option(transferValues(14)) :: Option(transferValues(15)) :: Option(transferValues(16)) :: Option(transferValues(17)) ::
                    //                      transferValues(18) :: transferValues(19) :: transferValues(20) :: transferValues(21) :: HNil)
                    marketFile.market :: date :: companyCode :: values(1) ::
                      Option(transferValues.head) :: Option(transferValues(1)) :: Option(transferValues(2)) :: Option(transferValues(3)) :: Option(transferValues(4)) :: Option(transferValues(5)) ::
                      transferValues(6) :: transferValues(7) :: transferValues(8) :: transferValues(9) :: transferValues(10) :: transferValues(11) ::
                      Option(transferValues(12)) :: Option(transferValues(13)) :: Option(transferValues(14)) :: Option(transferValues(15)) :: Option(transferValues(16)) :: Option(transferValues(17)) ::
                      transferValues(18) :: transferValues(19) :: transferValues(20) :: transferValues(21) :: HNil
                }
                //                val exists = dailyTradingDetails.filter(d => d.market === marketFile.market && d.date === date && d.companyCode === companyCode).exists
                //                val selectExpression = query.filterNot(_ => exists)
                //                dailyTradingDetails.map(d => d.market :: d.date :: d.companyCode :: d.companyName :: d.foreignInvestorsExcludeDealersTotalBuy :: d.foreignInvestorsExcludeDealersTotalSell :: d.foreignInvestorsExcludeDealersDifference :: d.foreignDealersTotalBuy :: d.foreignDealersTotalSell :: d.foreignDealersDifference :: d.foreignInvestorsTotalBuy :: d.foreignInvestorsTotalSell :: d.foreignInvestorsDifference :: d.securitiesInvestmentTrustCompaniesTotalBuy :: d.securitiesInvestmentTrustCompaniesTotalSell :: d.securitiesInvestmentTrustCompaniesDifference :: d.dealersProprietaryTotalBuy :: d.dealersProprietaryTotalSell :: d.dealersProprietaryDifference :: d.dealersHedgeTotalBuy :: d.dealersHedgeTotalSell :: d.dealersHedgeDifference :: d.dealersTotalBuy :: d.dealersTotalSell :: d.dealersDifference :: d.totalDifference :: HNil).forceInsertQuery(selectExpression)
                //dailyTradingDetails.map(d => d.market :: d.date :: d.companyCode :: d.companyName :: d.foreignInvestorsExcludeDealersTotalBuy :: d.foreignInvestorsExcludeDealersTotalSell :: d.foreignInvestorsExcludeDealersDifference :: d.foreignDealersTotalBuy :: d.foreignDealersTotalSell :: d.foreignDealersDifference :: d.foreignInvestorsTotalBuy :: d.foreignInvestorsTotalSell :: d.foreignInvestorsDifference :: d.securitiesInvestmentTrustCompaniesTotalBuy :: d.securitiesInvestmentTrustCompaniesTotalSell :: d.securitiesInvestmentTrustCompaniesDifference :: d.dealersProprietaryTotalBuy :: d.dealersProprietaryTotalSell :: d.dealersProprietaryDifference :: d.dealersHedgeTotalBuy :: d.dealersHedgeTotalSell :: d.dealersHedgeDifference :: d.dealersTotalBuy :: d.dealersTotalSell :: d.dealersDifference :: d.totalDifference :: HNil).forceInsertQuery(query)
                query
            }
        }

        val dbio = dailyTradingDetails.map(d => d.market :: d.date :: d.companyCode :: d.companyName :: d.foreignInvestorsExcludeDealersTotalBuy :: d.foreignInvestorsExcludeDealersTotalSell :: d.foreignInvestorsExcludeDealersDifference :: d.foreignDealersTotalBuy :: d.foreignDealersTotalSell :: d.foreignDealersDifference :: d.foreignInvestorsTotalBuy :: d.foreignInvestorsTotalSell :: d.foreignInvestorsDifference :: d.securitiesInvestmentTrustCompaniesTotalBuy :: d.securitiesInvestmentTrustCompaniesTotalSell :: d.securitiesInvestmentTrustCompaniesDifference :: d.dealersProprietaryTotalBuy :: d.dealersProprietaryTotalSell :: d.dealersProprietaryDifference :: d.dealersHedgeTotalBuy :: d.dealersHedgeTotalSell :: d.dealersHedgeDifference :: d.dealersTotalBuy :: d.dealersTotalSell :: d.dealersDifference :: d.totalDifference :: HNil) ++= dbIOActions
        val db = Database.forConfig("db")
        try {
          val resultFuture = db.run(dbio)
          Await.result(resultFuture, Duration.Inf)
        } finally db.close
        //dbRun(dbIOActions)
        reader.close()
        pb.step()
    }
    pb.stop()
  }

  def readMarginTransactions(): Unit = {
    val db = Database.forConfig("db")
    val marginTransactions = TableQuery[MarginTransactions]
    val query = marginTransactions.map(m => (m.market, m.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }
    db.close()

    val files = MarginTransactionsSetting().getMarketFiles.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name))).par
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
        val dbIOActions = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size == 17 && row.head != "" && row.head != "股票代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                val query = Query(marketFile.market,
                  date,
                  companyCode,
                  values(1),
                  values(2).toInt,
                  values(3).toInt,
                  values(4).toInt,
                  values(5).toInt,
                  values(6).toInt,
                  values(7).toInt,
                  values(8).toInt,
                  values(9).toInt,
                  values(10).toInt,
                  values(11).toInt,
                  values(12).toInt,
                  values(13).toInt,
                  values(14).toInt)
                //val exists = marginTransactions.filter(m => m.market === marketFile.market && m.date === date && m.companyCode === companyCode).exists
                //val selectExpression = query.filterNot(_ => exists)
                //marginTransactions.map(m => (m.market, m.date, m.companyCode, m.companyName, m.marginPurchase, m.marginSales, m.cashRedemption, m.marginBalanceOfPreviousDay, m.marginBalanceOfTheDay, m.marginQuota, m.shortCovering, m.shortSale, m.stockRedemption, m.shortBalanceOfPreviousDay, m.shortBalanceOfTheDay, m.shortQuota, m.offsettingOfMarginPurchasesAndShortSales)).forceInsertQuery(selectExpression)
                marginTransactions.map(m => (m.market, m.date, m.companyCode, m.companyName, m.marginPurchase, m.marginSales, m.cashRedemption, m.marginBalanceOfPreviousDay, m.marginBalanceOfTheDay, m.marginQuota, m.shortCovering, m.shortSale, m.stockRedemption, m.shortBalanceOfPreviousDay, m.shortBalanceOfTheDay, m.shortQuota, m.offsettingOfMarginPurchasesAndShortSales)).forceInsertQuery(query)
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size == 20 && row.head != "代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                val query = Query(marketFile.market,
                  date,
                  companyCode,
                  values(1),
                  values(3).toInt,
                  values(4).toInt,
                  values(5).toInt,
                  values(2).toInt,
                  values(6).toInt,
                  Try(values(9).toInt).getOrElse(values(8).toInt),
                  values(12).toInt,
                  values(11).toInt,
                  values(13).toInt,
                  values(10).toInt,
                  values(14).toInt,
                  values(17).toInt,
                  Try(values(18).toInt).orElse(Try(values(19).toInt)).getOrElse(0))
                //val exists = marginTransactions.filter(m => m.market === marketFile.market && m.date === date && m.companyCode === companyCode).exists
                //val selectExpression = query.filterNot(_ => exists)
                //marginTransactions.map(m => (m.market, m.date, m.companyCode, m.companyName, m.marginPurchase, m.marginSales, m.cashRedemption, m.marginBalanceOfPreviousDay, m.marginBalanceOfTheDay, m.marginQuota, m.shortCovering, m.shortSale, m.stockRedemption, m.shortBalanceOfPreviousDay, m.shortBalanceOfTheDay, m.shortQuota, m.offsettingOfMarginPurchasesAndShortSales)).forceInsertQuery(selectExpression)
                marginTransactions.map(m => (m.market, m.date, m.companyCode, m.companyName, m.marginPurchase, m.marginSales, m.cashRedemption, m.marginBalanceOfPreviousDay, m.marginBalanceOfTheDay, m.marginQuota, m.shortCovering, m.shortSale, m.stockRedemption, m.shortBalanceOfPreviousDay, m.shortBalanceOfTheDay, m.shortQuota, m.offsettingOfMarginPurchasesAndShortSales)).forceInsertQuery(query)
            }
        }

        dbRun(dbIOActions)
        reader.close()
        pb.step()
    }
    pb.stop()
  }

  def readStockPER_PBR_DividendYield(): Unit = {
    val db = Database.forConfig("db")
    val stockPER_PBR_DividendYield = TableQuery[StockPER_PBR_DividendYield]
    val query = stockPER_PBR_DividendYield.map(s => (s.market, s.date)).distinct.result
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_M_d")
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf).map { case (market, date) => (market, date.format(dateTimeFormatter) + ".csv") }
    db.close()

    val files = StockPER_PBR_DividendYieldSetting().getMarketFiles.filterNot(m => dataAlreadyInDB.contains(m.market, m.file.name)).par
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
        val dbIOActions = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size >= 5 && row.head != "證券代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                val dividendYield = if (values.size == 6) values(3).toDouble else values(2).toDouble
                val query = values.size match {
                  case 6 =>
                    Query(marketFile.market,
                      date,
                      companyCode,
                      values(1),
                      values(2) match {
                        case "-" => None
                        case _ => Some(values(2).toDouble)
                      },
                      values(4) match {
                        case "-" => None
                        case _ => Some(values(4).toDouble)
                      },
                      dividendYield)
                  case _ =>
                    Query(marketFile.market,
                      date,
                      companyCode,
                      values(1),
                      values(4) match {
                        case "-" => None
                        case _ => Some(values(4).toDouble)
                      },
                      values(5) match {
                        case "-" => None
                        case _ => Some(values(5).toDouble)
                      },
                      dividendYield)
                }
                //val exists = stockPER_PBR_DividendYield.filter(s => s.market === marketFile.market && s.date === date && s.companyCode === companyCode).exists
                //val selectExpression = query.filterNot(_ => exists)
                //stockPER_PBR_DividendYield.map(s => (s.market, s.date, s.companyCode, s.companyName, s.priceToEarningRatio, s.priceBookRatio, s.dividendYield)).forceInsertQuery(selectExpression)
                stockPER_PBR_DividendYield.map(s => (s.market, s.date, s.companyCode, s.companyName, s.priceToEarningRatio, s.priceBookRatio, s.dividendYield)).forceInsertQuery(query)
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size == 7 && row.head != "股票代號").map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val companyCode = values.head
                val query = Query(marketFile.market,
                  date,
                  companyCode,
                  values(1),
                  values(2) match {
                    case "N/A" => None
                    case _ => Some(values(2).toDouble)
                  },
                  values(6) match {
                    case "N/A" => None
                    case _ => Some(values(6).toDouble)
                  },
                  values(5).toDouble)
                //val exists = stockPER_PBR_DividendYield.filter(s => s.market === marketFile.market && s.date === date && s.companyCode === companyCode).exists
                //val selectExpression = query.filterNot(_ => exists)
                //stockPER_PBR_DividendYield.map(s => (s.market, s.date, s.companyCode, s.companyName, s.priceToEarningRatio, s.priceBookRatio, s.dividendYield)).forceInsertQuery(selectExpression)
                stockPER_PBR_DividendYield.map(s => (s.market, s.date, s.companyCode, s.companyName, s.priceToEarningRatio, s.priceBookRatio, s.dividendYield)).forceInsertQuery(query)
            }
        }

        dbRun(dbIOActions)
        reader.close()
        pb.step()
    }
    pb.stop()
  }

  private def dbRun(dbIOActions: Seq[FixedSqlAction[Int, NoStream, Effect.Write]]): Seq[Int] = {
    val db = Database.forConfig("db")
    try {
      val resultFuture = db.run(DBIO.sequence(dbIOActions))
      Await.result(resultFuture, Duration.Inf)
    } finally db.close
  }
}