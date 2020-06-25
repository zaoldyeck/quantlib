import java.time.LocalDate
import java.time.chrono.MinguoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import Settings._
import com.github.tototoshi.csv._
import db.table.{CapitalReduction, _}
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import slick.collection.heterogeneous.HNil
import slick.lifted.TableQuery
import util.QuantlibCSVReader
//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import slick.jdbc.H2Profile.api._

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class Reader {
  def readFinancialAnalysis(): Unit = {
    FinancialAnalysisSetting().getMarketFiles.par.foreach {
      marketFile =>
        println(s"Read financial analysis of ${marketFile.file.name}")
        val reader = CSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val year = marketFile.file.name.split('_').head.toInt
        val rows = reader.all().tail

        val financialAnalysis = TableQuery[FinancialAnalysis]
        val dbIOActions = rows.map {
          values =>
            val splitValues = values.splitAt(2)
            val transferValues = splitValues._2.map {
              case v if v == "NA" => None
              case v if v.contains("*") => None
              case value => Some(value.toDouble)
            }
            val companyCode = values.head
            val query = Query((marketFile.market :: year :: companyCode :: values(1) :: transferValues.head :: transferValues(1) ::
              transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) :: transferValues(6) :: transferValues(7) ::
              transferValues(8) :: transferValues(9) :: transferValues(10) :: transferValues(11) :: transferValues(12) :: transferValues(13) ::
              transferValues(14) :: transferValues(15) :: transferValues(16) :: transferValues(17) :: transferValues(18) :: HNil))
            val exists = financialAnalysis.filter(f => f.market === marketFile.market && f.year === year && f.companyCode === companyCode).exists
            val selectExpression = query.filterNot(_ => exists)
            financialAnalysis.map(f => (f.market :: f.year :: f.companyCode :: f.companyName :: f.liabilitiesOfAssetsRatioPercentage :: f.longTermFundsToPropertyAndPlantAndEquipmentPercentage :: f.currentRatioPercentage :: f.quickRatioPercentage :: f.timesInterestEarnedRatioPercentage :: f.averageCollectionTurnoverTimes :: f.averageCollectionDays :: f.averageInventoryTurnoverTimes :: f.averageInventoryDays :: f.propertyAndPlantAndEquipmentTurnoverTimes :: f.totalAssetsTurnoverTimes :: f.returnOnTotalAssetsPercentage :: f.returnOnEquityPercentage :: f.profitBeforeTaxToCapitalPercentage :: f.profitToSalesPercentage :: f.earningsPerShareNTD :: f.cashFlowRatioPercentage :: f.cashFlowAdequacyRatioPercentage :: f.cashFlowReinvestmentRatioPercentage :: HNil)).forceInsertQuery(selectExpression)
        }

        val db = Database.forConfig("db")
        try {
          val resultFuture = db.run(DBIO.sequence(dbIOActions))
          Await.result(resultFuture, Duration.Inf)
        } finally db.close
        reader.close()
    }
  }

  def readOperatingRevenue(): Unit = {
    val browser = JsoupBrowser()

    OperatingRevenueSetting().getMarketFiles.par.foreach {
      marketFile =>
        println(s"Read operating revenue of ${marketFile.file.name}")
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

        val operatingRevenues = TableQuery[OperatingRevenue]
        val dbIOActions = data.map {
          d =>
            val query = Query(d)
            val exists = operatingRevenues.filter(o => o.market === marketFile.market && o.year === year && o.month === month && o.companyCode === d._4).exists
            val selectExpression = query.filterNot(_ => exists)
            operatingRevenues.map(o => (o.market, o.year, o.month, o.companyCode, o.companyName, o.industry, o.monthlyRevenue, o.lastMonthRevenue, o.lastYearMonthlyRevenue, o.monthlyRevenueComparedLastMonthPercentage, o.monthlyRevenueComparedLastYearPercentage, o.cumulativeRevenue, o.lastYearCumulativeRevenue, o.cumulativeRevenueComparedLastYearPercentage)).forceInsertQuery(selectExpression)
        }

        val db = Database.forConfig("db")
        try {
          val resultFuture = db.run(DBIO.sequence(dbIOActions))
          Await.result(resultFuture, Duration.Inf)
        } finally db.close
    }
  }

  def readDailyQuote(): Unit = {
    DailyQuoteSetting().getMarketFiles.par.foreach {
      marketFile =>
        println(s"Read daily quote of ${marketFile.file.name}")
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)

        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val dailyQuotes = TableQuery[DailyQuote]

        val dbIOActions = marketFile.market match {
          case "twse" =>
            val rows = reader.all().dropWhile(_.head != "證券代號").tail.map(_.map(_.replace(",", "")))
            rows.map {
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
                val exists = dailyQuotes.filter(d => d.market === marketFile.market && d.date === date && d.companyCode === companyCode).exists
                val selectExpression = query.filterNot(_ => exists)
                dailyQuotes.map(d => (d.market, d.date, d.companyCode, d.companyName, d.tradeVolume, d.transaction, d.tradeValue, d.openingPrice, d.highestPrice, d.lowestPrice, d.closingPrice, d.change, d.lastBestBidPrice, d.lastBestBidVolume, d.lastBestAskPrice, d.lastBestAskVolume, d.priceEarningRatio)).forceInsertQuery(selectExpression)
            }
          case "tpex" =>
            val rows = reader.all().dropWhile(_.head != "代號")
            if (rows.isEmpty) Seq.empty else
              rows.init.tail.map(_.map(_.replace(" ", "").replace(",", ""))).map {
                values =>
                  println(values)
                  val columnSize = values.size
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
                  val query = Query((marketFile.market,
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
                    if (columnSize < 17) noneInt else transferValues(9).map(_.toInt),
                    if (columnSize < 17) transferValues(9) else transferValues(10),
                    if (columnSize < 17) noneInt else transferValues(11).map(_.toInt),
                    noneDouble))
                  val exists = dailyQuotes.filter(d => d.market === marketFile.market && d.date === date && d.companyCode === companyCode).exists
                  val selectExpression = query.filterNot(_ => exists)
                  dailyQuotes.map(d => (d.market, d.date, d.companyCode, d.companyName, d.tradeVolume, d.transaction, d.tradeValue, d.openingPrice, d.highestPrice, d.lowestPrice, d.closingPrice, d.change, d.lastBestBidPrice, d.lastBestBidVolume, d.lastBestAskPrice, d.lastBestAskVolume, d.priceEarningRatio)).forceInsertQuery(selectExpression)
              }
        }

        val db = Database.forConfig("db")
        try {
          val resultFuture = db.run(DBIO.sequence(dbIOActions))
          Await.result(resultFuture, Duration.Inf)
        } finally db.close
        reader.close()
    }
  }

  def readIndex(): Unit = {
    IndexSetting().getMarketFiles.par.foreach {
      marketFile =>
        println(s"Read index of ${marketFile.file.name}")
        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)

        val rows = reader.all().filter(row => row.size == 7 && row.head != "指數" && row.head != "報酬指數").map(_.map(_.replace(",", "")))
        val indices = TableQuery[Index]
        val dbIOActions = rows.map {
          values =>
            val index = values.head
            val close = values(1) match {
              case "--" => None
              case value => Some(value.toDouble)
            }
            val change = values(2) match {
              case "-" => -values(3).toDouble
              case "" => 0
              case "+" => values(3).toDouble
            }
            val changePercentage = values(4) match {
              case "--" => 0
              case value => value.toDouble
            }

            val query = Query((date,
              index,
              close,
              change,
              changePercentage))
            val exists = indices.filter(i => i.date === date && i.index === index).exists
            val selectExpression = query.filterNot(_ => exists)
            indices.map(i => (i.date, i.index, i.close, i.change, i.changePercentage)).forceInsertQuery(selectExpression)
        }

        val db = Database.forConfig("db")
        try {
          val resultFuture = db.run(DBIO.sequence(dbIOActions))
          Await.result(resultFuture, Duration.Inf)
        } finally db.close
        reader.close()
    }
  }

  def readExRightDividend(): Unit = {
    ExRightDividendSetting().getMarketFiles.par.foreach {
      marketFile =>
        println(s"Read ex-right dividend of ${marketFile.file.name}")
        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val exRightDividends = TableQuery[ExRightDividend]

        val dbIOActions = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size == 16 && row.head != "資料日期").map(_.map(_.replace(",", "")))
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
                val exists = exRightDividends.filter(e => e.market === marketFile.market && e.date === date && e.companyCode === companyCode).exists
                val selectExpression = query.filterNot(_ => exists)
                exRightDividends.map(e => (e.market, e.date, e.companyCode, e.companyName, e.closingPriceBeforeExRightExDividend, e.exRightExDividendReferencePrice, e.cashDividend, e.rightOrDividend, e.limitUp, e.limitDown, e.openingReferencePrice, e.exDividendReferencePrice)).forceInsertQuery(selectExpression)
            }
          case "tpex" =>
            val rows = reader.all().filter(row => row.size == 22 && row.head != "除權息日期").init.map(_.map(_.replace(" ", "").replace(",", "")))
            rows.map {
              values =>
                val datePattern = """(\d+)/(\d+)/(\d+)日""".r
                val datePattern(y, m, d) = values.head
                val year = y.toInt + 1911
                val month = m.toInt
                val day = d.toInt
                val date = LocalDate.of(year, month, day)
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
                val exists = exRightDividends.filter(e => e.market === marketFile.market && e.date === date && e.companyCode === companyCode).exists
                val selectExpression = query.filterNot(_ => exists)
                exRightDividends.map(e => (e.market, e.date, e.companyCode, e.companyName, e.closingPriceBeforeExRightExDividend, e.exRightExDividendReferencePrice, e.cashDividend, e.rightOrDividend, e.limitUp, e.limitDown, e.openingReferencePrice, e.exDividendReferencePrice)).forceInsertQuery(selectExpression)
            }
        }

        val db = Database.forConfig("db")
        try {
          val resultFuture = db.run(DBIO.sequence(dbIOActions))
          Await.result(resultFuture, Duration.Inf)
        } finally db.close
        reader.close()
    }
  }

  def readCapitalReduction(): Unit = {
    CapitalReductionSetting().getMarketFiles.filter(_.market == "tpex").par.foreach {
      marketFile =>
        println(s"Read capital reduction of ${marketFile.file.name}")
        val reader = QuantlibCSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val capitalReduction = TableQuery[CapitalReduction]

        val dbIOActions = marketFile.market match {
          case "twse" =>
            val rows = reader.all().filter(row => row.size == 12 && row.head != "恢復買賣日期").map(_.map(_.replace(",", "")))
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
                val exists = capitalReduction.filter(c => c.market === marketFile.market && c.date === date && c.companyCode === companyCode).exists
                val selectExpression = query.filterNot(_ => exists)
                capitalReduction.map(c => (c.market, c.date, c.companyCode, c.companyName, c.closingPriceOnTheLastTradingDate, c.postReductionReferencePrice, c.limitUp, c.limitDown, c.openingReferencePrice, c.exRightReferencePrice, c.reasonForCapitalReduction)).forceInsertQuery(selectExpression)
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
                val exists = capitalReduction.filter(c => c.market === marketFile.market && c.date === date && c.companyCode === companyCode).exists
                val selectExpression = query.filterNot(_ => exists)
                capitalReduction.map(c => (c.market, c.date, c.companyCode, c.companyName, c.closingPriceOnTheLastTradingDate, c.postReductionReferencePrice, c.limitUp, c.limitDown, c.openingReferencePrice, c.exRightReferencePrice, c.reasonForCapitalReduction)).forceInsertQuery(selectExpression)
            }
        }

        val db = Database.forConfig("db")
        try {
          val resultFuture = db.run(DBIO.sequence(dbIOActions))
          Await.result(resultFuture, Duration.Inf)
        } finally db.close
        reader.close()
    }
  }
}