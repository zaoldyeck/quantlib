import java.time.LocalDate

import Settings._
import com.github.tototoshi.csv._
import db.table._
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import slick.collection.heterogeneous.HNil
import slick.lifted.TableQuery
import util.QuantlibCSVReader

import scala.reflect.io.Path._
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
    financialAnalysis.dir.toDirectory.files.toSeq.par.foreach {
      file =>
        println(s"Read financial analysis of ${file.name}")
        val reader = CSVReader.open(file.jfile, "Big5")
        val year = file.name.split('_').head.toInt
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
            val query = Query((year :: companyCode :: values(1) :: transferValues.head :: transferValues(1) ::
              transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) :: transferValues(6) :: transferValues(7) ::
              transferValues(8) :: transferValues(9) :: transferValues(10) :: transferValues(11) :: transferValues(12) :: transferValues(13) ::
              transferValues(14) :: transferValues(15) :: transferValues(16) :: transferValues(17) :: transferValues(18) :: HNil))
            val exists = financialAnalysis.filter(f => f.year === year && f.companyCode === companyCode).exists
            val selectExpression = query.filterNot(_ => exists)
            financialAnalysis.map(f => (f.year :: f.companyCode :: f.companyName :: f.liabilitiesOfAssetsRatioPercentage :: f.longTermFundsToPropertyAndPlantAndEquipmentPercentage :: f.currentRatioPercentage :: f.quickRatioPercentage :: f.timesInterestEarnedRatioPercentage :: f.averageCollectionTurnoverTimes :: f.averageCollectionDays :: f.averageInventoryTurnoverTimes :: f.averageInventoryDays :: f.propertyAndPlantAndEquipmentTurnoverTimes :: f.totalAssetsTurnoverTimes :: f.returnOnTotalAssetsPercentage :: f.returnOnEquityPercentage :: f.profitBeforeTaxToCapitalPercentage :: f.profitToSalesPercentage :: f.earningsPerShareNTD :: f.cashFlowRatioPercentage :: f.cashFlowAdequacyRatioPercentage :: f.cashFlowReinvestmentRatioPercentage :: HNil)).forceInsertQuery(selectExpression)
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

    operatingRevenue.dir.toDirectory.files.toSeq.par.foreach {
      file =>
        println(s"Read operating revenue of ${file.name}")
        val fileNamePattern = """(\d+)_(\d+).*""".r
        val fileNamePattern(y, m) = file.name
        val year = y.toInt
        val month = m.toInt

        val data = file.extension match {
          case "html" =>
            val doc = browser.parseFile(file.jfile, "Big5")
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
            def getData(rowOption: Option[Iterable[String]], industry: String = "", data: Seq[(Int, Int, Option[String], String, String, Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])] = Seq()): Seq[(Int, Int, Option[String], String, String, Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])] = {
              rowOption match {
                case Some(v) =>
                  val values = v.toSeq
                  if (values.size == 10 && values.head != "公司 代號") {
                    val splitValues = values.splitAt(2)
                    val transferValues = splitValues._2.map(_.replace(",", "")).map {
                      case v if v == "" => None
                      case value: String => Some(value.toDouble)
                    }
                    val d = (year, month, Option(industry), values.head, values(1), transferValues.head, transferValues(1), transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7))
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
            val reader = CSVReader.open(file.jfile)
            val rows = reader.all().tail
            rows.map {
              values =>
                val splitValues = values.splitAt(5)
                val transferValues = splitValues._2.init.map {
                  case v if v == "" => None
                  case value => Some(value.toDouble)
                }
                val d = (year, month, Option(values(4)), values(2), values(3), transferValues.head, transferValues(1), transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7))
                reader.close()
                d
            }
        }

        val operatingRevenues = TableQuery[OperatingRevenue]
        val dbIOActions = data.map {
          d =>
            val query = Query(d)
            val exists = operatingRevenues.filter(o => o.companyCode === d._4 && o.year === year && o.month === month).exists
            val selectExpression = query.filterNot(_ => exists)
            operatingRevenues.map(o => (o.year, o.month, o.industry, o.companyCode, o.companyName, o.monthlyRevenue, o.lastMonthRevenue, o.lastYearMonthlyRevenue, o.monthlyRevenueComparedLastMonthPercentage, o.monthlyRevenueComparedLastYearPercentage, o.cumulativeRevenue, o.lastYearCumulativeRevenue, o.cumulativeRevenueComparedLastYearPercentage)).forceInsertQuery(selectExpression)
        }

        val db = Database.forConfig("db")
        try {
          val resultFuture = db.run(DBIO.sequence(dbIOActions))
          Await.result(resultFuture, Duration.Inf)
        } finally db.close
    }
  }

  def readDailyQuote(): Unit = {
    dailyQuote.dir.toDirectory.files.toSeq.map {
      file =>
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)
        (date, file)
    }.filter(_._1.isBefore(LocalDate.of(2004, 10, 1))).sortBy(_._1).reverse.par.foreach {
      case (date, file) =>
        println(s"Read daily quote of ${file.name}")
        val reader = QuantlibCSVReader.open(file.jfile, "Big5")
        val rows = reader.all().dropWhile(_.head != "0050").map(_.map(_.replace(",", "")))
        val dailyQuotes = TableQuery[DailyQuote]
        val dbIOActions = rows.map {
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

            val query = Query((date,
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
              transferValues(10).get.toInt,
              transferValues(11),
              transferValues(12).get.toInt,
              transferValues(13).get))
            val exists = dailyQuotes.filter(d => d.date === date && d.companyCode === companyCode).exists
            val selectExpression = query.filterNot(_ => exists)
            dailyQuotes.map(d => (d.date, d.companyCode, d.companyName, d.tradeVolume, d.transaction, d.tradeValue, d.openingPrice, d.highestPrice, d.lowestPrice, d.closingPrice, d.change, d.lastBestBidPrice, d.lastBestBidVolume, d.lastBestAskPrice, d.lastBestAskVolume, d.priceEarningRatio)).forceInsertQuery(selectExpression)
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
    index.dir.toDirectory.files.toSeq.par.foreach {
      file =>
        val reader = QuantlibCSVReader.open(file.jfile, "Big5")
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(y, m, d) = file.name
        val year = y.toInt
        val month = m.toInt
        val day = d.toInt
        val date = LocalDate.of(year, month, day)

        val rows = reader.all().filter(row => row.size == 7 && row.head != "指數" && row.head != "報酬指數").map(_.map(_.replace(",", "")))
        val indices = TableQuery[Index]
        val dbIOActions = rows.map {
          values =>
            val name = values.head
            val closingIndex = values(1) match {
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
              name,
              closingIndex,
              change,
              changePercentage))
            val exists = indices.filter(i => i.date === date && i.name === name).exists
            val selectExpression = query.filterNot(_ => exists)
            indices.map(i => (i.date, i.name, i.closingIndex, i.change, i.changePercentage)).forceInsertQuery(selectExpression)
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
    exRightDividend.dir.toDirectory.files.toSeq.par.foreach {
      file =>
        val reader = QuantlibCSVReader.open(file.jfile, "Big5")
        val rows = reader.all().filter(row => row.size == 16 && row.head != "資料日期").map(_.map(_.replace(",", "")))
        val exRightDividends = TableQuery[ExRightDividend]
        val dbIOActions = rows.map {
          values =>
            val datePattern = """(\d+)年(\d+)月(\d+)日""".r
            val datePattern(y, m, d) = values.head
            val year = y.toInt + 1911
            val month = m.toInt
            val day = d.toInt
            val date = LocalDate.of(year, month, day)
            val companyCode = values(1)
            val query = Query(date,
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
            val exists = exRightDividends.filter(e => e.date === date && e.companyCode === companyCode).exists
            val selectExpression = query.filterNot(_ => exists)
            exRightDividends.map(e => (e.date, e.companyCode, e.companyName, e.closingPriceBeforeExRightExDividend, e.exRightExDividendReferencePrice, e.cashDividend, e.rightOrDividend, e.limitUp, e.limitDown, e.openingReferencePrice, e.exDividendReferencePrice)).forceInsertQuery(selectExpression)
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