package reader

import java.time.LocalDate
import java.time.chrono.MinguoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.concurrent.ForkJoinPool

import _root_.Settings.{BalanceSheetSetting, FinancialAnalysisSetting, IncomeStatementSetting, OperatingRevenueSetting}
import com.github.tototoshi.csv._
import db.table.{IncomeStatement, OperatingRevenue, _}
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

import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

class FinancialReader {
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

  private def dbRun(dbIOActions: Seq[FixedSqlAction[Int, NoStream, Effect.Write]]): Seq[Int] = {
    val db = Database.forConfig("db")
    try {
      val resultFuture = db.run(DBIO.sequence(dbIOActions))
      Await.result(resultFuture, Duration.Inf)
    } finally db.close
  }
}
