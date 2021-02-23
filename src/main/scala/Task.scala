import java.io.File
import java.time.LocalDate

import db.table.{CapitalReduction, DailyQuote, ExRightDividend, FinancialAnalysis, Index, OperatingRevenue, _}
import setting.{Detail, _}
import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
import slick.lifted.TableQuery
import util.Helpers.SeqExtension

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.StreamConverters._
import scala.reflect.io.Path._

class Task {
  private val crawler = new Crawler()

  def createTables(): Unit = {
    val balanceSheet = TableQuery[BalanceSheet]
    val conciseBalanceSheet = TableQuery[ConciseBalanceSheet]
    val capitalReduction = TableQuery[CapitalReduction]
    val dailyQuote = TableQuery[DailyQuote]
    val dailyTradingDetails = TableQuery[DailyTradingDetails]
    val exRightDividend = TableQuery[ExRightDividend]
    val financialAnalysis = TableQuery[FinancialAnalysis]
    val incomeStatementProgressive = TableQuery[IncomeStatementProgressive]
    val conciseIncomeStatementProgressive = TableQuery[ConciseIncomeStatementProgressive]
    val cashFlowsProgressive = TableQuery[CashFlowsProgressive]
    val index = TableQuery[Index]
    val marginTransactions = TableQuery[MarginTransactions]
    val operatingRevenue = TableQuery[OperatingRevenue]
    val stockPER_PBR_DividendYield = TableQuery[StockPER_PBR_DividendYield]
    val setup = DBIO.seq(
      balanceSheet.schema.create,
      conciseBalanceSheet.schema.create,
      capitalReduction.schema.create,
      dailyQuote.schema.create,
      dailyTradingDetails.schema.create,
      exRightDividend.schema.create,
      financialAnalysis.schema.create,
      conciseIncomeStatementProgressive.schema.create,
      incomeStatementProgressive.schema.create,
      cashFlowsProgressive.schema.create,
      index.schema.create,
      marginTransactions.schema.create,
      operatingRevenue.schema.create,
      stockPER_PBR_DividendYield.schema.create)

    val db = Database.forConfig("db")
    try {
      val resultFuture = db.run(setup)
      Await.result(resultFuture, Duration.Inf)
    } finally db.close
  }

  def pullFinancialAnalysis(): Unit = {
    val existFiles = FinancialAnalysisSetting().twse.getYearsOfExistFiles
    val today = LocalDate.now()
    val thisYear = today.getYear
    val thisMonth = today.getMonthValue
    val lastYear = if (thisMonth > 3) thisYear - 1 else thisYear - 2
    val future = (1989 to lastYear).filterNot(existFiles).mapInSeries(crawler.getFinancialAnalysis)
    Await.result(future, Duration.Inf)
  }

  def pullBalanceSheet(): Unit = {
    pullQuarterlyFiles(BalanceSheetSetting().twse, crawler.getBalanceSheet)
  }

  def pullIncomeStatement(): Unit = {
    pullQuarterlyFiles(IncomeStatementSetting().twse, crawler.getIncomeStatement)
  }

  def pullFinancialStatements(): Unit = {
    val db = Database.forConfig("db")
    val operatingRevenue = TableQuery[OperatingRevenue]
    val detail = FinancialStatementsSetting().twse
    val existFiles = detail.dir.toDirectory.dirs.map {
      dir =>
        val fileNamePattern = """(\d+)_(\d+)""".r
        val fileNamePattern(y, q) = dir.name
        val year = y.toInt
        val quarter = q.toInt
        year match {
          case y if y < 2019 =>
            dir.files.filter(f => year < 2019 || f.length > 10000).toSeq.map {
              file =>
                val fileNamePattern = """(\w+).*""".r
                val fileNamePattern(companyCode) = file.name
                (year, quarter, companyCode)
            }
          case _ => Seq((year, quarter, ""))
        }
    }.reduce(_ ++ _).toSet

    val firstDate = detail.firstDate
    val firstYear = firstDate.getYear
    val thisYear = LocalDate.now.getYear
    val firstYearToQuarter = (firstDate.getMonthValue to 4).map(quarter => (firstYear, quarter))
    val yearToQuarter = for {
      year <- firstYear + 1 to thisYear
      quarter <- 1 to 4
    } yield (year, quarter)

    val excludeYearToQuarter = LocalDate.now.getMonthValue match {
      //3, 5, 8, 11
      //4, 1, 2, 3
      case m if m < 3 => (thisYear - 1, 4) +: (1 to 4).map(quarter => (thisYear, quarter))
      case m if m < 5 => (1 to 4).map(quarter => (thisYear, quarter))
      case m if m < 8 => (2 to 4).map(quarter => (thisYear, quarter))
      case m if m < 11 => (3 to 4).map(quarter => (thisYear, quarter))
      case _ => Seq((thisYear, 4))
    }

    val yearToQuarterToCompany = firstYearToQuarter.appendedAll(yearToQuarter).diff(excludeYearToQuarter).map {
      case (year, quarter) =>
        year match {
          case y if y < 2019 =>
            val filter = quarter match {
              case 1 =>
                operatingRevenue.filter(o => o.year === year && o.month < 4)
              case 2 =>
                operatingRevenue.filter(o => o.year === year && o.month > 3 && o.month < 7)
              case 3 =>
                operatingRevenue.filter(o => o.year === year && o.month > 6 && o.month < 10)
              case 4 =>
                operatingRevenue.filter(o => o.year === year && o.month > 9)
            }
            val companies = Await.result(db.run(filter.map(_.companyCode).distinct.result), Duration.Inf)
            companies.map((year, quarter, _))
          case _ => Seq((year, quarter, ""))
        }
    }.reduce(_ ++ _)
    db.close()

    val tuples = yearToQuarterToCompany.filterNot(existFiles)
    val future = tuples.mapInSeries {
      case (year, quarter, companyCode) => crawler.getFinancialStatements(year, quarter, companyCode)
    }
    Await.result(future, Duration.Inf)
  }

  def pullOperatingRevenue(): Unit = {
    val setting = OperatingRevenueSetting()
    val existFiles = setting.getTuplesOfExistFiles
    val firstDate = setting.twse.firstDate
    val firstYear = firstDate.getYear
    val thisYear = LocalDate.now.getYear
    val thisMonth = LocalDate.now.getMonthValue
    val firstYearToMonth = (firstDate.getMonthValue to 12).map(month => (firstYear, month))
    val yearToMonth = for {
      year <- firstYear + 1 until thisYear
      month <- 1 to 12
    } yield (year, month)
    val thisYearToMonth = (1 to (if (LocalDate.now.getDayOfMonth > 10) thisMonth - 1 else thisMonth - 2)).map(month => (thisYear, month))
    val future = firstYearToMonth.appendedAll(yearToMonth).appendedAll(thisYearToMonth).filterNot(existFiles).mapInSeries {
      case (year, month) => crawler.getOperatingRevenue(year, month)
    }
    Await.result(future, Duration.Inf)
  }

  def pullDailyQuote(): Unit = {
    //val dayOfWeek = date.getDayOfWeek.getValue
    //val linesSize = lines.size
    //if ((firstLineOption.isEmpty && dayOfWeek < 6) || firstLineOption == Option("<html>")) None else Some(date)
    //if (linesSize < 5 && dayOfWeek < 6) None else Some(date)
    pullDailyFiles(DailyQuoteSetting().twse, crawler.getDailyQuote)
  }

  def pullIndex(): Unit = {
    pullDailyFiles(IndexSetting().twse, crawler.getIndex)
  }

  def pullMarginTransactions(): Unit = {
    pullDailyFiles(MarginTransactionsSetting().twse, crawler.getMarginTransactions)
  }

  def pullDailyTradingDetails(): Unit = {
    pullDailyFiles(DailyTradingDetailsSetting().tpex, crawler.getDailyTradingDetails)
  }

  def pullStockPER_PBR_DividendYield(): Unit = {
    pullDailyFiles(StockPER_PBR_DividendYieldSetting().twse, crawler.getStockPER_PBR_DividendYield)
  }

  def pullCapitalReduction(): Unit = {
    val setting = CapitalReductionSetting()
    val existFiles = setting.getDatesOfExistFiles
    val endDate = LocalDate.now.minusDays(1)
    if (existFiles.isEmpty) {
      Await.result(crawler.getCapitalReduction(setting.twse.firstDate, endDate), Duration.Inf)
    } else if (existFiles.max != endDate) {
      Await.result(crawler.getCapitalReduction(existFiles.max.plusDays(1), endDate), Duration.Inf)
    }
  }

  def pullExRightDividend(): Unit = {
    val setting = ExRightDividendSetting()
    val existFiles = setting.getDatesOfExistFiles
    val endDate = LocalDate.now.minusDays(1)
    if (existFiles.isEmpty) {
      Await.result(crawler.getExRightDividend(setting.twse.firstDate, endDate), Duration.Inf)
    } else if (existFiles.max != endDate) {
      Await.result(crawler.getExRightDividend(existFiles.max.plusDays(1), endDate), Duration.Inf)
    }
  }

  private def pullDailyFiles(detail: Detail, crawlerFunction: LocalDate => Future[Seq[File]]): Unit = {
    val existFiles = detail.getDatesOfExistFiles
    val future = detail.firstDate.datesUntil(LocalDate.now()).toScala(Seq).filterNot(existFiles).mapInSeries(crawlerFunction)
    Await.result(future, Duration.Inf)
  }

  private def pullQuarterlyFiles(detail: Detail, crawlerFunction: (Int, Int) => Future[Seq[File]]): Unit = {
    val existFiles = detail.getTuplesOfExistFiles
    val thisYear = LocalDate.now.getYear
    val yearToQuarter = for {
      year <- detail.firstDate.getYear to thisYear
      quarter <- 1 to 4
    } yield (year, quarter)

    val excludeYearToQuarter = LocalDate.now.getMonthValue match {
      //3, 5, 8, 11
      //4, 1, 2, 3
      case m if m < 3 => (thisYear - 1, 4) +: (1 to 4).map(quarter => (thisYear, quarter))
      case m if m < 5 => (1 to 4).map(quarter => (thisYear, quarter))
      case m if m < 8 => (2 to 4).map(quarter => (thisYear, quarter))
      case m if m < 11 => (3 to 4).map(quarter => (thisYear, quarter))
      case _ => Seq((thisYear, 4))
    }

    val future = yearToQuarter.diff(excludeYearToQuarter).filterNot(existFiles).mapInSeries {
      case (year, quarter) => crawlerFunction(year, quarter)
    }
    Await.result(future, Duration.Inf)
  }
}
