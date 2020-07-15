import java.io.File
import java.time.LocalDate

import db.table.{CapitalReduction, DailyQuote, ExRightDividend, FinancialAnalysis, Index, OperatingRevenue, _}
import setting.{Detail, _}
import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
import slick.lifted.TableQuery

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.StreamConverters._
import scala.reflect.io.Path._

class Task {
  private val crawler = new Crawler()

  def createTables(): Unit = {
    val balanceSheet = TableQuery[BalanceSheet]
    val capitalReduction = TableQuery[CapitalReduction]
    val dailyQuote = TableQuery[DailyQuote]
    val dailyTradingDetails = TableQuery[DailyTradingDetails]
    val exRightDividend = TableQuery[ExRightDividend]
    val financialAnalysis = TableQuery[FinancialAnalysis]
    val incomeStatementProgressive = TableQuery[IncomeStatementProgressive]
    val index = TableQuery[Index]
    val marginTransactions = TableQuery[MarginTransactions]
    val operatingRevenue = TableQuery[OperatingRevenue]
    val stockPER_PBR_DividendYield = TableQuery[StockPER_PBR_DividendYield]
    val setup = DBIO.seq(
      balanceSheet.schema.create,
      capitalReduction.schema.create,
      dailyQuote.schema.create,
      dailyTradingDetails.schema.create,
      exRightDividend.schema.create,
      financialAnalysis.schema.create,
      incomeStatementProgressive.schema.create,
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
    val existFiles = FinancialAnalysisSetting().twse.dir.toDirectory.files.map {
      file =>
        val fileNamePattern = """(\d+)_.*.csv""".r
        val fileNamePattern(year) = file.name
        year.toInt
    }.toSet

    val today = LocalDate.now()
    val thisYear = today.getYear
    val thisMonth = today.getMonthValue
    val lastYear = if (thisMonth > 3) thisYear - 1 else thisYear - 2
    val futures = (1989 to lastYear).filterNot(existFiles).map(crawler.getFinancialAnalysis)

    runFutures(futures)
  }

  def pullBalanceSheet(): Unit = {
    pullQuarterlyFiles(BalanceSheetSetting().twse, crawler.getBalanceSheet)
  }

  def pullIncomeStatement(): Unit = {
    pullQuarterlyFiles(IncomeStatementSetting().twse, crawler.getIncomeStatement)
  }

  def pullOperatingRevenue(): Unit = {
    val detail = OperatingRevenueSetting().twse
    val existFiles = detail.dir.toDirectory.files.map {
      file =>
        val fileNamePattern = """(\d+)_(\d+).*""".r
        val fileNamePattern(year, month) = file.name
        (year.toInt, month.toInt)
    }.toSet

    val firstYear = detail.firstDate.getYear
    val thisYear = LocalDate.now.getYear
    val thisMonth = LocalDate.now.getMonthValue
    val firstYearToMonth = (detail.firstDate.getMonthValue to 12).map(month => (firstYear, month))
    val yearToMonth = for {
      year <- firstYear + 1 until thisYear
      month <- 1 to 12
    } yield (year, month)
    val thisYearToMonth = (1 to (if (LocalDate.now.getDayOfMonth > 10) thisMonth - 1 else thisMonth - 2)).map(month => (thisYear, month))

    val futures = firstYearToMonth.appendedAll(yearToMonth).appendedAll(thisYearToMonth).filterNot(existFiles).map {
      case (year, month) => crawler.getOperatingRevenue(year, month)
    }
    runFutures(futures)
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
    val detail = CapitalReductionSetting().twse
    val existFiles = detail.dir.toDirectory.files.toSeq.map {
      file =>
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(year, month, day) = file.name
        LocalDate.of(year.toInt, month.toInt, day.toInt)
    }
    val endDate = LocalDate.now.minusDays(1)
    if (existFiles.isEmpty) {
      runFutures(Seq(crawler.getCapitalReduction(detail.firstDate, endDate)))
    } else if (existFiles.max != endDate) {
      runFutures(Seq(crawler.getCapitalReduction(existFiles.max.plusDays(1), endDate)))
    }
  }

  def pullExRightDividend(): Unit = {
    val detail = ExRightDividendSetting().twse
    val existFiles = detail.dir.toDirectory.files.toSeq.map {
      file =>
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(year, month, day) = file.name
        LocalDate.of(year.toInt, month.toInt, day.toInt)
    }
    val endDate = LocalDate.now.minusDays(1)
    if (existFiles.isEmpty) {
      runFutures(Seq(crawler.getExRightDividend(detail.firstDate, endDate)))
    } else if (existFiles.max != endDate) {
      runFutures(Seq(crawler.getExRightDividend(existFiles.max.plusDays(1), endDate)))
    }
  }

  private def pullDailyFiles(detail: Detail, crawlerFunction: LocalDate => Future[Seq[File]]): Unit = {
    val existFiles = detail.dir.toDirectory.files.flatMap {
      file =>
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(year, month, day) = file.name
        val y = year.toInt
        val m = month.toInt
        val d = day.toInt
        val date = LocalDate.of(y, m, d)
        val lines = file.lines("Big5-HKSCS")
        val firstLineOption = lines.nextOption
        //if ((firstLineOption.isEmpty && date.getDayOfWeek.getValue < 6) || (firstLineOption == Option("<html>"))) None else Some(date)
        if (firstLineOption == Option("<html>")) None else Some(date)
    }.toSet

    val futures = detail.firstDate.datesUntil(LocalDate.now()).toScala(Seq).filterNot(existFiles).map(crawlerFunction)
    runFutures(futures)
  }

  private def pullQuarterlyFiles(detail: Detail, crawlerFunction: (Int, Int) => Future[Seq[File]]): Unit = {
    val existFiles = detail.dir.toDirectory.files.map {
      file =>
        val fileNamePattern = """(\d+)_(\d+).*.csv""".r
        val fileNamePattern(year, quarter) = file.name
        (year.toInt, quarter.toInt)
    }.toSet

    val thisYear = LocalDate.now.getYear
    val yearToQuarter = for {
      year <- detail.firstDate.getYear until thisYear
      quarter <- 1 to 4
    } yield (year, quarter)

    val thisYearToQuarter = LocalDate.now.getMonthValue match {
      case m if m > 11 => (1 to 3).map(quarter => (thisYear, quarter))
      case m if m > 8 => (1 to 2).map(quarter => (thisYear, quarter))
      case m if m > 5 => Seq((thisYear, 1))
      case _ => Seq()
    }

    val futures = yearToQuarter.appendedAll(thisYearToQuarter).filterNot(existFiles).map {
      case (year, quarter) => crawlerFunction(year, quarter)
    }
    runFutures(futures)
  }

  private def runFutures(futures: Seq[Future[Any]]): Unit = Await.result(Future.sequence(futures), Duration.Inf)
}
