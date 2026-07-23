import db.table.{CapitalReduction, DailyQuote, ExRightDividend, FinancialAnalysis, Index, OperatingRevenue, _}
import setting.{Detail, _}
import slick.jdbc.PostgresProfile.api._

import java.io.File
import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}
import scala.io.Source
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
import slick.lifted.TableQuery
import util.Helpers.SeqExtension
import util.Log

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.StreamConverters._
import scala.reflect.io.Path._
import scala.util.Try

class Task {
  private val crawler = new Crawler()

  def close(): Unit = crawler.close()

  /** Idempotent schema setup for tables only. Views / materialized views are
    * NOT touched here because their `.sql` files use plain `CREATE VIEW` (no
    * `IF NOT EXISTS` for views in PG) — re-running would fail. Fresh installs
    * should call `createTablesAndViews()` once. */
  def createTables(): Unit = {
    val balanceSheet = TableQuery[BalanceSheet]
    val conciseBalanceSheet = TableQuery[ConciseBalanceSheet]
    val capitalReduction = TableQuery[CapitalReduction]
    val dailyQuote = TableQuery[DailyQuote]
    val dailyTradingDetails = TableQuery[DailyTradingDetails]
    val etf = TableQuery[ETF]
    val exRightDividend = TableQuery[ExRightDividend]
    val financialAnalysis = TableQuery[FinancialAnalysis]
    val incomeStatementProgressive = TableQuery[IncomeStatementProgressive]
    val conciseIncomeStatementProgressive = TableQuery[ConciseIncomeStatementProgressive]
    val cashFlowsProgressive = TableQuery[CashFlowsProgressive]
    val index = TableQuery[Index]
    val marginTransactions = TableQuery[MarginTransactions]
    val operatingRevenue = TableQuery[OperatingRevenue]
    val stockPER_PBR_DividendYield = TableQuery[StockPER_PBR_DividendYield]
    val tdccShareholding = TableQuery[TdccShareholding]
    val sblBorrowing = TableQuery[SblBorrowing]
    val foreignHoldingRatio = TableQuery[ForeignHoldingRatio]
    val treasuryStockBuyback = TableQuery[TreasuryStockBuyback]
    val insiderHolding = TableQuery[InsiderHolding]
    val taifexFuturesDaily = TableQuery[TaifexFuturesDaily]
    val taifexFuturesInstitutional = TableQuery[TaifexFuturesInstitutional]
    val taifexFuturesFinalSettlement = TableQuery[TaifexFuturesFinalSettlement]
    val setup = DBIO.sequence(Seq(
      balanceSheet.schema.createIfNotExists.asTry,
      conciseBalanceSheet.schema.createIfNotExists.asTry,
      capitalReduction.schema.createIfNotExists.asTry,
      dailyQuote.schema.createIfNotExists.asTry,
      dailyTradingDetails.schema.createIfNotExists.asTry,
      etf.schema.createIfNotExists.asTry,
      exRightDividend.schema.createIfNotExists.asTry,
      financialAnalysis.schema.createIfNotExists.asTry,
      conciseIncomeStatementProgressive.schema.createIfNotExists.asTry,
      incomeStatementProgressive.schema.createIfNotExists.asTry,
      cashFlowsProgressive.schema.createIfNotExists.asTry,
      index.schema.createIfNotExists.asTry,
      marginTransactions.schema.createIfNotExists.asTry,
      operatingRevenue.schema.createIfNotExists.asTry,
      stockPER_PBR_DividendYield.schema.createIfNotExists.asTry,
      tdccShareholding.schema.createIfNotExists.asTry,
      sblBorrowing.schema.createIfNotExists.asTry,
      foreignHoldingRatio.schema.createIfNotExists.asTry,
      treasuryStockBuyback.schema.createIfNotExists.asTry,
      insiderHolding.schema.createIfNotExists.asTry,
      taifexFuturesDaily.schema.createIfNotExists.asTry,
      taifexFuturesInstitutional.schema.createIfNotExists.asTry,
      taifexFuturesFinalSettlement.schema.createIfNotExists.asTry))

    val db = Database.forConfig("db")
    try Await.result(db.run(setup), Duration.Inf)
    finally db.close
  }

  /** Fresh-install only: runs Slick tables + raw SQL views + matviews. */
  def createTablesAndViews(): Unit = {
    createTables()
    createViewsAndMaterializedViews()
  }

  /** Re-applies raw SQL views + matviews. Assumes tables already exist; views
    * themselves are NOT idempotent (plain CREATE VIEW fails if one exists),
    * so this is fresh-install / manual-refresh only. */
  private def createViewsAndMaterializedViews(): Unit = {
    val materializedViews = getClass.getResource("sql/materialized_view").getPath.toDirectory.files.toSeq.sortBy(_.name).map(f => Source.fromFile(f.jfile).mkString).map(s => sqlu"#$s")
    val views = getClass.getResource("sql/view").getPath.toDirectory.files.toSeq.sortBy(_.name).map(f => Source.fromFile(f.jfile).mkString).map(s => sqlu"#$s")
    val setup = DBIO.sequence(materializedViews ++ views)
    val db = Database.forConfig("db")
    try Await.result(db.run(setup), Duration.Inf)
    finally db.close
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
    // Publication-window refresh: the previous month's summary file grows daily
    // while companies file (1st-15th), so it is re-fetched unconditionally to
    // make newly published rows usable the same day (event-driven revenue).
    val prevMonthDate = LocalDate.now.minusMonths(1)
    val inWindow =
      if (LocalDate.now.getDayOfMonth <= 15) Seq((prevMonthDate.getYear, prevMonthDate.getMonthValue))
      else Seq.empty
    val gated = firstYearToMonth
      .appendedAll(yearToMonth)
      .appendedAll(thisYearToMonth)
      .filterNot(existFiles)
      .filterNot(inWindow.contains)
    val future = gated.appendedAll(inWindow).mapInSeries {
      case (year, month) => crawler.getOperatingRevenue(year, month)
    }
    Await.result(future, Duration.Inf)
  }

  def pullDailyQuote(since: Option[LocalDate] = None): Unit = {
    //val dayOfWeek = date.getDayOfWeek.getValue
    //val linesSize = lines.size
    //if ((firstLineOption.isEmpty && dayOfWeek < 6) || firstLineOption == Option("<html>")) None else Some(date)
    //if (linesSize < 5 && dayOfWeek < 6) None else Some(date)
    pullDailyFiles(DailyQuoteSetting().twse, crawler.getDailyQuote, since,
      // 15:30 lower bound: MI_INDEX notes say FX turnover uses TWSE's 15:30 rate.
      // Empirical: earliest same-day success 15:40 (see docs).
      publishAfter = LocalTime.of(15, 30))
  }

  def pullIndex(since: Option[LocalDate] = None): Unit = {
    val setting = IndexSetting()
    val existFiles = setting.twse.getDatesOfExistFiles
    val tradingDays = loadTwseTradingDays()
    val startDate = since.getOrElse(setting.twse.firstDate)
    val endExclusive = if (since.isDefined) LocalDate.now().plusDays(1) else dailyEndExclusive(LocalTime.of(15, 30))
    val future = startDate.datesUntil(endExclusive).toScala(Seq)
      .filterNot(existFiles)
      .filter(d => since.isDefined || tradingDays.isEmpty || tradingDays.contains(d))
      .mapInSeries(crawler.getIndex)
    Await.result(future, Duration.Inf)
  }

  def pullTaifexFuturesDaily(since: Option[LocalDate] = None): Unit = {
    val setting = TaifexFuturesDailySetting()
    val detail = setting.taifex
    val today = LocalDate.now()
    val rollingMonths = taifexRollingMonths(today, taifexBackfillMonths("QL_TAIFEX_DAILY_BACKFILL_MONTHS", 3))
    val staleMonths = loadTaifexStaleDailyMonths()
    val forcedMonths = rollingMonths ++ staleMonths
    Log.debug(s"TAIFEX futures daily forced months: ${forcedMonths.toSeq.sorted.map { case (y, m) => s"$y-$m" }.mkString(",")}")
    val forcedYears = forcedMonths.map(_._1)
    val lastCompletedYear = today.getYear - 1
    val startYear = since.map(_.getYear).getOrElse(detail.firstDate.getYear)
    val existingYears = detail.getYearsOfExistFiles

    val missingAnnualYears =
      if (startYear <= lastCompletedYear) (startYear to lastCompletedYear).filterNot(existingYears)
      else Seq.empty[Int]
    val annualYears = (missingAnnualYears ++ forcedYears.filter(_ <= lastCompletedYear)).distinct.sorted
    if (annualYears.nonEmpty) Log.debug(s"TAIFEX futures daily annual refresh years: ${annualYears.mkString(",")}")
    Await.result(annualYears.mapInSeries(crawler.getTaifexFuturesDailyYear), Duration.Inf)

    val currentYearStartMonth = since match {
      case Some(d) if d.getYear == today.getYear => d.getMonthValue
      case Some(d) if d.getYear > today.getYear => 13
      case _ => 1
    }
    val forcedCurrentYearStartMonth = forcedMonths.collect { case (year, month) if year == today.getYear => month }.minOption.getOrElse(currentYearStartMonth)
    val effectiveCurrentYearStartMonth = math.min(currentYearStartMonth, forcedCurrentYearStartMonth)
    val existingMonths = detail.getTuplesOfExistFiles
    val currentYearMonths = (effectiveCurrentYearStartMonth to today.getMonthValue).filter { month =>
      val ym = (today.getYear, month)
      forcedMonths.contains(ym) || !existingMonths.contains(ym)
    }
    if (currentYearMonths.nonEmpty) Log.debug(s"TAIFEX futures daily monthly refresh months: ${currentYearMonths.map(m => s"${today.getYear}-$m").mkString(",")}")
    Await.result(currentYearMonths.mapInSeries(month => crawler.getTaifexFuturesDailyMonth(today.getYear, month)), Duration.Inf)
  }

  def pullTaifexFuturesInstitutional(since: Option[LocalDate] = None): Unit = {
    val setting = TaifexFuturesInstitutionalSetting()
    val detail = setting.taifex
    val today = LocalDate.now()
    val freeStart = today.minusYears(3)
    val requestedStart = since.getOrElse(freeStart)
    val existingMonths = detail.getTuplesOfExistFiles
    val rollingMonths = taifexRollingMonths(today, taifexBackfillMonths("QL_TAIFEX_INSTITUTIONAL_BACKFILL_MONTHS", 2))
    val rollingStart = rollingMonths.map { case (year, month) => LocalDate.of(year, month, 1) }.minBy(_.toEpochDay)
    val start = Seq(detail.firstDate, freeStart, Seq(requestedStart, rollingStart).minBy(_.toEpochDay)).maxBy(_.toEpochDay)

    val months = monthsInclusive(start, today).filter { case (year, month) =>
      val ym = (year, month)
      rollingMonths.contains(ym) || !existingMonths.contains(ym)
    }
    if (months.nonEmpty) Log.debug(s"TAIFEX futures institutional monthly refresh months: ${months.map { case (y, m) => s"$y-$m" }.mkString(",")}")
    Await.result(months.mapInSeries { case (year, month) =>
      crawler.getTaifexFuturesInstitutionalMonth(year, month)
    }, Duration.Inf)
  }

  def pullTaifexFuturesFinalSettlement(since: Option[LocalDate] = None): Unit = {
    val detail = TaifexFuturesFinalSettlementSetting().taifex
    val today = LocalDate.now()
    val startYear = since.map(_.getYear).getOrElse(detail.firstDate.getYear)
    val existingYears = taifexExistingHtmlYears(detail.dir)
    val years = (startYear to today.getYear).filter { year =>
      year == today.getYear || year == today.getYear - 1 || !existingYears.contains(year)
    }
    if (years.nonEmpty) Log.debug(s"TAIFEX futures final-settlement refresh years: ${years.mkString(",")}")
    Await.result(years.mapInSeries(crawler.getTaifexFuturesFinalSettlementYear), Duration.Inf)
  }

  def pullTaifexIntradayRaw(): Unit = {
    val setting = TaifexIntradayRawSetting()
    Await.result(setting.sources.mapInSeries(crawler.getTaifexIntradayRawFiles), Duration.Inf)
  }

  def pullMarginTransactions(): Unit = {
    pullDailyFiles(MarginTransactionsSetting().twse, crawler.getMarginTransactions,
      // Official guarantee is only "before the next session opens" (融資融券操作辦法 §69);
      // evening availability is practice, not promise. No same-day evidence on record.
      publishAfter = LocalTime.of(21, 30))
  }

  def pullDailyTradingDetails(since: Option[LocalDate] = None): Unit = {
    // Intersection (not union): a date is "already downloaded" only when BOTH
    // markets have a file. Union silently dropped dates where only one market
    // was present — e.g. TWSE 2026-04-13~17 were skipped because TPEx had them.
    val setting = DailyTradingDetailsSetting()
    val existFiles = setting.twse.getDatesOfExistFiles & setting.tpex.getDatesOfExistFiles
    val startDate = since.getOrElse(setting.twse.firstDate)
    val future = startDate.datesUntil(dailyEndExclusive(LocalTime.of(16, 0))).toScala(Seq).filterNot(existFiles).mapInSeries(crawler.getDailyTradingDetails)
    Await.result(future, Duration.Inf)
  }

  def pullStockPER_PBR_DividendYield(): Unit = {
    pullDailyFiles(StockPER_PBR_DividendYieldSetting().twse, crawler.getStockPER_PBR_DividendYield,
      // No official time. Empirical closed loop (2026-07-09): 15:56 fail, 16:10 fail,
      // 18:28 success -> publishes between 16:10 and 18:28. The old 15:00 was 3h+ early.
      publishAfter = LocalTime.of(18, 30))
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
    // MOPS t108sb27 returns monthly snapshots. Iterate from 2024-07 (first month
    // the legacy endpoint started returning empty) through current month; skip
    // months whose YYYY_M.csv already exists and is non-trivial in size.
    val firstYear = 2024
    val firstMonth = 7
    val today = LocalDate.now
    val months: Seq[(Int, Int)] = for {
      y <- firstYear to today.getYear
      m <- 1 to 12
      if (y > firstYear || m >= firstMonth) && (y < today.getYear || m <= today.getMonthValue)
    } yield (y, m)

    // A month is "done" once its file exists at all — including the 0-byte
    // sentinel written when the month had no ex-right/dividend events yet.
    // Exception: the CURRENT month keeps refetching (events accrue through the
    // month), so an empty/small current-month file is never treated as final.
    def monthDone(year: Int, month: Int, dir: String): Boolean = {
      val f = new java.io.File(s"$dir/$year/${year}_${month}.csv")
      val isCurrentMonth = year == today.getYear && month == today.getMonthValue
      f.exists() && (!isCurrentMonth || f.length() > 200)
    }

    val setting = ExRightDividendSetting()
    val pending = months.filterNot { case (y, m) =>
      monthDone(y, m, setting.twse.dir) && monthDone(y, m, setting.tpex.dir)
    }
    if (pending.nonEmpty) {
      val future = pending.mapInSeries { case (y, m) => crawler.getExRightDividend(y, m) }
      Await.result(future, Duration.Inf)
    }
  }

  def pullETF(): Unit = {
    Await.result(crawler.getETF, Duration.Inf)
  }

  def pullTdccShareholding(): Unit = {
    // Endpoint returns only the LATEST week's snapshot; forward-accumulate one file
    // per invocation. Reader dedupes via unique(data_date, company_code, tier).
    // Historical backfill (2008+) is Task #20 — not done by this method.
    Await.result(crawler.getTdccShareholding(), Duration.Inf)
  }

  // Skip a date if both markets are "covered": either pre-firstDate (no upstream data)
  // or already saved locally. Plain intersection misbehaves when the two markets have
  // different firstDates (TPEx 2010+ vs TWSE 2005+ for QFII; TPEx 2013+ vs TWSE 2016+ for SBL).
  // Without this, post-resume runs systematically re-fetch dates where one market is
  // pre-firstDate and the other already has its file (e.g. QFII 2005-2009 was re-fetched
  // wasting ~9h on resume).
  private def coveredBoth(date: LocalDate, twse: Detail, tpex: Detail,
                           twseExist: Set[LocalDate], tpexExist: Set[LocalDate]): Boolean = {
    val twseCovered = date.isBefore(twse.firstDate) || twseExist.contains(date)
    val tpexCovered = date.isBefore(tpex.firstDate) || tpexExist.contains(date)
    twseCovered && tpexCovered
  }

  // Trading-day filter using daily_quote as ground truth. Avoids tens of hours
  // of [giveup] retries on weekends + national holidays + 颱風假 during bulk
  // backfill. Returns Set[LocalDate] for fast .contains() check.
  // Note: dates BEFORE daily_quote.minDate are kept (caller may want pre-2004).
  private def loadTwseTradingDays(): Set[LocalDate] = {
    val localTradingDays = loadLocalTwseDailyQuoteTradingDays()
    val db = Database.forConfig("db")
    try {
      val q = sql"""SELECT DISTINCT date FROM daily_quote WHERE market='twse'""".as[java.sql.Date]
      val raw = Await.result(db.run(q), Duration.Inf)
      raw.iterator.map(_.toLocalDate).toSet ++ localTradingDays
    } finally db.close()
  }

  private def loadLocalTwseDailyQuoteTradingDays(): Set[LocalDate] = {
    val dir = DailyQuoteSetting().twse.dir.toDirectory
    if (!dir.exists) Set.empty
    else {
      val fileNamePattern = """(\d+)_(\d+)_(\d+)\.csv""".r
      dir.deepFiles.flatMap { file =>
        file.name match {
          case fileNamePattern(y, m, d) if file.length > 1024 =>
            Try(LocalDate.of(y.toInt, m.toInt, d.toInt)).toOption.filter { expected =>
              twseDailyQuoteFileDate(file.jfile).contains(expected)
            }
          case _ => None
        }
      }.toSet
    }
  }

  private def twseDailyQuoteFileDate(file: File): Option[LocalDate] = {
    val datePattern = """(\d{3})年(\d{2})月(\d{2})日""".r
    val source = Source.fromFile(file, "Big5-HKSCS")
    try {
      source.getLines().take(5).collectFirst {
        case line if datePattern.findFirstMatchIn(line).nonEmpty =>
          val m = datePattern.findFirstMatchIn(line).get
          LocalDate.of(m.group(1).toInt + 1911, m.group(2).toInt, m.group(3).toInt)
      }
    } finally source.close()
  }

  def pullSbl(since: Option[LocalDate] = None): Unit = {
    val setting = SblBorrowingSetting()
    val twseExist = setting.twse.getDatesOfExistFiles
    val tpexExist = setting.tpex.getDatesOfExistFiles
    val tradingDays = loadTwseTradingDays()
    val startDate = since.getOrElse(setting.twse.firstDate)
    // SBL is republished twice nightly (~20:30 partial, ~22:30 final; TWSE TWT93U /
    // TPEx sbl notes). Fetching at 21:30 silently captures the PARTIAL file — it looks
    // complete and would never be re-fetched. Only ask after the second update.
    val future = startDate.datesUntil(dailyEndExclusive(LocalTime.of(22, 30))).toScala(Seq)
      .filterNot(d => coveredBoth(d, setting.twse, setting.tpex, twseExist, tpexExist))
      .filter(d => tradingDays.contains(d))   // skip weekends + holidays
      .mapInSeries(crawler.getSblBorrowing)
    Await.result(future, Duration.Inf)
  }

  def pullForeignHoldingRatio(since: Option[LocalDate] = None): Unit = {
    val setting = ForeignHoldingRatioSetting()
    val twseExist = setting.twse.getDatesOfExistFiles
    val tpexExist = setting.tpex.getDatesOfExistFiles
    val tradingDays = loadTwseTradingDays()
    val startDate = since.getOrElse(setting.twse.firstDate)
    // No official time and no same-day evidence on record (the old 17:00 was a guess).
    // Aligned with the other evening batches; the D+1 update is what actually gets it.
    val future = startDate.datesUntil(dailyEndExclusive(LocalTime.of(21, 30))).toScala(Seq)
      .filterNot(d => coveredBoth(d, setting.twse, setting.tpex, twseExist, tpexExist))
      .filter(d => tradingDays.contains(d))   // skip weekends + holidays
      .mapInSeries(crawler.getForeignHoldingRatio)
    Await.result(future, Duration.Inf)
  }

  // ============== Sprint B (MOPS structured filings) ==============
  // Common pattern: MOPS endpoints take (year, month) form. Iterate firstYear..now,
  // skip months whose `{year}_{month}.html` already exists (any size, including 0-byte
  // sentinel) on BOTH markets. `since` lets one-shot historical backfill jobs
  // start from a specific point (e.g. since=2005-01 for full history).

  private def pullMopsMonthly(setting: Setting,
                               firstYear: Int, firstMonth: Int,
                               crawl: (Int, Int) => Future[Seq[File]],
                               since: Option[LocalDate] = None): Unit = {
    val today = LocalDate.now
    val (sy, sm) = since match {
      case Some(d) => (d.getYear, d.getMonthValue)
      case None    => (firstYear, firstMonth)
    }
    val months: Seq[(Int, Int)] = for {
      y <- sy to today.getYear
      m <- 1 to 12
      if (y > sy || m >= sm) && (y < today.getYear || m <= today.getMonthValue)
    } yield (y, m)

    def monthDone(year: Int, month: Int, dir: String): Boolean = {
      // Either the year subdir holds it (preferred MOPS layout) or the legacy
      // flat layout — accept any presence (including 0-byte sentinel).
      val a = new java.io.File(s"$dir/$year/${year}_${month}.html")
      val b = new java.io.File(s"$dir/${year}_${month}.html")
      a.exists() || b.exists()
    }

    // Both markets must have the file before skipping (mirrors coveredBoth).
    val twseDir = setting.markets.head.dir
    val tpexDir = setting.markets(1).dir
    val pending = months.filterNot { case (y, m) => monthDone(y, m, twseDir) && monthDone(y, m, tpexDir) }
    if (pending.nonEmpty) {
      val future = pending.mapInSeries { case (y, m) => crawl(y, m) }
      Await.result(future, Duration.Inf)
    }
  }

  def pullTreasuryStockBuyback(since: Option[LocalDate] = None): Unit = {
    // 庫藏股 — endpoint t35sc09 returns full historical SNAPSHOT in one POST
    // (4.5MB / 2.8MB for TWSE / TPEx, all years). Monthly loop = redundant 1.1GB
    // of the same data. Single-shot for current month is enough; reader dedupes
    // by (market, announce_date, company_code).
    val today = LocalDate.now
    val future = crawler.getTreasuryStockBuyback(today.getYear, today.getMonthValue)
    Await.result(future, Duration.Inf)
  }

  def pullInsiderHolding(since: Option[LocalDate] = None): Unit = {
    // 內部人持股轉讓事前申報日報 — daily, 2-step ajax per (market, date).
    // Mirror pullSbl pattern: filter trading days, skip already-existing dates.
    val setting = InsiderHoldingSetting()
    val twseExist = setting.twse.getDatesOfExistFiles
    val tpexExist = setting.tpex.getDatesOfExistFiles
    val tradingDays = loadTwseTradingDays()
    val startDate = since.getOrElse(setting.twse.firstDate)
    val future = startDate.datesUntil(dailyEndExclusive(LocalTime.of(22, 0))).toScala(Seq)
      .filterNot(d => coveredBoth(d, setting.twse, setting.tpex, twseExist, tpexExist))
      .filter(d => tradingDays.contains(d))
      .mapInSeries(crawler.getInsiderHolding)
    Await.result(future, Duration.Inf)
  }

  private val taipeiZone = ZoneId.of("Asia/Taipei")

  /** End-exclusive crawl bound for daily sources.
    *
    * POLICY (docs/data_ops/twse_publish_times.md, first-hand sourced 2026-07-15):
    * the ONE complete update runs at D+1 (pre-open loop), because D's data is only
    * guaranteed complete then — margin balances are officially promised merely
    * "before the next session opens", and SBL is republished at ~22:30 (a 20:30
    * fetch silently captures a partial file that gets rewritten later).
    *
    * These per-source times therefore only avoid pointless same-day requests; they
    * are NOT what makes the data correct. Crawler refuses to sentinel any date that
    * has not passed its completeness time (D+1 00:30), so an over-optimistic time
    * costs one wasted request — never data. Evidence level per source is noted at
    * each call site; anything unsourced is flagged as such. */
  private def dailyEndExclusive(publishAfter: LocalTime): LocalDate = {
    val now = ZonedDateTime.now(taipeiZone)
    if (now.toLocalTime.isBefore(publishAfter)) now.toLocalDate else now.toLocalDate.plusDays(1)
  }

  private def pullDailyFiles(detail: Detail, crawlerFunction: LocalDate => Future[Seq[File]],
                             since: Option[LocalDate] = None,
                             publishAfter: LocalTime = LocalTime.MAX): Unit = {
    val existFiles = detail.getDatesOfExistFiles
    val startDate = since.getOrElse(detail.firstDate)
    // Explicit --since is an operator override for after-close backfills and
    // always includes today; the default bound is publish-time aware.
    val endExclusive = if (since.isDefined) LocalDate.now().plusDays(1) else dailyEndExclusive(publishAfter)
    val future = startDate.datesUntil(endExclusive).toScala(Seq).filterNot(existFiles).mapInSeries(crawlerFunction)
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

  private def monthsInclusive(start: LocalDate, end: LocalDate): Seq[(Int, Int)] = {
    val startMonth = LocalDate.of(start.getYear, start.getMonthValue, 1)
    val endMonth = LocalDate.of(end.getYear, end.getMonthValue, 1)
    Iterator.iterate(startMonth)(_.plusMonths(1))
      .takeWhile(!_.isAfter(endMonth))
      .map(d => (d.getYear, d.getMonthValue))
      .toSeq
  }

  private def taifexBackfillMonths(envName: String, defaultValue: Int): Int =
    sys.env.get(envName).flatMap(v => Try(v.toInt).toOption).filter(_ > 0).getOrElse(defaultValue)

  private def taifexRollingMonths(today: LocalDate, monthCount: Int): Set[(Int, Int)] =
    monthsInclusive(today.minusMonths(monthCount.toLong - 1L), today).toSet

  private def taifexExistingHtmlYears(dir: String): Set[Int] = {
    val fileNamePattern = """(\d{4})\.html""".r
    val directory = dir.toDirectory
    if (!directory.exists) Set.empty
    else directory.deepFiles.flatMap(file => fileNamePattern.findFirstMatchIn(file.name).map(_.group(1).toInt)).toSet
  }

  private def loadTaifexStaleDailyMonths(): Set[(Int, Int)] = {
    val db = Database.forConfig("db")
    val scanDays = sys.env.get("QL_TAIFEX_STALE_SCAN_DAYS")
      .flatMap(v => Try(v.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(14)
    val scanStart = java.sql.Date.valueOf(LocalDate.now().minusDays(scanDays.toLong))
    try {
      val staleDates = Await.result(db.run(sql"""
        WITH ranked AS (
          SELECT
            date,
            contract_code,
            contract_month,
            close,
            settlement_price,
            open_interest,
            row_number() OVER (
              PARTITION BY date, contract_code
              ORDER BY contract_month
            ) AS month_rank
          FROM taifex_futures_daily
          WHERE contract_code IN ('TX', 'MTX', 'TMF', 'TE', 'TF')
            AND date >= $scanStart
            AND trading_session = '一般'
            AND contract_month ~ '^[0-9]{6}'
            AND close IS NOT NULL
        ),
        terminal_contract_days AS (
          SELECT
            contract_code,
            contract_month,
            trading_session,
            max(date) AS terminal_date
          FROM taifex_futures_daily
          WHERE contract_code IN ('TX', 'MTX', 'TMF', 'TE', 'TF')
            AND trading_session = '一般'
            AND contract_month ~ '^[0-9]{6}'
          GROUP BY contract_code, contract_month, trading_session
        )
        SELECT DISTINCT r.date
        FROM ranked r
        LEFT JOIN terminal_contract_days t
          ON r.contract_code = t.contract_code
         AND r.contract_month = t.contract_month
         AND r.trading_session = t.trading_session
        WHERE r.month_rank = 1
          AND (r.settlement_price IS NULL OR r.open_interest IS NULL)
          -- TAIFEX daily market files keep the delivery-month contract's
          -- settlement_price blank on many final trading days even after the
          -- official annual archive is published. That is a stable source
          -- semantic, not a stale download. Final settlement prices are a
          -- separate TAIFEX clearing dataset.
          AND NOT (
            r.settlement_price IS NULL
            AND r.open_interest IS NOT NULL
            AND r.contract_month = to_char(r.date, 'YYYYMM')
            AND r.date = t.terminal_date
            AND r.date < CURRENT_DATE
          )
      """.as[java.sql.Date]), Duration.Inf)
      staleDates.iterator.map(_.toLocalDate).map(d => (d.getYear, d.getMonthValue)).toSet
    } catch {
      case _: Throwable => Set.empty
    } finally db.close()
  }
}
