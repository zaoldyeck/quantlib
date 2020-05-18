import Settings.dailyQuote
import db.table.{CapitalReduction, ExRightDividend}
import slick.jdbc.H2Profile.api._
import util.QuantlibCSVReader

import scala.util.Try
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.PostgresProfile.api._
import java.time.LocalDate

import db.table.{DailyQuote, FinancialAnalysis, Index, OperatingRevenue}
import slick.lifted.TableQuery

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.StreamConverters._
import scala.util.{Failure, Success}
import java.time.LocalDate

import Settings._
import com.github.tototoshi.csv._
import db.table._
import slick.collection.heterogeneous.HNil
import slick.lifted.TableQuery
import util.QuantlibCSVReader

import scala.reflect.io.Path._
//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
import slick.jdbc.H2Profile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class Task {
  private val crawler = new Crawler()
  private val reader = new Reader()

  def createDB(): Unit = {
    val financialAnalysis = TableQuery[FinancialAnalysis]
    val operatingRevenue = TableQuery[OperatingRevenue]
    val dailyQuote = TableQuery[DailyQuote]
    val exRightDividend = TableQuery[ExRightDividend]
    val capitalReduction = TableQuery[CapitalReduction]
    val index = TableQuery[Index]
    val setup = DBIO.seq(
      //      financialAnalysis.schema.create,
      //      operatingRevenue.schema.create,
      //      dailyQuote.schema.create,
      //      index.schema.create,
      //      exRightDividend.schema.create,
      capitalReduction.schema.create)

    val db = Database.forConfig("db")
    try {
      val resultFuture = db.run(setup)
      Await.result(resultFuture, Duration.Inf)
    } finally db.close
  }

  def pullDailyQuote(): Unit = {
    val existDailyQuotes = dailyQuote.dir.toDirectory.files.flatMap {
      file =>
        val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
        val fileNamePattern(year, month, day) = file.name
        val y = year.toInt
        val m = month.toInt
        val d = day.toInt
        val date = LocalDate.of(y, m, d)
        val dayOfWeek = date.getDayOfWeek.getValue

        val firstLineOption = Try(file.lines("Big5").nextOption).getOrElse(file.lines.nextOption)
        if ((firstLineOption.isEmpty && dayOfWeek < 6) || firstLineOption == Option("<html>")) None else Some(date)
    }.toSet

    val futures = LocalDate.of(2004, 2, 11)
      //.datesUntil(LocalDate.now().plusDays(1)).toScala(Seq).reverse
      .datesUntil(LocalDate.of(2004, 9, 16).plusDays(1)).toScala(Seq).reverse
      .filterNot(existDailyQuotes)
      .map(crawler.getDailyQuote)

    Future.sequence(futures) andThen {
      case _ => Http.terminate()
    } onComplete {
      case Success(_) =>
      case Failure(t) => t.printStackTrace()
    }
  }

  def pullIndex(): Unit = {
    val futures = LocalDate.of(2020, 4, 1)
      .datesUntil(LocalDate.now().plusDays(1)).toScala(Seq)
      .map(crawler.getIndex)

    Future.sequence(futures) andThen {
      case _ => Http.terminate()
    } onComplete {
      case Success(_) =>
      case Failure(t) => t.printStackTrace()
    }
  }

  def pullOperatingRevenue(): Unit = {
    val yearToMonth: Seq[(Int, Int)] = for {
      year <- 2001 to 2012
      month <- 1 to 12
    } yield (year, month)

    val futures = yearToMonth.filterNot {
      case (year, month) => year == 2001 && (month < 6)
    }.map {
      case (year, month) => crawler.getOperatingRevenue(year, month)
    }

    Future.sequence(futures) andThen {
      case _ => Http.terminate()
    } onComplete {
      case Success(_) =>
      case Failure(t) => t.printStackTrace()
    }
  }

  def pullFinancialAnalysis(): Unit = {
    val today = LocalDate.now()
    val thisYear = today.getYear
    val thisMonth = today.getMonthValue
    val lastYear = if (thisMonth > 3) thisYear - 1 else thisYear - 2
    val futures = (1989 to lastYear).map(year => crawler.getFinancialAnalysis(year))

    Future.sequence(futures) andThen {
      case _ => Http.terminate()
    } onComplete {
      case Success(_) =>
      case Failure(t) => t.printStackTrace()
    }
  }

  def pullStatementOfComprehensiveIncome(): Unit = {
    val yearToSeason: Seq[(Int, Int)] = for {
      year <- 2019 to 2019
      season <- 4 to 4
    } yield (year, season)

    val futures = yearToSeason.map {
      case (year: Int, season: Int) => crawler.getStatementOfComprehensiveIncome(year, season)
    }

    Future.sequence(futures) andThen {
      case _ => Http.terminate()
    } onComplete {
      case Success(_) =>
      case Failure(t) => t.printStackTrace()
    }
  }
}
