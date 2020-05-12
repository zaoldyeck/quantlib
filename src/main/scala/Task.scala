import db.table.ExRightDividend
import slick.jdbc.H2Profile.api._
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

class Task {
  private val crawler = new Crawler()
  private val reader = new Reader()

  def createDB(): Unit = {
    val financialAnalysis = TableQuery[FinancialAnalysis]
    val operatingRevenue = TableQuery[OperatingRevenue]
    val dailyQuote = TableQuery[DailyQuote]
    val exRightDividend = TableQuery[ExRightDividend]
    val index = TableQuery[Index]
    val setup = DBIO.seq(
      financialAnalysis.schema.create,
      operatingRevenue.schema.create,
      dailyQuote.schema.create,
      index.schema.create,
      exRightDividend.schema.create)

    val db = Database.forConfig("db")
    try {
      val resultFuture = db.run(setup)
      Await.result(resultFuture, Duration.Inf)
    } finally db.close
  }

  def pullDailyQuote(): Unit = {
    val futures = LocalDate.of(2020, 5, 11)
      .datesUntil(LocalDate.now().plusDays(1)).toScala(Seq)
      //.datesUntil(LocalDate.of(2020, 3, 1).plusDays(1)).toScala(Seq)
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
      year <- 2020 to 2020
      month <- 3 to 12
    } yield (year, month)

    val futures = yearToMonth.map {
      case (year: Int, month: Int) => crawler.getOperatingRevenue(year, month)
    }

    Future.sequence(futures) andThen {
      case _ => Http.terminate()
    } onComplete {
      case Success(_) =>
      case Failure(t) => t.printStackTrace()
    }
  }

  def pullFinancialAnalysis(): Unit = {
    val futures = (2015 to 2019).map(year => crawler.getFinancialAnalysis(year))

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
