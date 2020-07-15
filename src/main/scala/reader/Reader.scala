package reader

import java.time.chrono.MinguoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.concurrent.ForkJoinPool

import slick.dbio.Effect
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
import slick.sql.FixedSqlAction

import scala.collection.parallel.ForkJoinTaskSupport

trait Reader {
  protected[this] val db = Database.forConfig("db")
  protected[this] val forkJoinPool = new ForkJoinPool(20)
  protected[this] val taskSupport = new ForkJoinTaskSupport(forkJoinPool)
  protected[this] val minguoDateTimeFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseLenient
    .appendPattern("y/MM/dd")
    .toFormatter
    .withChronology(MinguoChronology.INSTANCE)

  protected[this] def dbRun(dbIO: FixedSqlAction[Option[Int], NoStream, Effect.Write]): Option[Int] = {
    try {
      val resultFuture = db.run(dbIO)
      Await.result(resultFuture, Duration.Inf)
    } //finally db.close
  }
}
