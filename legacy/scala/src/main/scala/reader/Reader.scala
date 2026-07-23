package reader

import java.time.chrono.MinguoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.concurrent.ForkJoinPool

import me.tongfei.progressbar.ProgressBar
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

  protected final class SafeProgressBar(label: String, total: Int) {
    private[this] val delegate =
      if (total <= 0) {
        println(s"$label no files to read")
        None
      } else Some(new ProgressBar(label, total))

    def step(): Unit = delegate.foreach(_.step())
    def close(): Unit = delegate.foreach(_.close())
  }

  protected[this] def progressBar(label: String, total: Int): SafeProgressBar =
    new SafeProgressBar(label, total)

  protected[this] def dbRun(dbIO: FixedSqlAction[Option[Int], NoStream, Effect.Write]): Option[Int] = {
    val resultFuture = db.run(dbIO)
    Await.result(resultFuture, Duration.Inf)
  }

  def close(): Unit = {
    db.close()
    forkJoinPool.shutdown()
  }
}
