package setting

import java.time.LocalDate

import com.typesafe.config.{Config, ConfigFactory}

import scala.reflect.io.File
import scala.reflect.io.Path._

case class MarketFile(market: String, file: File)

trait Setting {
  protected[this] val conf: Config = ConfigFactory.load
//  val twse: Detail
//  val tpex: Detail
  val markets: Seq[Detail]

  /** Files worth reading. 0-byte files are the crawler's "market closed" sentinels
    * (weekends / national holidays / typhoon days) — they hold no rows by
    * definition, so reading them inserts nothing, which leaves their date absent
    * from the DB, which makes every later run treat them as unread and parse them
    * again. Forever. As of 2026-07-16 that was 6,546 files re-opened on every
    * `Main update` (and 6,546 lines of log noise). Skipping them here fixes every
    * reader at once; the sentinels keep serving as our trading-day calendar on disk. */
  def getMarketFilesFromDirectory: Seq[MarketFile] = markets.map {
    detail =>
      val directory = detail.dir.toDirectory
      val files = directory.deepFiles.filter(f => f.isFile && f.length > 0)
      files.map(file => MarketFile(directory.name, file))
  }.reduce(_ ++ _).toSeq

  def getTuplesOfExistFiles: Set[(Int, Int)] = markets.map(_.getTuplesOfExistFiles).reduce(_ & _)

  def getDatesOfExistFiles: Set[LocalDate] = markets.map(_.getDatesOfExistFiles).reduce(_ & _)
}