package setting

import java.time.LocalDate

import com.typesafe.config.{Config, ConfigFactory}

import scala.reflect.io.File
import scala.reflect.io.Path._

case class MarketFile(market: String, file: File)

trait Setting {
  protected[this] val conf: Config = ConfigFactory.load
  val twse: Detail
  val tpex: Detail
  val markets: Seq[Detail]

  def getMarketFilesFromDirectory: Seq[MarketFile] = markets.map {
    detail =>
      val directory = detail.dir.toDirectory
      val files = directory.files
      files.map(file => MarketFile(directory.name, file))
  }.reduce(_ ++ _).toSeq

  def getTuplesOfExistFiles: Set[(Int, Int)] = markets.map(_.getTuplesOfExistFiles).reduce(_ & _)

  def getDatesOfExistFiles: Set[LocalDate] = markets.map(_.getDatesOfExistFiles).reduce(_ & _)
}