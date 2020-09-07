import java.time.LocalDate

import db.table.ConciseIncomeStatementIndividual
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL.Parse._
import net.ruippeixotog.scalascraper.model._
import slick.jdbc.PostgresProfile.api._
import reader.{FinancialReader, TradingReader}
import slick.lifted.TableQuery

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

object Main {
  def main(args: Array[String]): Unit = {
    val task = new Task
    val tradingReader = new TradingReader
    val financialReader = new FinancialReader
    val question = new Question
    val backtest = new Backtest
    val crawler = new Crawler
    val job = new Job
    //tradingReader.readExRightDividend()
    //task.pullFinancialStatements()
    //financialReader.readBalanceSheet()
    job.updateData()
    job.complete()
    //5601, 8086
    //crawler.getFinancialStatements(2020, 2, "") andThen {
    //  case _ => Http.terminate()
    //}
    //"0050", "0051", "0052", "0056", "006201", "006208", "00692"
    //question.compareROI(Set("0050", "0051", "0052", "0056", "006201", "006208", "00692"), LocalDate.of(2011, 1, 28))
    //backtest.dollarCostAveraging(Set("0050", "0051", "0052", "0056", "006201", "006208", "00692"), LocalDate.of(2011, 1, 28), LocalDate.now, Set(6, 16, 26), 1000)
    //backtest.dollarCostAveraging(Set("0050", "0051", "0052", "0056", "006201", "006208", "00692", "00850", "00878"), LocalDate.of(2019, 8, 23), LocalDate.now, Set(6, 16, 26), 1000)
  }

  // 每5秒指數統計 https://www.twse.com.tw/zh/page/trading/exchange/MI_5MINS_INDEX.html
}
