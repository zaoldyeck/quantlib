import java.time.LocalDate
import db.table.ConciseIncomeStatementIndividual
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL.Parse._
import net.ruippeixotog.scalascraper.model._
import slick.jdbc.PostgresProfile.api._
import reader.{FinancialReader, TradingReader}
import setting.Constant.{DEBTs, ETFs}
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

    // ===== Strategy entry points (uncomment to run) =====
    // val db = Database.forConfig("db")
    // try {
    //   val primary = strategy.Backtester.run(new strategy.MomentumValueStrategy(10),
    //     LocalDate.of(2018, 1, 2), LocalDate.of(2024, 12, 30), 1_000_000.0, db)
    //   val bench = strategy.Backtester.run(new strategy.Hold0050Strategy,
    //     LocalDate.of(2018, 1, 2), LocalDate.of(2024, 12, 30), 1_000_000.0, db)
    //   println(strategy.Metrics.summarize(primary).show)
    //   println(strategy.Metrics.summarize(bench).show)
    //   val outBase = strategy.Output.writeAll(primary, bench)
    //   println(s"Output written: $outBase.html + .csv")
    // } finally { db.close() }
    //backtest.dollarCostAveraging(ETFs.appended("2330").toSet, LocalDate.of(2018, 5, 17), LocalDate.now, Set(6, 16, 26), 5000, limit = 20)
    //backtest.dollarCostAveraging(ETFs.appended("2330").toSet, LocalDate.of(2021, 6, 10), LocalDate.of(2021, 12, 31), Set(6, 16, 26), 5000, limit = 30)
    //backtest.dollarCostAveraging(Set("006208", "0052", "2330", "2412", "00757"), LocalDate.of(2018, 12, 6), LocalDate.of(2022, 2, 26), Set(6, 16, 26), 10000)
    //backtest.dollarCostAveraging(Set("006208", "00692", "0052", "2330", "2412", "00757"), LocalDate.of(2017, 5, 17), LocalDate.of(2022, 2, 26), Set(6, 16, 26), 10000)
    //backtest.dollarCostAveraging(Set("006208", "00692", "00733", "00891", "00892", "0052", "2330", "2412", "00757"), LocalDate.of(2018, 5, 17), LocalDate.now, Set(6, 16, 26), 10000)
    //backtest.dollarCostAveraging(DEBTs.toSet, LocalDate.of(2000, 1, 1), LocalDate.now, Set(6, 16, 26), 10000)

    //task.createTables()
    //task.pullIndex()
    //financialReader.readETF()
    //tradingReader.readIndex()
    //job.pullAllData()
    //job.readAllData()
    job.updateData()
    job.complete()
    //twse-2020_4_a_c_4.csv
    /*
    crawler.getBalanceSheet(2020, 4) andThen {
      case _ => Http.terminate()
    }
    */
    //question.compareROI(Set("0050", "0051", "0052", "0056"), LocalDate.of(2007, 12, 26))
    //backtest.dollarCostAveraging(Set("0050", "0056"), LocalDate.of(2017, 1, 1), LocalDate.of(2020, 10, 30), Set(6, 16, 26), 1000)
    //backtest.dollarCostAveraging(Set("0050", "0051", "0052", "0053", "0054", "0055", "0056", "0057", "0061", "008201", "006203", "006205", "006204", "006206", "006207", "006208", "00631L", "00632R", "00633L", "00634R", "00636", "00635U", "00637L", "00638R", "00639", "00642U", "00640L", "00641R", "00645", "00643", "00646", "00647L", "00648R", "00650L", "00651R", "00655L", "00656R", "00652", "00653L", "00654R", "00657", "00660", "00661", "00662", "00663L", "00664R", "00665L", "00666R", "00675L", "00676R", "00673R", "00674R", "00669R", "00668", "00678", "00680L", "00681R", "00670L", "00671R", "00682U", "00683L", "00684R", "00685L", "00686R", "00690", "00688L", "00689R", "00693U", "00692", "00700", "00703", "00709", "00701", "00702", "00710B", "00711B", "00712", "00706L", "00707R", "00708L", "00713", "00714", "00715L", "00717", "00730", "00728", "00731", "00732", "00733", "00738U", "00735", "00736", "00737", "00742", "00739", "00743", "00752", "00753L", "00757", "00763U", "00762", "00770", "00775B", "00774B", "00783", "00830", "00771", "00851", "00852L", "00850", "00861", "00865B", "00875", "00876", "00878", "00881", "00882", "00885", "00891", "00892", "00893", "00895", "00894"), LocalDate.of(2021, 1, 1), LocalDate.now, Set(6, 16, 26), 1000)
    //backtest.dollarCostAveraging(Set("0050", "0051", "0052", "0053", "0054", "0055", "0056", "0057", "0061", "008201", "006203", "006205", "006204", "006206", "006207", "006208", "00631L", "00632R", "00633L", "00634R", "00636", "00635U", "00637L", "00638R", "00639", "00642U", "00640L", "00641R", "00645", "00643", "00646", "00647L", "00648R", "00650L", "00651R", "00655L", "00656R", "00652", "00653L", "00654R", "00657", "00660", "00661", "00662", "00663L", "00664R", "00665L", "00666R", "00675L", "00676R", "00673R", "00674R", "00669R", "00668", "00678", "00680L", "00681R", "00670L", "00671R", "00682U", "00683L", "00684R", "00685L", "00686R", "00690", "00688L", "00689R", "00693U", "00692", "00700", "00703", "00709", "00701", "00702", "00710B", "00711B", "00712", "00706L", "00707R", "00708L", "00713", "00714", "00715L", "00717", "00730", "00728", "00731", "00732", "00733", "00738U", "00735", "00736", "00737", "00742", "00739", "00743", "00752", "00753L", "00757", "00763U", "00762", "00770", "00775B", "00774B", "00783", "00830", "00771", "00851", "00852L", "00850", "00861", "00865B", "00875", "00876", "00878", "00881", "00882", "00885", "00891", "00892", "00893", "00895", "00894"), LocalDate.of(2018, 12, 6), LocalDate.now, Set(6, 16, 26), 1000)
    //backtest.dollarCostAveraging(Set("00757", "0052", "006208", "2330"), LocalDate.of(2018, 12, 6), LocalDate.now, Set(5,10,15,20,25), 2000)
    //0050, 2004-2-11
    //0052, 2006-9-12
    //0056, 2007-12-26
    //006208, 2012-7-17
    //00692, 2017-5-17
    //00757, 2018-12-06
  }

  // 每5秒指數統計 https://www.twse.com.tw/zh/page/trading/exchange/MI_5MINS_INDEX.html
}
