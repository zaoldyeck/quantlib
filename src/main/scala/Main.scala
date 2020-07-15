import java.time.LocalDate

import db.table.IncomeStatementIndividual
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
    //-1893
    job.pullAllData()
    job.complete()
    //    crawler.getIndex(LocalDate.of(2020, 4, 1)) andThen {
    //      case _ => Http.terminate()
    //    }
    //"0050", "0051", "0052", "0056", "006201", "006208", "00692"
    //question.compareROI(Set("0050", "0051", "0052", "0056", "006201", "006208", "00692"), LocalDate.of(2011, 1, 28))
    //backtest.dollarCostAveraging(Set("0050", "0051", "0052", "0056", "006201", "006208", "00692"), LocalDate.of(2011, 1, 28), LocalDate.now, Set(6, 16, 26), 1000)
    /*
    val browser = JsoupBrowser()
    val doc = browser.parseFile("data/operating_revenue/2001_6.html", "Big5")
    val uu = (doc >> elements("tr")).map {
      tr =>
        val x = tr >?> element("th")
        if (x.isDefined) (tr >> elements("th")).map(_.text) else {
          (tr >> elements("td")).map(_.text)
        }
    }

    uu.foreach(println)
     */
    //val c = (doc >> elements("tr").map(table)).map(_.map(_.text))
    //c.foreach(println)
  }

  // 月營收(90/6 - 102/12) https://mops.twse.com.tw/nas/t21/sii/t21sc03_101_12.html
  // https://mops.twse.com.tw/nas/t21/sii/t21sc03_90_6.html
  // 月營收(102/1 後) https://mops.twse.com.tw/nas/t21/sii/t21sc03_102_1_0.html
  // 除權息(92/5/5 後) https://www.twse.com.tw/exchangeReport/TWT49U?response=csv&strDate=20190701&endDate=20190719
  // 除權息(92/5/5 後) https://www.twse.com.tw/zh/page/trading/exchange/TWT48U.html
  // 減資(100/1/1 後) https://www.twse.com.tw/zh/page/trading/exchange/TWTAUU.html
  // 減資(100/1/1 後) https://www.twse.com.tw/exchangeReport/TWTAUU?response=csv&strDate=20190701&endDate=20190714
  // 每日成交價(93/2/11 後) https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=20190718&type=ALL
  // 每日本益比、殖利率及股價淨值比(94/9/2 後) https://www.twse.com.tw/zh/page/trading/exchange/BWIBBU_d.html
  // 每日本益比、殖利率及股價淨值比(94/9/2 後) https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=csv&date=20190718&selectType=ALL
  // 財務分析匯總表(101 年後) POST https://mops.twse.com.tw/mops/web/ajax_t51sb02 form: ncodeURIComponent=1&run=Y&step=1&TYPEK=sii&year=107&isnew=&firstin=1&off=1&ifrs=Y
  // 財務分析匯總表(78 年 - 102 年) POST https://mops.twse.com.tw/mops/web/ajax_t51sb02 form: encodeURIComponent=1&step=1&firstin=1&off=1&TYPEK=sii&year=96
  // 營益分析查詢彙總表(102 年第 1 季後) POST https://mops.twse.com.tw/mops/web/ajax_t163sb06 form: encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=sii&year=107&season=04
  // 營益分析查詢彙總表(78 年第 1 季 - 101 年第 4 季) POST https://mops.twse.com.tw/mops/web/ajax_t51sb06 form: encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=sii&year=96&season=04
  // 財報(從 98 年起) https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-"+str(year)+"Q"+str(season)+".zip&filePath=/home/html/nas/ifrs/"+str(year)+"/
  // 每5秒指數統計 https://www.twse.com.tw/zh/page/trading/exchange/MI_5MINS_INDEX.html
  // 三大法人買賣超日報 https://www.twse.com.tw/zh/page/trading/fund/T86.html
}
