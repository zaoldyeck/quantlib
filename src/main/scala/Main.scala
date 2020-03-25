import java.io.File
import java.time.LocalDate
import java.util.concurrent.Executors
import java.util.stream

import db.table.{DailyQuote, FinancialAnalysis, OperatingRevenue}
import slick.lifted.TableQuery
//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
import slick.jdbc.H2Profile.api._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import scala.jdk.StreamConverters._

object Main {
  def main(args: Array[String]): Unit = {
    //implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    /**
     * 1. 每日收盤價（上市、上櫃）
     * 2. 月報（上市、上櫃）
     * 3. 財務分析（上市、上櫃）
     */

      /*
    val financialAnalysis = TableQuery[FinancialAnalysis]
    val operatingRevenue = TableQuery[OperatingRevenue]
    val dailyQuote = TableQuery[DailyQuote]
    val setup = DBIO.seq(
      //financialAnalysis.schema.create,
      //operatingRevenue.schema.create,
      dailyQuote.schema.create)

    //financialAnalysis.schema.createStatements.foreach(println)
    //,
    //suppliers += (101, "Acme, Inc.", "99 Market Street", "Groundsville", "CA", "95199"),
    //suppliers += (49, "Superior Coffee", "1 Party Place", "Mendocino", "CA", "95460"),
    //suppliers += (150, "The High Ground", "100 Coffee Lane", "Meadows", "CA", "93966"))


    val db = Database.forConfig("db")
    try {
      val resultFuture = db.run(setup)
      Await.result(resultFuture, Duration.Inf)
    } finally db.close

       */

    /*
    val crawler = new Crawler()
    val futures = LocalDate.of(2020, 3, 1).datesUntil(LocalDate.now()).toScala(Seq).map(crawler.getDailyQuote)
    Future.sequence(futures) andThen {
      case _ => Http.terminate()
    } onComplete {
      case Success(_) =>
      case Failure(t) => t.printStackTrace()
    }

     */
    /*
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
     */

    /*
    val futures = (2015 to 2019).map(year => crawler.getFinancialAnalysis(year))
    Future.sequence(futures) andThen {
      case _ => Http.terminate()
    } onComplete {
      case Success(_) =>
      case Failure(t) => t.printStackTrace()
    }
    */

    val reader = new Reader()
    //reader.readFinancialAnalysis()
    reader.readDailyQuote()
  }

  // 月營收(90/6 - 102/12) https://mops.twse.com.tw/nas/t21/sii/t21sc03_101_12.html
  // https://mops.twse.com.tw/nas/t21/sii/t21sc03_90_6.html
  // 月營收(102/1 後) https://mops.twse.com.tw/nas/t21/sii/t21sc03_102_1_0.html
  // 除權息(92/5/5 後) https://www.twse.com.tw/exchangeReport/TWT49U?response=csv&strDate=20190701&endDate=20190719
  // 除權息(92/5/5 後) https://www.twse.com.tw/zh/page/trading/exchange/TWT48U.html
  // 減資(100/1/1 後)https://www.twse.com.tw/zh/page/trading/exchange/TWTAUU.html
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
