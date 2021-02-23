import java.time.LocalDate

import db.table.{DailyQuote, ExRightDividend}
import plotly.layout.Layout
import plotly.{Plotly, Scatter}
import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
import slick.lifted.TableQuery

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class Question {
  def compareROI(companyCodes: Set[String], strDate: LocalDate = LocalDate.of(2004, 2, 11), endDate: LocalDate): Unit = {
    val db = Database.forConfig("db")
    val dailyQuote = TableQuery[DailyQuote]
    val dailyQuoteAction = dailyQuote.filter(d => d.companyCode.inSet(companyCodes) && d.date >= strDate && d.date <= endDate).sortBy(_.date).result

    val exRightDividend = TableQuery[ExRightDividend]
    val exRightDividendAction = exRightDividend.filter(e => e.companyCode.inSet(companyCodes) && e.date >= strDate && e.date <= endDate).sortBy(_.date).result

    val render = for {
      dailyQuoteResult <- db.run(dailyQuoteAction)
      exRightDividendResult <- db.run(exRightDividendAction)
    } yield {
      val data = dailyQuoteResult.groupBy(_.companyCode).values.map {
        dailyQuotes =>
          val dateToPrice = dailyQuotes
            .filter(dailyQuote => dailyQuote.closingPrice.orElse(dailyQuote.lastBestBidPrice).isDefined)
            .map {
              dailyQuote =>
                val price = dailyQuote.closingPrice.getOrElse(dailyQuote.lastBestBidPrice.get) +
                  exRightDividendResult.filter(e => e.companyCode == dailyQuote.companyCode && (e.date.isBefore(dailyQuote.date) || e.date.isEqual(dailyQuote.date))).map(_.cashDividend).sum
                (dailyQuote.date.toString, price)
            }
          val buyPrice = dateToPrice.head._2
          val roi = dateToPrice.map(_._2).map(price => (price - buyPrice) / buyPrice)
          Scatter(dateToPrice.map(_._1), roi, name = dailyQuotes.head.companyCode)
      }.toSeq

      Plotly.plot("compare-roi.html", data, Layout(title = "Compare ROI", height = 768, autosize = true))
    }

    Await.result(render, Duration.Inf)
  }
}
