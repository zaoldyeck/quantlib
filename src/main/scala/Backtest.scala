import java.time.LocalDate

import db.table.{DailyQuote, ExRightDividend}
import plotly.layout.Layout
import plotly.{Plotly, Scatter}
import slick.jdbc.H2Profile.api._
import slick.lifted.TableQuery

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.jdk.StreamConverters._

class Backtest {
  /**
   * 1. 把每個購買日期點列出來
   * 2. 生成每個購買日到到期日的獨立還原股價序列，另紀錄投入資金、持股數
   * 3. reduce 每個序列 by date, 用(當日市值-當日至今投入金額)/當日至今投入金額，即可算出每日報酬率，也可以算出每日資產
   *
   * @param companyCodes
   * @param fromDate
   * @param toDate
   * @param daysOfMonth
   * @param amountPerInvestment
   * @param fees
   */
  def dollarCostAveraging(companyCodes: Set[String], fromDate: LocalDate, toDate: LocalDate, daysOfMonth: Set[Int], amountPerInvestment: Int, fees: Int = 0): Unit = {
    val td = toDate.plusDays(10)
    val db = Database.forConfig("db")
    val dailyQuote = TableQuery[DailyQuote]
    val dailyQuoteAction = dailyQuote.filter(d => d.companyCode.inSet(companyCodes) && d.date >= fromDate && d.date <= td).sortBy(_.date).result

    val exRightDividend = TableQuery[ExRightDividend]
    val exRightDividendAction = exRightDividend.filter(e => e.companyCode.inSet(companyCodes) && e.date >= fromDate && e.date <= td).sortBy(_.date).result

    val render = for {
      dailyQuoteResult <- db.run(dailyQuoteAction)
      exRightDividendResult <- db.run(exRightDividendAction)
    } yield {
      val estimatedTradingDates = fromDate.datesUntil(toDate).filter(localDate => daysOfMonth.contains(localDate.getDayOfMonth)).toScala(Seq)
      val data = dailyQuoteResult.groupBy(_.companyCode).values.map {
        dailyQuotes =>
          val dateToDailyQuote = dailyQuotes.map(dailyQuote => dailyQuote.date -> dailyQuote).toMap
          val lastDate = dailyQuotes.last.date
          val actualTradingDates = estimatedTradingDates.filter(_.isBefore(lastDate)).map {
            buyDate =>
              @scala.annotation.tailrec
              def getTradingDay(date: LocalDate): LocalDate = {
                val dailyQuoteOption = dateToDailyQuote.get(date)
                if (dailyQuoteOption.isDefined && (dailyQuoteOption.get.openingPrice.isDefined || dailyQuoteOption.get.lastBestAskPrice.isDefined)) {
                  date
                } else getTradingDay(date.plusDays(1))
              }

              getTradingDay(buyDate)
          }

          val actualDailyQuotes = dailyQuotes.filter(_.date.compareTo(toDate) <= 0)
          case class DailyIncome(date: LocalDate, investmentCost: Int, bookValue: Double)
          val dailyIncomes = actualTradingDates.map {
            date =>
              val holdDailyQuotes = actualDailyQuotes.filter(dailyQuote => dailyQuote.date.compareTo(date) >= 0)
              val tradingDay = holdDailyQuotes.head
              val bidPrice = tradingDay.openingPrice.getOrElse(tradingDay.lastBestAskPrice.get)
              val shareHolding = amountPerInvestment / bidPrice

              holdDailyQuotes
                .filter(dailyQuote => dailyQuote.closingPrice.orElse(dailyQuote.lastBestBidPrice).isDefined)
                .map {
                  dailyQuote =>
                    val price = dailyQuote.closingPrice.getOrElse(dailyQuote.lastBestBidPrice.get) +
                      exRightDividendResult.filter(e => e.companyCode == dailyQuote.companyCode && (e.date.isBefore(dailyQuote.date) || e.date.isEqual(dailyQuote.date))).map(_.cashDividend).sum
                    DailyIncome(dailyQuote.date, amountPerInvestment, shareHolding * price)
                }
          }.reduce(_ ++ _).groupBy(_.date).view.mapValues(_.reduce((a, b) => DailyIncome(a.date, a.investmentCost + b.investmentCost, a.bookValue + b.bookValue))).values.toSeq.sortBy(_.date)

          val dates = dailyIncomes.map(_.date.toString)
          val roi = dailyIncomes.map(dailyIncome => (dailyIncome.bookValue - dailyIncome.investmentCost) / dailyIncome.investmentCost)
          Scatter(dates, roi, name = actualDailyQuotes.head.companyCode)
      }.toSeq
      Plotly.plot("dollar-cost-averaging.html", data, Layout(title = "Dollar Cost Averaging", height = 768, autosize = true))
    }
    Await.result(render, Duration.Inf)
  }
}
