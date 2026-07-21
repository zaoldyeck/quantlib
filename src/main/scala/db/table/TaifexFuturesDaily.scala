package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._

/**
 * TAIFEX 期貨每日交易行情.
 *
 * Official free source:
 * https://www.taifex.com.tw/cht/3/dlFutDailyMarketView
 */
class TaifexFuturesDaily(tag: Tag) extends Table[TaifexFuturesDailyRow](tag, "taifex_futures_daily") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def date = column[LocalDate]("date")

  def contractCode = column[String]("contract_code")

  def contractMonth = column[String]("contract_month")

  def open = column[Option[Double]]("open")

  def high = column[Option[Double]]("high")

  def low = column[Option[Double]]("low")

  def close = column[Option[Double]]("close")

  def change = column[Option[Double]]("change")

  def changePercentage = column[Option[Double]]("change_pct")

  def volume = column[Long]("volume")

  def settlementPrice = column[Option[Double]]("settlement_price")

  def openInterest = column[Option[Long]]("open_interest")

  def bestBid = column[Option[Double]]("best_bid")

  def bestAsk = column[Option[Double]]("best_ask")

  def historicalHigh = column[Option[Double]]("historical_high")

  def historicalLow = column[Option[Double]]("historical_low")

  def tradingHalt = column[Option[String]]("trading_halt")

  def tradingSession = column[String]("trading_session")

  def spreadSingleVolume = column[Option[Long]]("spread_single_volume")

  def idx = index("idx_TaifexFuturesDaily_date_contract_month_session",
    (date, contractCode, contractMonth, tradingSession), unique = true)

  def idxContractDate = index("idx_TaifexFuturesDaily_contract_date", (contractCode, date), unique = false)

  def * = (
    id, date, contractCode, contractMonth, open, high, low, close, change, changePercentage,
    volume, settlementPrice, openInterest, bestBid, bestAsk, historicalHigh, historicalLow,
    tradingHalt, tradingSession, spreadSingleVolume
  ) <> (TaifexFuturesDailyRow.tupled, TaifexFuturesDailyRow.unapply)
}

case class TaifexFuturesDailyRow(
  id: Long,
  date: LocalDate,
  contractCode: String,
  contractMonth: String,
  open: Option[Double],
  high: Option[Double],
  low: Option[Double],
  close: Option[Double],
  change: Option[Double],
  changePercentage: Option[Double],
  volume: Long,
  settlementPrice: Option[Double],
  openInterest: Option[Long],
  bestBid: Option[Double],
  bestAsk: Option[Double],
  historicalHigh: Option[Double],
  historicalLow: Option[Double],
  tradingHalt: Option[String],
  tradingSession: String,
  spreadSingleVolume: Option[Long]
)
