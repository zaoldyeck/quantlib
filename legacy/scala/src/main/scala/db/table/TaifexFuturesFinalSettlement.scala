package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._

/**
 * TAIFEX index-futures final settlement prices.
 *
 * Official source:
 * https://www.taifex.com.tw/cht/5/futIndxFSP
 */
class TaifexFuturesFinalSettlement(tag: Tag)
    extends Table[TaifexFuturesFinalSettlementRow](tag, "taifex_futures_final_settlement") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def date = column[LocalDate]("date")

  def contractCode = column[String]("contract_code")

  def contractMonth = column[String]("contract_month")

  def finalSettlementPrice = column[Double]("final_settlement_price")

  def idx = index("idx_TaifexFuturesFinalSettlement_date_contract",
    (date, contractCode, contractMonth), unique = true)

  def * = (
    id, date, contractCode, contractMonth, finalSettlementPrice
  ) <> (TaifexFuturesFinalSettlementRow.tupled, TaifexFuturesFinalSettlementRow.unapply)
}

case class TaifexFuturesFinalSettlementRow(
  id: Long,
  date: LocalDate,
  contractCode: String,
  contractMonth: String,
  finalSettlementPrice: Double
)
