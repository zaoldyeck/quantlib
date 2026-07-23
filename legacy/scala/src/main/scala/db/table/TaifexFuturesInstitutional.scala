package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._

/**
 * TAIFEX 三大法人：區分各期貨契約-依日期.
 *
 * Official free download page exposes a rolling three-year window:
 * https://www.taifex.com.tw/cht/3/futContractsDateView?menuid1=03
 */
class TaifexFuturesInstitutional(tag: Tag)
  extends Table[TaifexFuturesInstitutionalRow](tag, "taifex_futures_institutional") {

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def date = column[LocalDate]("date")

  def contractCode = column[String]("contract_code")

  def productName = column[String]("product_name")

  def investorType = column[String]("investor_type")

  def longVolume = column[Long]("long_volume")

  def longValueThousands = column[Long]("long_value_thousands")

  def shortVolume = column[Long]("short_volume")

  def shortValueThousands = column[Long]("short_value_thousands")

  def netVolume = column[Long]("net_volume")

  def netValueThousands = column[Long]("net_value_thousands")

  def longOpenInterest = column[Long]("long_open_interest")

  def longOpenInterestValueThousands = column[Long]("long_oi_value_thousands")

  def shortOpenInterest = column[Long]("short_open_interest")

  def shortOpenInterestValueThousands = column[Long]("short_oi_value_thousands")

  def netOpenInterest = column[Long]("net_open_interest")

  def netOpenInterestValueThousands = column[Long]("net_oi_value_thousands")

  def idx = index("idx_TaifexFuturesInstitutional_date_contract_investor",
    (date, contractCode, investorType), unique = true)

  def idxContractDate = index("idx_TaifexFuturesInstitutional_contract_date", (contractCode, date), unique = false)

  def * = (
    id, date, contractCode, productName, investorType,
    longVolume, longValueThousands, shortVolume, shortValueThousands, netVolume, netValueThousands,
    longOpenInterest, longOpenInterestValueThousands, shortOpenInterest, shortOpenInterestValueThousands,
    netOpenInterest, netOpenInterestValueThousands
  ) <> (TaifexFuturesInstitutionalRow.tupled, TaifexFuturesInstitutionalRow.unapply)
}

case class TaifexFuturesInstitutionalRow(
  id: Long,
  date: LocalDate,
  contractCode: String,
  productName: String,
  investorType: String,
  longVolume: Long,
  longValueThousands: Long,
  shortVolume: Long,
  shortValueThousands: Long,
  netVolume: Long,
  netValueThousands: Long,
  longOpenInterest: Long,
  longOpenInterestValueThousands: Long,
  shortOpenInterest: Long,
  shortOpenInterestValueThousands: Long,
  netOpenInterest: Long,
  netOpenInterestValueThousands: Long
)
