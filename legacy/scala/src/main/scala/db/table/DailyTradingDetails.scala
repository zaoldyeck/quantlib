package db.table

import java.time.LocalDate

import slick.collection.heterogeneous.HNil
import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._

/**
 * 三大法人買賣超日報
 * twse https://www.twse.com.tw/zh/trading/foreign/t86.html from 2012-5-2
 * tpex https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade.php from 2007-4-23
 *
 * @param tag
 */
class DailyTradingDetails(tag: Tag) extends Table[DailyTradingDetailsRow](tag, "daily_trading_details") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def market = column[String]("market")

  def date = column[LocalDate]("date")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def foreignInvestorsExcludeDealersTotalBuy = column[Option[Long]]("foreign_investors_exclude_dealers_total_buy")

  def foreignInvestorsExcludeDealersTotalSell = column[Option[Long]]("foreign_investors_exclude_dealers_total_sell")

  def foreignInvestorsExcludeDealersDifference = column[Option[Long]]("foreign_investors_exclude_dealers_difference")

  def foreignDealersTotalBuy = column[Option[Long]]("foreign_dealers_total_buy")

  def foreignDealersTotalSell = column[Option[Long]]("foreign_dealers_total_sell")

  def foreignDealersDifference = column[Option[Long]]("foreign_dealers_difference")

  def foreignInvestorsTotalBuy = column[Long]("foreign_investors_total_buy")

  def foreignInvestorsTotalSell = column[Long]("foreign_investors_total_sell")

  def foreignInvestorsDifference = column[Long]("foreign_investors_difference")

  def securitiesInvestmentTrustCompaniesTotalBuy = column[Long]("securities_investment_trust_companies_total_buy")

  def securitiesInvestmentTrustCompaniesTotalSell = column[Long]("securities_investment_trust_companies_total_sell")

  def securitiesInvestmentTrustCompaniesDifference = column[Long]("securities_investment_trust_companies_difference")

  def dealersProprietaryTotalBuy = column[Option[Long]]("dealers_proprietary_total_buy")

  def dealersProprietaryTotalSell = column[Option[Long]]("dealers_proprietary_total_sell")

  def dealersProprietaryDifference = column[Option[Long]]("dealers_proprietary_difference")

  def dealersHedgeTotalBuy = column[Option[Long]]("dealers_hedge_total_buy")

  def dealersHedgeTotalSell = column[Option[Long]]("dealers_hedge_total_sell")

  def dealersHedgeDifference = column[Option[Long]]("dealers_hedge_difference")

  def dealersTotalBuy = column[Long]("dealers_total_buy")

  def dealersTotalSell = column[Long]("dealers_total_sell")

  def dealersDifference = column[Long]("dealers_difference")

  def totalDifference = column[Long]("total_difference")

  def idx = index("idx_DailyTradingDetails_market_date_companyCode", (market, date, companyCode), unique = true)

  def * = (id :: market :: date :: companyCode :: companyName :: foreignInvestorsExcludeDealersTotalBuy :: foreignInvestorsExcludeDealersTotalSell :: foreignInvestorsExcludeDealersDifference :: foreignDealersTotalBuy :: foreignDealersTotalSell :: foreignDealersDifference :: foreignInvestorsTotalBuy :: foreignInvestorsTotalSell :: foreignInvestorsDifference :: securitiesInvestmentTrustCompaniesTotalBuy :: securitiesInvestmentTrustCompaniesTotalSell :: securitiesInvestmentTrustCompaniesDifference :: dealersProprietaryTotalBuy :: dealersProprietaryTotalSell :: dealersProprietaryDifference :: dealersHedgeTotalBuy :: dealersHedgeTotalSell :: dealersHedgeDifference :: dealersTotalBuy :: dealersTotalSell :: dealersDifference :: totalDifference :: HNil).mapTo[DailyTradingDetailsRow]
}

case class DailyTradingDetailsRow(id: Long, market: String, date: LocalDate, companyCode: String, companyName: String, foreignInvestorsExcludeDealersTotalBuy: Option[Long], foreignInvestorsExcludeDealersTotalSell: Option[Long], foreignInvestorsExcludeDealersDifference: Option[Long], foreignDealersTotalBuy: Option[Long], foreignDealersTotalSell: Option[Long], foreignDealersDifference: Option[Long], foreignInvestorsTotalBuy: Long, foreignInvestorsTotalSell: Long, foreignInvestorsDifference: Long, securitiesInvestmentTrustCompaniesTotalBuy: Long, securitiesInvestmentTrustCompaniesTotalSell: Long, securitiesInvestmentTrustCompaniesDifference: Long, dealersProprietaryTotalBuy: Option[Long], dealersProprietaryTotalSell: Option[Long], dealersProprietaryDifference: Option[Long], dealersHedgeTotalBuy: Option[Long], dealersHedgeTotalSell: Option[Long], dealersHedgeDifference: Option[Long], dealersTotalBuy: Long, dealersTotalSell: Long, dealersDifference: Long, totalDifference: Long)