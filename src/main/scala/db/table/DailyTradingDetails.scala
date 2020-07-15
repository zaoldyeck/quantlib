package db.table

import java.time.LocalDate

import slick.collection.heterogeneous.HNil
import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._

/**
 * 三大法人買賣超日報
 * twse https://www.twse.com.tw/zh/page/trading/fund/T86.html from 2012-5-2
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

  def foreignInvestorsExcludeDealersTotalBuy = column[Option[Int]]("foreign_investors_exclude_dealers_total_buy")

  def foreignInvestorsExcludeDealersTotalSell = column[Option[Int]]("foreign_investors_exclude_dealers_total_sell")

  def foreignInvestorsExcludeDealersDifference = column[Option[Int]]("foreign_investors_exclude_dealers_difference")

  def foreignDealersTotalBuy = column[Option[Int]]("foreign_dealers_total_buy")

  def foreignDealersTotalSell = column[Option[Int]]("foreign_dealers_total_sell")

  def foreignDealersDifference = column[Option[Int]]("foreign_dealers_difference")

  def foreignInvestorsTotalBuy = column[Int]("foreign_investors_total_buy")

  def foreignInvestorsTotalSell = column[Int]("foreign_investors_total_sell")

  def foreignInvestorsDifference = column[Int]("foreign_investors_difference")

  def securitiesInvestmentTrustCompaniesTotalBuy = column[Int]("securities_investment_trust_companies_total_buy")

  def securitiesInvestmentTrustCompaniesTotalSell = column[Int]("securities_investment_trust_companies_total_sell")

  def securitiesInvestmentTrustCompaniesDifference = column[Int]("securities_investment_trust_companies_difference")

  def dealersProprietaryTotalBuy = column[Option[Int]]("dealers_proprietary_total_buy")

  def dealersProprietaryTotalSell = column[Option[Int]]("dealers_proprietary_total_sell")

  def dealersProprietaryDifference = column[Option[Int]]("dealers_proprietary_difference")

  def dealersHedgeTotalBuy = column[Option[Int]]("dealers_hedge_total_buy")

  def dealersHedgeTotalSell = column[Option[Int]]("dealers_hedge_total_sell")

  def dealersHedgeDifference = column[Option[Int]]("dealers_hedge_difference")

  def dealersTotalBuy = column[Int]("dealers_total_buy")

  def dealersTotalSell = column[Int]("dealers_total_sell")

  def dealersDifference = column[Int]("dealers_difference")

  def totalDifference = column[Int]("total_difference")

  def idx = index("idx_DailyTradingDetails_market_date_companyCode", (market, date, companyCode), unique = true)

  def * = (id :: market :: date :: companyCode :: companyName :: foreignInvestorsExcludeDealersTotalBuy :: foreignInvestorsExcludeDealersTotalSell :: foreignInvestorsExcludeDealersDifference :: foreignDealersTotalBuy :: foreignDealersTotalSell :: foreignDealersDifference :: foreignInvestorsTotalBuy :: foreignInvestorsTotalSell :: foreignInvestorsDifference :: securitiesInvestmentTrustCompaniesTotalBuy :: securitiesInvestmentTrustCompaniesTotalSell :: securitiesInvestmentTrustCompaniesDifference :: dealersProprietaryTotalBuy :: dealersProprietaryTotalSell :: dealersProprietaryDifference :: dealersHedgeTotalBuy :: dealersHedgeTotalSell :: dealersHedgeDifference :: dealersTotalBuy :: dealersTotalSell :: dealersDifference :: totalDifference :: HNil).mapTo[DailyTradingDetailsRow]
}

case class DailyTradingDetailsRow(id: Long, market: String, date: LocalDate, companyCode: String, companyName: String, foreignInvestorsExcludeDealersTotalBuy: Option[Int], foreignInvestorsExcludeDealersTotalSell: Option[Int], foreignInvestorsExcludeDealersDifference: Option[Int], foreignDealersTotalBuy: Option[Int], foreignDealersTotalSell: Option[Int], foreignDealersDifference: Option[Int], foreignInvestorsTotalBuy: Int, foreignInvestorsTotalSell: Int, foreignInvestorsDifference: Int, securitiesInvestmentTrustCompaniesTotalBuy: Int, securitiesInvestmentTrustCompaniesTotalSell: Int, securitiesInvestmentTrustCompaniesDifference: Int, dealersProprietaryTotalBuy: Option[Int], dealersProprietaryTotalSell: Option[Int], dealersProprietaryDifference: Option[Int], dealersHedgeTotalBuy: Option[Int], dealersHedgeTotalSell: Option[Int], dealersHedgeDifference: Option[Int], dealersTotalBuy: Int, dealersTotalSell: Int, dealersDifference: Int, totalDifferenceInt: Int)