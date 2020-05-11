package db.table

import slick.collection.heterogeneous.HNil
//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
import slick.jdbc.H2Profile.api._

/**
 * https://mops.twse.com.tw/mops/web/t51sb02_q1
 * 財務分析資料查詢彙總表
 *
 * @param tag
 */
class FinancialAnalysis(tag: Tag) extends Table[FinancialAnalysisRow](tag, "financial_analysis") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def year = column[Int]("year")

  def companyCode = column[String]("company_code")

  def companyName = column[String]("company_name")

  def liabilitiesOfAssetsRatioPercentage = column[Option[Double]]("liabilities/assets_ratio(%)")

  def longTermFundsToPropertyAndPlantAndEquipmentPercentage = column[Option[Double]]("Long-term_funds_to_property&plant&equipment(%)")

  def currentRatioPercentage = column[Option[Double]]("current_ratio(%)")

  def quickRatioPercentage = column[Option[Double]]("quick_ratio(%)")

  def timesInterestEarnedRatioPercentage = column[Option[Double]]("times_interest_earned_ratio(%)")

  def averageCollectionTurnoverTimes = column[Option[Double]]("average_collection_turnover(times)")

  def averageCollectionDays = column[Option[Double]]("average_collection_days")

  def averageInventoryTurnoverTimes = column[Option[Double]]("average_inventory_turnover(times)")

  def averageInventoryDays = column[Option[Double]]("average_inventory_days")

  def propertyAndPlantAndEquipmentTurnoverTimes = column[Option[Double]]("property&plant&equipment_turnover(times)")

  def totalAssetsTurnoverTimes = column[Option[Double]]("total_assets_turnover(times)")

  def returnOnTotalAssetsPercentage = column[Option[Double]]("return_on_total_assets(%)")

  def returnOnEquityPercentage = column[Option[Double]]("return_on_equity(%)")

  def profitBeforeTaxToCapitalPercentage = column[Option[Double]]("profit_before_tax_to_capital(%)")

  def profitToSalesPercentage = column[Option[Double]]("profit_to_sales(%)")

  def earningsPerShareNTD = column[Option[Double]]("earnings_per_share(NTD)")

  def cashFlowRatioPercentage = column[Option[Double]]("cash_flow_ratio(%)")

  def cashFlowAdequacyRatioPercentage = column[Option[Double]]("cash_flow_adequacy_ratio(%)")

  def cashFlowReinvestmentRatioPercentage = column[Option[Double]]("cash_flow_reinvestment_ratio(%)")

  def idx = index("idx_FinancialAnalysis_year_companyCode", (year, companyCode), unique = true)

  def * = (id :: year :: companyCode :: companyName :: liabilitiesOfAssetsRatioPercentage :: longTermFundsToPropertyAndPlantAndEquipmentPercentage :: currentRatioPercentage :: quickRatioPercentage :: timesInterestEarnedRatioPercentage :: averageCollectionTurnoverTimes :: averageCollectionDays :: averageInventoryTurnoverTimes :: averageInventoryDays :: propertyAndPlantAndEquipmentTurnoverTimes :: totalAssetsTurnoverTimes :: returnOnTotalAssetsPercentage :: returnOnEquityPercentage :: profitBeforeTaxToCapitalPercentage :: profitToSalesPercentage :: earningsPerShareNTD :: cashFlowRatioPercentage :: cashFlowAdequacyRatioPercentage :: cashFlowReinvestmentRatioPercentage :: HNil).mapTo[FinancialAnalysisRow]
}

case class FinancialAnalysisRow(id: Long, year: Int, companyCode: String, companyName: String, liabilitiesOfAssetsRatioPercentage: Option[Double], longTermFundsToPropertyAndPlantAndEquipmentPercentage: Option[Double], currentRatioPercentage: Option[Double], quickRatioPercentage: Option[Double], timesInterestEarnedRatioPercentage: Option[Double], averageCollectionTurnoverTimes: Option[Double], averageCollectionDays: Option[Double], averageInventoryTurnoverTimes: Option[Double], averageInventoryDays: Option[Double], propertyAndPlantAndEquipmentTurnoverTimes: Option[Double], totalAssetsTurnoverTimes: Option[Double], returnOnTotalAssetsPercentage: Option[Double], returnOnEquityPercentage: Option[Double], profitBeforeTaxToCapitalPercentage: Option[Double], profitToSalesPercentage: Option[Double], earningsPerShareNTD: Option[Double], cashFlowRatioPercentage: Option[Double], cashFlowAdequacyRatioPercentage: Option[Double], cashFlowReinvestmentRatioPercentage: Option[Double])