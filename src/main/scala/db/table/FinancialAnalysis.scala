package db.table

import slick.collection.heterogeneous.HNil
import slick.jdbc.H2Profile.api._

case class FinancialAnalysisRow(id: Long, year: Int, companyCode: String, companyName: String, liabilitiesOfAssetsRatioPercentage: Double, longTermFundsToPropertyAndPlantAndEquipmentPercentage: Double, currentRatioPercentage: Double, quickRatioPercentage: Double, timesInterestEarnedRatioPercentage: Double, averageCollectionTurnoverTimes: Double, averageCollectionDays: Double, averageInventoryTurnoverTimes: Double, averageInventoryDays: Double, propertyAndPlantAndEquipmentTurnoverTimes: Double, totalAssetsTurnoverTimes: Double, returnOnTotalAssetsPercentage: Double, returnOnEquityPercentage: Double, profitBeforeTaxToCapitalPercentage: Double, profitToSalesPercentage: Double, earningsPerShareNTD: Double, cashFlowRatioPercentage: Double, cashFlowAdequacyRatioPercentage: Double, cashFlowReinvestmentRatioPercentage: Double)

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

  def liabilitiesOfAssetsRatioPercentage = column[Double]("liabilities/assets_ratio(%)")

  def longTermFundsToPropertyAndPlantAndEquipmentPercentage = column[Double]("Long-term_funds_to_property&plant&equipment(%)")

  def currentRatioPercentage = column[Double]("current_ratio(%)")

  def quickRatioPercentage = column[Double]("quick_ratio(%)")

  def timesInterestEarnedRatioPercentage = column[Double]("times_interest_earned_ratio(%)")

  def averageCollectionTurnoverTimes = column[Double]("average_collection_turnover(times)")

  def averageCollectionDays = column[Double]("average_collection_days")

  def averageInventoryTurnoverTimes = column[Double]("average_inventory_turnover(times)")

  def averageInventoryDays = column[Double]("average_inventory_days")

  def propertyAndPlantAndEquipmentTurnoverTimes = column[Double]("property&plant&equipment_turnover(times)")

  def totalAssetsTurnoverTimes = column[Double]("total_assets_turnover(times)")

  def returnOnTotalAssetsPercentage = column[Double]("return_on_total_assets(%)")

  def returnOnEquityPercentage = column[Double]("return_on_equity(%)")

  def profitBeforeTaxToCapitalPercentage = column[Double]("profit_before_tax_to_capital(%)")

  def profitToSalesPercentage = column[Double]("profit_to_sales(%)")

  def earningsPerShareNTD = column[Double]("earnings_per_share(NTD)")

  def cashFlowRatioPercentage = column[Double]("cash_flow_ratio(%)")

  def cashFlowAdequacyRatioPercentage = column[Double]("cash_flow_adequacy_ratio(%)")

  def cashFlowReinvestmentRatioPercentage = column[Double]("cash_flow_reinvestment_ratio(%)")

  def idx = index("idx_a", (year, companyCode), unique = true)

  def * = (id :: year :: companyCode :: companyName :: liabilitiesOfAssetsRatioPercentage :: longTermFundsToPropertyAndPlantAndEquipmentPercentage :: currentRatioPercentage :: quickRatioPercentage :: timesInterestEarnedRatioPercentage :: averageCollectionTurnoverTimes :: averageCollectionDays :: averageInventoryTurnoverTimes :: averageInventoryDays :: propertyAndPlantAndEquipmentTurnoverTimes :: totalAssetsTurnoverTimes :: returnOnTotalAssetsPercentage :: returnOnEquityPercentage :: profitBeforeTaxToCapitalPercentage :: profitToSalesPercentage :: earningsPerShareNTD :: cashFlowRatioPercentage :: cashFlowAdequacyRatioPercentage :: cashFlowReinvestmentRatioPercentage :: HNil).mapTo[FinancialAnalysisRow]
}
