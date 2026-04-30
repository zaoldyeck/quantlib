package db.table

import java.time.LocalDate

import slick.jdbc.PostgresProfile.api._

/**
 * TDCC 集保戶股權分散表 — weekly per-tier holder distribution.
 * Source: https://opendata.tdcc.com.tw/getOD.ashx?id=1-5
 *
 * Holding tier codes (1-17):
 *   1  : 1-999 股 (零股)
 *   2  : 1,000-5,000
 *   3  : 5,001-10,000
 *   4  : 10,001-15,000
 *   5  : 15,001-20,000
 *   6  : 20,001-30,000
 *   7  : 30,001-40,000
 *   8  : 40,001-50,000
 *   9  : 50,001-100,000
 *   10 : 100,001-200,000
 *   11 : 200,001-400,000
 *   12 : 400,001-600,000
 *   13 : 600,001-800,000
 *   14 : 800,001-1,000,000 (千張以下最大級距)
 *   15 : >1,000,000 股（千張大戶）
 *   16 : 差異數（帳列/實際不符）
 *   17 : 合計
 *
 * 千張大戶分析常用 tier 15 (人數變化) + tier 15 (股數占比變化) 作為 smart-money signal.
 */
class TdccShareholding(tag: Tag)
  extends Table[(Long, LocalDate, String, Short, Int, Long, Double)](tag, "tdcc_shareholding") {

  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def dataDate = column[LocalDate]("data_date")

  def companyCode = column[String]("company_code")

  def holdingTier = column[Short]("holding_tier")

  def numHolders = column[Int]("num_holders")

  def numShares = column[Long]("num_shares")

  def pctOfOutstanding = column[Double]("pct_of_outstanding")

  def idx = index("idx_TdccShareholding_date_code_tier", (dataDate, companyCode, holdingTier), unique = true)

  def * = (id, dataDate, companyCode, holdingTier, numHolders, numShares, pctOfOutstanding)
}
