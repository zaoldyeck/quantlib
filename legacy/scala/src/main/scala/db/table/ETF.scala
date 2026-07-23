package db.table

import slick.jdbc.PostgresProfile.api._

import java.time.LocalDate
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._

/**
 * ETF
 * all https://www.twse.com.tw/rwd/zh/ETF/list?response=json
 * domestic https://www.twse.com.tw/rwd/zh/ETF/domestic?response=json
 * foreign https://www.twse.com.tw/rwd/zh/ETF/foreign?response=json
 *
 * @param tag
 */
class ETF(tag: Tag) extends Table[(Long, LocalDate, String, String, String, String, String)](tag, "etf") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

  def listingDate = column[LocalDate]("listing_date")

  def companyCode = column[String]("company_code")

  def name = column[String]("name")

  def issuer = column[String]("issuer")

  def index = column[String]("index")

  def region = column[String]("region")

  def idx = index("idx_ETF_companyCode", (companyCode), unique = true)

  def * = (id, listingDate, companyCode, name, issuer, index, region)
}
