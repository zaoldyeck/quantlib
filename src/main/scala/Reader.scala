import Settings._
import com.github.tototoshi.csv._
import db.table.{FinancialAnalysis, OperatingRevenue}
import slick.collection.heterogeneous.HNil
import slick.lifted.TableQuery

import scala.reflect.io.Path._
//import slick.jdbc.PostgresProfile.api._
//import slick.jdbc.MySQLProfile.api._
import slick.jdbc.H2Profile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class Reader {
  def readFinancialAnalysis(): Unit = {
    financialAnalysis.dir.toDirectory.files.foreach { file =>
      val reader = CSVReader.open(file.jfile, "Big5")
      val year = file.name.split('_').head.toInt + 1911
      val rows = reader.all().tail

      val financialAnalysis = TableQuery[FinancialAnalysis]
      val dbIOActions = rows.map {
        values =>
          val splitValues = values.splitAt(2)
          val transferValues = splitValues._2.map {
            case v if v == "NA" => None
            case v if v.contains("*") => None
            case value => Some(value.toDouble)
          }
          val companyCode = values(0)
          val data = Query((year :: companyCode :: values(1) :: transferValues(0) :: transferValues(1) ::
            transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) :: transferValues(6) :: transferValues(7) ::
            transferValues(8) :: transferValues(9) :: transferValues(10) :: transferValues(11) :: transferValues(12) :: transferValues(13) ::
            transferValues(14) :: transferValues(15) :: transferValues(16) :: transferValues(17) :: transferValues(18) :: HNil))
          val exists = financialAnalysis.filter(f => f.year === year && f.companyCode === companyCode).exists
          val selectExpression = data.filterNot(_ => exists)
          financialAnalysis.map(f => (f.year :: f.companyCode :: f.companyName :: f.liabilitiesOfAssetsRatioPercentage :: f.longTermFundsToPropertyAndPlantAndEquipmentPercentage :: f.currentRatioPercentage :: f.quickRatioPercentage :: f.timesInterestEarnedRatioPercentage :: f.averageCollectionTurnoverTimes :: f.averageCollectionDays :: f.averageInventoryTurnoverTimes :: f.averageInventoryDays :: f.propertyAndPlantAndEquipmentTurnoverTimes :: f.totalAssetsTurnoverTimes :: f.returnOnTotalAssetsPercentage :: f.returnOnEquityPercentage :: f.profitBeforeTaxToCapitalPercentage :: f.profitToSalesPercentage :: f.earningsPerShareNTD :: f.cashFlowRatioPercentage :: f.cashFlowAdequacyRatioPercentage :: f.cashFlowReinvestmentRatioPercentage :: HNil)).forceInsertQuery(selectExpression)
      }

      val db = Database.forConfig("db")
      try {
        val resultFuture = db.run(DBIO.sequence(dbIOActions))
        Await.result(resultFuture, Duration.Inf)
      } finally db.close
      reader.close()
    }
  }

  def readOperatingRevenue(): Unit = {
    operatingRevenue.dir.toDirectory.files.foreach { file =>
      val reader = CSVReader.open(file.jfile)
      val strings = file.name.split('_')
      val year = strings.head.toInt + 1911
      val month = strings.last.split('.').head.toInt
      val rows = reader.all().tail

      val operatingRevenues = TableQuery[OperatingRevenue]
      val dbIOActions = rows.map {
        values =>
          val splitValues = values.splitAt(5)
          val transferValues = splitValues._2.init.map {
            case v if v == "" => None
            case value => Some(value.toDouble)
          }
          val companyCode = values(2)
          val data = Query((year, month, Option(values(4)), companyCode, values(3), transferValues(0), transferValues(1), transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7)))
          val exists = operatingRevenues.filter(o => o.companyCode === companyCode && o.year === year && o.month === month).exists
          val selectExpression = data.filterNot(_ => exists)
          operatingRevenues.map(o => (o.year, o.month, o.industry, o.companyCode, o.companyName, o.monthlyRevenue, o.lastMonthRevenue, o.lastYearMonthlyRevenue, o.monthlyRevenueComparedLastMonthPercentage, o.monthlyRevenueComparedLastYearPercentage, o.cumulativeRevenue, o.lastYearCumulativeRevenue, o.cumulativeRevenueComparedLastYearPercentage)).forceInsertQuery(selectExpression)
      }

      val db = Database.forConfig("db")
      try {
        val resultFuture = db.run(DBIO.sequence(dbIOActions))
        Await.result(resultFuture, Duration.Inf)
      } finally db.close
      reader.close()
    }
  }
}
