import java.io.{FileInputStream, InputStreamReader, StringReader}
import java.time.LocalDate

import Settings._
import com.github.tototoshi.csv.{CSVFormat, _}
import db.table.{DailyQuote, FinancialAnalysis, OperatingRevenue}
import slick.collection.heterogeneous.HNil
import slick.lifted.TableQuery

import scala.io.Source
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
      val fileNamePattern = """(\d+)_(\d+).csv""".r
      val fileNamePattern(year, month) = file.name
      val y = year.toInt + 1911
      val m = month.toInt

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
          val data = Query((y, m, Option(values(4)), companyCode, values(3), transferValues(0), transferValues(1), transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7)))
          val exists = operatingRevenues.filter(o => o.companyCode === companyCode && o.year === y && o.month === m).exists
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

  def readDailyQuote(): Unit = {
    //CSVReader
    dailyQuote.dir.toDirectory.files.foreach { file =>
      val fin = new FileInputStream(file.jfile)
      val reader = new InputStreamReader(fin, "Big5")
      val readerLineReader = new ReaderLineReader(reader)
      object MyFormat extends DefaultCSVFormat
      val parser = new CSVParser(MyFormat)
      (1 until 200).foreach(readerLineReader.readLineWithTerminator())

      //new CSVReader(readerLineReader)
      parser.parseLine(readerLineReader.readLineWithTerminator())
    }
    /*
        val reader = CSVReader
        object MyFormat extends DefaultCSVFormat {
          //override val quoteChar = 0
          override val escapeChar = 0
        }
        val parser = new CSVParser(MyFormat)

        dailyQuote.dir.toDirectory.files.foreach { file =>
          //val reader = new InputStreamReader(new FileInputStream(file.jfile), "Big5")
          //new ReaderLineReader(reader)
          println(file.name)
          val reader2 = new StringReader(Source.fromFile(file.jfile, "Big5").getLines().drop(194).mkString("\n"))
          val r = new ReaderLineReader(reader2)
          class MyReader extends CSVReader(r) {

          }
          reader.open(reader2).all().foreach(println)




          /*
          Source.fromFile(file.jfile, "Big5").getLines().drop(194).map(parser.parseLine).takeWhile(_.isDefined).map(_.get).toSeq.foreach {
            values =>
              println(values)
          }

           */
        }

     */


    /*
    dailyQuote.dir.toDirectory.files.foreach { file =>
      implicit object MyFormat extends DefaultCSVFormat {
        override val quoteChar = 0
        override val escapeChar = 0
      }
      val reader = CSVReader.open(file.jfile, "Big5")
      val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
      val fileNamePattern(year, month, day) = file.name
      val y = year.toInt
      val m = month.toInt
      val d = day.toInt
      val date = LocalDate.of(y, m, d)
      println(date)

      val rows = reader.all().drop(194).map(_.map(_.replaceAll("[\"=]", "")))
      //rows.foreach(println)
      val dailyQuotes = TableQuery[DailyQuote]
      val dbIOActions = rows.map {
        values =>
          println(values)
          val splitValues = values.splitAt(2)
          val transferValues: Seq[Option[Double]] = splitValues._2.init.map {
            case v if v == "--" => None
            case v if v == " " || v == "X" => Some(0)
            case v if v == "+" => Some(1)
            case v if v == "-" => Some(-1)
            case value => Some(value.toDouble)
          }
          val companyCode = values(0)

          val data = Query((date,
            companyCode,
            values(1),
            transferValues(0).get.toLong,
            transferValues(1).get.toInt,
            transferValues(2).get.toLong,
            transferValues(3),
            transferValues(4),
            transferValues(5),
            transferValues(6),
            transferValues(7).get.toInt,
            transferValues(8).get,
            transferValues(9),
            transferValues(10).get.toInt,
            transferValues(11),
            transferValues(12).get.toInt,
            transferValues(13).get))
          val exists = dailyQuotes.filter(d => d.date === date && d.companyCode === companyCode).exists
          val selectExpression = data.filterNot(_ => exists)
          dailyQuotes.map(d => (d.date, d.companyCode, d.companyName, d.tradeVolume, d.transaction, d.tradeValue, d.openingPrice, d.highestPrice, d.lowestPrice, d.closingPrice, d.direction, d.change, d.lastBestBidPrice, d.lastBestBidVolume, d.lastBestAskPrice, d.lastBestAskVolume, d.priceEarningRatio)).forceInsertQuery(selectExpression)
      }

      val db = Database.forConfig("db")
      try {
        val resultFuture = db.run(DBIO.sequence(dbIOActions))
        Await.result(resultFuture, Duration.Inf)
      } finally db.close
      reader.close()
    }
    */
  }
}
