import com.github.tototoshi.csv._

import scala.io.Source
import Settings._

import scala.reflect.io.Path._
import db.table.{FinancialAnalysis, FinancialAnalysisRow, OperatingRevenue}
import slick.lifted.TableQuery
import slick.jdbc.H2Profile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class Reader {
  def readFinancialAnalysis(): Unit = {
    financialAnalysis.dir.toDirectory.files.foreach { file =>
      val reader = CSVReader.open(file.jfile, "Big5")
      //      val value: Seq[Map[String, String]] = reader.allWithHeaders()
      //      value.foreach(x => println(x))

      val year = file.name.split('_').head.toInt + 1911
      val rows = reader.all().tail
      val financialAnalysisRows = rows.map {
        values =>
          val splitValues = values.splitAt(2)
          val transferValues = splitValues._2.map {
            case v if v == "NA" => None
            case v if v.contains("*") => None
            case value => Some(value.toDouble)
          }
          FinancialAnalysisRow(0, year, values(0), values(1), transferValues(0), transferValues(1),
            transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7),
            transferValues(8), transferValues(9), transferValues(10), transferValues(11), transferValues(12), transferValues(13),
            transferValues(14), transferValues(15), transferValues(16), transferValues(17), transferValues(18))
      }
      val financialAnalysis = TableQuery[FinancialAnalysis]
      val db = Database.forConfig("h2mem1")
      try {
        val resultFuture = db.run(financialAnalysis ++= financialAnalysisRows)
        //val resultFuture = db.run(financialAnalysis.schema.create)
        Await.result(resultFuture, Duration.Inf)
      } finally db.close
      reader.close()
    }
  }

  def readOperatingRevenue(): Unit = {
    operatingRevenue.dir.toDirectory.files.foreach { file =>
      val reader = CSVReader.open(file.jfile, "Big5")
      val strings = file.name.split('_')
      val year = strings.head.toInt + 1911
      val month = strings.last.split('.').head.toInt
      val rows = reader.all().tail
      val operatingRevenueRows = rows.map {
        values =>
          val splitValues = values.splitAt(5)
          val transferValues = splitValues._2.init.map {
            case v if v == "" => None
            case value => Some(value.toDouble)
          }
          (0L, year, month, Some(values(4)), values(2), values(3), transferValues(0), transferValues(1), transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7))
      }

      val operatingRevenue = TableQuery[OperatingRevenue]
      val db = Database.forConfig("h2mem1")
      try {
        val resultFuture = db.run(operatingRevenue ++= operatingRevenueRows)
        //val resultFuture = db.run(financialAnalysis.schema.create)
        Await.result(resultFuture, Duration.Inf)
      } finally db.close
      reader.close()
    }
  }
}
