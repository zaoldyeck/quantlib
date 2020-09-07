package reader

import com.github.tototoshi.csv._
import db.table._
import me.tongfei.progressbar.ProgressBar
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.model.Element
import slick.collection.heterogeneous.HNil
import slick.jdbc.PostgresProfile.api._
import util.QuantlibCSVReader
//import slick.jdbc.MySQLProfile.api._
//import slick.jdbc.H2Profile.api._
import setting._
import slick.lifted.TableQuery

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.io.Path._
import scala.util.Try

class FinancialReader extends Reader {
  def readFinancialAnalysis(): Unit = {
    val financialAnalysis = TableQuery[FinancialAnalysis]
    val query = financialAnalysis.map(f => (f.market, f.year)).distinct.result
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf)

    val files = FinancialAnalysisSetting().getMarketFilesFromDirectory.filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name.split('_')(0).toInt))).par
    val pb = new ProgressBar("Read financial analysis -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read financial analysis of ${marketFile.market}-${marketFile.file.name}")
        val year = marketFile.file.name.split('_').head.toInt
        val reader = CSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val rows = reader.all().tail
        val data = rows.map {
          values =>
            val splitValues = values.splitAt(2)
            val transferValues = splitValues._2.map {
              case v if v == "NA" => None
              case v if v.contains("*") => None
              case value => Some(value.toDouble)
            }
            val companyCode = values.head
            marketFile.market :: year :: companyCode :: values(1) :: transferValues.head :: transferValues(1) ::
              transferValues(2) :: transferValues(3) :: transferValues(4) :: transferValues(5) :: transferValues(6) :: transferValues(7) ::
              transferValues(8) :: transferValues(9) :: transferValues(10) :: transferValues(11) :: transferValues(12) :: transferValues(13) ::
              transferValues(14) :: transferValues(15) :: transferValues(16) :: transferValues(17) :: transferValues(18) :: HNil
        }

        val dbIO = financialAnalysis.map(f => f.market :: f.year :: f.companyCode :: f.companyName :: f.liabilitiesOfAssetsRatioPercentage :: f.longTermFundsToPropertyAndPlantAndEquipmentPercentage :: f.currentRatioPercentage :: f.quickRatioPercentage :: f.timesInterestEarnedRatioPercentage :: f.averageCollectionTurnoverTimes :: f.averageCollectionDays :: f.averageInventoryTurnoverTimes :: f.averageInventoryDays :: f.propertyAndPlantAndEquipmentTurnoverTimes :: f.totalAssetsTurnoverTimes :: f.returnOnTotalAssetsPercentage :: f.returnOnEquityPercentage :: f.profitBeforeTaxToCapitalPercentage :: f.profitToSalesPercentage :: f.earningsPerShareNTD :: f.cashFlowRatioPercentage :: f.cashFlowAdequacyRatioPercentage :: f.cashFlowReinvestmentRatioPercentage :: HNil) ++= data
        dbRun(dbIO)
        reader.close()
        pb.step()
    }
    pb.close()
  }

  def readBalanceSheet(): Unit = {
    val conciseBalanceSheet = TableQuery[ConciseBalanceSheet]
    val query = conciseBalanceSheet.map(b => (b.market, b.`type`, b.year, b.quarter)).distinct.result
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf)
    val fileNamePattern = """(\d+)_(\d+)_(\w)_(\w).*""".r
    val files = BalanceSheetSetting().getMarketFilesFromDirectory.filterNot(m => {
      val fileNamePattern(y, q, _, t) = m.file.name
      val `type` = t match {
        case "i" => "individual"
        case "c" => "consolidated"
      }
      dataAlreadyInDB.contains((m.market, `type`, y.toInt, q.toInt))
    }).par
    val pb = new ProgressBar("Read balance sheet -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read balance sheet of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern(y, q, _, t) = marketFile.file.name
        val year = y.toInt
        val quarter = q.toInt
        val `type` = t match {
          case "i" => "individual"
          case "c" => "consolidated"
        }

        val reader = CSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val rows = reader.allWithHeaders()
        val data = rows.flatMap {
          values =>
            val companyCode = values("公司代號")
            val companyName = values("公司名稱")

            values
              .filterNot { case (k, v) => k == "公司代號" || k == "公司名稱" || k == "出表日期" || k == "年度" || k == "季別" }
              .map { case (k, v) => k.replace(" ", "") -> v.replace(" ", "").replace(",", "") }
              .filter(v => Try(v._2.toDouble).isSuccess)
              .map {
                case (k, v) => (marketFile.market, `type`, year, quarter, companyCode, companyName, k, v.toDouble)
              }
        }

        val dbIO = conciseBalanceSheet.map(b => (b.market, b.`type`, b.year, b.quarter, b.companyCode, b.companyName, b.title, b.value)) ++= data
        dbRun(dbIO)
        reader.close()
        pb.step()
    }
    if (files.nonEmpty) Await.result(db.run(sqlu"""refresh materialized view concise_balance_sheet_individual"""), Duration.Inf)
    pb.close()
  }

  def readIncomeStatement(): Unit = {
    val conciseIncomeStatementProgressive = TableQuery[ConciseIncomeStatementProgressive]
    val query = conciseIncomeStatementProgressive.map(i => (i.market, i.`type`, i.year, i.quarter)).distinct.result
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf)
    val fileNamePattern = """(\d+)_(\d+)_(\w)_(\w).*""".r
    val files = IncomeStatementSetting().getMarketFilesFromDirectory.filterNot(m => {
      val fileNamePattern(y, q, _, t) = m.file.name
      val `type` = t match {
        case "i" => "individual"
        case "c" => "consolidated"
      }
      dataAlreadyInDB.contains((m.market, `type`, y.toInt, q.toInt))
    }).par
    val pb = new ProgressBar("Read income statement -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read income statement of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern(y, q, _, t) = marketFile.file.name
        val year = y.toInt
        val quarter = q.toInt
        val `type` = t match {
          case "i" => "individual"
          case "c" => "consolidated"
        }

        val reader = CSVReader.open(marketFile.file.jfile, "Big5-HKSCS")
        val rows = reader.allWithHeaders()
        val data = rows.flatMap {
          values =>
            val companyCode = values.get("公司代號")
            val companyName = values.get("公司名稱")

            values
              .filterNot { case (k, v) => k == "公司代號" || k == "公司名稱" || k == "出表日期" || k == "年度" || k == "季別" }
              .map { case (k, v) => k.replace(" ", "") -> v.replace(" ", "").replace(",", "") }
              .filter(v => Try(v._2.toDouble).isSuccess)
              .map {
                case (k, v) => (marketFile.market, `type`, year, quarter, companyCode.get, companyName.get, k, v.toDouble)
              }
        }

        val dbIO = conciseIncomeStatementProgressive.map(i => (i.market, i.`type`, i.year, i.quarter, i.companyCode, i.companyName, i.title, i.value)) ++= data
        dbRun(dbIO)
        reader.close()
        pb.step()
    }
    if (files.nonEmpty) Await.result(db.run(DBIO.seq(sqlu"""refresh materialized view concise_income_statement_individual""", sqlu"""refresh materialized view concise_financial_statement_with_titles""")), Duration.Inf)
    pb.close()
  }

  def readFinancialStatements(): Unit = {
    val balanceSheet = TableQuery[BalanceSheet]
    val incomeStatementProgressive = TableQuery[IncomeStatementProgressive]
    val cashFlowsProgressive = TableQuery[CashFlowsProgressive]
    val balanceSheetQuery = balanceSheet.map(b => (b.market, b.year, b.quarter, b.companyCode)).distinct.result
    val incomeStatementProgressiveQuery = incomeStatementProgressive.map(i => (i.market, i.year, i.quarter, i.companyCode)).distinct.result
    val cashFlowsProgressiveQuery = cashFlowsProgressive.map(c => (c.market, c.year, c.quarter, c.companyCode)).distinct.result
    val future = for {
      b <- db.run(balanceSheetQuery)
      i <- db.run(incomeStatementProgressiveQuery)
      c <- db.run(cashFlowsProgressiveQuery)
    } yield {
      (b, i, c, b.toSet & i.toSet & c.toSet)
    }
    val (balanceSheetDataAlreadyInDB, incomeStatementProgressiveDataAlreadyInDB, cashFlowsProgressiveDataAlreadyInDB, dataAlreadyInDB) = Await.result(future, Duration.Inf)
    val files = FinancialStatementsSetting().twse.dir.toDirectory.dirs.map {
      dir =>
        val dirNamePattern = """(\d+)_(\d+).*""".r
        val dirNamePattern(y, q) = dir.name
        val year = y.toInt
        val quarter = q.toInt
        year match {
          case y if y < 2019 =>
            dir.files.toSeq.map {
              file =>
                val fileNamePattern = """(\w+).*""".r
                val fileNamePattern(companyCode) = file.name
                ("tw", year, quarter, companyCode, file)
            }
          case _ =>
            dir.files.toSeq.map {
              file =>
                val splitName = file.name.split('-')
                val `type` = splitName(4)
                val companyCode = splitName(5)
                (`type`, companyCode, file)
            }.sortBy(_._1).distinctBy(_._2).map { case (_, companyCode, file) => ("tw", year, quarter, companyCode, file) }
        }
    }.reduce(_ ++ _).filterNot { case (market, year, quarter, companyCode, _) => dataAlreadyInDB.contains((market, year, quarter, companyCode)) }.par
    case class Data(balanceSheetData: Seq[(String, Int, Int, String, String, Double)], incomeStatementData: Seq[(String, Int, Int, String, String, Double)], cashFlowsProgressiveData: Seq[(String, Int, Int, String, String, Double)])
    val browser = JsoupBrowser()
    val pb = new ProgressBar("Read financial statements -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      case (market, year, quarter, companyCode, file) =>
        //println(s"Read financial statements of $year-$quarter-$companyCode")
        val data = year match {
          case y if y < 2013 =>
            def getData(rows: Iterable[Iterable[String]]): Seq[(String, Int, Int, String, String, Double)] = {
              rows.filter(values => values.size > 1)
                .map(_.take(2).toSeq)
                .filter(values => values(1).nonEmpty)
                .map(_.map(_.filterNot(_.isWhitespace)))
                .map(values => (values.head, values(1).replace(",", "").toDouble))
                .toSeq.distinctBy(_._1)
                .map { case (k, v) => (market, year, quarter, companyCode, k, v) }
            }

            val doc = browser.parseFile(file.jfile, "Big5-HKSCS")
            lazy val rows = (doc >?> element("#content_d > div > table.result_table.hasBorder > tbody"))
              .map(tbody => (tbody >> elements("tr")).map(tr => (tr >> elements("td")).map(_.text)))
              .getOrElse(Seq.empty)
            lazy val spanBalanceSheetRowsAndOthers = rows.drop(2).span(_.nonEmpty)
            lazy val spanIncomeStatementRowsAndCashFlowsRows = spanBalanceSheetRowsAndOthers._2.drop(2).span(_.nonEmpty)
            val balanceSheetData = if (balanceSheetDataAlreadyInDB.contains((market, year, quarter, companyCode))) Seq.empty else getData(spanBalanceSheetRowsAndOthers._1)
            val incomeStatementData = if (incomeStatementProgressiveDataAlreadyInDB.contains((market, year, quarter, companyCode))) Seq.empty else getData(spanIncomeStatementRowsAndCashFlowsRows._1)
            val cashFlowsData = if (cashFlowsProgressiveDataAlreadyInDB.contains((market, year, quarter, companyCode))) Seq.empty else getData(spanIncomeStatementRowsAndCashFlowsRows._2)
            Data(balanceSheetData, incomeStatementData, cashFlowsData)
          case y if y < 2019 =>
            def getData(element: Element, isIncomeStatement: Boolean = false): Seq[(String, Int, Int, String, String, Double)] = {
              val valueIndex = if (isIncomeStatement && quarter != 4) 3 else 1
              (element >> elements("tr")).map(tr => (tr >> elements("td")).map(_.text).toSeq)
                .filter(values => values.size > 1)
                .map(values => (values.head, Try(values(valueIndex)).getOrElse(values(1))))
                .filter(_._2.nonEmpty)
                .map { case (s, v) => (s.filterNot(_.isWhitespace), v.filterNot(_.isWhitespace).replace(",", "").toDouble) }
                .toSeq.distinctBy(_._1)
                .map { case (s, v) => (market, year, quarter, companyCode, s, v) }
            }

            val doc = browser.parseFile(file.jfile)
            lazy val balanceSheetOption = doc >?> element("#content_d > center > table.result_table.hasBorder > tbody")
            val balanceSheetData = if (balanceSheetDataAlreadyInDB.contains((market, year, quarter, companyCode))) Seq.empty else balanceSheetOption.map(getData(_)).getOrElse(Seq.empty)

            lazy val incomeStatementOption = doc >?> element("#content_d > center > table:nth-child(11) > tbody")
            val incomeStatementData = if (incomeStatementProgressiveDataAlreadyInDB.contains((market, year, quarter, companyCode))) Seq.empty else incomeStatementOption.map(getData(_, isIncomeStatement = true)).getOrElse(Seq.empty)

            lazy val cashFlowsOption = doc >?> element("#content_d > center > table:nth-child(13) > tbody")
            val cashFlowsData = if (cashFlowsProgressiveDataAlreadyInDB.contains((market, year, quarter, companyCode))) Seq.empty else cashFlowsOption.map(getData(_)).getOrElse(Seq.empty)
            Data(balanceSheetData, incomeStatementData, cashFlowsData)
          case _ =>
            def getData(element: Element, isIncomeStatement: Boolean = false): Seq[(String, Int, Int, String, String, Double)] = {
              val valueIndex = if (isIncomeStatement && quarter != 4) 4 else 2
              (element >> elements("tr")).map(tr => (tr >> elements("td")).map(td => (td >?> text("span.zh")).getOrElse(td.text)).toSeq)
                .filter(values => values.size > 1)
                .map(values => (values(1), Try(values(valueIndex)).getOrElse(values(2))))
                .filter(_._2.nonEmpty)
                .map {
                  case (s, v) =>
                    val valueString = v.filterNot(_.isWhitespace).replace(",", "")
                    val vDouble = valueString.replace("(", "").replace(")", "").toDouble
                    val value = if (valueString.contains('(') && valueString.contains(')')) -vDouble else vDouble
                    (s.filterNot(_.isWhitespace), value)
                }.toSeq.distinctBy(_._1)
                .map { case (s, v) => (market, year, quarter, companyCode, s, v) }
            }

            val doc = browser.parseFile(file.jfile)
            lazy val balanceSheetOption = doc >?> element("body > div.container > div.content > table:nth-child(3) > tbody")
            val balanceSheetData = if (balanceSheetDataAlreadyInDB.contains((market, year, quarter, companyCode))) Seq.empty else balanceSheetOption.map(getData(_)).getOrElse(Seq.empty)

            lazy val incomeStatementOption = doc >?> element("body > div.container > div.content > table:nth-child(7) > tbody")
            val incomeStatementData = if (incomeStatementProgressiveDataAlreadyInDB.contains((market, year, quarter, companyCode))) Seq.empty else incomeStatementOption.map(getData(_, isIncomeStatement = true)).getOrElse(Seq.empty)

            lazy val cashFlowsOption = doc >?> element("body > div.container > div.content > table:nth-child(11) > tbody")
            val cashFlowsData = if (cashFlowsProgressiveDataAlreadyInDB.contains((market, year, quarter, companyCode))) Seq.empty else cashFlowsOption.map(getData(_)).getOrElse(Seq.empty)
            Data(balanceSheetData, incomeStatementData, cashFlowsData)
        }
        val balanceSheetDBIO = balanceSheet.map(b => (b.market, b.year, b.quarter, b.companyCode, b.title, b.value)) ++= data.balanceSheetData
        val incomeStatementDBIO = incomeStatementProgressive.map(i => (i.market, i.year, i.quarter, i.companyCode, i.title, i.value)) ++= data.incomeStatementData
        val cashFlowsDBIO = cashFlowsProgressive.map(c => (c.market, c.year, c.quarter, c.companyCode, c.title, c.value)) ++= data.cashFlowsProgressiveData
        Await.result(db.run(DBIO.seq(balanceSheetDBIO, incomeStatementDBIO, cashFlowsDBIO)), Duration.Inf)
        pb.step()
    }
    if (files.nonEmpty) Await.result(db.run(DBIO.seq(sqlu"""refresh materialized view income_statement_individual""", sqlu"""refresh materialized view cash_flows_individual""", sqlu"""refresh materialized view balance_sheet_with_titles""", sqlu"""refresh materialized view cash_flows_with_titles""")), Duration.Inf)
    pb.close()
  }

  def readOperatingRevenue(): Unit = {
    val operatingRevenue = TableQuery[OperatingRevenue]
    val query = operatingRevenue.map(o => (o.market, o.`type`, o.year, o.month)).distinct.result
    val dataAlreadyInDB = Await.result(db.run(query), Duration.Inf)
    val fileNamePattern = """(\d+)_(\d+)_(\w).*""".r
    val browser = JsoupBrowser()
    val files = OperatingRevenueSetting().getMarketFilesFromDirectory.filterNot(mf => {
      val fileNamePattern(y, m, t) = mf.file.name
      val `type` = t match {
        case "i" => "individual"
        case "c" => "consolidated"
      }
      dataAlreadyInDB.contains((mf.market, `type`, y.toInt, m.toInt))
    }).par
    val pb = new ProgressBar("Read operating revenue -", files.size)
    files.tasksupport = taskSupport
    files.foreach {
      marketFile =>
        //println(s"Read operating revenue of ${marketFile.market}-${marketFile.file.name}")
        val fileNamePattern(y, m, t) = marketFile.file.name
        val year = y.toInt
        val month = m.toInt
        val `type` = t match {
          case "i" => "individual"
          case "c" => "consolidated"
        }

        val data = marketFile.file.extension match {
          case "html" =>
            val doc = browser.parseFile(marketFile.file.jfile, "Big5-HKSCS")
            val rows = (doc >> elements("tr")).map {
              tr =>
                val thOption = tr >?> element("th")
                if (thOption.isDefined) {
                  (tr >> elements("th")).map(_.text)
                } else {
                  (tr >> elements("td")).map(_.text)
                }
            }.iterator

            val industryPattern = """產業別：(.*)""".r

            @scala.annotation.tailrec
            def getData(rowOption: Option[Iterable[String]], industry: String = "", data: Seq[(String, String, Int, Int, String, String, Option[String], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])] = Seq()): Seq[(String, String, Int, Int, String, String, Option[String], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])] = {
              rowOption match {
                case Some(v) =>
                  val values = v.toSeq
                  if (values.size == 10 && values.head != "公司 代號") {
                    val splitValues = values.splitAt(2)
                    val transferValues = splitValues._2.map(_.replace(",", "")).map {
                      case v if v == "" => None
                      case value: String => Some(value.toDouble)
                    }
                    val d = (marketFile.market, `type`, year, month, values.head, values(1), Option(industry), transferValues.head, transferValues(1), transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7))
                    getData(rows.nextOption(), industry, data :+ d)
                  } else {
                    val in = values.head match {
                      case industryPattern(v) => v
                      case _ => industry
                    }
                    getData(rows.nextOption(), in, data)
                  }
                case None => data
              }
            }

            getData(rows.nextOption)
          case "csv" =>
            val reader = QuantlibCSVReader.open(marketFile.file.jfile, if (year > 2012) "UTF-8" else "Big5-HKSCS")
            val rows = reader.all().tail
            val data = rows.map {
              values =>
                values.size match {
                  case 11 =>
                    val splitValues = values.splitAt(2)
                    val transferValues = splitValues._2.init.map {
                      case v if v == "N/A" => None
                      case value => Some(value.toDouble)
                    }
                    (marketFile.market, `type`, year, month, values.head, values(1), None, transferValues.head, transferValues(1), transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7))
                  case _ =>
                    val splitValues = values.splitAt(5)
                    val transferValues = splitValues._2.init.map {
                      case v if v == "" => None
                      case value => Some(value.toDouble)
                    }
                    (marketFile.market, `type`, year, month, values(2), values(3), Option(values(4)), transferValues.head, transferValues(1), transferValues(2), transferValues(3), transferValues(4), transferValues(5), transferValues(6), transferValues(7))
                }
            }
            reader.close()
            data
        }

        val dbIO = operatingRevenue.map(o => (o.market, o.`type`, o.year, o.month, o.companyCode, o.companyName, o.industry, o.monthlyRevenue, o.lastMonthRevenue, o.lastYearMonthlyRevenue, o.monthlyRevenueComparedLastMonthPercentage, o.monthlyRevenueComparedLastYearPercentage, o.cumulativeRevenue, o.lastYearCumulativeRevenue, o.cumulativeRevenueComparedLastYearPercentage)) ++= data.distinctBy(o => (o._1, o._5))
        dbRun(dbIO)
        pb.step()
    }
    pb.close()
  }
}
