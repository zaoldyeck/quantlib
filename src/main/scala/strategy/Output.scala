package strategy

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import plotly._
import plotly.element._
import plotly.layout._

/** Writes backtest results to disk: Plotly HTML chart, CSV of trades and holdings. */
object Output {
  val ResultDir: String = "./result"
  private val df = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")

  def writeAll(primary: BacktestResult, benchmark: BacktestResult): String = {
    new File(ResultDir).mkdirs()
    val stamp = java.time.LocalDateTime.now.format(df)
    val base = s"$ResultDir/${primary.strategy}_$stamp"

    writeNavChart(primary, benchmark, s"${base}.html")
    writeTradesCsv(primary, s"${base}_trades.csv")
    writeMonthlyNavCsv(primary, benchmark, s"${base}_monthly.csv")

    base
  }

  private def writeNavChart(primary: BacktestResult, benchmark: BacktestResult, path: String): Unit = {
    val primaryTrace = Scatter(
      primary.dailyNav.map(_._1.toString),
      primary.dailyNav.map(_._2),
      name = primary.strategy,
      mode = ScatterMode(ScatterMode.Lines)
    )

    val benchTrace = Scatter(
      benchmark.dailyNav.map(_._1.toString),
      benchmark.dailyNav.map(_._2),
      name = benchmark.strategy,
      mode = ScatterMode(ScatterMode.Lines)
    )

    val layout = Layout(
      title = s"${primary.strategy} vs ${benchmark.strategy}",
      xaxis = Axis(title = "Date"),
      yaxis = Axis(title = "NAV (TWD)"),
      height = 600,
      autosize = true
    )

    Plotly.plot(path, Seq(primaryTrace, benchTrace), layout, openInBrowser = false)
  }

  private def writeTradesCsv(result: BacktestResult, path: String): Unit = {
    val w = new PrintWriter(path)
    try {
      w.println("date,code,kind,shares,price,cost")
      result.trades.foreach { t =>
        w.println(s"${t.date},${t.code},${t.kind},${t.shares},${t.price},${t.cost}")
      }
    } finally w.close()
  }

  private def writeMonthlyNavCsv(primary: BacktestResult, benchmark: BacktestResult, path: String): Unit = {
    val primaryByMonth = primary.dailyNav
      .groupBy { case (d, _) => (d.getYear, d.getMonthValue) }
      .view.mapValues(_.maxBy(_._1))
      .toMap

    val benchByMonth = benchmark.dailyNav
      .groupBy { case (d, _) => (d.getYear, d.getMonthValue) }
      .view.mapValues(_.maxBy(_._1))
      .toMap

    val allMonths = (primaryByMonth.keySet ++ benchByMonth.keySet).toSeq.sorted

    val w = new PrintWriter(path)
    try {
      w.println(s"month,${primary.strategy}_nav,${benchmark.strategy}_nav")
      allMonths.foreach { ym =>
        val p = primaryByMonth.get(ym).map(_._2).getOrElse(0.0)
        val b = benchByMonth.get(ym).map(_._2).getOrElse(0.0)
        w.println(f"${ym._1}%04d-${ym._2}%02d,$p%.2f,$b%.2f")
      }
    } finally w.close()
  }
}
