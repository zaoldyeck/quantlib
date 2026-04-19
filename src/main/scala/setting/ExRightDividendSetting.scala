package setting

import java.time.LocalDate

/**
 * MOPS t108sb27 除權息公告 — monthly granularity, POST two-step flow.
 *
 * `markets` provides per-market form-data maps; Crawler uses the shared `page`
 * and `file` URLs from config. File output path follows the project standard
 * `{dir}/{year}/{year}_{month}.csv`.
 *
 * Firstly supported month: 2024-07 (when legacy TWT49U / exDailyQ_result.php
 * stopped returning data). Older months already in DB used the legacy endpoint
 * format; Reader detects format by filename (monthly YYYY_M.csv = MOPS format,
 * day-range YYYY_M_D.csv = legacy format).
 */
case class ExRightDividendSetting(year: Int = LocalDate.now.getYear,
                                   month: Int = LocalDate.now.getMonthValue) extends Setting {
  val page: String = conf.getString("data.exRightDividend.page")
  val fileUrl: String = conf.getString("data.exRightDividend.file")

  val twse: MopsDetail = new MopsDetail(year, month, "sii",
    conf.getString("data.exRightDividend.twse.dir"), page, fileUrl)
  val tpex: MopsDetail = new MopsDetail(year, month, "otc",
    conf.getString("data.exRightDividend.tpex.dir"), page, fileUrl)

  val markets: Seq[MopsDetail] = Seq(twse, tpex)
}

/**
 * Detail for a MOPS two-step query. `url` in this context is the step-1 page
 * (returns HTML with a `filename` hidden field); Crawler uses `fileUrl` for
 * step 2 (streams the CSV).
 */
class MopsDetail(year: Int, month: Int, typek: String,
                 override val dir: String,
                 override val page: String,
                 val fileUrl: String)
  extends Detail(LocalDate.of(2003, 1, 1), None, LocalDate.of(year, month, 1)) {

  protected[this] val file: String = page
  val minguoYear: Int = year - 1911

  override val fileName: String = s"${year}_${month}.csv"

  override def url: String = page

  override def formData: Map[String, String] = Map(
    "step" -> "1",
    "firstin" -> "ture",
    "off" -> "1",
    "TYPEK" -> typek,
    "year" -> minguoYear.toString,
    "month" -> month.toString,
    "b_date" -> "1",
    "e_date" -> "31",
    "type" -> "0"
  )
}
