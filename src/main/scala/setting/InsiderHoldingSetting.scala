package setting

import java.time.LocalDate

/**
 * 內部人持股轉讓「事前申報」日報表 — MOPS t56sb12_q1 (上市) / q2 (上櫃).
 *
 * Endpoint pattern (verified via Playwright 2026-04-29):
 *   1. POST `/mops/web/ajax_t56sb12_q1` (or q2) — returns intermediate HTML form
 *      Body: encodeURIComponent=1&step=0&firstin=1&off=1&year=YYY&month=MM&day=DD
 *   2. POST `/mops/web/ajax_t56sb12` — returns actual data table
 *      Body: encodeURIComponent=1&run=&step=2&year=YYY&month=MM&day=DD&report=SY|OY&firstin=true
 *      `report=SY` = 上市 / `OY` = 上櫃
 *
 * Daily granularity × all stocks per market — true bulk, no per-company query.
 * 興櫃/公開發行 (q3/q4) and 未轉讓 (q5/q6/q7) skipped — outside our liquidity universe.
 *
 * Schema (12 cols, verified 2026-04-28 — sub-headers in row 1 expand 自有/信託 split):
 *   [0] 異動情形（轉讓方式 + 轉讓股數 sub-cols）
 *   [1] 申報日期 (民國 yyy/mm/dd)
 *   [2] 公司代號
 *   [3] 公司名稱
 *   [4] 申報人身分（董事 / 監察人 / 經理人 / 持股 ≥ 10% 大股東）
 *   [5] 姓名
 *   [6] 預定轉讓方式及股數
 *   [7] 每日於盤中交易最大得轉讓股數
 *   [8] 受讓人
 *   [9-10] 目前持有股數（自有 / 信託）
 *   [11-12] 預定轉讓總股數（自有 / 信託）
 *   [13-14] 預定轉讓後持股（自有 / 信託）
 *
 * Forward signal:
 *   申報「事前」轉讓 → 內部人即將賣出 → forward 5-30d -2~-5% CAR (TW academic)
 *
 * Historical depth: ~2007+ (date selector starts there). One file per (market, date)
 * = ~2 markets × ~250 trading days/year × 18 years ≈ 9,000 files for full backfill.
 */
case class InsiderHoldingSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: InsiderHoldingDetail = new InsiderHoldingDetail(date, "twse", "q1", "SY",
    "https://mopsov.twse.com.tw/mops/web/ajax_t56sb12_q1",
    "./data/insider_holding/twse")
  val tpex: InsiderHoldingDetail = new InsiderHoldingDetail(date, "tpex", "q2", "OY",
    "https://mopsov.twse.com.tw/mops/web/ajax_t56sb12_q2",
    "./data/insider_holding/tpex")

  val markets: Seq[InsiderHoldingDetail] = Seq(twse, tpex)
}

/**
 * Detail for InsiderHolding 2-step ajax. Carries `report` ID for step 2.
 * `firstDate` set to 2007-01 (rough backfill start).
 */
class InsiderHoldingDetail(_date: LocalDate, val market: String, val qSuffix: String,
                            val reportCode: String,
                            val pageUrl: String,
                            override val dir: String)
  extends Detail(LocalDate.of(2007, 1, 1), None, _date) {

  val minguoYear: Int = _date.getYear - 1911

  override val page: String = pageUrl
  protected[this] val file: String = pageUrl
  override def url: String = pageUrl

  // Filename: YYYY_M_D.html (year-prefixed, like daily quote)
  override val fileName: String = s"${_date.getYear}_${_date.getMonthValue}_${_date.getDayOfMonth}.html"

  // Step 1 form data — firstin=1 returns intermediate page.
  override def formData: Map[String, String] = Map(
    "encodeURIComponent" -> "1",
    "step" -> "0",
    "firstin" -> "1",
    "off" -> "1",
    "year" -> minguoYear.toString,
    "month" -> f"${_date.getMonthValue}%02d",
    "day" -> f"${_date.getDayOfMonth}%02d"
  )

  // Step 2 — actual data fetch URL (shared across q1/q2/.../q7).
  val dataUrl: String = "https://mopsov.twse.com.tw/mops/web/ajax_t56sb12"

  def step2FormData: Map[String, String] = Map(
    "encodeURIComponent" -> "1",
    "run" -> "",
    "step" -> "2",
    "year" -> minguoYear.toString,
    "month" -> f"${_date.getMonthValue}%02d",
    "day" -> f"${_date.getDayOfMonth}%02d",
    "report" -> reportCode,
    "firstin" -> "true"
  )
}
