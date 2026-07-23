package setting

import java.time.LocalDate

/**
 * Detail for a MOPS single-step ajax endpoint that returns the HTML body directly
 * (no `<input name="filename">` filename hidden field, unlike t108sb27 ex-right dividend).
 *
 * Common params:
 *   step=1, firstin=1, off=1, TYPEK=sii|otc, year=民國 yyy, month=MM (01..12)
 *
 * Used by:
 *   - 庫藏股 t35sc09 (treasury stock buyback) — yearb/monthb/yeare/monthe range form
 *   - 內部人持股異動 t56sb01 — year/month form (single month)
 *   - 現金增資 t24sb03 — year/month form (single month)
 *
 * Endpoints share the same MOPS rate-limiting + Big5-HKSCS encoding contract.
 * Response saved as `{dir}/{year}/{year}_{month}.html`.
 */
abstract class MopsDirectDetail(year: Int, month: Int, typek: String,
                                 override val dir: String,
                                 override val page: String)
  extends Detail(LocalDate.of(2003, 1, 1), None, LocalDate.of(year, month, 1)) {

  protected[this] val file: String = page
  val minguoYear: Int = year - 1911

  // HTML output (not CSV) — readers parse via scala-scraper.
  override val fileName: String = s"${year}_${month}.html"

  override def url: String = page

  override def formData: Map[String, String]
}
