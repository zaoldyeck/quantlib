package setting

import java.time.LocalDate

/**
 * 庫藏股執行情形（公司買回自家股票）— MOPS t35sc09.
 *
 * Endpoint: https://mopsov.twse.com.tw/mops/web/ajax_t35sc09
 * Method: single POST → Big5-HKSCS HTML body containing a result table.
 *
 * Form takes a YEAR-MONTH RANGE (yearb/monthb to yeare/monthe) — we always
 * pull a single calendar month at a time so output filename `{year}_{month}.html`
 * lines up with the per-month idempotence pattern used elsewhere.
 *
 * Schema (per row, after parse):
 *   公告日 / 公司代號 / 公司名稱 / 預定買回張數 / 每股買回價格區間（low ~ high）/
 *   執行起 / 執行迄 / 已買回張數 / 已買回占資本比率(%)
 *
 * Historical depth: ~2002+ (verify on first run; older months may return empty).
 *
 * Value: ⭐⭐⭐ — buyback announcement is a +3-5% same-day signal; combined with
 * SBL build-up may indicate a managed-floor / squeeze setup.
 */
case class TreasuryStockBuybackSetting(year: Int = LocalDate.now.getYear,
                                        month: Int = LocalDate.now.getMonthValue) extends Setting {
  val page: String = conf.getString("data.treasuryStockBuyback.page")

  val twse: MopsDirectDetail = new MopsDirectDetail(year, month, "sii",
    conf.getString("data.treasuryStockBuyback.twse.dir"), page) {
    override def formData: Map[String, String] = Map(
      "step" -> "1",
      "firstin" -> "1",
      "TYPEK" -> "sii",
      "yearb" -> minguoYear.toString,
      "monthb" -> f"$month%02d",
      "yeare" -> minguoYear.toString,
      "monthe" -> f"$month%02d"
    )
  }

  val tpex: MopsDirectDetail = new MopsDirectDetail(year, month, "otc",
    conf.getString("data.treasuryStockBuyback.tpex.dir"), page) {
    override def formData: Map[String, String] = Map(
      "step" -> "1",
      "firstin" -> "1",
      "TYPEK" -> "otc",
      "yearb" -> minguoYear.toString,
      "monthb" -> f"$month%02d",
      "yeare" -> minguoYear.toString,
      "monthe" -> f"$month%02d"
    )
  }

  val markets: Seq[MopsDirectDetail] = Seq(twse, tpex)
}
