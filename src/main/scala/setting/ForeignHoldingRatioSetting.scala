package setting

import java.time.LocalDate

/**
 * 外資及陸資投資持股比率 (snapshot ratio, daily)
 *
 * twse https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS?response=csv&date=YYYYMMDD&selectType=ALLBUT0999 from 2005 (CSV Big5)
 * tpex https://www.tpex.org.tw/www/zh-tw/insti/qfii?date=YYY/MM/DD from ~2010 (JSON UTF-8)
 *
 * 用途：外資接頂訊號（外資持股比率逼近 50% / 法令上限 → topping-out signal）。
 * 與 daily_trading_details 的 foreign_investors_difference 是兩回事：
 *   - _difference = daily FLOW（今天淨買賣超股數）
 *   - foreign_held_ratio = cumulative SNAPSHOT（累積總持股佔發行股數 %）
 *
 * 主欄位：
 *   - outstanding_shares (發行股數)
 *   - foreign_remaining_shares (外資尚可投資股數)
 *   - foreign_held_shares (外資持有股數)
 *   - foreign_remaining_ratio (尚可投資比率)
 *   - foreign_held_ratio (持股比率) ← key signal
 *   - foreign_limit_ratio (法令投資上限)
 */
case class ForeignHoldingRatioSetting(date: LocalDate = LocalDate.now) extends Setting {
  val twse: TwseDetail = new TwseDetail(LocalDate.of(2005, 1, 3), None, date) {
    val file: String = conf.getString("data.foreignHoldingRatio.twse.file")
    val dir: String = conf.getString("data.foreignHoldingRatio.twse.dir")

    override def validate(downloaded: java.io.File): Option[String] =
      // Header text changed in ~2009 ECFA era: 2005-2009 uses "外資投資持股統計",
      // 2010+ uses "外資及陸資投資持股統計". Match the common substring tokens.
      validateCSVSchema(
        downloaded,
        expectedHeaderKeywords = Seq("外資", "投資持股統計", "證券代號"),
        minDataRows = 20,
        encoding = "Big5-HKSCS"
      )
  }

  val tpex: TpexDetail = new TpexDetail(LocalDate.of(2010, 1, 4), None, date) {
    val file: String = conf.getString("data.foreignHoldingRatio.tpex.file")
    val dir: String = conf.getString("data.foreignHoldingRatio.tpex.dir")

    // TPEx JSON; empty days handled as holiday upstream.
    override def validate(downloaded: java.io.File): Option[String] = None
  }

  val markets: Seq[Detail] = Seq(twse, tpex)
}
