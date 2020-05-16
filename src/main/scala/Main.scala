import java.time.LocalDate

object Main {
  def main(args: Array[String]): Unit = {
    val task = new Task
    val reader = new Reader
    val question = new Question
    val backtest = new Backtest
    //task.pullOperatingRevenue()
    //reader.readFinancialAnalysis()
    //question.compareROI(Set("0050", "0052", "0056", "006208", "00692", "006201", "0051"), LocalDate.of(2006, 9, 12))//, LocalDate.of(2006, 9, 12)) //, LocalDate.of(2017, 5, 17)) //, LocalDate.of(2017, 5, 17))
    //backtest.dollarCostAveraging(Set("0050", "006208","2330"), LocalDate.of(2018, 1, 1), LocalDate.now, Set(6, 16, 26), 10000)
  }

  // 月營收(90/6 - 102/12) https://mops.twse.com.tw/nas/t21/sii/t21sc03_101_12.html
  // https://mops.twse.com.tw/nas/t21/sii/t21sc03_90_6.html
  // 月營收(102/1 後) https://mops.twse.com.tw/nas/t21/sii/t21sc03_102_1_0.html
  // 除權息(92/5/5 後) https://www.twse.com.tw/exchangeReport/TWT49U?response=csv&strDate=20190701&endDate=20190719
  // 除權息(92/5/5 後) https://www.twse.com.tw/zh/page/trading/exchange/TWT48U.html
  // 減資(100/1/1 後) https://www.twse.com.tw/zh/page/trading/exchange/TWTAUU.html
  // 減資(100/1/1 後) https://www.twse.com.tw/exchangeReport/TWTAUU?response=csv&strDate=20190701&endDate=20190714
  // 每日成交價(93/2/11 後) https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=20190718&type=ALL
  // 每日本益比、殖利率及股價淨值比(94/9/2 後) https://www.twse.com.tw/zh/page/trading/exchange/BWIBBU_d.html
  // 每日本益比、殖利率及股價淨值比(94/9/2 後) https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=csv&date=20190718&selectType=ALL
  // 財務分析匯總表(101 年後) POST https://mops.twse.com.tw/mops/web/ajax_t51sb02 form: ncodeURIComponent=1&run=Y&step=1&TYPEK=sii&year=107&isnew=&firstin=1&off=1&ifrs=Y
  // 財務分析匯總表(78 年 - 102 年) POST https://mops.twse.com.tw/mops/web/ajax_t51sb02 form: encodeURIComponent=1&step=1&firstin=1&off=1&TYPEK=sii&year=96
  // 營益分析查詢彙總表(102 年第 1 季後) POST https://mops.twse.com.tw/mops/web/ajax_t163sb06 form: encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=sii&year=107&season=04
  // 營益分析查詢彙總表(78 年第 1 季 - 101 年第 4 季) POST https://mops.twse.com.tw/mops/web/ajax_t51sb06 form: encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=sii&year=96&season=04
  // 財報(從 98 年起) https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-"+str(year)+"Q"+str(season)+".zip&filePath=/home/html/nas/ifrs/"+str(year)+"/
  // 每5秒指數統計 https://www.twse.com.tw/zh/page/trading/exchange/MI_5MINS_INDEX.html
  // 三大法人買賣超日報 https://www.twse.com.tw/zh/page/trading/fund/T86.html
}
