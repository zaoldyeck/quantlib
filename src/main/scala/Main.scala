import java.time.LocalDate

import plotly._
import element._
import layout._
import Plotly._
import db.table.{DailyQuote, DailyQuoteRow}
import plotly._
import plotly.element._

import scala.util.Random
import slick.lifted.TableQuery
import slick.jdbc.H2Profile.api._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Main {
  def main(args: Array[String]): Unit = {
    val task = new Task()
    val reader = new Reader()
    //task.pullIndex()
    //reader.readDailyQuote()
    //reader.readIndex()
    /*
    val 銀行及票券業 = List("公司代號", "公司名稱", "利息淨收益", "利息以外淨損益", "呆帳費用、承諾及保證責任準備提存", "營業費用", "繼續營業單位稅前淨利（淨損）", "所得稅費用（利益）", "繼續營業單位本期稅後淨利（淨損）", "停業單位損益", "合併前非屬共同控制股權損益", "本期稅後淨利（淨損）", "其他綜合損益（稅後）", "合併前非屬共同控制股權綜合損益淨額", "本期綜合損益總額（稅後）", "淨利（損）歸屬於母公司業主", "淨利（損）歸屬於共同控制下前手權益", "淨利（損）歸屬於非控制權益", "綜合損益總額歸屬於母公司業主", "綜合損益總額歸屬於共同控制下前手權益", "綜合損益總額歸屬於非控制權益", "基本每股盈餘（元）")
    val 金控業 = List("公司代號", "公司名稱", "收益", "支出及費用", "營業利益", "營業外損益", "稅前淨利（淨損）", "所得稅費用（利益）", "繼續營業單位本期淨利（淨損）", "停業單位損益", "合併前非屬共同控制股權損益", "本期淨利（淨損）", "本期其他綜合損益（稅後淨額）", "合併前非屬共同控制股權綜合損益淨額", "本期綜合損益總額", "淨利（損）歸屬於母公司業主", "淨利（淨損）歸屬於共同控制下前手權益", "淨利（損）歸屬於非控制權益", "綜合損益總額歸屬於母公司業主", "綜合損益總額歸屬於共同控制下前手權益", "綜合損益總額歸屬於非控制權益", "基本每股盈餘（元）")
    val 證券業 = List("公司代號", "公司名稱", "營業收入", "營業成本", "原始認列生物資產及農產品之利益（損失）", "生物資產當期公允價值減出售成本之變動利益（損失）", "營業毛利（毛損）", "未實現銷貨（損）益", "已實現銷貨（損）益", "營業毛利（毛損）淨額", "營業費用", "其他收益及費損淨額", "營業利益（損失）", "營業外收入及支出", "稅前淨利（淨損）", "所得稅費用（利益）", "繼續營業單位本期淨利（淨損）", "停業單位損益", "合併前非屬共同控制股權損益", "本期淨利（淨損）", "其他綜合損益（淨額）", "合併前非屬共同控制股權綜合損益淨額", "本期綜合損益總額", "淨利（淨損）歸屬於母公司業主", "淨利（淨損）歸屬於共同控制下前手權益", "淨利（淨損）歸屬於非控制權益", "綜合損益總額歸屬於母公司業主", "綜合損益總額歸屬於共同控制下前手權益", "綜合損益總額歸屬於非控制權益", "基本每股盈餘（元）")
    val 保險業 = List("公司代號", "公司名稱", "利息淨收益", "利息以外淨收益", "淨收益", "呆帳費用、承諾及保證責任準備提存", "保險負債準備淨變動", "營業費用", "繼續營業單位稅前損益", "所得稅（費用）利益", "繼續營業單位本期淨利（淨損）", "停業單位損益", "本期稅後淨利（淨損）", "本期其他綜合損益（稅後淨額）", "本期綜合損益總額", "淨利（淨損）歸屬於母公司業主", "淨利（淨損）歸屬於共同控制下前手權益", "淨利（淨損）歸屬於非控制權益", "綜合損益總額歸屬於母公司業主", "綜合損益總額歸屬於共同控制下前手權益", "綜合損益總額歸屬於非控制權益", "基本每股盈餘（元）")
    val 一般行業 = List("公司代號", "公司名稱", "營業收入", "營業成本", "營業費用", "營業利益（損失）", "營業外收入及支出", "繼續營業單位稅前純益（純損）", "所得稅費用（利益）", "繼續營業單位本期純益（純損）", "停業單位損益", "合併前非屬共同控制股權損益", "本期淨利（淨損）", "其他綜合損益（稅後淨額）", "合併前非屬共同控制股權綜合損益淨額", "本期綜合損益總額", "淨利（淨損）歸屬於母公司業主", "淨利（淨損）歸屬於共同控制下前手權益", "淨利（淨損）歸屬於非控制權益", "綜合損益總額歸屬於母公司業主", "綜合損益總額歸屬於共同控制下前手權益", "綜合損益總額歸屬於非控制權益", "基本每股盈餘（元）")
    val 其他行業 = List("公司代號", "公司名稱", "收入", "支出", "繼續營業單位稅前淨利（淨損）", "所得稅費用（利益）", "繼續營業單位本期淨利（淨損）", "停業單位損益", "本期淨利（淨損）", "其他綜合損益", "本期綜合損益總額", "淨利（淨損）歸屬於母公司業主", "淨利（淨損）歸屬於共同控制下前手權益", "淨利（淨損）歸屬於非控制權益", "綜合損益總額歸屬於母公司業主", "綜合損益總額歸屬於共同控制下前手權益", "綜合損益總額歸屬於非控制權益", "基本每股盈餘（元）")
    val all = 銀行及票券業.appendedAll(金控業).appendedAll(證券業).appendedAll(保險業).appendedAll(一般行業).appendedAll(其他行業).distinct
    println(all.size)
    println(all.sorted)
    List("保險負債準備淨變動", "停業單位損益", "公司代號", "公司名稱", "其他收益及費損淨額", "其他綜合損益", "其他綜合損益（淨額）", "其他綜合損益（稅後淨額）", "其他綜合損益（稅後）", "利息以外淨損益", "利息以外淨收益", "利息淨收益", "原始認列生物資產及農產品之利益（損失）", "合併前非屬共同控制股權損益", "合併前非屬共同控制股權綜合損益淨額", "呆帳費用、承諾及保證責任準備提存", "基本每股盈餘（元）", "已實現銷貨（損）益", "所得稅費用（利益）", "所得稅（費用）利益", "支出", "支出及費用", "收入", "收益", "未實現銷貨（損）益", "本期其他綜合損益（稅後淨額）", "本期淨利（淨損）", "本期稅後淨利（淨損）", "本期綜合損益總額", "本期綜合損益總額（稅後）", "淨利（損）歸屬於共同控制下前手權益", "淨利（損）歸屬於母公司業主", "淨利（損）歸屬於非控制權益", "淨利（淨損）歸屬於共同控制下前手權益", "淨利（淨損）歸屬於母公司業主", "淨利（淨損）歸屬於非控制權益", "淨收益", "營業利益", "營業利益（損失）", "營業外損益", "營業外收入及支出", "營業成本", "營業收入", "營業毛利（毛損）", "營業毛利（毛損）淨額", "營業費用", "生物資產當期公允價值減出售成本之變動利益（損失）", "稅前淨利（淨損）", "綜合損益總額歸屬於共同控制下前手權益", "綜合損益總額歸屬於母公司業主", "綜合損益總額歸屬於非控制權益", "繼續營業單位本期淨利（淨損）", "繼續營業單位本期稅後淨利（淨損）", "繼續營業單位本期純益（純損）", "繼續營業單位稅前損益", "繼續營業單位稅前淨利（淨損）", "繼續營業單位稅前純益（純損）")
     */
    //task.createDB()
    //task.pullDailyQuote()
    //reader.readDailyQuote()
    val dailyQuote = TableQuery[DailyQuote]
    val action = dailyQuote.filter(d => d.companyCode === "0050" || d.companyCode === "0052").sortBy(_.date).result
    val db = Database.forConfig("db")
    val result: Seq[DailyQuoteRow] = Await.result(db.run(action), Duration.Inf)
    val tpe0050 = result.filter(_.companyCode == "0050")
    val tpe0052 = result.filter(_.companyCode == "0052")
    val tpe0050Price: Seq[Double] = tpe0050.map(_.closingPrice.getOrElse(0))
    val tpe0052Price: Seq[Double] = tpe0052.map(_.closingPrice.getOrElse(0))

    val data = Seq(
      Scatter(tpe0050.map(_.date.toString), tpe0050Price, name = "0050"),
      Scatter(tpe0052.map(_.date.toString), tpe0052Price, name = "0052"))
    Plotly.plot("time-series.html", data, Layout(title = "0050"))
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
