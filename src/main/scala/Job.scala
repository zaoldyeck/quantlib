import reader.{FinancialReader, TradingReader}

import java.time.LocalDate

class Job {
  private val task = new Task

  def pullAllData(): Unit = {
    task.pullFinancialAnalysis()
    task.pullBalanceSheet()
    task.pullIncomeStatement()
    task.pullFinancialStatements()
    task.pullOperatingRevenue()
    task.pullDailyQuote()
    task.pullDailyTradingDetails()
    task.pullCapitalReduction()
    task.pullExRightDividend()
    task.pullIndex()
    task.pullMarginTransactions()
    task.pullStockPER_PBR_DividendYield()
    task.pullETF()
    task.pullTdccShareholding()
    task.pullSbl()
    task.pullForeignHoldingRatio()
    task.pullTreasuryStockBuyback()  // Sprint B: 庫藏股 (working)
    // Sprint B: 內部人持股轉讓事前申報日報 — only incremental last 30 days here.
    // Full historical backfill (2007 → today) takes ~2-3 days due to MOPS rate-limit;
    // run separately as overnight task: `sbt "runMain Main pull insider --since 2007-01-02"`
    task.pullInsiderHolding(Some(LocalDate.now().minusDays(30)))
    // 現金增資 / CB 不在標準 update 範圍；新增 crawler 前先更新 repo-local 文件與 AGENTS.md 規則。
  }

  def readAllData(): Unit = {
    val tradingReader = new TradingReader
    val financialReader = new FinancialReader
    try {
      tradingReader.readDailyQuote()
      tradingReader.readDailyTradingDetails()
      tradingReader.readCapitalReduction()
      tradingReader.readExRightDividend()
      tradingReader.readIndex()
      tradingReader.readMarginTransactions()
      tradingReader.readStockPER_PBR_DividendYield()
      tradingReader.readTdccShareholding()
      tradingReader.readSblBorrowing()
      tradingReader.readForeignHoldingRatio()
      tradingReader.readTreasuryStockBuyback()  // Sprint B: 庫藏股
      tradingReader.readInsiderHolding()        // Sprint B: 內部人持股轉讓
      // TAIFEX futures are target-driven (`pull/read taifex`) because the first
      // run backfills 1998+ annual archives and should not surprise equity updates.
      financialReader.readFinancialAnalysis()
      financialReader.readBalanceSheet()
      financialReader.readIncomeStatement()
      financialReader.readFinancialStatements()
      financialReader.readOperatingRevenue()
      financialReader.readETF()
    } finally {
      tradingReader.close()
      financialReader.close()
    }
  }

  def updateData(): Unit = {
    pullAllData()
    readAllData()
  }

  def complete(): Unit = {
    task.close()
    Http.terminate()
  }
}
