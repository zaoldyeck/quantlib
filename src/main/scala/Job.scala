import reader.{FinancialReader, TradingReader}

class Job {
  def pullAllData(): Unit = {
    val task = new Task
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
    task.pullInsiderHolding()        // Sprint B: 內部人持股轉讓事前申報日報 (working)
    // 現金增資 / CB 已取消 (見 FUTURE_CRAWLERS_SPEC.md)
  }

  def readAllData(): Unit = {
    val tradingReader = new TradingReader
    val financialReader = new FinancialReader
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
    financialReader.readFinancialAnalysis()
    financialReader.readBalanceSheet()
    financialReader.readIncomeStatement()
    financialReader.readFinancialStatements()
    financialReader.readOperatingRevenue()
    financialReader.readETF()
  }

  def updateData(): Unit = {
    pullAllData()
    readAllData()
  }

  def complete(): Unit = Http.terminate()
}
