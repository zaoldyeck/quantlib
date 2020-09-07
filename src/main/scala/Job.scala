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
  }

  def readAllData(): Unit = {
    val financialReader = new FinancialReader
    val tradingReader = new TradingReader
    financialReader.readFinancialAnalysis()
    financialReader.readBalanceSheet()
    financialReader.readIncomeStatement()
    financialReader.readFinancialStatements()
    financialReader.readOperatingRevenue()
    tradingReader.readDailyQuote()
    tradingReader.readDailyTradingDetails()
    tradingReader.readCapitalReduction()
    tradingReader.readExRightDividend()
    tradingReader.readIndex()
    tradingReader.readMarginTransactions()
    tradingReader.readStockPER_PBR_DividendYield()
  }

  def updateData(): Unit = {
    pullAllData()
    readAllData()
  }

  def complete(): Unit = Http.terminate()
}
