package web.controllers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import play.api.test._
import play.api.test.Helpers._
import play.api.libs.json._

class StockControllerSpec extends AnyFlatSpec with Matchers {

  "StockController" should "return stocks list with default parameters" in {
    val request = FakeRequest(GET, "/api/v1/stocks")
    val result = route(app, request).get

    status(result) shouldBe OK
    contentType(result) shouldBe Some("application/json")

    val json = contentAsJson(result)
    (json \ "data").isDefined shouldBe true
    (json \ "total").isDefined shouldBe true
    (json \ "limit").isDefined shouldBe true
    (json \ "offset").isDefined shouldBe true
  }

  it should "filter stocks by industry" in {
    val request = FakeRequest(GET, "/api/v1/stocks?industries=半導體業,電子零組件業")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val stocks = (json \ "data").as[JsArray]

    stocks.value.foreach { stock =>
      val industry = (stock \ "industry").as[String]
      industry should (equal("半導體業") or equal("電子零組件業"))
    }
  }

  it should "filter stocks by market cap range" in {
    val request = FakeRequest(GET, "/api/v1/stocks?marketCap=large")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    (json \ "data").as[JsArray].value should not be empty
  }

  it should "sort stocks by DCF error" in {
    val request = FakeRequest(GET, "/api/v1/stocks?sortBy=dcf_error&limit=10")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val stocks = (json \ "data").as[JsArray]

    // Verify sorting order (most undervalued first - negative DCF error)
    val dcfErrors = stocks.value.map(stock => (stock \ "dcfError").as[Double])
    dcfErrors should equal(dcfErrors.sorted)
  }

  it should "limit results correctly" in {
    val request = FakeRequest(GET, "/api/v1/stocks?limit=5")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val stocks = (json \ "data").as[JsArray]

    stocks.value.length should be <= 5
    (json \ "limit").as[Int] shouldBe 5
  }

  it should "handle pagination correctly" in {
    val request = FakeRequest(GET, "/api/v1/stocks?limit=10&offset=20")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)

    (json \ "limit").as[Int] shouldBe 10
    (json \ "offset").as[Int] shouldBe 20
  }

  it should "return 400 for invalid parameters" in {
    val request = FakeRequest(GET, "/api/v1/stocks?limit=abc")
    val result = route(app, request).get

    status(result) shouldBe BAD_REQUEST
    val json = contentAsJson(result)
    (json \ "error").isDefined shouldBe true
    (json \ "code").isDefined shouldBe true
  }

  it should "return specific stock details" in {
    val request = FakeRequest(GET, "/api/v1/stocks/2330")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)

    (json \ "companyCode").as[String] shouldBe "2330"
    (json \ "companyName").isDefined shouldBe true
    (json \ "market").isDefined shouldBe true
    (json \ "industry").isDefined shouldBe true
    (json \ "currentPrice").isDefined shouldBe true
    (json \ "marketCap").isDefined shouldBe true
    (json \ "priceChannel").isDefined shouldBe true
  }

  it should "return 404 for non-existent stock" in {
    val request = FakeRequest(GET, "/api/v1/stocks/9999")
    val result = route(app, request).get

    status(result) shouldBe NOT_FOUND
    val json = contentAsJson(result)
    (json \ "error").isDefined shouldBe true
    (json \ "code").isDefined shouldBe true
  }

  it should "return 400 for invalid stock code format" in {
    val request = FakeRequest(GET, "/api/v1/stocks/invalid")
    val result = route(app, request).get

    status(result) shouldBe BAD_REQUEST
  }

  it should "return valuation analysis for stock" in {
    val request = FakeRequest(GET, "/api/v1/stocks/2330/valuation")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)

    (json \ "companyCode").as[String] shouldBe "2330"
    (json \ "date").isDefined shouldBe true
    (json \ "currentPrice").isDefined shouldBe true
    (json \ "priceChannel").isDefined shouldBe true
    (json \ "evaluation").isDefined shouldBe true
  }

  it should "return valuation analysis for specific date" in {
    val request = FakeRequest(GET, "/api/v1/stocks/2330/valuation?date=2024-01-15")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    (json \ "date").as[String] shouldBe "2024-01-15"
  }

  it should "return financial scores for stock" in {
    val request = FakeRequest(GET, "/api/v1/stocks/2330/financial-score")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val scores = json.as[JsArray]

    scores.value should not be empty
    scores.value.foreach { score =>
      (score \ "companyCode").as[String] shouldBe "2330"
      (score \ "year").isDefined shouldBe true
      (score \ "quarter").isDefined shouldBe true
      (score \ "fScore").isDefined shouldBe true
    }
  }

  it should "filter financial scores by year and quarter" in {
    val request = FakeRequest(GET, "/api/v1/stocks/2330/financial-score?year=2023&quarter=4")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val scores = json.as[JsArray]

    scores.value.foreach { score =>
      (score \ "year").as[Int] shouldBe 2023
      (score \ "quarter").as[Int] shouldBe 4
    }
  }

  it should "return market data for stock" in {
    val request = FakeRequest(GET, "/api/v1/market-data/2330")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val marketData = json.as[JsArray]

    marketData.value should not be empty
    marketData.value.foreach { data =>
      (data \ "companyCode").as[String] shouldBe "2330"
      (data \ "date").isDefined shouldBe true
      (data \ "openPrice").isDefined shouldBe true
      (data \ "closePrice").isDefined shouldBe true
      (data \ "volume").isDefined shouldBe true
    }
  }

  it should "filter market data by date range" in {
    val request = FakeRequest(GET, "/api/v1/market-data/2330?startDate=2024-01-01&endDate=2024-01-31")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val marketData = json.as[JsArray]

    marketData.value.foreach { data =>
      val date = (data \ "date").as[String]
      date should be >= "2024-01-01"
      date should be <= "2024-01-31"
    }
  }

  it should "limit market data results" in {
    val request = FakeRequest(GET, "/api/v1/market-data/2330?limit=10")
    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val marketData = json.as[JsArray]

    marketData.value.length should be <= 10
  }
}