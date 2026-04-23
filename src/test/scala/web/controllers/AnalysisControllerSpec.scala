package web.controllers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import play.api.test._
import play.api.test.Helpers._
import play.api.libs.json._

class AnalysisControllerSpec extends AnyFlatSpec with Matchers {

  "AnalysisController" should "execute value investing backtest" in {
    val requestBody = Json.obj(
      "strategy" -> "value_investing",
      "startDate" -> "2020-01-01",
      "endDate" -> "2023-12-31",
      "initialAmount" -> 1000000,
      "rebalanceFrequency" -> "quarterly"
    )

    val request = FakeRequest(POST, "/api/v1/analysis/backtest")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)

    // Verify required performance metrics
    (json \ "totalReturn").isDefined shouldBe true
    (json \ "annualizedReturn").isDefined shouldBe true
    (json \ "maxDrawdown").isDefined shouldBe true
    (json \ "sharpeRatio").isDefined shouldBe true
    (json \ "volatility").isDefined shouldBe true

    // Verify benchmark comparison
    val benchmark = (json \ "benchmark").as[JsObject]
    (benchmark \ "totalReturn").isDefined shouldBe true
    (benchmark \ "annualizedReturn").isDefined shouldBe true

    // Verify performance chart data
    val performanceChart = (json \ "performanceChart").as[JsArray]
    performanceChart.value should not be empty

    performanceChart.value.foreach { point =>
      (point \ "date").isDefined shouldBe true
      (point \ "value").isDefined shouldBe true
    }

    // Performance metrics should be reasonable
    val totalReturn = (json \ "totalReturn").as[Double]
    val maxDrawdown = (json \ "maxDrawdown").as[Double]
    val sharpeRatio = (json \ "sharpeRatio").as[Double]

    totalReturn should be >= -0.5 // Not worse than -50%
    totalReturn should be <= 5.0  // Not unrealistic gains
    maxDrawdown should be >= 0.0
    maxDrawdown should be <= 1.0  // Max 100% drawdown
    sharpeRatio should be >= -3.0
    sharpeRatio should be <= 10.0
  }

  it should "execute growth investing backtest" in {
    val requestBody = Json.obj(
      "strategy" -> "growth_investing",
      "startDate" -> "2019-01-01",
      "endDate" -> "2023-12-31",
      "initialAmount" -> 500000,
      "rebalanceFrequency" -> "monthly"
    )

    val request = FakeRequest(POST, "/api/v1/analysis/backtest")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)

    (json \ "strategy").asOpt[String] shouldBe None // Not returned in response
    val annualizedReturn = (json \ "annualizedReturn").as[Double]
    val volatility = (json \ "volatility").as[Double]

    // Growth investing might have higher volatility
    volatility should be >= 0.0
    annualizedReturn should be >= -0.5
  }

  it should "execute combined strategy backtest" in {
    val requestBody = Json.obj(
      "strategy" -> "combined",
      "startDate" -> "2018-01-01",
      "endDate" -> "2022-12-31",
      "initialAmount" -> 2000000,
      "rebalanceFrequency" -> "yearly"
    )

    val request = FakeRequest(POST, "/api/v1/analysis/backtest")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)

    val totalReturn = (json \ "totalReturn").as[Double]
    val maxDrawdown = (json \ "maxDrawdown").as[Double]
    val sharpeRatio = (json \ "sharpeRatio").as[Double]

    // Combined strategy should show balanced risk/return
    totalReturn should be >= -0.3
    maxDrawdown should be <= 0.4 // Less than 40% max drawdown expected
  }

  it should "handle different rebalance frequencies" in {
    val testCases = Seq("monthly", "quarterly", "yearly")

    testCases.foreach { frequency =>
      val requestBody = Json.obj(
        "strategy" -> "value_investing",
        "startDate" -> "2021-01-01",
        "endDate" -> "2023-12-31",
        "initialAmount" -> 1000000,
        "rebalanceFrequency" -> frequency
      )

      val request = FakeRequest(POST, "/api/v1/analysis/backtest")
        .withJsonBody(requestBody)
        .withHeaders("Content-Type" -> "application/json")

      val result = route(app, request).get

      status(result) shouldBe OK
      val json = contentAsJson(result)
      (json \ "totalReturn").isDefined shouldBe true
    }
  }

  it should "return 400 for invalid strategy" in {
    val requestBody = Json.obj(
      "strategy" -> "invalid_strategy",
      "startDate" -> "2020-01-01",
      "endDate" -> "2023-12-31",
      "initialAmount" -> 1000000,
      "rebalanceFrequency" -> "quarterly"
    )

    val request = FakeRequest(POST, "/api/v1/analysis/backtest")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe BAD_REQUEST
    val json = contentAsJson(result)
    (json \ "error").isDefined shouldBe true
    (json \ "code").isDefined shouldBe true
  }

  it should "return 400 for invalid date range" in {
    val requestBody = Json.obj(
      "strategy" -> "value_investing",
      "startDate" -> "2023-12-31", // End date before start date
      "endDate" -> "2020-01-01",
      "initialAmount" -> 1000000,
      "rebalanceFrequency" -> "quarterly"
    )

    val request = FakeRequest(POST, "/api/v1/analysis/backtest")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe BAD_REQUEST
  }

  it should "return 400 for insufficient initial amount" in {
    val requestBody = Json.obj(
      "strategy" -> "value_investing",
      "startDate" -> "2020-01-01",
      "endDate" -> "2023-12-31",
      "initialAmount" -> 50000, // Less than minimum 100,000
      "rebalanceFrequency" -> "quarterly"
    )

    val request = FakeRequest(POST, "/api/v1/analysis/backtest")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe BAD_REQUEST
  }

  it should "return 400 for missing required fields" in {
    val requestBody = Json.obj(
      "strategy" -> "value_investing",
      "startDate" -> "2020-01-01",
      // Missing endDate, initialAmount, rebalanceFrequency
      "rebalanceFrequency" -> "quarterly"
    )

    val request = FakeRequest(POST, "/api/v1/analysis/backtest")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe BAD_REQUEST
  }

  it should "return 400 for invalid rebalance frequency" in {
    val requestBody = Json.obj(
      "strategy" -> "value_investing",
      "startDate" -> "2020-01-01",
      "endDate" -> "2023-12-31",
      "initialAmount" -> 1000000,
      "rebalanceFrequency" -> "daily" // Invalid frequency
    )

    val request = FakeRequest(POST, "/api/v1/analysis/backtest")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe BAD_REQUEST
  }

  it should "provide complete performance chart data" in {
    val requestBody = Json.obj(
      "strategy" -> "value_investing",
      "startDate" -> "2022-01-01",
      "endDate" -> "2022-12-31",
      "initialAmount" -> 1000000,
      "rebalanceFrequency" -> "monthly"
    )

    val request = FakeRequest(POST, "/api/v1/analysis/backtest")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val performanceChart = (json \ "performanceChart").as[JsArray]

    // Should have at least monthly data points for 1 year
    performanceChart.value.length should be >= 12

    // Verify data is sorted by date
    val dates = performanceChart.value.map(point => (point \ "date").as[String])
    dates should equal(dates.sorted)

    // All values should be positive (portfolio value)
    performanceChart.value.foreach { point =>
      val value = (point \ "value").as[Double]
      value should be > 0.0
    }
  }

  it should "compare performance against benchmark correctly" in {
    val requestBody = Json.obj(
      "strategy" -> "combined",
      "startDate" -> "2020-01-01",
      "endDate" -> "2023-12-31",
      "initialAmount" -> 1000000,
      "rebalanceFrequency" -> "quarterly"
    )

    val request = FakeRequest(POST, "/api/v1/analysis/backtest")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)

    val strategyReturn = (json \ "totalReturn").as[Double]
    val benchmarkReturn = (json \ "benchmark" \ "totalReturn").as[Double]

    // Both should be reasonable values
    strategyReturn should be >= -0.6
    strategyReturn should be <= 3.0
    benchmarkReturn should be >= -0.6
    benchmarkReturn should be <= 3.0

    // The difference should be reasonable (strategy can outperform or underperform)
    math.abs(strategyReturn - benchmarkReturn) should be <= 2.0
  }
}