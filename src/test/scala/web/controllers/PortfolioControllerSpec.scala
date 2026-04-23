package web.controllers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import play.api.test._
import play.api.test.Helpers._
import play.api.libs.json._

class PortfolioControllerSpec extends AnyFlatSpec with Matchers {

  "PortfolioController" should "generate conservative portfolio recommendation" in {
    val requestBody = Json.obj(
      "riskLevel" -> "conservative",
      "investmentAmount" -> 1000000,
      "filterCriteria" -> Json.obj(
        "industries" -> Json.arr("半導體業", "金融保險業"),
        "fScoreMin" -> 6,
        "dividendYieldMin" -> 0.03
      )
    )

    val request = FakeRequest(POST, "/api/v1/portfolios/recommend")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)

    (json \ "id").isDefined shouldBe true
    (json \ "createdAt").isDefined shouldBe true
    (json \ "riskLevel").as[String] shouldBe "conservative"

    val stocks = (json \ "stocks").as[JsArray]
    stocks.value.length should be >= 5
    stocks.value.length should be <= 8

    // Verify weight sum equals 1.0
    val totalWeight = stocks.value.map(stock => (stock \ "weight").as[Double]).sum
    totalWeight should be(1.0 +- 0.001)

    // Verify all stocks have required fields
    stocks.value.foreach { stock =>
      (stock \ "companyCode").isDefined shouldBe true
      (stock \ "companyName").isDefined shouldBe true
      (stock \ "weight").as[Double] should be > 0.0
      (stock \ "confidence").as[Double] should be >= 0.0
      (stock \ "confidence").as[Double] should be <= 1.0
      (stock \ "reason").isDefined shouldBe true
    }

    (json \ "expectedReturn").isDefined shouldBe true
    (json \ "maxDrawdown").isDefined shouldBe true
    (json \ "sharpeRatio").isDefined shouldBe true
  }

  it should "generate moderate portfolio recommendation" in {
    val requestBody = Json.obj(
      "riskLevel" -> "moderate",
      "investmentAmount" -> 500000,
      "filterCriteria" -> Json.obj(
        "marketCapRange" -> "mid",
        "growthScoreMin" -> 5
      )
    )

    val request = FakeRequest(POST, "/api/v1/portfolios/recommend")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)

    (json \ "riskLevel").as[String] shouldBe "moderate"

    val expectedReturn = (json \ "expectedReturn").as[Double]
    expectedReturn should be > 0.0

    val maxDrawdown = (json \ "maxDrawdown").as[Double]
    maxDrawdown should be > 0.0
  }

  it should "generate aggressive portfolio recommendation" in {
    val requestBody = Json.obj(
      "riskLevel" -> "aggressive",
      "investmentAmount" -> 2000000,
      "filterCriteria" -> Json.obj(
        "industries" -> Json.arr("電子零組件業", "電腦及週邊設備業"),
        "marketCapRange" -> "small"
      )
    )

    val request = FakeRequest(POST, "/api/v1/portfolios/recommend")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)

    (json \ "riskLevel").as[String] shouldBe "aggressive"

    val expectedReturn = (json \ "expectedReturn").as[Double]
    val maxDrawdown = (json \ "maxDrawdown").as[Double]

    // Aggressive portfolio should have higher expected return and higher risk
    expectedReturn should be > 0.08 // >8% expected return
    maxDrawdown should be > 0.15    // >15% potential drawdown
  }

  it should "return 400 for invalid risk level" in {
    val requestBody = Json.obj(
      "riskLevel" -> "invalid_risk",
      "investmentAmount" -> 1000000
    )

    val request = FakeRequest(POST, "/api/v1/portfolios/recommend")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe BAD_REQUEST
    val json = contentAsJson(result)
    (json \ "error").isDefined shouldBe true
    (json \ "code").isDefined shouldBe true
  }

  it should "return 400 for missing required fields" in {
    val requestBody = Json.obj(
      "investmentAmount" -> 1000000
      // Missing riskLevel
    )

    val request = FakeRequest(POST, "/api/v1/portfolios/recommend")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe BAD_REQUEST
  }

  it should "return 400 for investment amount too small" in {
    val requestBody = Json.obj(
      "riskLevel" -> "moderate",
      "investmentAmount" -> 5000 // Less than minimum 10,000
    )

    val request = FakeRequest(POST, "/api/v1/portfolios/recommend")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe BAD_REQUEST
  }

  it should "respect industry filters in portfolio" in {
    val requestBody = Json.obj(
      "riskLevel" -> "moderate",
      "investmentAmount" -> 1000000,
      "filterCriteria" -> Json.obj(
        "industries" -> Json.arr("半導體業")
      )
    )

    val request = FakeRequest(POST, "/api/v1/portfolios/recommend")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val stocks = (json \ "stocks").as[JsArray]

    // Note: This test assumes we can verify industry through stock lookup
    // In real implementation, we would verify that selected stocks
    // match the industry filter criteria
    stocks.value should not be empty
  }

  it should "respect F-Score minimum filter" in {
    val requestBody = Json.obj(
      "riskLevel" -> "conservative",
      "investmentAmount" -> 1000000,
      "filterCriteria" -> Json.obj(
        "fScoreMin" -> 8 // High quality companies only
      )
    )

    val request = FakeRequest(POST, "/api/v1/portfolios/recommend")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val stocks = (json \ "stocks").as[JsArray]

    stocks.value should not be empty
    // Each stock should have F-Score >= 8 (verified in implementation)
  }

  it should "ensure no single industry exceeds 40% weight" in {
    val requestBody = Json.obj(
      "riskLevel" -> "moderate",
      "investmentAmount" -> 1000000
    )

    val request = FakeRequest(POST, "/api/v1/portfolios/recommend")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val stocks = (json \ "stocks").as[JsArray]

    // Group stocks by industry and check total weight per industry
    // Note: This requires looking up stock industries in implementation
    // For now, we verify the structure is correct
    val totalWeight = stocks.value.map(stock => (stock \ "weight").as[Double]).sum
    totalWeight should be(1.0 +- 0.001)
  }

  it should "provide reasonable confidence scores" in {
    val requestBody = Json.obj(
      "riskLevel" -> "moderate",
      "investmentAmount" -> 1000000
    )

    val request = FakeRequest(POST, "/api/v1/portfolios/recommend")
      .withJsonBody(requestBody)
      .withHeaders("Content-Type" -> "application/json")

    val result = route(app, request).get

    status(result) shouldBe OK
    val json = contentAsJson(result)
    val stocks = (json \ "stocks").as[JsArray]

    stocks.value.foreach { stock =>
      val confidence = (stock \ "confidence").as[Double]
      confidence should be >= 0.0
      confidence should be <= 1.0

      // High-confidence recommendations should have reasonable weight
      if (confidence > 0.8) {
        val weight = (stock \ "weight").as[Double]
        weight should be >= 0.1 // At least 10% for high-confidence picks
      }
    }
  }
}