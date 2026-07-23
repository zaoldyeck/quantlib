import akka.actor.ActorSystem
import akka.actor.Scheduler
import akka.stream.Materializer
import play.api.libs.ws.ahc.{StandaloneAhcWSClient, StandaloneAhcWSRequest}

import scala.concurrent.Await
import scala.concurrent.duration._

object Http {
  private implicit val system: ActorSystem = ActorSystem()
  implicit val scheduler: Scheduler = system.scheduler
  implicit val materializer: Materializer = Materializer.matFromSystem
  val client = new StandaloneAhcWSClientWithProxyPool()

  def terminate(): Unit = {
    client.close()
    Await.result(system.terminate(), 30.seconds)
  }

  class StandaloneAhcWSClientWithProxyPool {
    private val client = StandaloneAhcWSClient()

    // Browser User-Agent so TWSE's anti-scraping layer lets us through. Without
    // this, the default Play WS UA gets 307-redirected to "頁面無法執行 THE PAGE
    // CANNOT BE ACCESSED!" on endpoints like /fund/T86 from mid-2024 onwards.
    private val BrowserUA =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 " +
      "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"

    def url(url: String, disableUrlEncoding: Boolean = false): StandaloneAhcWSRequest = {
      StandaloneAhcWSRequest(client = client,
        url = url,
        //proxyServer = Some(DefaultWSProxyServer("118.171.31.75", 3128)),
        headers = Map("User-Agent" -> Seq(BrowserUA)),
        disableUrlEncoding = Some(disableUrlEncoding))
    }

    def close(): Unit = client.close()
  }

}
