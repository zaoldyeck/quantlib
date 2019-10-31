import akka.actor.ActorSystem
import akka.stream.Materializer
import play.api.libs.ws.ahc.{StandaloneAhcWSClient, StandaloneAhcWSRequest}

object Http {
  implicit private val system = ActorSystem()
  system.registerOnTermination {
    System.exit(0)
  }

  implicit val materializer = Materializer.matFromSystem
  val client = new StandaloneAhcWSClientWithProxyPool()

  def terminate(): Unit = {
    client.close()
    system.terminate()
  }

  class StandaloneAhcWSClientWithProxyPool {
    private val client = StandaloneAhcWSClient()

    def url(url: String, disableUrlEncoding: Boolean = false): StandaloneAhcWSRequest = {
      StandaloneAhcWSRequest(client = client,
        url = url,
        //proxyServer = Some(DefaultWSProxyServer("118.171.31.75", 3128)),
        disableUrlEncoding = Some(disableUrlEncoding))
    }

    def close(): Unit = client.close()
  }

}
