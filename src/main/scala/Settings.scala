import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object Settings {
  private val conf: Config = ConfigFactory.load
  val dataDir: String = conf.getString("data.dir")
}
