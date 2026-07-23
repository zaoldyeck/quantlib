package setting

import com.typesafe.config.{Config, ConfigFactory}
import db.table.ETF
import slick.jdbc.PostgresProfile.api._
import slick.lifted.TableQuery

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Constant {
  private val conf: Config = ConfigFactory.load
  private val db = Database.forConfig("db")
  val ETFs = Await.result(db.run(TableQuery[ETF]
    .filterNot(_.name.like("%正%"))
    .filterNot(_.name.like("%反%"))
    .map(_.companyCode).result), Duration.Inf)
  val DEBTs = Await.result(db.run(TableQuery[ETF]
    .filter(_.name.like("%債%"))
    .filterNot(_.name.like("%正%"))
    .filterNot(_.name.like("%反%"))
    .map(_.companyCode).result), Duration.Inf)
}
