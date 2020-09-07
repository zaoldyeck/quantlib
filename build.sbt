name := "quantlib"

version := "0.1"

scalaVersion := "2.13.3"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick" % "3.3.2",
  "org.slf4j" % "slf4j-nop" % "1.7.28",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2",
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.1.2",
  "com.typesafe.play" %% "play-ws-standalone-json" % "2.1.2",
  "net.ruippeixotog" %% "scala-scraper" % "2.2.0",
  "com.h2database" % "h2" % "1.4.200",
  "com.github.tototoshi" %% "scala-csv" % "1.3.6",
  "org.plotly-scala" %% "plotly-render" % "0.7.2",
  "org.scala-lang.modules" %% "scala-parallel-collections" % "0.2.0",
  "me.tongfei" % "progressbar" % "0.8.1",
  "org.postgresql" % "postgresql" % "42.2.14"
  //"mysql" % "mysql-connector-java" % "8.0.19"
)