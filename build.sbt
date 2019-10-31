name := "quantlib"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick" % "3.3.2",
  "org.slf4j" % "slf4j-nop" % "1.7.28",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2",
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.1.0-RC2",
  "com.typesafe.play" %% "play-ws-standalone-json" % "2.1.0-RC2",
  "com.github.gaocegege" % "scrala" % "0.1.5"
)