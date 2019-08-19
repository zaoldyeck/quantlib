name := "quantlib"

version := "0.1"

scalaVersion := "2.13.0"

libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick" % "3.3.2",
  "org.slf4j" % "slf4j-nop" % "1.7.28",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2"
)