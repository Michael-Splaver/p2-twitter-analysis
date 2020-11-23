name := "baseball"

version := "1.0"

scalaVersion := "2.12.10"

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

libraryDependencies += "org.scalaj" % "scalaj-http_2.12" % "2.4.2"

libraryDependencies += "net.liftweb" %% "lift-json" % "3.4.2"

libraryDependencies += "com.danielasfregola" %% "twitter4s" % "7.0"