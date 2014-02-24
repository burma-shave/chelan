name := "chelan"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "RoundEights" at "http://maven.spikemark.net/roundeights"

resolvers += "spray repo" at "http://repo.spray.io"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2.3"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.2.3" % "test"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "com.roundeights" %% "hasher" % "1.0.0"

libraryDependencies += "io.spray" % "spray-routing" % "1.2.0"

libraryDependencies += "io.spray" % "spray-can" % "1.2.0"

libraryDependencies += "io.spray" % "spray-testkit" % "1.2.0" % "test"


