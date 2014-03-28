name := "chelan"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "RoundEights" at "http://maven.spikemark.net/roundeights"

resolvers += "spray repo" at "http://repo.spray.io"

val akkaVersion = "2.3.1"

val sprayVersion = "1.2.0"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion

libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "com.roundeights" %% "hasher" % "1.0.0"

libraryDependencies += "io.spray" % "spray-routing" % sprayVersion

libraryDependencies += "io.spray" % "spray-can" % sprayVersion

libraryDependencies += "io.spray" % "spray-testkit" % sprayVersion % "test"

libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.0.1" % "test"


scalariformSettings
