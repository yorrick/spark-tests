name := "Spark Tests"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.0.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.0.2"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

