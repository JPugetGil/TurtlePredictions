import sbt.Keys.parallelExecution

name := "TurtlePredictions"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.8"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.4"

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.1",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.13.1"
  )
}

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
Test / parallelExecution := false