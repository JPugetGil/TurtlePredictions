name := "TurtlePredictions"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided"

libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.9.1"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
parallelExecution in Test := false