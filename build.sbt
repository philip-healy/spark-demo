name := "SparkDemo"

version := "0.1.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.1"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"