name := "functional-spark-pipeline"

version := "0.1"

scalaVersion := "2.12.19"

val sparkVersion = "3.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)