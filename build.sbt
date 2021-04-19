name := "SCP-Project"

version := "0.2"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-mllib" % "3.0.1",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.0.1"
/*
  "rg.apache.hadoop" %% "hadoop-common" % "3.0.0",
  "rg.apache.hadoop" %% "hadoop-client" % "3.0.0",
  "rg.apache.hadoop" %% "hadoop-hadoop-aws" % "3.0.0",*/

)

