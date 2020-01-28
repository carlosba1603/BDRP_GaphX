name := "BDRP_GraphX"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "neo4j-contrib" % "neo4j-spark-connector" % "2.2.1-M5",
  "com.amazonaws" % "aws-java-sdk" % "1.11.710",
 "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
)
