name := "big_data"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "com.databricks" %% "spark-xml" % "0.10.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.6.0"
