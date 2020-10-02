import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

import spark.implicits._

val sc = spark.sparkContext
val filePath = "D:\\data\\crime_in_context_19752015.csv"
val file = sc.textFile(filePath)
val headers = file.collect()
file.collect().foreach(println)
