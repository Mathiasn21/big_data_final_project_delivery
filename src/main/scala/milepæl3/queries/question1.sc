import org.apache.log4j.{Level, Logger}
import org.apache.spark
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

val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(filePath)
df.show()

val file = sc.textFile(filePath)
val headers = file.first()
val filtered = file.filter(line => line != headers)

val splitFile = filtered.map(line => line.split(","))
print(splitFile.toDebugString)

splitFile.collect().foreach(println)
